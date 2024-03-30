/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificDatumWriter;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.generated.User;
import org.apache.flink.formats.avro.generated.UserForSql;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.avro.typeutils.AvroSchemaUtils;
import org.apache.flink.formats.avro.typeutils.conversion.ConversionContext;
import org.apache.flink.formats.avro.utils.AvroTestUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Preconditions;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;


/** Unit tests for the Avro bulk format with SQL. */
@ExtendWith(MiniClusterExtension.class)
public class AvroBulkFormatSqlITCase {
    private static final String SOURCE_FILE_NAME_PREFIX = "avro-bulk-format-sql-it-case-source";
    private static final String SOURCE_WITH_ORIGINAL_SCHEMA_FOR_GENERIC_RECORD =
            "SourceWithOriginalSchemaForGenericRecords";
    private static final String SOURCE_WITH_ORIGINAL_SCHEMA_FOR_SPECIFIC_RECORD =
            "SourceWithOriginalSchemaForSpecificRecords";

    private static final String SOURCE_WITH_ORIGINAL_SCHEMA_FOR_ROW =
            "SourceWithOriginalSchemaForRow";
    private static final String SOURCE_WITH_GENERATED_SCHEMA_FOR_GENERIC_RECORD =
            "SourceWithGeneratedSchemaForGenericRecords";
    private static final String SOURCE_WITH_GENERATED_SCHEMA_FOR_ROW =
            "SourceWithGeneratedSchemaForRow";
    private static final String ORIGINAL_AVRO_SCHEMA_STRING = UserForSql.getClassSchema().toString();

    private static final Schema TABLE_SCHEMA_FOR_ORIGINAL_SPECIFIC_RECORD =
            AvroSchemaUtils.convertToTableSchema(
                    UserForSql.getClassSchema(),
                    ConversionContext
                            .builder()
                            .conversionVersion(2)
                            .conversionRecordType(ConversionContext.ConversionRecordType.FOR_SPECIFIC_RECORD)
                            .build());
    private static final Schema TABLE_SCHEMA_FOR_ORIGINAL_GENERIC_RECORD =
            AvroSchemaUtils.convertToTableSchema(
                    UserForSql.getClassSchema(),
                    ConversionContext
                            .builder()
                            .conversionVersion(2)
                            .conversionRecordType(ConversionContext.ConversionRecordType.FOR_GENERIC_RECORD)
                            .build());

    private static final Schema TABLE_SCHEMA_FOR_ORIGINAL_ROW =
            AvroSchemaUtils.convertToTableSchema(
                    UserForSql.getClassSchema(),
                    ConversionContext.v0());
    private static final Schema TABLE_SCHEMA_FOR_GENERATED_ROW =
            AvroSchemaUtils.convertToTableSchema(
                    getGeneratedSchema(),
                    ConversionContext.v0());
    private static final Schema TABLE_SCHEMA_FOR_GENERATED_GENERIC_RECORD =
            AvroSchemaUtils.convertToTableSchema(
                    getGeneratedSchema(),
                    ConversionContext
                            .builder()
                            .conversionVersion(2)
                            .conversionRecordType(ConversionContext.ConversionRecordType.FOR_GENERIC_RECORD)
                            .build());

    private static StreamExecutionEnvironment env;
    private static StreamTableEnvironment tEnv;
    /** An avro file whose writer schema is the original UserForSql class schema. */
    private static File sourceFileWithOriginalSchema;
    /**
     * An Avro file whose writer schema was generated from the table logical type.
     * The record types in this schema have no compiled Java classes. And the schema
     * does not contain Union type with more than one non-null types.
     */
    private static File sourceFileWithGeneratedSchema;

    @BeforeAll
    public static void setUp() throws Exception {
        Configuration config = new Configuration().set(DeploymentOptions.ATTACHED, true);
        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        tEnv = StreamTableEnvironment.create(env);

        // Prepare the source file data.
        // The source file with original schema represents the records backed by concrete
        // Avro record classes.
        sourceFileWithOriginalSchema = prepareAvroSourceFileWithOriginalSchema();
        // The source file with generated schema represents the records that were written
        // with Flink avro format V0.
        sourceFileWithGeneratedSchema = prepareAvroSourceFileWithGeneratedSchema(sourceFileWithOriginalSchema);

        // register the source table with original schema.
        registerSourceTable(
                SOURCE_WITH_ORIGINAL_SCHEMA_FOR_GENERIC_RECORD,
                TABLE_SCHEMA_FOR_ORIGINAL_GENERIC_RECORD,
                sourceFileWithOriginalSchema,
                ORIGINAL_AVRO_SCHEMA_STRING);

        registerSourceTable(
                SOURCE_WITH_ORIGINAL_SCHEMA_FOR_SPECIFIC_RECORD,
                TABLE_SCHEMA_FOR_ORIGINAL_SPECIFIC_RECORD,
                sourceFileWithOriginalSchema,
                ORIGINAL_AVRO_SCHEMA_STRING);

        registerSourceTable(
                SOURCE_WITH_ORIGINAL_SCHEMA_FOR_ROW,
                TABLE_SCHEMA_FOR_ORIGINAL_ROW,
                sourceFileWithOriginalSchema,
                ORIGINAL_AVRO_SCHEMA_STRING);

        // register the source table with generated schema.
        // The table does not contain "test_union" field.
        registerSourceTable(
                SOURCE_WITH_GENERATED_SCHEMA_FOR_ROW,
                TABLE_SCHEMA_FOR_GENERATED_ROW,
                sourceFileWithGeneratedSchema,
                null);

        registerSourceTable(
                SOURCE_WITH_GENERATED_SCHEMA_FOR_GENERIC_RECORD,
                TABLE_SCHEMA_FOR_GENERATED_GENERIC_RECORD,
                sourceFileWithGeneratedSchema,
                null);
    }

    private static Stream<Arguments> getTestParameters() {
        return Stream.of(
                // Source with original schema and conversion option of FOR_GENERIC_RECORD,
                // Sink with original schema provided.
                Arguments.of(
                        SOURCE_WITH_ORIGINAL_SCHEMA_FOR_GENERIC_RECORD,
                        TABLE_SCHEMA_FOR_ORIGINAL_GENERIC_RECORD,
                        ORIGINAL_AVRO_SCHEMA_STRING,
                        true),
                Arguments.of(
                        SOURCE_WITH_ORIGINAL_SCHEMA_FOR_SPECIFIC_RECORD,
                        TABLE_SCHEMA_FOR_ORIGINAL_GENERIC_RECORD,
                        ORIGINAL_AVRO_SCHEMA_STRING,
                        true),
                // Source with generated schema and conversion option of FOR_GENERIC_RECORD,
                // Sink without specifying the generated schema.
                Arguments.of(
                        SOURCE_WITH_GENERATED_SCHEMA_FOR_GENERIC_RECORD,
                        TABLE_SCHEMA_FOR_GENERATED_GENERIC_RECORD,
                        null,
                        false),
                // Source with generated schema and conversion option of FOR_ROW,
                // Sink without specifying the generated schema.
                Arguments.of(
                        SOURCE_WITH_GENERATED_SCHEMA_FOR_ROW,
                        TABLE_SCHEMA_FOR_GENERATED_GENERIC_RECORD,
                        null,
                        false)
        );
    }

    /**
     * Read from source with original schema, write to sink with generated schema.
     */
    @ParameterizedTest
    @MethodSource("getTestParameters")
    public void testSqlReadAndWrite(
            String sourceTableName,
            Schema sinkTableSchema,
            String sinkTableAvroSchemaString,
            boolean checkWithOriginalSchema) throws Exception {
        final String sinkTableName = "testSqlReadAndWriteWithOriginalSchema_Output";

        File sinkDir = registerSinkTable(
                sinkTableName,
                sinkTableSchema,
                sinkTableAvroSchemaString);
        String sqlStatement =
                String.format("INSERT INTO %s SELECT * FROM %s",
                        sinkTableName, sourceTableName);
        JobClient jobClient =
                tEnv.executeSql(sqlStatement)
                        .getJobClient()
                        .orElseThrow(() -> new RuntimeException("No job client exists."));
        jobClient.getJobExecutionResult().join();
        checkOutput(sinkDir, checkWithOriginalSchema);
    }

    @Test
    public void testTableToSpecificRecordStream() throws Exception {
        final String dataStreamSinkName = "testTableToSpecificRecordDataStream_Output";

        DataStream<UserForSql> userDataStream = createDataStreamFromTable();

        File dataStreamSinkDir = Files.createTempDirectory(dataStreamSinkName).toFile();
        userDataStream.sinkTo(
                FileSink.forBulkFormat(
                                new Path(new URI(dataStreamSinkDir.getAbsolutePath())),
                                AvroWriters.forSpecificRecord(UserForSql.class))
                        .build());
        env.execute();
        checkOutput(dataStreamSinkDir, true);
    }

    /**
     * When converting a DataStream to a Table, the DataStream being converted may come from two
     * cases: 1. Provided directly by the users. 2. Converted from another Table. It turns out that
     * sometimes the actual data type of the Avro records may be different between the two cases.
     * For example, the CharSequence object may be String or Utf8. This method tests the second
     * case.
     */
    @Test
    public void testTableToSpecificRecordStreamToTable()
            throws TableAlreadyExistException, TableNotExistException, DatabaseNotExistException,
                   IOException {
        final String sinkTableName = "testTableConvertedSpecificRecordStreamToTable_Output";
        // get a DataStream of SpecificRecord from a Table.
        DataStream<UserForSql> userDataStream = createDataStreamFromTable();
        // Convert the DataStream to a Table.
        Table table =
                tEnv.fromDataStream(
                        userDataStream,
                        AvroSchemaUtils.convertToTableSchema(
                                UserForSql.getClassSchema(),
                                ConversionContext
                                        .builder()
                                        .conversionRecordType(ConversionContext.ConversionRecordType.FOR_GENERIC_RECORD)
                                        .build()));
        // Verify the converted table.
        File tmpSinkDir = registerSinkTable(
                sinkTableName,
                AvroSchemaUtils.convertToTableSchema(
                        UserForSql.getClassSchema(),
                        ConversionContext
                                .builder()
                                .conversionRecordType(ConversionContext.ConversionRecordType.FOR_GENERIC_RECORD)
                                .build()),
                ORIGINAL_AVRO_SCHEMA_STRING);
        table.executeInsert(sinkTableName)
                .getJobClient()
                .map(JobClient::getJobExecutionResult)
                .map(CompletableFuture::join);
        checkOutput(tmpSinkDir, true);
    }

    /**
     * When converting a DataStream to a Table, the DataStream being converted may come from two
     * cases: 1. Provided directly by the users. 2. Converted from another Table. It turns out that
     * sometimes the actual data type of the Avro records may be different between the two cases.
     * For example, the CharSequence object may be String or Utf8. This method tests the first case.
     *
     * @see #testTableToSpecificRecordStreamToTable()
     */
    @Test
    public void testUserProvidedSpecificRecordDataStreamToTable()
            throws TableAlreadyExistException, TableNotExistException, DatabaseNotExistException,
                   IOException {
        final String sinkTableName = "testUserProvidedSpecificRecordDataStreamToTable_Output";
        // get a DataStream of SpecificRecord from a collection of records.
        DataStream<UserForSql> userDataStream = env.fromCollection(getRecords());
        // Convert the DataStream to a Table.
        Table table =
                tEnv.fromDataStream(
                        userDataStream,
                        AvroSchemaUtils.convertToTableSchema(
                                UserForSql.getClassSchema(),
                                ConversionContext
                                        .builder()
                                        .conversionRecordType(ConversionContext.ConversionRecordType.FOR_SPECIFIC_RECORD)
                                        .build()));
        // verify the converted table.
        File tmpSinkDir = registerSinkTable(
                sinkTableName,
                AvroSchemaUtils.convertToTableSchema(
                        UserForSql.getClassSchema(),
                        ConversionContext
                                .builder()
                                .conversionRecordType(ConversionContext.ConversionRecordType.FOR_SPECIFIC_RECORD)
                                .build()),
                ORIGINAL_AVRO_SCHEMA_STRING);
        table.executeInsert(sinkTableName)
                .getJobClient()
                .map(JobClient::getJobExecutionResult)
                .map(CompletableFuture::join);
        checkOutput(tmpSinkDir, true);
    }

    // -------------------- private helper methods ----------------------
    DataStream<UserForSql> createDataStreamFromTable() {
        Table table = tEnv.sqlQuery("SELECT * FROM " + SOURCE_WITH_ORIGINAL_SCHEMA_FOR_SPECIFIC_RECORD);
        return tEnv.toDataStream(
                table, AvroSchemaUtils.convertToDataType(
                        UserForSql.getClassSchema(),
                        ConversionContext
                                .builder()
                                .conversionRecordType(ConversionContext.ConversionRecordType.FOR_SPECIFIC_RECORD)
                                .build()));
    }

    private static Map<String, String> getCommonConnectorOptions(String avroSchemaString) {
        Map<String, String> sourceConnectorOptions = new HashMap<>();
        sourceConnectorOptions.put(FactoryUtil.CONNECTOR.key(), "filesystem");
        sourceConnectorOptions.put("format", "avro");
        if (avroSchemaString != null) {
            sourceConnectorOptions.put("avro.schema", avroSchemaString);
        }
        return sourceConnectorOptions;
    }

    private static void registerSourceTable(
            String sourceTableName,
            Schema tableSchema,
            File sourceFile,
            String avroSchemaString) throws TableAlreadyExistException, DatabaseNotExistException {
        Map<String, String> sourceConnectorOptions = getCommonConnectorOptions(avroSchemaString);
        sourceConnectorOptions.put("path", sourceFile.getAbsolutePath());
        // Create catalog source table.
        CatalogTable sourceTable =
                CatalogTable.of(
                        Schema.newBuilder().fromSchema(tableSchema).build(),
                        "User Source Catalog Table " + sourceTableName,
                        Collections.emptyList(),
                        sourceConnectorOptions);

        // register the catalog tables
        Catalog catalog =
                tEnv.getCatalog(tEnv.getCurrentCatalog())
                        .orElseThrow(() -> new RuntimeException("Catalog does not exist."));
        catalog.createTable(
                new ObjectPath(tEnv.getCurrentDatabase(), sourceTableName), sourceTable, true);
    }

    /**
     * Register a table with the given table name to store the sink data.
     *
     * @param sinkTableName the name of the sink table.
     * @return the temporary file that stores the sink data.
     */
    private File registerSinkTable(
            String sinkTableName,
            Schema tableSchema,
            String avroSchemaString)
            throws IOException, TableAlreadyExistException, DatabaseNotExistException,
                   TableNotExistException {
        File tmpFile = Files.createTempDirectory(sinkTableName).toFile();
        // Set sink connector options.
        Map<String, String> sinkConnectorOptions = getCommonConnectorOptions(avroSchemaString);
        sinkConnectorOptions.put("path", tmpFile.getAbsolutePath());

        // Create catalog sink table.
        CatalogTable sinkTable =
                CatalogTable.of(
                        Schema.newBuilder().fromSchema(tableSchema).build(),
                        "User Sink Catalog Table",
                        Collections.emptyList(),
                        sinkConnectorOptions);

        Catalog catalog =
                tEnv.getCatalog(tEnv.getCurrentCatalog())
                        .orElseThrow(() -> new RuntimeException("Catalog does not exist."));
        ObjectPath sinkTablePath = new ObjectPath(tEnv.getCurrentDatabase(), sinkTableName);
        catalog.dropTable(sinkTablePath, true);
        catalog.createTable(sinkTablePath, sinkTable, false);
        return tmpFile;
    }

    /**
     * The generated schema will not have a Union schema with more than one non-null
     * types. So we need to exclude the `type_union` field.
     */
    private static org.apache.avro.Schema getGeneratedSchema() {
        RowType rowType = (RowType) AvroSchemaConverter.convertToDataType(
                UserForSql.getClassSchema()).getLogicalType();
        List<RowType.RowField> fieldsWithoutRawType = new ArrayList<>(rowType.getFields());
        // exclude the type_union field.
        fieldsWithoutRawType.removeIf(f -> f.getType() instanceof RawType);
        return AvroSchemaConverter.convertToSchema(
                new RowType(rowType.isNullable(), fieldsWithoutRawType), false);
    }

    private static File prepareAvroSourceFileWithOriginalSchema() throws IOException {
        File tmpFile = Files.createTempFile(SOURCE_FILE_NAME_PREFIX, ".avro").toFile();

        SpecificDatumWriter<UserForSql> datumWriter = new SpecificDatumWriter<>(UserForSql.getClassSchema());
        try (DataFileWriter<UserForSql> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(UserForSql.getClassSchema(), tmpFile);
            for (UserForSql userForSql : getRecords()) {
                dataFileWriter.append(userForSql);
            }
        }
        return tmpFile;
    }

    private static File prepareAvroSourceFileWithGeneratedSchema(File sourceFileWithOriginalSchema)
            throws IOException {
        File tmpFile = Files.createTempFile(SOURCE_FILE_NAME_PREFIX, ".avro").toFile();
        org.apache.avro.Schema generatedSchema = getGeneratedSchema();
        List<GenericRecord> genericRecords =
                getRecordsForGeneratedSchema(sourceFileWithOriginalSchema);
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(generatedSchema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(generatedSchema, tmpFile);
            for (GenericRecord userForSql : genericRecords) {
                dataFileWriter.append(userForSql);
            }
        }
        return tmpFile;
    }

    private void checkOutput(File sinkDir, boolean checkWithOriginalSchema) {
        File sourceFile = checkWithOriginalSchema ?
                sourceFileWithOriginalSchema : sourceFileWithGeneratedSchema;
        List<GenericRecord> expected = readAvroFile(sourceFile);
        List<GenericRecord> actual = readAvroFiles(sinkDir);
        assertFalse(expected.isEmpty());
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), actual.get(i));
        }
    }

    private List<GenericRecord> readAvroFiles(File sinkDir) {
        File[] sinkFiles = Preconditions.checkNotNull(sinkDir.listFiles());
        List<GenericRecord> users = new ArrayList<>();
        for (File sinkFile : sinkFiles) {
            if (sinkFile.isDirectory()) {
                users.addAll(readAvroFiles(sinkFile));
            } else {
                users.addAll(readAvroFile(sinkFile));
            }
        }
        users.sort(Comparator.comparing(
                userForSql -> (int) ((GenericRecord) userForSql.get("type_nested")).get("num")));
        return users;
    }

    private static List<GenericRecord> readAvroFile(File avroFile) {
        List<GenericRecord> users = new ArrayList<>();
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<>(avroFile, datumReader)) {
            while (dataFileReader.hasNext()) {
                users.add(dataFileReader.next());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return users;
    }

    private static List<UserForSql> getRecords() {
        List<UserForSql> list = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            User user = (User) AvroTestUtils.getSpecificTestData(i).f1;
            UserForSql userForSql = new UserForSql();
            for (org.apache.avro.Schema.Field field : UserForSql.getClassSchema().getFields()) {
                userForSql.put(field.name(), user.get(field.name()));
            }
            list.add(userForSql);
        }
        return list;
    }

    private static List<GenericRecord> getRecordsForGeneratedSchema(
            File sourceFileWithOriginalSchema) {
        org.apache.avro.Schema generatedSchema = getGeneratedSchema();
        List<GenericRecord> genericRecords = readAvroFile(sourceFileWithOriginalSchema);
        List<GenericRecord> result = new ArrayList<>();
        for (GenericRecord genericRecord : genericRecords) {
            GenericRecord newGenericRecord = new GenericData.Record(generatedSchema);
            copyFields(genericRecord, newGenericRecord);
            result.add(newGenericRecord);
        }
        return result;
    }

    private static void copyFields(GenericRecord from, GenericRecord to) {
        for (org.apache.avro.Schema.Field field : to.getSchema().getFields()) {
            Object object = from.get(field.name());
            if (object instanceof IndexedRecord) {
                GenericRecord newRecord = new GenericData.Record(handleUnion(field.schema()));
                copyFields((GenericRecord) object, newRecord);
                object = newRecord;
            } else if (object instanceof GenericFixed) {
                // Replace Fixed with ByteBuffer type because the generated schema treats Fixed as Bytes.
                object = ByteBuffer.wrap(((GenericFixed) object).bytes());
            } else if (object instanceof GenericEnumSymbol) {
                // Replace enum with String type because the generated schema treats enum as String.
                object = object.toString();
            }
            to.put(field.name(), object);
        }
    }

    private static org.apache.avro.Schema handleUnion(org.apache.avro.Schema schema) {
        if (schema.getType() == org.apache.avro.Schema.Type.UNION) {
            List<org.apache.avro.Schema> types = schema.getTypes();
            Preconditions.checkState(types.size() == 2);
            if (types.get(0).getType() == org.apache.avro.Schema.Type.NULL) {
                return types.get(1);
            } else {
                return types.get(0);
            }
        } else {
            return schema;
        }
    }
}
