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

package org.apache.flink.formats.avro.typeutils;

import org.apache.avro.generic.GenericData;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowSerializationSchema;
import org.apache.flink.formats.avro.typeutils.conversion.AvroTypeConverter;
import org.apache.flink.formats.avro.typeutils.conversion.ConversionContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.conversion.DataTypeConverter;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaParseException;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.formats.avro.typeutils.conversion.MapOrMultiSetConverter.extractValueTypeToAvroMap;

/**
 * Converts an Avro schema into Flink's type information. It uses {@link RowTypeInfo} for
 * representing objects and converts Avro types into types that are compatible with Flink's Table &
 * SQL API.
 *
 * <p>Note: Changes in this class need to be kept in sync with the corresponding runtime classes
 * {@link AvroRowDeserializationSchema} and {@link AvroRowSerializationSchema}.
 */
public class AvroSchemaConverter {
    private static final String DEFAULT_NAME_SPACE = "org.apache.flink.avro.generated";
    private AvroSchemaConverter() {
        // private
    }

    /**
     * Converts an Avro class into a nested row structure with deterministic field order and data
     * types that are compatible with Flink's Table & SQL API.
     *
     * @param avroClass Avro specific record that contains schema information
     * @return type information matching the schema
     */
    @SuppressWarnings("unchecked")
    public static <T extends SpecificRecord> TypeInformation<Row> convertToTypeInfo(
            Class<T> avroClass) {
        return convertToTypeInfo(avroClass, true);
    }

    /**
     * Converts an Avro class into a nested row structure with deterministic field order and data
     * types that are compatible with Flink's Table & SQL API.
     *
     * @param avroClass Avro specific record that contains schema information
     * @param legacyTimestampMapping legacy mapping of timestamp types
     * @return type information matching the schema
     */
    @SuppressWarnings("unchecked")
    public static <T extends SpecificRecord> TypeInformation<Row> convertToTypeInfo(
            Class<T> avroClass, boolean legacyTimestampMapping) {
        Preconditions.checkNotNull(avroClass, "Avro specific record class must not be null.");
        // determine schema to retrieve deterministic field order
        final Schema schema = SpecificData.get().getSchema(avroClass);
        return (TypeInformation<Row>) convertToTypeInfo(schema, true);
    }

    /**
     * Converts an Avro schema string into a nested row structure with deterministic field order and
     * data types that are compatible with Flink's Table & SQL API.
     *
     * @param avroSchemaString Avro schema definition string
     * @return type information matching the schema
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeInformation<T> convertToTypeInfo(String avroSchemaString) {
        return convertToTypeInfo(avroSchemaString, true);
    }

    /**
     * Converts an Avro schema string into a nested row structure with deterministic field order and
     * data types that are compatible with Flink's Table & SQL API.
     *
     * @param avroSchemaString Avro schema definition string
     * @param legacyTimestampMapping legacy mapping of timestamp types
     * @return type information matching the schema
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeInformation<T> convertToTypeInfo(
            String avroSchemaString, boolean legacyTimestampMapping) {
        Preconditions.checkNotNull(avroSchemaString, "Avro schema must not be null.");
        final Schema schema;
        try {
            schema = new Schema.Parser().parse(avroSchemaString);
        } catch (SchemaParseException e) {
            throw new IllegalArgumentException("Could not parse Avro schema string.", e);
        }
        return (TypeInformation<T>) convertToTypeInfo(schema, legacyTimestampMapping);
    }

    private static TypeInformation<?> convertToTypeInfo(
            Schema schema, boolean legacyTimestampMapping) {
        switch (schema.getType()) {
            case RECORD:
                final List<Schema.Field> fields = schema.getFields();

                final TypeInformation<?>[] types = new TypeInformation<?>[fields.size()];
                final String[] names = new String[fields.size()];
                for (int i = 0; i < fields.size(); i++) {
                    final Schema.Field field = fields.get(i);
                    types[i] = convertToTypeInfo(field.schema(), legacyTimestampMapping);
                    names[i] = field.name();
                }
                return Types.ROW_NAMED(names, types);
            case ENUM:
            case STRING:
                // convert Avro's Utf8/CharSequence to String
                return Types.STRING;
            case ARRAY:
                // result type might either be ObjectArrayTypeInfo or BasicArrayTypeInfo for Strings
                return Types.OBJECT_ARRAY(
                        convertToTypeInfo(schema.getElementType(), legacyTimestampMapping));
            case MAP:
                return Types.MAP(
                        Types.STRING,
                        convertToTypeInfo(schema.getValueType(), legacyTimestampMapping));
            case UNION:
                final Schema actualSchema;
                if (schema.getTypes().size() == 2
                        && schema.getTypes().get(0).getType() == Schema.Type.NULL) {
                    actualSchema = schema.getTypes().get(1);
                } else if (schema.getTypes().size() == 2
                        && schema.getTypes().get(1).getType() == Schema.Type.NULL) {
                    actualSchema = schema.getTypes().get(0);
                } else if (schema.getTypes().size() == 1) {
                    actualSchema = schema.getTypes().get(0);
                } else {
                    // use Kryo for serialization
                    return Types.GENERIC(Object.class);
                }
                return convertToTypeInfo(actualSchema, legacyTimestampMapping);
            case FIXED:
            case BYTES:
                // logical decimal type
                if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
                    return Types.BIG_DEC;
                }
                // convert fixed size binary data to primitive byte arrays
                return Types.PRIMITIVE_ARRAY(Types.BYTE);
            case INT:
                // logical date and time type
                final org.apache.avro.LogicalType logicalType = schema.getLogicalType();
                if (logicalType == LogicalTypes.date()) {
                    return Types.SQL_DATE;
                } else if (logicalType == LogicalTypes.timeMillis()) {
                    return Types.SQL_TIME;
                }
                return Types.INT;
            case LONG:
                if (legacyTimestampMapping) {
                    if (schema.getLogicalType() == LogicalTypes.timestampMillis()
                            || schema.getLogicalType() == LogicalTypes.timestampMicros()) {
                        return Types.SQL_TIMESTAMP;
                    } else if (schema.getLogicalType() == LogicalTypes.timeMicros()
                            || schema.getLogicalType() == LogicalTypes.timeMillis()) {
                        return Types.SQL_TIME;
                    }
                } else {
                    // Avro logical timestamp types to Flink DataStream timestamp types
                    if (schema.getLogicalType() == LogicalTypes.timestampMillis()
                            || schema.getLogicalType() == LogicalTypes.timestampMicros()) {
                        return Types.INSTANT;
                    } else if (schema.getLogicalType() == LogicalTypes.localTimestampMillis()
                            || schema.getLogicalType() == LogicalTypes.localTimestampMicros()) {
                        return Types.LOCAL_DATE_TIME;
                    } else if (schema.getLogicalType() == LogicalTypes.timeMicros()
                            || schema.getLogicalType() == LogicalTypes.timeMillis()) {
                        return Types.SQL_TIME;
                    }
                }
                return Types.LONG;
            case FLOAT:
                return Types.FLOAT;
            case DOUBLE:
                return Types.DOUBLE;
            case BOOLEAN:
                return Types.BOOLEAN;
            case NULL:
                return Types.VOID;
        }
        throw new IllegalArgumentException("Unsupported Avro type '" + schema.getType() + "'.");
    }

    /**
     * Converts an Avro schema string into a nested row structure with deterministic field order and
     * data types that are compatible with Flink's Table & SQL API.
     *
     * @param avroSchemaString Avro schema definition string
     * @return data type matching the schema
     */
    public static DataType convertToDataType(String avroSchemaString) {
        return convertToDataType(avroSchemaString, true);
    }

    /**
     * Converts an Avro schema string into a nested row structure with deterministic field order and
     * data types that are compatible with Flink's Table & SQL API.
     *
     * @param avroSchemaString Avro schema definition string
     * @param legacyTimestampMapping legacy mapping of local timestamps
     * @return data type matching the schema
     */
    public static DataType convertToDataType(
            String avroSchemaString, boolean legacyTimestampMapping) {
        Preconditions.checkNotNull(avroSchemaString, "Avro schema must not be null.");
        final Schema schema;
        try {
            schema = new Schema.Parser().parse(avroSchemaString);
        } catch (SchemaParseException e) {
            throw new IllegalArgumentException("Could not parse Avro schema string.", e);
        }
        return convertToDataType(
                schema,
                legacyTimestampMapping ? ConversionContext.v0() : ConversionContext.v1());
    }

    /**
     * Converts an Avro schema string into a nested row structure with deterministic field order and
     * data types that are compatible with Flink's Table & SQL API.
     *
     * @param schema Avro schema definition
     * @return data type matching the schema
     */
    public static DataType convertToDataType(Schema schema) {
        Preconditions.checkNotNull(schema, "Avro schema must not be null.");
        return convertToDataType(schema, ConversionContext.v0());
    }

    /**
     * Converts an Avro schema string into a nested row structure with deterministic field order and
     * data types that are compatible with Flink's Table & SQL API.
     *
     * @param avroSchemaString Avro schema definition string
     * @return data type matching the schema
     */
    public static DataType convertToDataType(
            String avroSchemaString, ConversionContext conversionContext) {
        Preconditions.checkNotNull(avroSchemaString, "Avro schema must not be null.");
        final Schema schema;
        try {
            schema = new Schema.Parser().parse(avroSchemaString);
        } catch (SchemaParseException e) {
            throw new IllegalArgumentException("Could not parse Avro schema string.", e);
        }
        return convertToDataType(schema, conversionContext);
    }

    public static DataType convertToDataType(
            Schema schema, ConversionContext conversionContext) {
        final int conversionVersion = conversionContext.getConversionVersion();
        final boolean forSpecific =
                conversionContext.getConversionRecordType() == ConversionContext.ConversionRecordType.FOR_SPECIFIC_RECORD;
        final Class<?> recordClass = SpecificData.get().getClass(schema);
        if (forSpecific && recordClass == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot find SpecificData class for schema %s. "
                                    + "Please make sure that the schema is compiled "
                                    + "with Avro's SpecificCompiler.",
                            schema));
        }
        switch (schema.getType()) {
            case RECORD:
                final List<Schema.Field> schemaFields = schema.getFields();

                final DataTypes.Field[] fields = new DataTypes.Field[schemaFields.size()];
                for (int i = 0; i < schemaFields.size(); i++) {
                    final Schema.Field field = schemaFields.get(i);
                    fields[i] = DataTypes.FIELD(
                            field.name(), convertToDataType(field.schema(), conversionContext));
                }

                if (conversionVersion < 2) {
                    // Use Row as the logical type.
                    return DataTypes.ROW(fields).notNull();
                } else {
                    // Use Row as the logical type.
                    // The conversion class is the extracted record class for SpecificRecord,
                    // or IndexedRecord for GenericRecord.
                    Class<?> conversionClass = forSpecific ? recordClass : GenericData.Record.class;
                    DataType dataType = DataTypes.ROW(fields).notNull();
                    DataTypeConverter<Object, Object> dataTypeConverter =
                            AvroTypeConverter.getAvroTypeConverter(
                                    dataType.getLogicalType(), schema, conversionContext);
                    return dataType.bridgedTo(conversionClass, dataTypeConverter);
                }
            case ENUM:
                return conversionVersion < 2 ?
                        DataTypes.STRING().notNull()
                        : getDataTypeForAvroRecord(
                            new VarCharType(Integer.MAX_VALUE),
                            schema,
                            conversionContext);
            case ARRAY:
                DataType type = DataTypes
                        .ARRAY(convertToDataType(schema.getElementType(), conversionContext))
                        .notNull();
                return conversionVersion < 2 ?
                        type
                        : type.bridgedTo(
                                List.class,
                                AvroTypeConverter.getAvroTypeConverter(
                                        type.getLogicalType(), schema, conversionContext));
            case MAP:
                DataType valueType = convertToDataType(schema.getValueType(), conversionContext);
                return conversionVersion < 2 ?
                        DataTypes.MAP(DataTypes.STRING().notNull(), valueType).notNull()
                        : DataTypes.MAP(
                                getDataTypeForAvroRecord(
                                        new VarCharType(Integer.MAX_VALUE),
                                        Schema.create(Schema.Type.STRING),
                                        conversionContext),
                                valueType).notNull();
            case UNION:
                final Schema actualSchema;
                final boolean nullable;
                if (schema.getTypes().size() == 2
                        && schema.getTypes().get(0).getType() == Schema.Type.NULL) {
                    actualSchema = schema.getTypes().get(1);
                    nullable = true;
                } else if (schema.getTypes().size() == 2
                        && schema.getTypes().get(1).getType() == Schema.Type.NULL) {
                    actualSchema = schema.getTypes().get(0);
                    nullable = true;
                } else if (schema.getTypes().size() == 1) {
                    actualSchema = schema.getTypes().get(0);
                    nullable = false;
                } else {
                    // use Kryo for serialization
                    return new AtomicDataType(
                            new RawType<>(
                                    false,
                                    Object.class,
                                    new KryoSerializer<>(Object.class, new ExecutionConfig())));
                }
                DataType converted = convertToDataType(actualSchema, conversionContext);
                return nullable ? converted.nullable() : converted;
            case FIXED:
                // logical decimal type
                if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
                    return dataTypeForDecimal(schema, conversionContext);
                } else {
                    // Fixed type without logical type.
                    return conversionVersion < 2 ?
                            DataTypes.VARBINARY(schema.getFixedSize()).notNull()
                            : getDataTypeForAvroRecord(
                                    new VarBinaryType(schema.isNullable(), schema.getFixedSize()),
                                    schema,
                                    conversionContext);
                }
            case STRING:
                // convert Avro's Utf8/CharSequence to String
                if (conversionVersion < 2) {
                    return DataTypes.STRING().notNull();
                } else {
                    return getDataTypeForAvroRecord(
                            new VarCharType(Integer.MAX_VALUE), schema, conversionContext);
                }
            case BYTES:
                // logical decimal type
                if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
                    return dataTypeForDecimal(schema, conversionContext);
                } else {
                    // Bytes type without logical type.
                    return conversionVersion < 2 ?
                            DataTypes.BYTES().notNull()
                            : getDataTypeForAvroRecord(
                                    new VarBinaryType(schema.isNullable(), Integer.MAX_VALUE),
                                    schema,
                                    conversionContext);
                }
            case INT:
                // logical date and time type
                final org.apache.avro.LogicalType logicalType = schema.getLogicalType();
                if (logicalType == LogicalTypes.date()) {
                    return conversionVersion < 2
                            ? DataTypes.DATE().notNull()
                            : getDataTypeForAvroRecord(new DateType(), schema, conversionContext);
                } else if (logicalType == LogicalTypes.timeMillis()) {
                    return conversionVersion < 2
                            ? DataTypes.TIME(3).notNull()
                            : getDataTypeForAvroRecord(new TimeType(3), schema, conversionContext);
                } else {
                    return conversionVersion < 2
                            ? DataTypes.INT().notNull()
                            : getDataTypeForAvroRecord(new IntType(), schema, conversionContext);
                }
            case LONG:
                if (conversionContext.getConversionVersion() == 0) {
                    // Avro logical timestamp types to Flink SQL timestamp types
                    if (schema.getLogicalType() == LogicalTypes.timestampMillis()) {
                        return DataTypes.TIMESTAMP(3).notNull();
                    } else if (schema.getLogicalType() == LogicalTypes.timestampMicros()) {
                        return DataTypes.TIMESTAMP(6).notNull();
                    } else if (schema.getLogicalType() == LogicalTypes.timeMillis()) {
                        return DataTypes.TIME(3).notNull();
                    } else if (schema.getLogicalType() == LogicalTypes.timeMicros()) {
                        return DataTypes.TIME(6).notNull();
                    }
                } else if (conversionContext.getConversionVersion() == 1) {
                    // Avro logical timestamp types to Flink SQL timestamp types
                    if (schema.getLogicalType() == LogicalTypes.timestampMillis()) {
                        return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull();
                    } else if (schema.getLogicalType() == LogicalTypes.timestampMicros()) {
                        return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6).notNull();
                    } else if (schema.getLogicalType() == LogicalTypes.timeMillis()) {
                        return DataTypes.TIME(3).notNull();
                    } else if (schema.getLogicalType() == LogicalTypes.timeMicros()) {
                        return DataTypes.TIME(6).notNull();
                    } else if (schema.getLogicalType() == LogicalTypes.localTimestampMillis()) {
                        return DataTypes.TIMESTAMP(3).notNull();
                    } else if (schema.getLogicalType() == LogicalTypes.localTimestampMicros()) {
                        return DataTypes.TIMESTAMP(6).notNull();
                    }
                } else {
                    // Avro logical timestamp types to Flink SQL timestamp types
                    if (schema.getLogicalType() == LogicalTypes.timestampMillis()) {
                        return conversionVersion == 0
                                ? DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull()
                                : getDataTypeForAvroRecord(new LocalZonedTimestampType(3), schema, conversionContext);
                    } else if (schema.getLogicalType() == LogicalTypes.timestampMicros()) {
                        return conversionVersion == 0
                                ? DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6).notNull()
                                : getDataTypeForAvroRecord(new LocalZonedTimestampType(6), schema, conversionContext);
                    } else if (schema.getLogicalType() == LogicalTypes.timeMillis()) {
                        return conversionVersion == 0
                                ? DataTypes.TIME(3).notNull()
                                : getDataTypeForAvroRecord(new TimeType(3), schema, conversionContext);
                    } else if (schema.getLogicalType() == LogicalTypes.timeMicros()) {
                        return conversionVersion == 0
                                ? DataTypes.TIME(6).notNull()
                                : getDataTypeForAvroRecord(new TimeType(6), schema, conversionContext);
                    } else if (schema.getLogicalType() == LogicalTypes.localTimestampMillis()) {
                        return conversionVersion == 0
                                ? DataTypes.TIMESTAMP(3).notNull()
                                : getDataTypeForAvroRecord(new TimestampType(3), schema, conversionContext);
                    } else if (schema.getLogicalType() == LogicalTypes.localTimestampMicros()) {
                        return conversionVersion == 0
                                ? DataTypes.TIMESTAMP(6).notNull()
                                : getDataTypeForAvroRecord(new TimestampType(6), schema, conversionContext);
                    }
                }
                return DataTypes.BIGINT().notNull();
            case FLOAT:
                return DataTypes.FLOAT().notNull();
            case DOUBLE:
                return DataTypes.DOUBLE().notNull();
            case BOOLEAN:
                return DataTypes.BOOLEAN().notNull();
            case NULL:
                return DataTypes.NULL();
        }
        throw new IllegalArgumentException("Unsupported Avro type '" + schema.getType() + "'.");
    }

    public static org.apache.flink.table.api.Schema convertToTableSchema(
            String avroSchemaString, ConversionContext conversionContext) {
        Preconditions.checkNotNull(avroSchemaString, "Avro schema must not be null.");
        final Schema schema;
        try {
            schema = new Schema.Parser().parse(avroSchemaString);
        } catch (SchemaParseException e) {
            throw new IllegalArgumentException("Could not parse Avro schema string.", e);
        }
        return convertToTableSchema(schema, conversionContext);
    }

    public static org.apache.flink.table.api.Schema convertToTableSchema(
            Schema schema, ConversionContext conversionContext) {
        Preconditions.checkState(schema.getType() == Schema.Type.RECORD);

        final List<Schema.Field> schemaFields = schema.getFields();

        final AbstractDataType<?>[] fieldDataTypes = new AbstractDataType<?>[schemaFields.size()];
        final String[] fieldNames = new String[schemaFields.size()];
        for (int i = 0; i < schemaFields.size(); i++) {
            final Schema.Field field = schemaFields.get(i);
            fieldDataTypes[i] = convertToDataType(field.schema(), conversionContext);
            fieldNames[i] = field.name();
        }
        return org.apache.flink.table.api.Schema.newBuilder()
                .fromFields(fieldNames, fieldDataTypes)
                .build();
    }

    /**
     * Converts Flink SQL {@link LogicalType} (can be nested) into an Avro schema.
     *
     * <p>Use "org.apache.flink.avro.generated.record" as the type name.
     *
     * @param schema the schema type, usually it should be the top level record type, e.g. not a
     *     nested type
     * @return Avro's {@link Schema} matching this logical type.
     */
    public static Schema convertToSchema(LogicalType schema) {
        return convertToSchema(schema, DEFAULT_NAME_SPACE + ".record", true, null, null);
    }

    /**
     * Converts Flink SQL {@link LogicalType} (can be nested) into an Avro schema.
     *
     * <p>Use "org.apache.flink.avro.generated.record" as the type name.
     *
     * @param schema the schema type, usually it should be the top level record type, e.g. not a
     *     nested type
     * @param legacyTimestampMapping whether to use the legacy timestamp mapping
     * @return Avro's {@link Schema} matching this logical type.
     */
    public static Schema convertToSchema(LogicalType schema, boolean legacyTimestampMapping) {
        return convertToSchema(
                schema, "org.apache.flink.avro.generated.record", legacyTimestampMapping, null, null);
    }

    /**
     * Converts Flink SQL {@link LogicalType} (can be nested) into an Avro schema.
     *
     * <p>The "{rowName}_" is used as the nested row type name prefix in order to generate the right
     * schema. Nested record type that only differs with type name is still compatible.
     *
     * @param logicalType logical type
     * @param rowName the record name
     * @return Avro's {@link Schema} matching this logical type.
     */
    public static Schema convertToSchema(LogicalType logicalType, String rowName) {
        return convertToSchema(logicalType, rowName, true, null, null);
    }

    /**
     * Converts Flink SQL {@link LogicalType} (can be nested) into an Avro schema.
     *
     * <p>The "{rowName}_" is used as the nested row type name prefix in order to generate the right
     * schema. Nested record type that only differs with type name is still compatible.
     *
     * @param logicalType logical type
     * @param rowName the record name
     * @param originalSchema the original avro schema associated with the given logical type.
     * @return Avro's {@link Schema} matching this logical type.
     */
    public static Schema convertToSchema(
            LogicalType logicalType, String rowName, @Nullable Schema originalSchema) {
        return convertToSchema(logicalType, rowName, true, originalSchema, null);
    }

    /**
     * Converts Flink SQL {@link LogicalType} (can be nested) into an Avro schema.
     *
     * <p>The "{rowName}_" is used as the nested row type name prefix in order to generate the right
     * schema. Nested record type that only differs with type name is still compatible.
     *
     * @param logicalType logical type
     * @param rowName the record name
     * @param originalSchema the original avro schema associated with the given logical type.
     * @return Avro's {@link Schema} matching this logical type.
     */
    public static Schema convertToSchema(
            LogicalType logicalType,
            String rowName,
            boolean legacyTimestampMapping,
            @Nullable Schema originalSchema,
            @Nullable Object defaultVal) {
        int precision;
        boolean nullable =
                originalSchema != null ? originalSchema.isNullable() : logicalType.isNullable();
        Schema schema = handleNullableUnion(originalSchema);
        if (schema != null
                && !logicalType.isAnyOf(
                        LogicalTypeFamily.CONSTRUCTED, LogicalTypeFamily.USER_DEFINED)) {
            // Honor the original schema if it is not a constructed or user defined type.
            // This cover four supported types: RowType, MapType, ArrayType, and StructuredType
            //
            // A RowType or StructuredType may only have a subset of the fields in the
            // original schema, so we cannot naively use the original schema.
            //
            // A MapType(MultisetType) or an ArrayType needs to be handled with an
            // iteration on the elements.
            return originalSchema;
        }
        switch (logicalType.getTypeRoot()) {
            case NULL:
                return SchemaBuilder.builder().nullType();
            case BOOLEAN:
                Schema bool = SchemaBuilder.builder().booleanType();
                return nullable ? nullableSchema(bool) : bool;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                Schema integer = SchemaBuilder.builder().intType();
                return nullable ? nullableSchema(integer) : integer;
            case BIGINT:
                Schema bigint = SchemaBuilder.builder().longType();
                return nullable ? nullableSchema(bigint) : bigint;
            case FLOAT:
                Schema f = SchemaBuilder.builder().floatType();
                return nullable ? nullableSchema(f) : f;
            case DOUBLE:
                Schema d = SchemaBuilder.builder().doubleType();
                return nullable ? nullableSchema(d) : d;
            case CHAR:
            case VARCHAR:
                Schema str = SchemaBuilder.builder().stringType();
                return nullable ? nullableSchema(str) : str;
            case BINARY:
            case VARBINARY:
                Schema binary = SchemaBuilder.builder().bytesType();
                return nullable ? nullableSchema(binary) : binary;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                // use long to represents Timestamp
                final TimestampType timestampType = (TimestampType) logicalType;
                precision = timestampType.getPrecision();
                org.apache.avro.LogicalType avroLogicalType;
                if (legacyTimestampMapping) {
                    if (precision <= 3) {
                        avroLogicalType = LogicalTypes.timestampMillis();
                    } else {
                        throw new IllegalArgumentException(
                                "Avro does not support TIMESTAMP type "
                                        + "with precision: "
                                        + precision
                                        + ", it only supports precision less than 3.");
                    }
                } else {
                    if (precision <= 3) {
                        avroLogicalType = LogicalTypes.localTimestampMillis();
                    } else if (precision <= 6) {
                        avroLogicalType = LogicalTypes.localTimestampMicros();
                    } else {
                        throw new IllegalArgumentException(
                                "Avro does not support LOCAL TIMESTAMP type "
                                        + "with precision: "
                                        + precision
                                        + ", it only supports precision less than 6.");
                    }
                }
                Schema timestamp = avroLogicalType.addToSchema(SchemaBuilder.builder().longType());
                return nullable ? nullableSchema(timestamp) : timestamp;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (legacyTimestampMapping) {
                    throw new UnsupportedOperationException(
                            "Unsupported to derive Schema for type: " + logicalType);
                } else {
                    final LocalZonedTimestampType localZonedTimestampType =
                            (LocalZonedTimestampType) logicalType;
                    precision = localZonedTimestampType.getPrecision();
                    if (precision <= 3) {
                        avroLogicalType = LogicalTypes.timestampMillis();
                    } else if (precision <= 6) {
                        avroLogicalType = LogicalTypes.timestampMicros();
                    } else {
                        throw new IllegalArgumentException(
                                "Avro does not support TIMESTAMP type "
                                        + "with precision: "
                                        + precision
                                        + ", it only supports precision less than 6.");
                    }
                    timestamp = avroLogicalType.addToSchema(SchemaBuilder.builder().longType());
                    return nullable ? nullableSchema(timestamp) : timestamp;
                }
            case DATE:
                // use int to represents Date
                Schema date = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
                return nullable ? nullableSchema(date) : date;
            case TIME_WITHOUT_TIME_ZONE:
                precision = ((TimeType) logicalType).getPrecision();
                if (precision > 3) {
                    throw new IllegalArgumentException(
                            "Avro does not support TIME type with precision: "
                                    + precision
                                    + ", it only supports precision less than 3.");
                }
                // use int to represents Time, we only support millisecond when deserialization
                Schema time =
                        LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType());
                return nullable ? nullableSchema(time) : time;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                // store BigDecimal as byte[]
                Schema decimal =
                        LogicalTypes.decimal(decimalType.getPrecision(), decimalType.getScale())
                                .addToSchema(SchemaBuilder.builder().bytesType());
                return nullable ? nullableSchema(decimal) : decimal;
            case ROW:
                RowType rowType = (RowType) logicalType;
                List<String> fieldNames = rowType.getFieldNames();
                // we have to make sure the record name is different in a Schema
                SchemaBuilder.FieldAssembler<Schema> builder =
                        SchemaBuilder.builder().record(rowName).fields();
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    String fieldName = fieldNames.get(i);
                    LogicalType fieldType = rowType.getTypeAt(i);
                    Optional<Schema.Field> field = getFieldIfAvailable(fieldName, schema);
                    SchemaBuilder.GenericDefault<Schema> fieldBuilder =
                            builder.name(fieldName)
                                    .type(
                                            convertToSchema(
                                                    fieldType,
                                                    rowName + "_" + fieldName,
                                                    legacyTimestampMapping,
                                                    field.map(Schema.Field::schema).orElse(null),
                                                    field.map(Schema.Field::defaultVal)
                                                            .orElse(null)));
                    if (schema != null) {
                        // Field is guaranteed to be present when schema is not null.
                        Schema.Field fieldInSchema = field.get();
                        if (fieldInSchema.hasDefaultValue()) {
                            Object fieldDefaultValue = fieldInSchema.defaultVal();
                            if (fieldDefaultValue == JsonProperties.NULL_VALUE) {
                                fieldDefaultValue = null;
                            }
                            builder = fieldBuilder.withDefault(fieldDefaultValue);
                        } else {
                            builder = fieldBuilder.noDefault();
                        }
                    } else {
                        // When the original schema is not provided, keep the legacy logic
                        // for backward compatibility.
                        if (fieldType.isNullable()) {
                            builder = fieldBuilder.withDefault(null);
                        } else {
                            builder = fieldBuilder.noDefault();
                        }
                    }
                }
                Schema record = builder.endRecord();
                return nullable ? nullableSchema(record, defaultVal) : record;
            case MULTISET:
            case MAP:
                Schema map =
                        SchemaBuilder.builder()
                                .map()
                                .values(
                                        convertToSchema(
                                                extractValueTypeToAvroMap(logicalType),
                                                rowName,
                                                legacyTimestampMapping,
                                                schema == null ? null : schema.getValueType(),
                                                null));
                return nullable ? nullableSchema(map, defaultVal) : map;
            case ARRAY:
                ArrayType arrayType = (ArrayType) logicalType;
                Schema array =
                        SchemaBuilder.builder()
                                .array()
                                .items(
                                        convertToSchema(
                                                arrayType.getElementType(),
                                                rowName,
                                                legacyTimestampMapping,
                                                schema == null ? null : schema.getElementType(),
                                                null));
                return nullable ? nullableSchema(array, defaultVal) : array;
            case STRUCTURED_TYPE:
                StructuredType structuredType = (StructuredType) logicalType;
                String recordName = schema != null ? schema.getFullName() : rowName;
                SchemaBuilder.FieldAssembler<Schema> schemaBuilder =
                        SchemaBuilder.builder().record(recordName).fields();
                List<StructuredType.StructuredAttribute> attributes =
                        structuredType.getAttributes();
                for (StructuredType.StructuredAttribute attr : attributes) {
                    String fieldName = attr.getName();
                    LogicalType fieldType = attr.getType();
                    Optional<Schema.Field> field = getFieldIfAvailable(fieldName, schema);
                    SchemaBuilder.GenericDefault<Schema> fieldBuilder =
                            schemaBuilder
                                    .name(fieldName)
                                    .type(
                                            convertToSchema(
                                                    fieldType,
                                                    fieldName,
                                                    legacyTimestampMapping,
                                                    field.map(Schema.Field::schema).orElse(null),
                                                    field.map(Schema.Field::defaultVal)
                                                            .orElse(null)));

                    if (fieldType.isNullable()) {
                        boolean hasDefaultValue =
                                schema != null && schema.getField(fieldName).hasDefaultValue();
                        Object defaultValue =
                                hasDefaultValue ? schema.getField(fieldName).defaultVal() : null;
                        if (defaultValue == JsonProperties.NULL_VALUE) {
                            defaultValue = null;
                        }
                        schemaBuilder = fieldBuilder.withDefault(defaultValue);
                    } else {
                        schemaBuilder = fieldBuilder.noDefault();
                    }
                }
                Schema recordSchema = schemaBuilder.endRecord();
                return nullable ? nullableSchema(recordSchema, defaultVal) : recordSchema;
            case RAW:
                // This is the case that we have a union with more than one non-null children type.
                if (schema == null) {
                    throw new UnsupportedOperationException("The schema for RAW type should not "
                            + "be null. Make sure the original Avro schema for field "
                            + rowName + " is provided.");
                }
                return schema;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported to derive Schema for type: " + logicalType);
        }
    }

    /** Returns schema with nullable true. */
    private static Schema nullableSchema(Schema schema) {
        return nullableSchema(schema, null);
    }

    /** Returns schema with nullable true and a potential default value. */
    private static Schema nullableSchema(Schema schema, @Nullable Object defaultValue) {
        if (isNullable(schema)) {
            return schema;
        } else if (defaultValue != null && !(defaultValue instanceof JsonProperties.Null)) {
            // There is a non-null default value,  the non-null type should be put as the
            // first type in the union.
            return Schema.createUnion(schema, SchemaBuilder.builder().nullType());
        } else {
            // A nullable type without a default value.
            return Schema.createUnion(SchemaBuilder.builder().nullType(), schema);
        }
    }

    private static boolean isNullable(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema.getType().equals(Schema.Type.NULL);
        } else {
            for (Schema childSchema : schema.getTypes()) {
                if (isNullable(childSchema)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static Optional<Schema.Field> getFieldIfAvailable(
            String fieldName, @Nullable Schema schema) {
        if (schema == null) {
            return Optional.empty();
        } else {
            return Optional.of(
                    Preconditions.checkNotNull(
                            schema.getField(fieldName),
                            String.format(
                                    "Field %s doesn't exist in schema %s", fieldName, schema)));
        }
    }

    /**
     * Because a Union schema with null child type is mapped to a nullable field in Flink, we have
     * to unwrap the union schema to its non-null child schema.
     */
    private static @Nullable Schema handleNullableUnion(Schema schema) {
        if (schema != null && schema.getType() == Schema.Type.UNION) {
            List<Schema> childSchemas = schema.getTypes();
            if (childSchemas.size() != 2) {
                // This is a normal union instead of union for a nullable field.
                return schema;
            }

            Schema resultSchema = null;
            Schema nullSchema = null;
            for (Schema childSchema : childSchemas) {
                if (childSchema.getType() == Schema.Type.NULL) {
                    nullSchema = childSchema;
                } else {
                    resultSchema = childSchema;
                }
            }
            if (resultSchema == null) {
                throw new IllegalStateException("Union schema only contains null children schema.");
            } else if (nullSchema == null) {
                // For the case a Union schema contains only non-null children schema,
                // This is a normal union instead of union for a nullable field.
                return schema;
            } else {
                return resultSchema;
            }
        } else {
            return schema;
        }
    }

    private static DataType dataTypeForDecimal(Schema schema, ConversionContext ctx) {
        final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        if (ctx.getConversionVersion() < 2) {
            // For row the default conversion class is java.math.BigDecimal.
            return DataTypes.DECIMAL(precision, scale).notNull();
        } else {
            return getDataTypeForAvroRecord(new DecimalType(precision, scale), schema, ctx)
                    .notNull();
        }
    }

    private static DataType getDataTypeForAvroRecord(
            LogicalType logicalType, Schema schema, ConversionContext ctx) {
        DataTypeConverter<Object, Object> converter =
                AvroTypeConverter.getAvroTypeConverter(logicalType, schema, ctx);
        return new AtomicDataType(
                logicalType.withCustomConversion(Object.class, converter),
                Object.class).notNull();
    }
}
