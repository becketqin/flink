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

package org.apache.flink.formats.avro.typeutils.conversion;

import org.apache.flink.table.types.conversion.DataTypeConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A collection of {@link DataTypeConverter} to convert between Avro types and Flink internal data
 * structures.
 */
public class AvroTypeConverter {
    // The conversions before FLIP-378.
    private static final ConversionContext CONTEXT_V0 = ConversionContext.v0();
    // The conversions including FLIP-378.
    private static final ConversionContext CONTEXT_V1 =
            ConversionContext.builder().conversionVersion(1).build();
    // The conversions including FLIP-358.
    private static final ConversionContext CONTEXT_V2_SPECIFIC =
            ConversionContext
                    .builder()
                    .conversionVersion(2)
                    .conversionRecordType(ConversionContext.ConversionRecordType.FOR_SPECIFIC_RECORD)
                    .build();

    private static final ConversionContext CONTEXT_V2_GENERIC =
            ConversionContext
                    .builder()
                    .conversionVersion(2)
                    .conversionRecordType(ConversionContext.ConversionRecordType.FOR_GENERIC_RECORD)
                    .build();

    private static final Map<AvroTypeConverterIdentifier, AvroTypeConverterProvider> CONVERTERS =
            new HashMap<>();

    static {
        // Converters for conversion V0.
        // The converters that are static and same for both SpecificRecords and GenericRecords.
        put(LogicalTypeRoot.NULL, Schema.Type.NULL, CONTEXT_V0, forNull());
        put(LogicalTypeRoot.TINYINT, Schema.Type.INT, CONTEXT_V0, forTinyInt());
        put(LogicalTypeRoot.SMALLINT, Schema.Type.INT, CONTEXT_V0, forSmallInt());
        put(LogicalTypeRoot.INTEGER, Schema.Type.INT, CONTEXT_V0, identity());
        put(LogicalTypeRoot.BIGINT, Schema.Type.LONG, CONTEXT_V0, identity());
        put(LogicalTypeRoot.BOOLEAN, Schema.Type.BOOLEAN, CONTEXT_V0, identity());
        put(LogicalTypeRoot.FLOAT, Schema.Type.FLOAT, CONTEXT_V0, identity());
        put(LogicalTypeRoot.DOUBLE, Schema.Type.DOUBLE, CONTEXT_V0, identity());

        put(
                LogicalTypeRoot.DECIMAL,
                Schema.Type.BYTES,
                CONTEXT_V0,
                (x, schema, y) -> DecimalTypeConverter.forDecimalBytes(schema));

        put(
                LogicalTypeRoot.CHAR,
                Schema.Type.STRING,
                CONTEXT_V0,
                CharOrVarCharConverter.forStringCharSequenceV0());

        put(
                LogicalTypeRoot.VARCHAR,
                Schema.Type.STRING,
                CONTEXT_V0,
                CharOrVarCharConverter.forStringCharSequenceV0());

        put(
                LogicalTypeRoot.VARCHAR,
                Schema.Type.ENUM,
                CONTEXT_V0,
                CharOrVarCharConverter.forStringCharSequenceV0());

        put(
                LogicalTypeRoot.BINARY,
                Schema.Type.BYTES,
                CONTEXT_V0,
                BinaryOrVarBinaryConverter.forBytesBytes());

        put(
                LogicalTypeRoot.BINARY,
                Schema.Type.FIXED,
                CONTEXT_V0,
                BinaryOrVarBinaryConverter.forBytesBytes());

        put(
                LogicalTypeRoot.VARBINARY,
                Schema.Type.BYTES,
                CONTEXT_V0,
                BinaryOrVarBinaryConverter.forBytesBytes());

        put(
                LogicalTypeRoot.VARBINARY,
                Schema.Type.FIXED,
                CONTEXT_V0,
                BinaryOrVarBinaryConverter.forBytesBytes());
        put(
                LogicalTypeRoot.DATE,
                Schema.Type.INT,
                CONTEXT_V0,
                DateConverter.forDateV0());

        put(
                LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                Schema.Type.INT,
                CONTEXT_V0,
                TimeConverter.forTimeMillisV0());

        put(
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                Schema.Type.LONG,
                CONTEXT_V0,
                TimestampConverter.forTimestampWithoutTimeZoneV0());

        put(
                LogicalTypeRoot.ROW,
                Schema.Type.RECORD,
                CONTEXT_V0,
                RowOrStructuredConverter::forRowOrStructured);

        // Collection converters.
        put(LogicalTypeRoot.ARRAY, Schema.Type.ARRAY, CONTEXT_V0, ArrayConverter::forArray);
        put(LogicalTypeRoot.MAP, Schema.Type.MAP, CONTEXT_V0, MapOrMultiSetConverter::forMapOrMultiSet);
        put(LogicalTypeRoot.MULTISET, Schema.Type.MAP, CONTEXT_V0, MapOrMultiSetConverter::forMapOrMultiSet);

        // Additional conversion introduced in V1.
        put(
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                Schema.Type.LONG,
                CONTEXT_V1,
                (x, schema, forSpecific) ->
                        TimestampConverter.forTimestampWithoutTimeZoneV1());

        put(
                LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                Schema.Type.LONG,
                CONTEXT_V1,
                (x, schema, forSpecific) ->
                        TimestampConverter.forTimestampWithLocalTimeZoneV1());

        // Additional converters in V2.
        putV2(
                LogicalTypeRoot.DECIMAL,
                Schema.Type.FIXED,
                (x, schema, ctx) ->
                        DecimalTypeConverter.forDecimalFixed(schema, isForSpecific(ctx)));

        putV2(
                LogicalTypeRoot.CHAR,
                Schema.Type.STRING,
                CharOrVarCharConverter.forStringCharSequenceV2());

        putV2(
                LogicalTypeRoot.VARCHAR,
                Schema.Type.STRING,
                CharOrVarCharConverter.forStringCharSequenceV2());

        putV2(
                LogicalTypeRoot.VARCHAR,
                Schema.Type.ENUM,
                (x, schema, ctx) ->
                        CharOrVarCharConverter.forStringEnum(schema, isForSpecific(ctx)));

        putV2(
                LogicalTypeRoot.BINARY,
                Schema.Type.FIXED,
                (x, schema, ctx) ->
                        BinaryOrVarBinaryConverter.forBytesFixed(schema, isForSpecific(ctx)));

        putV2(
                LogicalTypeRoot.VARBINARY,
                Schema.Type.FIXED,
                (x, schema, ctx) ->
                        BinaryOrVarBinaryConverter.forBytesFixed(schema, isForSpecific(ctx)));

        putV2(
                LogicalTypeRoot.DATE,
                Schema.Type.INT,
                DateConverter.forDateV2(false),
                DateConverter.forDateV2(true));

        putV2(
                LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                Schema.Type.INT,
                TimeConverter.forTimeMillisV2(false),
                TimeConverter.forTimeMillisV2(true));

        putV2(
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                Schema.Type.LONG,
                (x, schema, ctx) ->
                        TimestampConverter.forTimestampV2(schema, isForSpecific(ctx), false));

        putV2(
                LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                Schema.Type.LONG,
                (x, schema, ctx) ->
                        TimestampConverter.forTimestampV2(schema, isForSpecific(ctx), true));

        putV2(
                LogicalTypeRoot.RAW,
                Schema.Type.UNION,
                (logicalType, x, y) -> RawConverter.forRaw(logicalType));

        putV2(
                LogicalTypeRoot.STRUCTURED_TYPE,
                Schema.Type.RECORD,
                RowOrStructuredConverter::forRowOrStructured);
    }

    // ------------------------- Main API to get converters ------------------------
    public static DataTypeConverter<Object, Object> getAvroTypeConverter(
            LogicalType logicalType, Schema schema) {
        return getAvroTypeConverter(logicalType, schema, ConversionContext.v0());
    }

    public static DataTypeConverter<Object, Object> getAvroTypeConverter(
            LogicalType logicalType, Schema schema, ConversionContext conversionContext) {
        Schema actualSchema = handleUnionSchema(schema);
        AvroTypeConverterProvider converterProvider;
        ConversionContext lookupContext = conversionContext;
        do {
            converterProvider = CONVERTERS
                    .get(new AvroTypeConverterIdentifier(logicalType.getTypeRoot(),
                            actualSchema.getType(),
                            lookupContext));
            lookupContext = lookupContext.toPreviousVersion();
        } while (converterProvider == null && lookupContext != null);

        if (converterProvider != null) {
            DataTypeConverter<Object, Object> converter =
                    converterProvider.getConverter(logicalType, actualSchema, conversionContext);
            return nullableConverter(converter);
        } else {
            throw new UnsupportedOperationException(String.format("Cannot find Avro type converter"
                            + "for the logical type %s and avro schema type %s for conversion "
                            + "context %s.",
                    logicalType, schema.getType(), conversionContext));
        }
    }

    // ------------------------- private helper methods  ------------------------
    private static void put(
            LogicalTypeRoot logicalTypeRoot,
            Schema.Type schemaType,
            ConversionContext context,
            DataTypeConverter<Object, Object> converter) {
        CONVERTERS.put(
                new AvroTypeConverterIdentifier(logicalTypeRoot, schemaType, context),
                (x, y, z) -> converter);
    }

    private static void put(
            LogicalTypeRoot logicalTypeRoot,
            Schema.Type schemaType,
            ConversionContext context,
            AvroTypeConverterProvider provider) {
        CONVERTERS.put(
                new AvroTypeConverterIdentifier(logicalTypeRoot, schemaType, context), provider);
    }

    private static void putV2(
            LogicalTypeRoot logicalTypeRoot,
            Schema.Type schemaType,
            DataTypeConverter<Object, Object> converter) {
        CONVERTERS.put(
                new AvroTypeConverterIdentifier(logicalTypeRoot, schemaType, CONTEXT_V2_GENERIC),
                (x, y, z) -> converter);
        CONVERTERS.put(
                new AvroTypeConverterIdentifier(logicalTypeRoot, schemaType, CONTEXT_V2_SPECIFIC),
                (x, y, z) -> converter);
    }

    private static void putV2(
            LogicalTypeRoot logicalTypeRoot,
            Schema.Type schemaType,
            DataTypeConverter<Object, Object> genericConverter,
            DataTypeConverter<Object, Object> specificConverter) {
        CONVERTERS.put(
                new AvroTypeConverterIdentifier(logicalTypeRoot, schemaType, CONTEXT_V2_GENERIC),
                (x, y, z) -> genericConverter);
        CONVERTERS.put(
                new AvroTypeConverterIdentifier(logicalTypeRoot, schemaType, CONTEXT_V2_SPECIFIC),
                (x, y, z) -> specificConverter);
    }

    private static void putV2(
            LogicalTypeRoot logicalTypeRoot,
            Schema.Type schemaType,
            AvroTypeConverterProvider provider) {
        CONVERTERS.put(
                new AvroTypeConverterIdentifier(logicalTypeRoot, schemaType, CONTEXT_V2_GENERIC), provider);
        CONVERTERS.put(
                new AvroTypeConverterIdentifier(logicalTypeRoot, schemaType, CONTEXT_V2_SPECIFIC), provider);
    }

    private static boolean isForSpecific(ConversionContext ctx) {
        return ctx.getConversionRecordType() == ConversionContext.ConversionRecordType.FOR_SPECIFIC_RECORD;
    }

    private static class AvroTypeConverterIdentifier {
        private final LogicalTypeRoot logicalTypeRoot;
        private final Schema.Type schemaType;
        private final ConversionContext conversionContext;

        private AvroTypeConverterIdentifier(
                LogicalTypeRoot logicalTypeRoot,
                Schema.Type schemaType,
                ConversionContext conversionContext) {
            this.logicalTypeRoot = logicalTypeRoot;
            this.schemaType = schemaType;
            this.conversionContext = conversionContext;
        }

        @Override
        public int hashCode() {
            return Objects.hash(logicalTypeRoot, schemaType, conversionContext);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof AvroTypeConverterIdentifier) {
                AvroTypeConverterIdentifier other = (AvroTypeConverterIdentifier) obj;
                return logicalTypeRoot.equals(other.logicalTypeRoot)
                        && schemaType.equals(other.schemaType)
                        && conversionContext.equals(other.conversionContext);
            }
            return false;
        }
    }

    @FunctionalInterface
    private interface AvroTypeConverterProvider {
        DataTypeConverter<Object, Object> getConverter(
                LogicalType logicalType, Schema schema, ConversionContext conversionContext);
    }

    private static Schema handleUnionSchema(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        } else {
            List<Schema> types = schema.getTypes();
            int size = types.size();
            if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
                return types.get(0);
            } else if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
                return types.get(1);
            } else if (size == 1) {
                return types.get(0);
            } else {
                // The union schema is mapped to a Raw, simply use the original schema.
                return schema;
            }
        }
    }

    private static DataTypeConverter<Object, Object> nullableConverter(
            DataTypeConverter<Object, Object> converter) {
        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object toInternal(Object external) {
                return external == null ? null : converter.toInternal(external);
            }

            @Override
            public Object toExternal(Object internal) {
                return internal == null ? null : converter.toExternal(internal);
            }
        };
    }

    // --------------------- Some Simple Avro Type Converter Definitions --------------------------

    // --------------------------------------------------------------------------------
    // IMPORTANT! We use anonymous classes instead of lambdas for a reason here. It is
    // necessary because the maven shade plugin cannot relocate classes in
    // SerializedLambdas (MSHADE-260). On the other hand we want to relocate Avro for
    // sql-client uber jars.
    // --------------------------------------------------------------------------------

    public static DataTypeConverter<Object, Object> identity() {
        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object toInternal(Object external) {
                return external;
            }

            @Override
            public Object toExternal(Object internal) {
                return internal;
            }
        };
    }

    public static DataTypeConverter<Object, Object> forNull() {
        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object toInternal(Object external) {
                return null;
            }

            @Override
            public Object toExternal(Object internal) {
                return null;
            }
        };
    }

    public static DataTypeConverter<Object, Object> forTinyInt() {
        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Byte toInternal(Object external) {
                return ((Integer) external).byteValue();
            }

            @Override
            public Integer toExternal(Object internal) {
                return ((Byte) internal).intValue();
            }
        };
    }

    public static DataTypeConverter<Object, Object> forSmallInt() {
        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Short toInternal(Object external) {
                return ((Integer) external).shortValue();
            }

            @Override
            public Integer toExternal(Object internal) {
                return ((Short) internal).intValue();
            }
        };
    }
}
