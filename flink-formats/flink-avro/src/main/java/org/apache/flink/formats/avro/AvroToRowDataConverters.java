/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro;

import org.apache.avro.Schema;

import org.apache.flink.annotation.Internal;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.avro.typeutils.conversion.AvroTypeConverter;
import org.apache.flink.formats.avro.typeutils.conversion.BinaryOrVarBinaryConverter;
import org.apache.flink.formats.avro.typeutils.conversion.ConversionContext;
import org.apache.flink.formats.avro.typeutils.conversion.DateConverter;
import org.apache.flink.formats.avro.typeutils.conversion.DecimalTypeConverter;
import org.apache.flink.formats.avro.typeutils.conversion.MapOrMultiSetConverter;
import org.apache.flink.formats.avro.typeutils.conversion.RawConverter;
import org.apache.flink.formats.avro.typeutils.conversion.TimeConverter;
import org.apache.flink.formats.avro.typeutils.conversion.TimestampConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.conversion.DataTypeConverter;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.util.Preconditions;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.formats.avro.typeutils.conversion.AvroTypeConverter.forNull;
import static org.apache.flink.formats.avro.typeutils.conversion.AvroTypeConverter.forSmallInt;
import static org.apache.flink.formats.avro.typeutils.conversion.AvroTypeConverter.forTinyInt;
import static org.apache.flink.formats.avro.typeutils.conversion.AvroTypeConverter.identity;

/** Tool class used to convert from Avro {@link GenericRecord} to {@link RowData}. * */
@Internal
public class AvroToRowDataConverters {

    /**
     * Runtime converter that converts Avro data structures into objects of Flink Table & SQL
     * internal data structures.
     */
    @FunctionalInterface
    public interface AvroToRowDataConverter extends Serializable {
        Object convert(Object object);
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    public static AvroToRowDataConverter createRowConverter(RowType rowType) {
        return createRowOrStructuredConverter(rowType, true);
    }

    public static AvroToRowDataConverter createRowConverter(
            RowType rowType, boolean legacyTimestampMapping) {
        return createRowOrStructuredConverter(rowType, legacyTimestampMapping);
    }

    private static AvroToRowDataConverter createRowOrStructuredConverter(
            LogicalType type, boolean legacyTimestampMapping) {
        List<LogicalType> fieldTypes = type.getChildren();
        final AvroToRowDataConverter[] fieldConverters =
                fieldTypes.stream()
                        .map(t -> createNullableConverter(t, legacyTimestampMapping))
                        .toArray(AvroToRowDataConverter[]::new);
        final int arity = fieldTypes.size();

        return avroObject -> {
            IndexedRecord record = (IndexedRecord) avroObject;
            GenericRowData row = new GenericRowData(arity);
            for (int i = 0; i < arity; ++i) {
                // avro always deserialize successfully even though the type isn't matched
                // so no need to throw exception about which field can't be deserialized
                row.setField(i, fieldConverters[i].convert(record.get(i)));
            }
            return row;
        };
    }

    /** Creates a runtime converter which is null safe. */
    private static AvroToRowDataConverter createNullableConverter(
            LogicalType type, boolean legacyTimestampMapping) {
        final AvroToRowDataConverter converter = createConverter(
                type,
                legacyTimestampMapping ? ConversionContext.v0() : ConversionContext.v1());
        return avroObject -> {
            if (avroObject == null) {
                return null;
            }
            return converter.convert(avroObject);
        };
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private static AvroToRowDataConverter createConverter(
            LogicalType type, ConversionContext ctx) {
        // This schema may be different from the original Avro schema, but it is OK because
        // the type converters for a given Flink SQL logical type can handle all the possible
        // Avro schema that can be mapped to that Flink SQL logical type, i.e. the converters
        // for the same Flink SQL logical type has the same toInternal() implementation which
        // is needed here.
        Schema schema = AvroSchemaConverter.convertToSchema(
                type,
                ctx.getConversionVersion() == 0);
        DataTypeConverter<Object, Object> converter =
                AvroTypeConverter.getAvroTypeConverter(type, schema, ctx);
        return converter::toInternal;
    }
}
