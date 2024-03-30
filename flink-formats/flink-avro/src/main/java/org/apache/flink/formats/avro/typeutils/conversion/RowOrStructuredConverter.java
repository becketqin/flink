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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.conversion.DataTypeConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.util.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;

import java.util.List;

import static org.apache.flink.formats.avro.typeutils.conversion.AvroTypeConverter.getAvroTypeConverter;

/**
 * Converter for {@link RowType} and {@link StructuredType}.
 *
 * <p>Ideally we should reuse <code>org.apache.flink.table.data.conversion.StructuredObjectConverter
 * </code>. However, it will pull in flink-table-runtime as a dependency, which needs to be avoided.
 * Therefore, we created this class as a replacement.
 */
public class RowOrStructuredConverter {

    /**
     * Internal Class: {@link RowData}.
     *
     * <p>Supported Input External Classes for {@link DataTypeConverter#toInternal(Object)
     * toInternal()}: <br>
     * {@link IndexedRecord}
     *
     * <p>Supported Output External Class for {@link DataTypeConverter#toExternal(Object)
     * toExternal()}: <br>
     * {@link org.apache.avro.specific.SpecificRecord SpecificRecord} when
     * <code>LogicalType</code> is {@link StructuredType} and <code>forSpecific</code> is
     * <code>TRUE</code>. <br>
     * {@link org.apache.avro.generic.GenericRecord GenericRecord} otherwise.
     */
    public static DataTypeConverter<Object, Object> forRowOrStructured(
            LogicalType logicalType, Schema schema, ConversionContext ctx) {
        final boolean forSpecific =
                ctx.getConversionRecordType() == ConversionContext.ConversionRecordType.FOR_SPECIFIC_RECORD;
        Preconditions.checkState(
                logicalType instanceof RowType || logicalType instanceof StructuredType);
        List<LogicalType> fieldLogicalTypes = logicalType.getChildren();
        List<Schema.Field> fields = schema.getFields();
        final int fieldCount = logicalType.getChildren().size();
        final DataTypeConverter<Object, Object>[] fieldConverters =
                new DataTypeConverter[fieldCount];
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            LogicalType fieldType = fieldLogicalTypes.get(i);
            Schema fieldSchema = fields.get(i).schema();
            fieldGetters[i] = RowData.createFieldGetter(fieldType, i);
            fieldConverters[i] = getAvroTypeConverter(fieldType, fieldSchema, ctx);
        }

        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object toInternal(Object external) {
                IndexedRecord record = (IndexedRecord) external;
                GenericRowData row = new GenericRowData(fieldCount);
                for (int i = 0; i < fieldCount; ++i) {
                    row.setField(i, fieldConverters[i].toInternal(record.get(i)));
                }
                return row;
            }

            @Override
            public Object toExternal(Object internal) {
                final RowData row = (RowData) internal;
                final List<Schema.Field> fields = schema.getFields();
                final IndexedRecord record;
                if (!forSpecific || logicalType instanceof RowType) {
                    // The external type is GenericRecord.
                    record = new GenericData.Record(schema);
                } else {
                    // The external type is SpecificRecord.
                    record = (IndexedRecord) SpecificData.get().newRecord(null, schema);
                }
                for (int i = 0; i < fieldGetters.length; ++i) {
                    final Schema.Field schemaField = fields.get(i);
                    try {
                        Object avroObject =
                                fieldConverters[i].toExternal(fieldGetters[i].getFieldOrNull(row));
                        record.put(i, avroObject);
                    } catch (Throwable t) {
                        throw new RuntimeException(
                                String.format(
                                        "Fail to serialize at field: %s.", schemaField.name()),
                                t);
                    }
                }
                return record;
            }
        };
    }
}
