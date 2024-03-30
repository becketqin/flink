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

import org.apache.flink.annotation.Internal;
import org.apache.flink.formats.avro.typeutils.conversion.AvroTypeConverter;
import org.apache.flink.formats.avro.typeutils.conversion.ConversionContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.conversion.DataTypeConverter;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;

/** Tool class used to convert from {@link RowData} to Avro {@link GenericRecord}. */
@Internal
public class RowDataToAvroConverters {

    // --------------------------------------------------------------------------------
    // Runtime Converters
    // --------------------------------------------------------------------------------

    /**
     * Runtime converter that converts objects of Flink Table & SQL internal data structures to
     * corresponding Avro data structures.
     */
    @FunctionalInterface
    public interface RowDataToAvroConverter extends Serializable {
        Object convert(Schema schema, Object object);
    }

    /**
     * Creates a runtime converter according to the given logical type that converts objects of
     * Flink Table & SQL internal data structures to corresponding Avro data structures.
     */
    public static RowDataToAvroConverter createConverter(LogicalType type) {
        return createConverter(type, ConversionContext.v0());
    }

    public static RowDataToAvroConverter createConverter(
            LogicalType type, boolean legacyTimestampMapping) {
        return createConverter(
                type,
                legacyTimestampMapping ? ConversionContext.v0() : ConversionContext.v1());
    }

    /**
     * Creates a runtime converter according to the given logical type that converts objects of
     * Flink Table & SQL internal data structures to corresponding Avro data structures.
     */
    public static RowDataToAvroConverter createConverter(LogicalType type, ConversionContext ctx) {
        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;
            private DataTypeConverter<Object, Object> converter;

            @Override
            public Object convert(Schema schema, Object object) {
                if (converter == null) {
                    converter = AvroTypeConverter.getAvroTypeConverter(type, schema, ctx);
                }
                return converter.toExternal(object);
            }
        };
    }
}
