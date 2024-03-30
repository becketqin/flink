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

import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.types.conversion.DataTypeConverter;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SerializableFunction;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificFixed;

import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Converters to deal with the conversion related to {@link DecimalType}.
 */
public class DecimalTypeConverter {

    /**
     * Internal Class: {@link DecimalData}.
     *
     * <p>Supported Input External Classes for {@link DataTypeConverter#toInternal(Object)
     * toInternal()}: <br>
     * {@link GenericFixed}, {@link ByteBuffer}, or byte[].
     *
     * <p>Supported Output External Class for {@link DataTypeConverter#toExternal(Object)
     * toExternal()}: <br>
     * {@link SpecificFixed} for SpecificRecords. <br>
     * {@link GenericFixed} for GenericRecords. <br>
     */
    public static DataTypeConverter<Object, Object> forDecimalFixed(
            Schema schema, boolean forSpecific) {
        final Class<?> fixedClass = SpecificData.get().getClass(schema);
        Preconditions.checkState(
                SpecificFixed.class.isAssignableFrom(fixedClass),
                "Not a SpecificFixed class: " + fixedClass);

        LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        final Function<Object, DecimalData> toDecimalFunction =
                getToDecimalFunction(precision, scale);

        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public DecimalData toInternal(Object external) {
                return toDecimalFunction.apply(external);
            }

            @Override
            public GenericFixed toExternal(Object internal) {
                byte[] decimalBytes = ((DecimalData) internal).toUnscaledBytes();
                if (forSpecific) {
                    SpecificFixed fixed = (SpecificFixed) InstantiationUtil.instantiate(fixedClass);
                    fixed.bytes(decimalBytes);
                    return fixed;
                } else {
                    return (GenericFixed) GenericData.get().createFixed(null, decimalBytes, schema);
                }
            }
        };
    }

    /**
     * Internal Class: {@link DecimalData}.
     *
     * <p>Supported Input External Classes for {@link DataTypeConverter#toInternal(Object)
     * toInternal()}: <br>
     * {@link GenericFixed}, {@link ByteBuffer}, or byte[].
     *
     * <p>Supported Output External Class for {@link DataTypeConverter#toExternal(Object)
     * toExternal()}: <br>
     * {@link ByteBuffer} for both SpecificRecords and GenericRecords.
     */
    public static DataTypeConverter<Object, Object> forDecimalBytes(Schema schema) {
        LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        final Function<Object, DecimalData> toDecimalFunction =
                getToDecimalFunction(precision, scale);

        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public DecimalData toInternal(Object external) {
                return toDecimalFunction.apply(external);
            }

            @Override
            public ByteBuffer toExternal(Object internal) {
                return ByteBuffer.wrap(((DecimalData) internal).toUnscaledBytes());
            }
        };
    }

    /**
     * There are multiple kinds of Avro types that can map to a {@link DecimalType} type. To avoid
     * invoking <code>instanceOf</code> on a per-record basis, we take a look at the first external
     * record and decide how to convert all the records.
     *
     * <p><b>NOTE:</b> Public for the legacy {@link AvroToRowDataConverters}.
     */
    public static Function<Object, DecimalData> getToDecimalFunction(int precision, int scale) {

        return new SerializableFunction<Object, DecimalData>() {
            private Function<Object, byte[]> toBytes;

            @Override
            public DecimalData apply(Object external) {
                if (toBytes == null) {
                    toBytes = BinaryOrVarBinaryConverter.getToBytesFunction(external);
                }
                final byte[] bytes = (byte[]) toBytes.apply(external);
                return DecimalData.fromUnscaledBytes(bytes, precision, scale);
            }
        };
    }
}
