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
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SerializableFunction;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificFixed;

import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Converters to deal with the conversion related to {@link
 * org.apache.flink.table.types.logical.BinaryType BinaryType} and {@link
 * org.apache.flink.table.types.logical.VarBinaryType VarBinaryType}.
 */
public class BinaryOrVarBinaryConverter {

    /**
     * Internal Class: byte[].
     *
     * <p>Supported Input External Classes for {@link DataTypeConverter#toInternal(Object)
     * toInternal()}: <br>
     * {@link SpecificFixed}, {@link ByteBuffer} or byte[]. <br>
     *
     * <p>Supported Output External Class for {@link DataTypeConverter#toExternal(Object)
     * toExternal()}: <br>
     * {@link SpecificFixed} for SpecificRecords. <br>
     * {@link GenericFixed} for GenericRecords. <br>
     */
    public static DataTypeConverter<Object, Object> forBytesFixed(
            Schema schema, boolean forSpecific) {
        final Class<?> fixedClass = SpecificData.get().getClass(schema);
        Preconditions.checkState(
                SpecificFixed.class.isAssignableFrom(fixedClass),
                "Not a SpecificFixed class: " + fixedClass);

        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;
            private Function<Object, byte[]> toBytes;

            @Override
            public byte[] toInternal(Object external) {
                if (toBytes == null) {
                    toBytes = getToBytesFunction(external);
                }
                return toBytes.apply(external);
            }

            @Override
            public GenericFixed toExternal(Object internal) {
                if (forSpecific) {
                    SpecificFixed fixed = (SpecificFixed) InstantiationUtil.instantiate(fixedClass);
                    fixed.bytes((byte[]) internal);
                    return fixed;
                } else {
                    return (GenericFixed)
                            GenericData.get().createFixed(null, (byte[]) internal, schema);
                }
            }
        };
    }

    /**
     * Internal Class: byte[].
     *
     * <p>Supported Input External Classes for {@link DataTypeConverter#toInternal(Object)
     * toInternal()}: <br>
     * {@link SpecificFixed}, {@link ByteBuffer} or byte[]. <br>
     *
     * <p>Supported Output External Class for {@link DataTypeConverter#toExternal(Object)
     * toExternal()}: <br>
     * {@link ByteBuffer} for both SpecificRecords and GenericRecords. <br>
     */
    public static DataTypeConverter<Object, Object> forBytesBytes() {
        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;
            private Function<Object, byte[]> toBytes;

            @Override
            public byte[] toInternal(Object external) {
                if (toBytes == null) {
                    toBytes = getToBytesFunction(external);
                }
                return toBytes.apply(external);
            }

            @Override
            public ByteBuffer toExternal(Object internal) {
                return ByteBuffer.wrap((byte[]) internal);
            }
        };
    }

    /**
     * Public for access from {@link DecimalTypeConverter} and {@link
     * org.apache.flink.formats.avro.AvroToRowDataConverters}.
     */
    public static Function<Object, byte[]> getToBytesFunction(Object external) {
        if (external instanceof GenericFixed) {
            return new SerializableFunction<Object, byte[]>() {
                private static final long serialVersionUID = 1L;

                @Override
                public byte[] apply(Object o) {
                    return ((GenericFixed) o).bytes();
                }
            };
        } else if (external instanceof ByteBuffer) {
            return new SerializableFunction<Object, byte[]>() {
                private static final long serialVersionUID = 1L;

                @Override
                public byte[] apply(Object o) {
                    ByteBuffer byteBuffer = (ByteBuffer) o;
                    byte[] bytes = new byte[byteBuffer.remaining()];
                    byteBuffer.get(bytes);
                    return bytes;
                }
            };
        } else {
            return new SerializableFunction<Object, byte[]>() {
                private static final long serialVersionUID = 1L;

                @Override
                public byte[] apply(Object o) {
                    return (byte[]) o;
                }
            };
        }
    }
}
