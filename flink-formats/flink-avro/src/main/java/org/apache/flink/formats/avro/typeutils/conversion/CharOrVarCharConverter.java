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

import org.apache.avro.util.Utf8;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.conversion.DataTypeConverter;
import org.apache.flink.util.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;

/**
 * Converters to deal with the conversion related to {@link
 * org.apache.flink.table.types.logical.CharType} and {@link
 * org.apache.flink.table.types.logical.VarCharType}.
 */
public class CharOrVarCharConverter {

    /**
     * Internal Class: {@link StringData}
     *
     * <p>Supported Input External Classes for {@link DataTypeConverter#toInternal(Object)
     * toInternal()}: <br>
     * {@link Enum} and its subclasses, {@link CharSequence}
     *
     * <p>Supported Output External Class for {@link DataTypeConverter#toExternal(Object)
     * toExternal()}: <br>
     * The Specific {@link Enum} subclass for SpecificRecord.<br>
     * The {@link org.apache.avro.generic.GenericData.EnumSymbol EnumSymbol} for GenericRecord.<br>
     */
    public static DataTypeConverter<Object, Object> forStringEnum(
            Schema schema, boolean forSpecific) {
        final Class<?> enumClass = SpecificData.get().getClass(schema);
        Preconditions.checkState(
                Enum.class.isAssignableFrom(enumClass), "Not an enum class: " + enumClass);
        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public StringData toInternal(Object external) {
                return StringData.fromString(external.toString());
            }

            @Override
            public Object toExternal(Object internal) {
                if (forSpecific) {
                    return Enum.valueOf(enumClass.asSubclass(Enum.class), internal.toString());
                } else {
                    return GenericData.get().createEnum(internal.toString(), schema);
                }
            }
        };
    }

    /**
     * Internal Class: {@link StringData}
     *
     * <p>Supported Input External Classes for {@link DataTypeConverter#toInternal(Object)
     * toInternal()}: <br>
     * {@link Enum} and its subclasses, {@link CharSequence}
     *
     * <p>Supported Output External Class for {@link DataTypeConverter#toExternal(Object)
     * toExternal()}: <br>
     * {@link String} for both SpecificRecord and GenericRecord.
     */
    public static DataTypeConverter<Object, Object> forStringCharSequenceV0() {
        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public StringData toInternal(Object external) {
                return StringData.fromString(external.toString());
            }

            @Override
            public Object toExternal(Object internal) {
                return new Utf8(internal.toString());
            }
        };
    }

    /**
     * Internal Class: {@link StringData}
     *
     * <p>Supported Input External Classes for {@link DataTypeConverter#toInternal(Object)
     * toInternal()}: <br>
     * {@link Enum} and its subclasses, {@link CharSequence}
     *
     * <p>Supported Output External Class for {@link DataTypeConverter#toExternal(Object)
     * toExternal()}: <br>
     * {@link String} for both SpecificRecord and GenericRecord.
     */
    public static DataTypeConverter<Object, Object> forStringCharSequenceV2() {
        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public StringData toInternal(Object external) {
                return StringData.fromString(external.toString());
            }

            @Override
            public String toExternal(Object internal) {
                return internal.toString();
            }
        };
    }
}
