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
import org.apache.flink.formats.avro.JodaConverter;
import org.apache.flink.table.types.conversion.DataTypeConverter;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.util.function.SerializableFunction;

import java.time.LocalDate;
import java.util.function.Function;

/**
 * Converters to deal with the conversion related to {@link
 * org.apache.flink.table.types.logical.DateType DateType}.
 */
public class DateConverter {

    /**
     * Internal Class: Integer.
     *
     * <p>Supported Input External Classes for {@link DataTypeConverter#toInternal(Object)
     * toInternal()}: <br>
     * {@link Integer}, {@link LocalDate java.time.LocalDate}, or {@link org.joda.time.LocalDate}
     *
     * <p>Supported Output External Class for {@link DataTypeConverter#toExternal(Object)
     * toExternal()}: <br>
     * {@link Integer}
     */
    public static DataTypeConverter<Object, Object> forDateV0() {
        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;
            private Function<Object, Integer> toDateInt;

            @Override
            public Integer toInternal(Object external) {
                if (toDateInt == null) {
                    toDateInt = getToDateIntFunction(external);
                }
                return toDateInt.apply(external);
            }

            @Override
            public Object toExternal(Object internal) {
                return internal;
            }
        };
    }

    /**
     * Internal Class: Integer.
     *
     * <p>Supported Input External Classes for {@link DataTypeConverter#toInternal(Object)
     * toInternal()}: <br>
     * {@link Integer}, {@link LocalDate java.time.LocalDate}, or {@link org.joda.time.LocalDate}
     *
     * <p>Supported Output External Class for {@link DataTypeConverter#toExternal(Object)
     * toExternal()}: <br>
     * {@link Integer} for GenericRecords. <br>
     * {@link LocalDate java.time.LocalDate} for SpecificRecords.
     */
    public static DataTypeConverter<Object, Object> forDateV2(boolean forSpecific) {
        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;
            private Function<Object, Integer> toDateInt;

            @Override
            public Integer toInternal(Object external) {
                if (toDateInt == null) {
                    toDateInt = getToDateIntFunction(external);
                }
                return toDateInt.apply(external);
            }

            @Override
            public Object toExternal(Object internal) {
                return forSpecific ? DateTimeUtils.toLocalDate((int) internal) : (int) internal;
            }
        };
    }

    /**
     * There are multiple kinds of Avro types that can map to a {@link
     * org.apache.flink.table.types.logical.DateType DateType} type. To avoid invoking <code>
     * instanceOf</code> on a per-record basis, we take a look at the first external record and
     * decide how to convert all the records.
     *
     * <p><b>NOTE:</b> Public for the legacy {@link AvroToRowDataConverters}.
     */
    private static Function<Object, Integer> getToDateIntFunction(Object external) {
        if (external instanceof Integer) {
            return new SerializableFunction<Object, Integer>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Integer apply(Object o) {
                    return (int) o;
                }
            };
        } else if (external instanceof LocalDate) {
            return new SerializableFunction<Object, Integer>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Integer apply(Object o) {
                    return (int) ((LocalDate) o).toEpochDay();
                }
            };
        } else if (external instanceof org.joda.time.LocalDate) {
            JodaConverter jodaConverter = JodaConverter.getConverter();
            if (jodaConverter != null) {
                return new SerializableFunction<Object, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer apply(Object o) {
                        return (int) jodaConverter.convertDate(o);
                    }
                };
            }
        }
        throw new IllegalArgumentException(
                "Unexpected object type for DATE logical type. Received: " + external);
    }
}
