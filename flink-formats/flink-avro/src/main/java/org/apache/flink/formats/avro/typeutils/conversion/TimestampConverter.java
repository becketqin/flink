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

import java.time.LocalDateTime;

import java.time.ZoneOffset;

import org.apache.flink.formats.avro.JodaConverter;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.conversion.DataTypeConverter;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SerializableFunction;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.time.Instant;
import java.util.function.Function;

/**
 * Converters to deal with the conversion related to {@link
 * org.apache.flink.table.types.logical.TimestampType TimestampType}.
 */
public class TimestampConverter {

    public static DataTypeConverter<Object, Object> forTimestampWithoutTimeZoneV0() {

        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;
            private Function<Object, TimestampData> toTimestampData;

            @Override
            public TimestampData toInternal(Object external) {
                final long millis;
                if (external instanceof Long) {
                    millis = (Long) external;
                } else if (external instanceof Instant) {
                    millis = ((Instant) external).toEpochMilli();
                } else if (external instanceof LocalDateTime) {
                    return TimestampData.fromLocalDateTime((LocalDateTime) external);
                } else {
                    JodaConverter jodaConverter = JodaConverter.getConverter();
                    if (jodaConverter != null) {
                        millis = jodaConverter.convertTimestamp(external);
                    } else {
                        throw new IllegalArgumentException(
                                "Unexpected object type for TIMESTAMP logical type. Received: " + external);
                    }
                }
                return TimestampData.fromEpochMillis(millis);
            }

            @Override
            public Object toExternal(Object internal) {
                return ((TimestampData) internal).toInstant().toEpochMilli();
            }
        };
    }

    public static DataTypeConverter<Object, Object> forTimestampWithoutTimeZoneV1() {

        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;
            private Function<Object, TimestampData> toTimestampData;

            @Override
            public TimestampData toInternal(Object external) {
                final long millis;
                if (external instanceof Long) {
                    millis = (Long) external;
                } else if (external instanceof Instant) {
                    millis = ((Instant) external).toEpochMilli();
                } else if (external instanceof LocalDateTime) {
                    return TimestampData.fromLocalDateTime((LocalDateTime) external);
                } else {
                    JodaConverter jodaConverter = JodaConverter.getConverter();
                    if (jodaConverter != null) {
                        millis = jodaConverter.convertTimestamp(external);
                    } else {
                        throw new IllegalArgumentException(
                                "Unexpected object type for TIMESTAMP logical type. Received: " + external);
                    }
                }
                return TimestampData.fromEpochMillis(millis);
            }

            @Override
            public Object toExternal(Object internal) {
                return ((TimestampData) internal)
                        .toLocalDateTime()
                        .toInstant(ZoneOffset.UTC)
                        .toEpochMilli();
            }
        };
    }

    public static DataTypeConverter<Object, Object> forTimestampWithLocalTimeZoneV1() {
        return forTimestampWithoutTimeZoneV0();
    }


    /**
     * Internal Class: {@link Integer}.
     *
     * <p>Supported Input External Classes for {@link DataTypeConverter#toInternal(Object)
     * toInternal()}: <br>
     * {@link Long} <br>
     * {@link java.time.Instant} <br>
     * {@link org.joda.time.DateTime} (only for millis).
     *
     * <p>Supported Output External Class for {@link DataTypeConverter#toExternal(Object)
     * toExternal()}: <br>
     * {@link Long} for GenericRecords; <br>
     * {@link Instant} for SpecificRecords.
     */
    public static DataTypeConverter<Object, Object> forTimestampV2(
            Schema schema, boolean forSpecific, boolean localTimeZone) {
        if (schema.getLogicalType() == LogicalTypes.timestampMillis()) {
            return forTimestamp(3, forSpecific, localTimeZone);
        } else if (schema.getLogicalType() == LogicalTypes.timestampMicros()) {
            return forTimestamp(6, forSpecific, localTimeZone);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported Avro Timestamp logical type '" + schema.getLogicalType() + "'.");
        }
    }

    /**
     * Internal Class: {@link Integer}.
     *
     * <p>Supported Input External Classes for {@link DataTypeConverter#toInternal(Object)
     * toInternal()}: <br>
     * {@link Long} <br>
     * {@link java.time.Instant} <br>
     * {@link org.joda.time.DateTime} (only for millis).
     *
     * <p>Supported Output External Class for {@link DataTypeConverter#toExternal(Object)
     * toExternal()}: <br>
     * {@link Long} for GenericRecords; <br>
     * {@link Instant} for SpecificRecords.
     */
    public static DataTypeConverter<Object, Object> forTimestampV2(
            int precision, boolean forSpecific, boolean localTimeZone) {
        Preconditions.checkState(precision == 3 || precision == 6,
                "Unsupported Avro Timestamp precision '" + precision + "'."
                        + "Only precision of 3 or 6 is supported.");
        return forTimestamp(precision, forSpecific, localTimeZone);
    }

    private static DataTypeConverter<Object, Object> forTimestamp(
            int precision, boolean forSpecific, boolean localTimeZone) {
        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;
            private Function<Object, TimestampData> toTimestampData;

            @Override
            public TimestampData toInternal(Object external) {
                if (toTimestampData == null) {
                    toTimestampData = getToTimestampDataFunction(external, precision);
                }
                return toTimestampData.apply(external);
            }

            @Override
            public Object toExternal(Object internal) {
                Instant instant = localTimeZone ?
                        ((TimestampData) internal).toInstant() :
                        ((TimestampData) internal).toLocalDateTime().toInstant(ZoneOffset.UTC);
                if (forSpecific) {
                    return instant;
                } else if (precision == 3) {
                    return instant.toEpochMilli();
                } else {
                    return instant.getNano() / 1000;
                }
            }
        };
    }

    private static Function<Object, TimestampData> getToTimestampDataFunction(
            Object external, int precision) {
        if (external instanceof Long) {
            if (precision == 3) {
                // For timestamp millis in Long class..
                return new SerializableFunction<Object, TimestampData>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public TimestampData apply(Object o) {
                        return TimestampData.fromEpochMillis((long) o, 0);
                    }
                };
            } else {
                // For timestamp micros in Long class.
                return new SerializableFunction<Object, TimestampData>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public TimestampData apply(Object o) {
                        long val = (long) o;
                        long millis = val / 1000;
                        long nanosOfMillisecond = val % 1000 * 1000;
                        return TimestampData.fromEpochMillis(millis, (int) nanosOfMillisecond);
                    }
                };
            }
        } else if (external instanceof Instant) {
            // For timestamp millis or timestamp micros in Instant class.
            return new SerializableFunction<Object, TimestampData>() {
                private static final long serialVersionUID = 1L;

                @Override
                public TimestampData apply(Object o) {
                    return TimestampData.fromInstant((Instant) o);
                }
            };
        } else if (external instanceof LocalDateTime) {
                return new SerializableFunction<Object, TimestampData>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public TimestampData apply(Object o) {
                        return TimestampData.fromLocalDateTime((LocalDateTime) external);
                    }
                };
        } else if (external instanceof org.joda.time.DateTime && precision == 3) {
            final JodaConverter jodaConverter = JodaConverter.getConverter();
            if (jodaConverter != null) {
                // For timestamp millis, maybe the object is in
                return new SerializableFunction<Object, TimestampData>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public TimestampData apply(Object o) {
                        return DateTimeUtils.toTimestampData(jodaConverter.convertTimestamp(o), 3);
                    }
                };
            }
        }
        throw new IllegalArgumentException(
                String.format("Unexpected object type or precision for TIMESTAMP logical type. "
                        + "Received: (%s, %d)", external, precision));
    }
}
