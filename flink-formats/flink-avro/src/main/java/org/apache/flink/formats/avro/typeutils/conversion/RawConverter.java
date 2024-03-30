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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.binary.BinaryRawValueData;
import org.apache.flink.table.types.conversion.DataTypeConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;

/**
 * Converters to deal with the conversion related to the {@link RawType}. The only case for RawType
 * is an Avro Union with more than one non-null types.
 */
public class RawConverter {

    /**
     * Internal Class: {@link RawValueData}.
     *
     * <p>Supported Input External Classes for {@link DataTypeConverter#toInternal(Object)
     * toInternal()}: <br>
     * Any class of the type in the Avro Union Type.
     *
     * <p>Supported Output External Class for {@link DataTypeConverter#toExternal(Object)
     * toExternal()}: <br>
     * The corresponding class that was serialized to the {@link RawValueData}. <br>
     * SpecificRecord and GenericRecord are not distinguished.
     */
    public static DataTypeConverter<Object, Object> forRaw(LogicalType type) {
        final TypeSerializer<Object> serializer = ((RawType) type).getTypeSerializer();
        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public RawValueData<Object> toInternal(Object external) {
                return BinaryRawValueData.fromObject(external);
            }

            @Override
            public Object toExternal(Object internal) {
                RawValueData<Object> rawValueData = (RawValueData<Object>) internal;
                return rawValueData.toObject(serializer);
            }
        };
    }
}
