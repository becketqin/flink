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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.types.conversion.DataTypeConverter;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.util.CollectionUtil;

import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.formats.avro.typeutils.conversion.AvroTypeConverter.getAvroTypeConverter;

/**
 * Converters to deal with the conversion related to {@link MapType} and {@link MultisetType}.
 */
public class MapOrMultiSetConverter {

    /**
     * Internal Class: {@link MapData}.
     *
     * <p>Supported Input External Classes for {@link DataTypeConverter#toInternal(Object)
     * toInternal()}: <br>
     * {@link Map Map&lt;CharSequence, ?&gt;}
     *
     * <p>Supported Output External Class for {@link DataTypeConverter#toExternal(Object)
     * toExternal()}: <br>
     * {@link Map Map&lt;CharSequence, ?&gt;} for MapType <br>
     * {@link Map Map&lt;CharSequence, Integer&gt;} for MultiSetType <br>
     * SpecificRecord and GenericRecord are not distinguished.
     */
    public static DataTypeConverter<Object, Object> forMapOrMultiSet(
            LogicalType type, Schema schema, ConversionContext ctx) {
        final Schema valueSchema = schema.getValueType();
        final DataTypeConverter<Object, Object> keyConverter =
                CharOrVarCharConverter.forStringCharSequenceV2();
        final DataTypeConverter<Object, Object> valueConverter =
                getAvroTypeConverter(extractValueTypeToAvroMap(type), valueSchema, ctx);

        LogicalType valueType = extractValueTypeToAvroMap(type);
        final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);

        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public MapData toInternal(Object external) {
                final Map<?, ?> map = (Map<?, ?>) external;
                Map<Object, Object> result = new HashMap<>();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    Object key = keyConverter.toInternal(entry.getKey());
                    Object value = valueConverter.toInternal(entry.getValue());
                    result.put(key, value);
                }
                return new GenericMapData(result);
            }

            @Override
            public Map<Object, Object> toExternal(Object internal) {
                final MapData mapData = (MapData) internal;
                final ArrayData keyArray = mapData.keyArray();
                final ArrayData valueArray = mapData.valueArray();
                final Map<Object, Object> map =
                        CollectionUtil.newHashMapWithExpectedSize(mapData.size());
                for (int i = 0; i < mapData.size(); ++i) {
                    final String key = keyArray.getString(i).toString();
                    Object from = valueGetter.getElementOrNull(valueArray, i);
                    final Object value = valueConverter.toExternal(from);
                    map.put(key, value);
                }
                return map;
            }
        };
    }

    public static LogicalType extractValueTypeToAvroMap(LogicalType type) {
        LogicalType keyType;
        LogicalType valueType;
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            keyType = mapType.getKeyType();
            valueType = mapType.getValueType();
        } else {
            MultisetType multisetType = (MultisetType) type;
            keyType = multisetType.getElementType();
            valueType = new IntType();
        }
        if (!keyType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "Avro format doesn't support non-string as key type of map. "
                            + "The key type is: "
                            + keyType.asSummaryString());
        }
        return valueType;
    }
}
