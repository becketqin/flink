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
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.types.conversion.DataTypeConverter;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.util.Preconditions;

import org.apache.avro.Schema;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.formats.avro.typeutils.conversion.AvroTypeConverter.getAvroTypeConverter;

/**
 * Converters to deal with the conversion related to {@link ArrayType}.
 *
 * <p>Ideally we should reuse <code>org.apache.flink.table.data.conversion.ArrayObjectArrayConverter
 * </code>. However, it will pull in flink-table-runtime as a dependency, which needs to be avoided.
 * Therefore, we created this class as a replacement.
 */
public class ArrayConverter {

    /**
     * Internal Class: {@link ArrayData}.
     *
     * <p>Supported Input External Classes for {@link DataTypeConverter#toInternal(Object)
     * toInternal()}: <br>
     * {@link List}
     *
     * <p>Supported Output External Class for {@link DataTypeConverter#toExternal(Object)
     * toExternal()}: <br>
     * {@link List} for both SpecificRecord and GenericRecord<br>
     */
    public static DataTypeConverter<Object, Object> forArray(
            LogicalType logicalType, Schema schema, ConversionContext ctx) {
        ArrayType arrayType = (ArrayType) logicalType;
        final LogicalType elementType = arrayType.getElementType();
        final Schema elementSchema = schema.getElementType();
        final DataTypeConverter<Object, Object> elementConverter =
                getAvroTypeConverter(elementType, elementSchema, ctx);
        Preconditions.checkNotNull(
                elementConverter,
                String.format(
                        "Cannot find element converter for Array of "
                                + "logical type %s and schema %s",
                        elementType, elementSchema));
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);

        return new DataTypeConverter<Object, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public ArrayData toInternal(Object external) {
                final List<?> list = (List<?>) external;
                final int length = list.size();
                final Object[] array = (Object[]) Array.newInstance(elementClass, length);
                for (int i = 0; i < length; ++i) {
                    Object externalElement = list.get(i);
                    array[i] =
                            externalElement == null
                                    ? null
                                    : elementConverter.toInternal(list.get(i));
                }
                return new GenericArrayData(array);
            }

            @Override
            public List<Object> toExternal(Object internal) {
                ArrayData arrayData = (ArrayData) internal;
                List<Object> list = new ArrayList<>();
                for (int i = 0; i < arrayData.size(); ++i) {
                    list.add(
                            elementConverter.toExternal(
                                    elementGetter.getElementOrNull(arrayData, i)));
                }
                return list;
            }
        };
    }
}
