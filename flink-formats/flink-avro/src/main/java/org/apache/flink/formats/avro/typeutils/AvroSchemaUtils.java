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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.formats.avro.typeutils.conversion.ConversionContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * A user facing public util class to help converting {@link org.apache.avro.Schema Avro Schemas} to
 * Flink {@link DataType} and {@link Schema Table Schema}.
 */
@PublicEvolving
public class AvroSchemaUtils {

    /** private constructor for util class to prevent instantiation. */
    private AvroSchemaUtils() {}

    /**
     * Converts the given Avro schema string to a Flink {@link DataType}. This is useful when user
     * wants to convert a Table to a DataStream. This method defaults the conversion option to
     * FOR_ROW, which corresponds to DataStream&lt;Row&gt;.
     *
     * @param avroSchemaString Avro schema string
     * @return the DataType converted from Avro schema string.
     * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream(Table,
     *     AbstractDataType)
     * @see #convertToDataType(String, ConversionContext)
     */
    public static DataType convertToDataType(String avroSchemaString) {
        return AvroSchemaConverter.convertToDataType(avroSchemaString);
    }

    /**
     * Converts the given Avro schema string to a Flink {@link DataType}. This is useful when user
     * wants to convert a Table to a DataStream. The records in the DataStream can be one of
     * {@link Row}, {@link org.apache.avro.generic.GenericRecord GenericRecord},
     * or {@link org.apache.avro.specific.SpecificRecord SpecificRecord} depending on the specified
     * {@link ConversionContext.ConversionRecordType ConversionClass}.
     *
     * @param avroSchemaString Avro schema string
     * @param conversionContext one of FOR_ROW, FOR_SPECIFIC_RECORD, FOR_GENERIC_RECORD
     * @return the DataType converted from Avro schema string.
     * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream(Table,
     *     AbstractDataType)
     */
    public static DataType convertToDataType(
            String avroSchemaString, ConversionContext conversionContext) {
        return AvroSchemaConverter.convertToDataType(avroSchemaString, conversionContext);
    }

    /**
     * Converts the given Avro schema to a Flink {@link DataType}. This is useful when user wants to
     * convert a Table to a DataStream.
     *
     * @param schema Avro schema
     * @return the DataType converted from Avro schema string.
     * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream(Table,
     *     AbstractDataType)
     */
    public static DataType convertToDataType(org.apache.avro.Schema schema) {
        return AvroSchemaConverter.convertToDataType(schema);
    }

    /**
     * Converts the given Avro schema to a Flink {@link DataType}. This is useful when user wants to
     * convert a Table to a DataStream.
     *
     * @param schema Avro schema
     * @param conversionContext one of FOR_ROW, FOR_SPECIFIC_RECORD, FOR_GENERIC_RECORD
     * @return the DataType converted from Avro schema string.
     * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream(Table,
     *     AbstractDataType)
     */
    public static DataType convertToDataType(
            org.apache.avro.Schema schema, ConversionContext conversionContext) {
        return AvroSchemaConverter.convertToDataType(schema, conversionContext);
    }

    /**
     * Converts the given Avro schema string to a Flink {@link org.apache.flink.table.api.Schema
     * Table Schema}.This is useful when user wants to convert a DataStream into a Table.
     *
     * @param avroSchemaString Avro schema string.
     * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#fromDataStream(DataStream,
     *     Schema)
     * @return the Table Schema converted from Avro schema string.
     */
    public static Schema convertToTableSchema(
            String avroSchemaString, ConversionContext conversionContext) {
        return AvroSchemaConverter.convertToTableSchema(avroSchemaString, conversionContext);
    }

    /**
     * Converts the given Avro schema to a Flink {@link org.apache.flink.table.api.Schema Table
     * Schema}.This is useful when user wants to convert a DataStream into a Table.
     *
     * @param schema Avro schema.
     * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#fromDataStream(DataStream,
     *     Schema)
     * @return the Table Schema converted from Avro schema string.
     */
    public static Schema convertToTableSchema(
            org.apache.avro.Schema schema, ConversionContext conversionContext) {
        return AvroSchemaConverter.convertToTableSchema(schema, conversionContext);
    }
}
