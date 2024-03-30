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

import java.io.Serializable;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.flink.util.Preconditions;


/**
 * The Avro schema conversion may need to be handled differently in different cases.
 * This class specifies the context for the schema conversion so the conversion can
 * be performed accordingly.
 */
public class ConversionContext implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * What is the class the conversion is for. This is used to decide how to convert a
     * Flink DataType to an external type. For example, a timestamp type in Flink SQL
     * may need to be converted to:
     * - an {@link java.time.Instant Instant} for an Avro specific record.
     * - a {@link Long} for an Avro generic record.
     */
    public enum ConversionRecordType {
        /** Convert the Schema to DataType with the conversion class of Avro SpecificRecord. */
        FOR_SPECIFIC_RECORD,
        /** Convert the Schema to DataType with the conversion class of Avro GenericRecord. */
        FOR_GENERIC_RECORD
    }

    private final int conversionVersion;
    // Available starting from V2.
    private final @Nullable ConversionRecordType conversionRecordType;

    private ConversionContext(
            int conversionVersion,
            @Nullable ConversionRecordType conversionRecordType) {
        this.conversionVersion = conversionVersion;
        this.conversionRecordType = conversionRecordType;
        if (conversionVersion >= 2) {
            Preconditions.checkArgument(conversionRecordType != null,
                    "The conversion record type must be specified for Avro"
                            + "type conversion V2 and above.");
        }
    }

    /**
     * If a converter cannot be found for this conversion version, that means the converter
     * should be inherited from the previous conversion version. In that case, this method
     * will be invoked to get a ConversionContext object of the previous version for
     * the converter lookup.
     *
     * <p>This method should only be invoked by {@link AvroTypeConverter}. Therefore it is
     * package private.
     */
    ConversionContext toPreviousVersion() {
        if (conversionVersion == 2) {
            // Ignore the conversion record type when fallback from V2 to V1.
            return ConversionContext.builder().conversionVersion(1).build();
        } else if (conversionVersion == 1) {
            return ConversionContext.builder().conversionVersion(0).build();
        } else {
            return null;
        }
    }

    public int getConversionVersion() {
        return conversionVersion;
    }

    public ConversionRecordType getConversionRecordType() {
        return conversionRecordType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(conversionVersion, conversionRecordType);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ConversionContext)) {
            return false;
        }
        ConversionContext other = (ConversionContext) obj;
        return this.conversionVersion == other.conversionVersion
                && this.conversionRecordType == other.conversionRecordType;
    }

    @Override
    public String toString() {
        return String.format("[version = %d, ConversionRecordType = %s]",
                conversionVersion, conversionRecordType);
    }

    public static ConversionContext v0() {
        return new Builder().conversionVersion(0).build();
    }

    public static ConversionContext v1() {
        return new Builder().conversionVersion(1).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    //------------------- builder class ----------------

    /**
     * The builder class for {@link ConversionContext}.
     */
    public static class Builder {
        private int conversionVersion = 0;
        private ConversionRecordType conversionRecordType;

        public Builder conversionVersion(int version) {
            this.conversionVersion = version;
            return this;
        }

        public Builder conversionRecordType(ConversionRecordType conversionRecordType) {
            this.conversionRecordType = conversionRecordType;
            return this;
        }

        public ConversionContext build() {
            return new ConversionContext(conversionVersion, conversionRecordType);
        }
    }
}
