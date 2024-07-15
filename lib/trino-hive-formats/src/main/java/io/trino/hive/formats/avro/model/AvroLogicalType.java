/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.hive.formats.avro.model;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

// Avro logical types supported in Trino natively cross product with supported Avro schema encoding
public sealed interface AvroLogicalType
        permits
        AvroLogicalType.DateLogicalType,
        AvroLogicalType.BytesDecimalLogicalType,
        AvroLogicalType.FixedDecimalLogicalType,
        AvroLogicalType.TimeMillisLogicalType,
        AvroLogicalType.TimeMicrosLogicalType,
        AvroLogicalType.TimestampMillisLogicalType,
        AvroLogicalType.TimestampMicrosLogicalType,
        AvroLogicalType.StringUUIDLogicalType
{
    // Copied from org.apache.avro.LogicalTypes
    String DATE = "date";
    String DECIMAL = "decimal";
    String TIME_MILLIS = "time-millis";
    String TIME_MICROS = "time-micros";
    String TIMESTAMP_MILLIS = "timestamp-millis";
    String TIMESTAMP_MICROS = "timestamp-micros";
    String LOCAL_TIMESTAMP_MILLIS = "local-timestamp-millis";
    String LOCAL_TIMESTAMP_MICROS = "local-timestamp-micros";
    String UUID = "uuid";

    static AvroLogicalType fromAvroLogicalType(LogicalType logicalType, Schema schema)
    {
        switch (logicalType.getName()) {
            case DATE -> {
                if (schema.getType() == Schema.Type.INT) {
                    return new DateLogicalType();
                }
            }
            case DECIMAL -> {
                LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
                switch (schema.getType()) {
                    case BYTES -> {
                        return new BytesDecimalLogicalType(decimal.getPrecision(), decimal.getScale());
                    }
                    case FIXED -> {
                        return new FixedDecimalLogicalType(decimal.getPrecision(), decimal.getScale(), schema.getFixedSize());
                    }
                    default -> throw new IllegalArgumentException("Unsupported Logical Type %s and schema pairing %s".formatted(logicalType.getName(), schema));
                }
            }
            case TIME_MILLIS -> {
                if (schema.getType() == Schema.Type.INT) {
                    return new TimeMillisLogicalType();
                }
            }
            case TIME_MICROS -> {
                if (schema.getType() == Schema.Type.LONG) {
                    return new TimeMicrosLogicalType();
                }
            }
            case TIMESTAMP_MILLIS -> {
                if (schema.getType() == Schema.Type.LONG) {
                    return new TimestampMillisLogicalType();
                }
            }
            case TIMESTAMP_MICROS -> {
                if (schema.getType() == Schema.Type.LONG) {
                    return new TimestampMicrosLogicalType();
                }
            }
            case UUID -> {
                if (schema.getType() == Schema.Type.STRING) {
                    return new StringUUIDLogicalType();
                }
            }
        }
        throw new IllegalArgumentException("Unsupported Logical Type %s and schema pairing %s".formatted(logicalType.getName(), schema));
    }

    record DateLogicalType()
            implements AvroLogicalType {}

    record BytesDecimalLogicalType(int precision, int scale)
            implements AvroLogicalType {}

    record FixedDecimalLogicalType(int precision, int scale, int fixedSize)
            implements AvroLogicalType {}

    record TimeMillisLogicalType()
            implements AvroLogicalType {}

    record TimeMicrosLogicalType()
            implements AvroLogicalType {}

    record TimestampMillisLogicalType()
            implements AvroLogicalType {}

    record TimestampMicrosLogicalType()
            implements AvroLogicalType {}

    record StringUUIDLogicalType()
            implements AvroLogicalType {}
}
