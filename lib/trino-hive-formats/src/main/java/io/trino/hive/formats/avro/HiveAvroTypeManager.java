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
package io.trino.hive.formats.avro;

import io.airlift.slice.Slice;
import io.trino.hive.formats.avro.model.AvroLogicalType.BytesDecimalLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.DateLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.TimestampMillisLogicalType;
import io.trino.spi.block.Block;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;
import org.joda.time.DateTimeZone;

import java.util.Optional;
import java.util.TimeZone;
import java.util.function.BiFunction;

import static io.trino.hive.formats.avro.AvroHiveConstants.CHAR_TYPE_LOGICAL_NAME;
import static io.trino.hive.formats.avro.AvroHiveConstants.VARCHAR_TYPE_LOGICAL_NAME;
import static io.trino.hive.formats.avro.model.AvroLogicalType.DATE;
import static io.trino.hive.formats.avro.model.AvroLogicalType.DECIMAL;
import static io.trino.hive.formats.avro.model.AvroLogicalType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.VarcharType.createVarcharType;

public class HiveAvroTypeManager
        extends NativeLogicalTypesAvroTypeManager
{
    @Override
    public Optional<BiFunction<Block, Integer, Object>> overrideBlockToAvroObject(Schema schema, Type type)
            throws AvroTypeException
    {
        ValidateLogicalTypeResult result = validateLogicalType(schema);
        return switch (result) {
            case NoLogicalType ignored -> Optional.empty();
            case NonNativeAvroLogicalType nonNativeAvroLogicalType -> switch (nonNativeAvroLogicalType.getLogicalTypeName()) {
                case VARCHAR_TYPE_LOGICAL_NAME, CHAR_TYPE_LOGICAL_NAME -> {
                    Type expectedType = getHiveLogicalVarCharOrCharType(schema, nonNativeAvroLogicalType);
                    if (!expectedType.equals(type)) {
                        throw new AvroTypeException("Type provided for column [%s] is incompatible with type for schema: %s".formatted(type, expectedType));
                    }
                    yield Optional.of((block, pos) -> ((Slice) expectedType.getObject(block, pos)).toStringUtf8());
                }
                default -> Optional.empty();
            };
            case InvalidNativeAvroLogicalType invalidNativeAvroLogicalType -> switch (invalidNativeAvroLogicalType.getLogicalTypeName()) {
                case TIMESTAMP_MILLIS, DATE, DECIMAL -> throw invalidNativeAvroLogicalType.getCause();
                default -> Optional.empty(); // Other logical types ignored by hive/don't map to hive types
            };
            case ValidNativeAvroLogicalType validNativeAvroLogicalType -> switch (validNativeAvroLogicalType.getLogicalType()) {
                case TimestampMillisLogicalType __ -> {
                    if (!(type instanceof TimestampType timestampType)) {
                        throw new AvroTypeException("Can't represent avro logical type %s with Trino Type %s".formatted(validNativeAvroLogicalType.getLogicalType(), type));
                    }
                    if (timestampType.isShort()) {
                        yield Optional.of((block, pos) -> {
                            long millis = roundDiv(timestampType.getLong(block, pos), Timestamps.MICROSECONDS_PER_MILLISECOND);
                            // see org.apache.hadoop.hive.serde2.avro.AvroSerializer.serializePrimitive
                            return DateTimeZone.forTimeZone(TimeZone.getDefault()).convertLocalToUTC(millis, false);
                        });
                    }
                    else {
                        yield Optional.of((block, pos) ->
                        {
                            SqlTimestamp timestamp = (SqlTimestamp) timestampType.getObject(block, pos);
                            // see org.apache.hadoop.hive.serde2.avro.AvroSerializer.serializePrimitive
                            return DateTimeZone.forTimeZone(TimeZone.getDefault()).convertLocalToUTC(timestamp.getMillis(), false);
                        });
                    }
                }
                case DateLogicalType __ -> super.overrideBlockToAvroObject(schema, type);
                case BytesDecimalLogicalType __ -> super.overrideBlockToAvroObject(schema, type);
                default -> Optional.empty(); // Other logical types ignored by hive/don't map to hive types
            };
        };
    }

    static Type getHiveLogicalVarCharOrCharType(Schema schema, NonNativeAvroLogicalType nonNativeAvroLogicalType)
            throws AvroTypeException
    {
        if (schema.getType() != Schema.Type.STRING) {
            throw new AvroTypeException("Unsupported Avro type for Hive Logical Type in schema " + schema);
        }
        Object maxLengthObject = schema.getObjectProp(AvroHiveConstants.VARCHAR_AND_CHAR_LOGICAL_TYPE_LENGTH_PROP);
        if (maxLengthObject == null) {
            throw new AvroTypeException("Missing property maxLength in schema for Hive Type " + nonNativeAvroLogicalType.getLogicalTypeName());
        }
        try {
            int maxLength;
            if (maxLengthObject instanceof String maxLengthString) {
                maxLength = Integer.parseInt(maxLengthString);
            }
            else if (maxLengthObject instanceof Number maxLengthNumber) {
                maxLength = maxLengthNumber.intValue();
            }
            else {
                throw new AvroTypeException("Unrecognized property type for " + AvroHiveConstants.VARCHAR_AND_CHAR_LOGICAL_TYPE_LENGTH_PROP + " in schema " + schema);
            }
            if (nonNativeAvroLogicalType.getLogicalTypeName().equals(VARCHAR_TYPE_LOGICAL_NAME)) {
                return createVarcharType(maxLength);
            }
            else {
                return createCharType(maxLength);
            }
        }
        catch (NumberFormatException numberFormatException) {
            throw new AvroTypeException("Property maxLength not convertible to Integer in Hive Logical type schema " + schema);
        }
    }
}
