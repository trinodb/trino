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
package io.trino.plugin.hive.avro;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.hive.formats.avro.AvroTypeException;
import io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;
import io.trino.spi.type.Varchars;
import org.apache.avro.Schema;
import org.joda.time.DateTimeZone;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static io.trino.plugin.hive.avro.AvroHiveConstants.CHAR_TYPE_LOGICAL_NAME;
import static io.trino.plugin.hive.avro.AvroHiveConstants.VARCHAR_AND_CHAR_LOGICAL_TYPE_LENGTH_PROP;
import static io.trino.plugin.hive.avro.AvroHiveConstants.VARCHAR_TYPE_LOGICAL_NAME;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public class HiveAvroTypeManager
        extends NativeLogicalTypesAvroTypeManager
{
    private final AtomicReference<ZoneId> convertToTimezone = new AtomicReference<>(UTC);
    private final TimestampType hiveSessionTimestamp;

    public HiveAvroTypeManager(HiveTimestampPrecision hiveTimestampPrecision)
    {
        hiveSessionTimestamp = createTimestampType(requireNonNull(hiveTimestampPrecision, "hiveTimestampPrecision is null").getPrecision());
    }

    @Override
    public void configure(Map<String, byte[]> fileMetadata)
    {
        if (fileMetadata.containsKey(AvroHiveConstants.WRITER_TIME_ZONE)) {
            convertToTimezone.set(ZoneId.of(new String(fileMetadata.get(AvroHiveConstants.WRITER_TIME_ZONE), StandardCharsets.UTF_8)));
        }
        else {
            // legacy path allows this conversion to be skipped with {@link org.apache.hadoop.conf.Configuration} param
            // currently no way to set that configuration in Trino
            convertToTimezone.set(TimeZone.getDefault().toZoneId());
        }
    }

    @Override
    public Optional<Type> overrideTypeForSchema(Schema schema)
            throws AvroTypeException
    {
        if (schema.getType() == Schema.Type.NULL) {
            // allows of dereference when no base columns from file used
            // BooleanType chosen rather arbitrarily to be stuffed with null
            // in response to behavior defined by io.trino.tests.product.hive.TestAvroSchemaStrictness.testInvalidUnionDefaults
            return Optional.of(BooleanType.BOOLEAN);
        }
        ValidateLogicalTypeResult result = validateLogicalType(schema);
        // mapped in from HiveType translator
        // TODO replace with sealed class case match syntax when stable
        if (result instanceof NativeLogicalTypesAvroTypeManager.NoLogicalType ignored) {
            return Optional.empty();
        }
        if (result instanceof NonNativeAvroLogicalType nonNativeAvroLogicalType) {
            return switch (nonNativeAvroLogicalType.getLogicalTypeName()) {
                case VARCHAR_TYPE_LOGICAL_NAME, CHAR_TYPE_LOGICAL_NAME -> Optional.of(getHiveLogicalVarCharOrCharType(schema, nonNativeAvroLogicalType));
                default -> Optional.empty();
            };
        }
        if (result instanceof NativeLogicalTypesAvroTypeManager.InvalidNativeAvroLogicalType invalidNativeAvroLogicalType) {
            return switch (invalidNativeAvroLogicalType.getLogicalTypeName()) {
                case TIMESTAMP_MILLIS, DATE, DECIMAL -> throw invalidNativeAvroLogicalType.getCause();
                default -> Optional.empty(); // Other logical types ignored by hive/don't map to hive types
            };
        }
        if (result instanceof NativeLogicalTypesAvroTypeManager.ValidNativeAvroLogicalType validNativeAvroLogicalType) {
            return switch (validNativeAvroLogicalType.getLogicalType().getName()) {
                case DATE -> super.overrideTypeForSchema(schema);
                case TIMESTAMP_MILLIS -> Optional.of(hiveSessionTimestamp);
                case DECIMAL -> {
                    if (schema.getType() == Schema.Type.FIXED) {
                        // for backwards compatibility
                        throw new AvroTypeException("Hive does not support fixed decimal types");
                    }
                    yield super.overrideTypeForSchema(schema);
                }
                default -> Optional.empty(); // Other logical types ignored by hive/don't map to hive types
            };
        }
        throw new IllegalStateException("Unhandled validate logical type result");
    }

    @Override
    public Optional<BiConsumer<BlockBuilder, Object>> overrideBuildingFunctionForSchema(Schema schema)
            throws AvroTypeException
    {
        ValidateLogicalTypeResult result = validateLogicalType(schema);
        // TODO replace with sealed class case match syntax when stable
        if (result instanceof NativeLogicalTypesAvroTypeManager.NoLogicalType ignored) {
            return Optional.empty();
        }
        if (result instanceof NonNativeAvroLogicalType nonNativeAvroLogicalType) {
            return switch (nonNativeAvroLogicalType.getLogicalTypeName()) {
                case VARCHAR_TYPE_LOGICAL_NAME, CHAR_TYPE_LOGICAL_NAME -> {
                    Type type = getHiveLogicalVarCharOrCharType(schema, nonNativeAvroLogicalType);
                    if (nonNativeAvroLogicalType.getLogicalTypeName().equals(VARCHAR_TYPE_LOGICAL_NAME)) {
                        yield Optional.of(((blockBuilder, obj) -> {
                            type.writeSlice(blockBuilder, Varchars.truncateToLength(Slices.utf8Slice(obj.toString()), type));
                        }));
                    }
                    else {
                        yield Optional.of(((blockBuilder, obj) -> {
                            type.writeSlice(blockBuilder, Chars.truncateToLengthAndTrimSpaces(Slices.utf8Slice(obj.toString()), type));
                        }));
                    }
                }
                default -> Optional.empty();
            };
        }
        if (result instanceof NativeLogicalTypesAvroTypeManager.InvalidNativeAvroLogicalType invalidNativeAvroLogicalType) {
            return switch (invalidNativeAvroLogicalType.getLogicalTypeName()) {
                case TIMESTAMP_MILLIS, DATE, DECIMAL -> throw invalidNativeAvroLogicalType.getCause();
                default -> Optional.empty(); // Other logical types ignored by hive/don't map to hive types
            };
        }
        if (result instanceof NativeLogicalTypesAvroTypeManager.ValidNativeAvroLogicalType validNativeAvroLogicalType) {
            return switch (validNativeAvroLogicalType.getLogicalType().getName()) {
                case TIMESTAMP_MILLIS -> {
                    if (hiveSessionTimestamp.isShort()) {
                        yield Optional.of((blockBuilder, obj) -> {
                            Long millisSinceEpochUTC = (Long) obj;
                            hiveSessionTimestamp.writeLong(blockBuilder, DateTimeZone.forTimeZone(TimeZone.getTimeZone(convertToTimezone.get())).convertUTCToLocal(millisSinceEpochUTC) * Timestamps.MICROSECONDS_PER_MILLISECOND);
                        });
                    }
                    else {
                        yield Optional.of((blockBuilder, obj) -> {
                            Long millisSinceEpochUTC = (Long) obj;
                            LongTimestamp longTimestamp = new LongTimestamp(DateTimeZone.forTimeZone(TimeZone.getTimeZone(convertToTimezone.get())).convertUTCToLocal(millisSinceEpochUTC) * Timestamps.MICROSECONDS_PER_MILLISECOND, 0);
                            hiveSessionTimestamp.writeObject(blockBuilder, longTimestamp);
                        });
                    }
                }
                case DATE, DECIMAL -> super.overrideBuildingFunctionForSchema(schema);
                default -> Optional.empty(); // Other logical types ignored by hive/don't map to hive types
            };
        }
        throw new IllegalStateException("Unhandled validate logical type result");
    }

    @Override
    public Optional<BiFunction<Block, Integer, Object>> overrideBlockToAvroObject(Schema schema, Type type)
            throws AvroTypeException
    {
        ValidateLogicalTypeResult result = validateLogicalType(schema);
        // TODO replace with sealed class case match syntax when stable
        if (result instanceof NativeLogicalTypesAvroTypeManager.NoLogicalType ignored) {
            return Optional.empty();
        }
        if (result instanceof NonNativeAvroLogicalType nonNativeAvroLogicalType) {
            return switch (nonNativeAvroLogicalType.getLogicalTypeName()) {
                case VARCHAR_TYPE_LOGICAL_NAME, CHAR_TYPE_LOGICAL_NAME -> {
                    Type expectedType = getHiveLogicalVarCharOrCharType(schema, nonNativeAvroLogicalType);
                    if (!expectedType.equals(type)) {
                        throw new AvroTypeException("Type provided for column [%s] is incompatible with type for schema: %s".formatted(type, expectedType));
                    }
                    yield Optional.of((block, pos) -> ((Slice) expectedType.getObject(block, pos)).toStringUtf8());
                }
                default -> Optional.empty();
            };
        }
        if (result instanceof NativeLogicalTypesAvroTypeManager.InvalidNativeAvroLogicalType invalidNativeAvroLogicalType) {
            return switch (invalidNativeAvroLogicalType.getLogicalTypeName()) {
                case TIMESTAMP_MILLIS, DATE, DECIMAL -> throw invalidNativeAvroLogicalType.getCause();
                default -> Optional.empty(); // Other logical types ignored by hive/don't map to hive types
            };
        }
        if (result instanceof NativeLogicalTypesAvroTypeManager.ValidNativeAvroLogicalType validNativeAvroLogicalType) {
            return switch (validNativeAvroLogicalType.getLogicalType().getName()) {
                case TIMESTAMP_MILLIS -> {
                    if (!(type instanceof TimestampType timestampType)) {
                        throw new AvroTypeException("Can't represent avro logical type %s with Trino Type %s".formatted(validNativeAvroLogicalType.getLogicalType().getName(), type));
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
                case DATE, DECIMAL -> super.overrideBlockToAvroObject(schema, type);
                default -> Optional.empty(); // Other logical types ignored by hive/don't map to hive types
            };
        }
        throw new IllegalStateException("Unhandled validate logical type result");
    }

    private static Type getHiveLogicalVarCharOrCharType(Schema schema, NonNativeAvroLogicalType nonNativeAvroLogicalType)
            throws AvroTypeException
    {
        if (schema.getType() != Schema.Type.STRING) {
            throw new AvroTypeException("Unsupported Avro type for Hive Logical Type in schema " + schema);
        }
        Object maxLengthObject = schema.getObjectProp(VARCHAR_AND_CHAR_LOGICAL_TYPE_LENGTH_PROP);
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
                throw new AvroTypeException("Unrecognized property type for " + VARCHAR_AND_CHAR_LOGICAL_TYPE_LENGTH_PROP + " in schema " + schema);
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
