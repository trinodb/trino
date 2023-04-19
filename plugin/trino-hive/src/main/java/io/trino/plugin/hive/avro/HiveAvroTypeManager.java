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

import io.airlift.slice.Slices;
import io.trino.hive.formats.avro.AvroNativeLogicalTypeManager;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;
import io.trino.spi.type.Varchars;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.joda.time.DateTimeZone;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static io.trino.plugin.hive.avro.AvroHiveConstants.CHAR_TYPE_LOGICAL_NAME;
import static io.trino.plugin.hive.avro.AvroHiveConstants.VARCHAR_AND_CHAR_LOGICAL_TYPE_LENGTH_PROP;
import static io.trino.plugin.hive.avro.AvroHiveConstants.VARCHAR_TYPE_LOGICAL_NAME;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public class HiveAvroTypeManager
        extends AvroNativeLogicalTypeManager
{
    private final AtomicReference<ZoneId> convertToTimezone = new AtomicReference<>(UTC);
    private boolean skipConversion;

    public HiveAvroTypeManager(Configuration configuration)
    {
        this.skipConversion = HiveConf.getBoolVar(
                requireNonNull(configuration, "configuration is null"), HiveConf.ConfVars.HIVE_AVRO_TIMESTAMP_SKIP_CONVERSION);
    }

    @Override
    public void configure(Map<String, byte[]> fileMetaData)
    {
        if (fileMetaData.containsKey(AvroHiveConstants.WRITER_TIME_ZONE)) {
            convertToTimezone.set(ZoneId.of(new String(fileMetaData.get(AvroHiveConstants.WRITER_TIME_ZONE), StandardCharsets.UTF_8)));
        }
        else if (!skipConversion) {
            convertToTimezone.set(TimeZone.getDefault().toZoneId());
        }
    }

    @Override
    public Optional<Type> overrideTypeForSchema(Schema schema)
    {
        if (schema.getType().equals(Schema.Type.NULL)) {
            // allows of dereference when no base columns from file used
            // BooleanType chosen rather arbitrarily to be stuffed with null
            return Optional.of(BooleanType.BOOLEAN);
        }
        ValidateLogicalTypeResult result = validateLogicalType(schema);
        // mapped in from HiveType translator
        // TODO replace with sealed class case match syntax when stable
        if (result instanceof AvroNativeLogicalTypeManager.NoLogicalType ignored) {
            return Optional.empty();
        }
        if (result instanceof NonNativeAvroLogicalType nonNativeAvroLogicalType) {
            return switch (nonNativeAvroLogicalType.getLogicalTypeName()) {
                case VARCHAR_TYPE_LOGICAL_NAME, CHAR_TYPE_LOGICAL_NAME -> Optional.of(getHiveLogicalVarCharOrCharType(schema, nonNativeAvroLogicalType));
                default -> Optional.empty();
            };
        }
        if (result instanceof AvroNativeLogicalTypeManager.InvalidNativeAvroLogicalType invalidNativeAvroLogicalType) {
            return switch (invalidNativeAvroLogicalType.getLogicalTypeName()) {
                case TIMESTAMP_MILLIS, DATE, DECIMAL -> throw invalidNativeAvroLogicalType.getCause();
                default -> Optional.empty(); // Other logical types ignored by hive/don't map to hive types
            };
        }
        if (result instanceof AvroNativeLogicalTypeManager.ValidNativeAvroLogicalType validNativeAvroLogicalType) {
            return switch (validNativeAvroLogicalType.getLogicalType().getName()) {
                case TIMESTAMP_MILLIS, DATE -> super.overrideTypeForSchema(schema);
                case DECIMAL -> {
                    if (schema.getType().equals(Schema.Type.FIXED)) {
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
    {
        ValidateLogicalTypeResult result = validateLogicalType(schema);
        // TODO replace with sealed class case match syntax when stable

        if (result instanceof AvroNativeLogicalTypeManager.NoLogicalType ignored) {
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
        if (result instanceof AvroNativeLogicalTypeManager.InvalidNativeAvroLogicalType invalidNativeAvroLogicalType) {
            return switch (invalidNativeAvroLogicalType.getLogicalTypeName()) {
                case TIMESTAMP_MILLIS, DATE, DECIMAL -> throw invalidNativeAvroLogicalType.getCause();
                default -> Optional.empty(); // Other logical types ignored by hive/don't map to hive types
            };
        }
        if (result instanceof AvroNativeLogicalTypeManager.ValidNativeAvroLogicalType validNativeAvroLogicalType) {
            return switch (validNativeAvroLogicalType.getLogicalType().getName()) {
                case TIMESTAMP_MILLIS -> {
                    yield Optional.of(((blockBuilder, obj) -> {
                        Long millisSinceEpochUTC = (Long) obj;
                        TimestampType.TIMESTAMP_MILLIS.writeLong(blockBuilder, DateTimeZone.forTimeZone(TimeZone.getTimeZone(convertToTimezone.get())).convertUTCToLocal(millisSinceEpochUTC) * Timestamps.MICROSECONDS_PER_MILLISECOND);
                    }));
                }
                case DATE -> super.overrideBuildingFunctionForSchema(schema);
                case DECIMAL -> {
                    if (schema.getType().equals(Schema.Type.FIXED)) {
                        // for backwards compatibility
                        throw new AvroTypeException("Hive does not support fixed decimal types");
                    }
                    yield super.overrideBuildingFunctionForSchema(schema);
                }
                default -> Optional.empty(); // Other logical types ignored by hive/don't map to hive types
            };
        }
        throw new IllegalStateException("Unhandled validate logical type result");
    }

    private static Type getHiveLogicalVarCharOrCharType(Schema schema, NonNativeAvroLogicalType nonNativeAvroLogicalType)
    {
        if (!schema.getType().equals(Schema.Type.STRING)) {
            throw new AvroTypeException("Unsupported Avro type for Hive Logical Type in schema " + schema.toString());
        }
        Object maxLengthObject = schema.getObjectProp(VARCHAR_AND_CHAR_LOGICAL_TYPE_LENGTH_PROP);
        if (maxLengthObject == null) {
            throw new AvroTypeException("Missing property maxLength in schema for Hive Type " + nonNativeAvroLogicalType.getLogicalTypeName());
        }
        try {
            int maxLength = 0;
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
            throw new AvroTypeException("Property maxLength not convertible to Integer in Hive Logical type schema " + schema.toString());
        }
    }
}
