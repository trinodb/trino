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
package io.trino.util.variant;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.operator.scalar.time.TimeOperators;
import io.trino.operator.scalar.timestamp.VarcharToTimestampCast;
import io.trino.operator.scalar.timestamptz.VarcharToTimestampWithTimeZoneCast;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.spi.type.VariantType;
import io.trino.spi.variant.Header;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;
import io.trino.type.BigintOperators;
import io.trino.type.BooleanOperators;
import io.trino.type.DateOperators;
import io.trino.type.DateTimes;
import io.trino.type.DoubleOperators;
import io.trino.type.IntegerOperators;
import io.trino.type.JsonType;
import io.trino.type.SmallintOperators;
import io.trino.type.TinyintOperators;
import io.trino.type.UnknownType;
import io.trino.type.UuidOperators;
import io.trino.type.VarcharOperators;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.json.JsonWriteFeature.COMBINE_UNICODE_SURROGATES_IN_UTF8;
import static com.fasterxml.jackson.core.json.JsonWriteFeature.ESCAPE_NON_ASCII;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonUtils.jsonFactoryBuilder;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.MAX_PRECISION;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.UNBOUNDED_LENGTH;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.spi.variant.Header.BasicType.PRIMITIVE;
import static io.trino.spi.variant.Header.PrimitiveType.BINARY;
import static io.trino.type.DateTimes.MICROSECONDS_PER_DAY;
import static io.trino.type.DateTimes.NANOSECONDS_PER_DAY;
import static io.trino.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.trino.type.DateTimes.round;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.JsonUtil.createJsonGenerator;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.time.ZoneOffset.UTC;

public final class VariantUtil
{
    private static final JsonFactory JSON_FACTORY = jsonFactoryBuilder()
            .disable(CANONICALIZE_FIELD_NAMES)
            // prevents characters outside BMP (e.g., emoji) from being escaped as surrogate pairs
            .enable(COMBINE_UNICODE_SURROGATES_IN_UTF8)
            .build();

    private VariantUtil() {}

    public static boolean canCastToVariant(Type type)
    {
        if (type instanceof UnknownType ||
                type instanceof BooleanType ||
                type instanceof TinyintType ||
                type instanceof SmallintType ||
                type instanceof IntegerType ||
                type instanceof BigintType ||
                type instanceof RealType ||
                type instanceof DoubleType ||
                type instanceof DecimalType ||
                type instanceof VarcharType ||
                type instanceof VarbinaryType ||
                type instanceof VariantType ||
                type instanceof TimestampType ||
                type instanceof TimestampWithTimeZoneType ||
                type instanceof DateType ||
                type instanceof TimeType ||
                type instanceof UuidType ||
                type instanceof JsonType) {
            return true;
        }
        if (type instanceof ArrayType arrayType) {
            return canCastToVariant(arrayType.getElementType());
        }
        if (type instanceof MapType mapType) {
            return mapType.getKeyType() instanceof VarcharType &&
                    canCastToVariant(mapType.getValueType());
        }
        if (type instanceof RowType) {
            return type.getTypeParameters().stream().allMatch(VariantUtil::canCastToVariant);
        }
        return false;
    }

    public static boolean canCastFromVariant(Type type)
    {
        if (type instanceof UnknownType ||
                type instanceof BooleanType ||
                type instanceof TinyintType ||
                type instanceof SmallintType ||
                type instanceof IntegerType ||
                type instanceof BigintType ||
                type instanceof RealType ||
                type instanceof DoubleType ||
                type instanceof DecimalType ||
                type instanceof VarcharType ||
                type instanceof VarbinaryType ||
                type instanceof VariantType ||
                type instanceof TimestampType ||
                type instanceof TimestampWithTimeZoneType ||
                type instanceof DateType ||
                type instanceof TimeType ||
                type instanceof UuidType) {
            return true;
        }
        if (type instanceof ArrayType arrayType) {
            return canCastFromVariant(arrayType.getElementType());
        }
        if (type instanceof MapType mapType) {
            return mapType.getKeyType() instanceof VarcharType && canCastFromVariant(mapType.getValueType());
        }
        if (type instanceof RowType) {
            return type.getTypeParameters().stream().allMatch(VariantUtil::canCastFromVariant);
        }
        return false;
    }

    // utility classes and functions for cast from VARIANT
    public static Slice asVarchar(Variant variant)
    {
        return switch (variant.basicType()) {
            case PRIMITIVE -> switch (variant.primitiveType()) {
                case NULL -> null;
                case STRING -> variant.getString();
                case BOOLEAN_TRUE -> BooleanOperators.castToVarchar(true);
                case BOOLEAN_FALSE -> BooleanOperators.castToVarchar(false);
                case INT8 -> utf8Slice(String.valueOf(variant.getByte()));
                case INT16 -> utf8Slice(String.valueOf(variant.getShort()));
                case INT32 -> utf8Slice(String.valueOf(variant.getInt()));
                case INT64 -> utf8Slice(String.valueOf(variant.getLong()));
                case DECIMAL4, DECIMAL8, DECIMAL16 -> utf8Slice(variant.getDecimal().toString());
                case FLOAT -> DoubleOperators.castToVarchar(UNBOUNDED_LENGTH, variant.getFloat());
                case DOUBLE -> DoubleOperators.castToVarchar(UNBOUNDED_LENGTH, variant.getDouble());
                case DATE -> DateOperators.castToVarchar(UNBOUNDED_LENGTH, variant.getDate());
                case TIMESTAMP_UTC_MICROS -> {
                    long micros = variant.getTimestampMicros();
                    long epochMillis = Math.floorDiv(micros, 1_000L);
                    int picosOfMilli = toIntExact(Math.floorMod(micros, 1_000L) * 1_000_000L);
                    yield utf8Slice(DateTimes.formatTimestampWithTimeZone(6, epochMillis, picosOfMilli, UTC_KEY.getZoneId()));
                }
                case TIMESTAMP_NTZ_MICROS -> utf8Slice(DateTimes.formatTimestamp(6, variant.getTimestampMicros(), 0, UTC));
                case TIMESTAMP_UTC_NANOS -> {
                    long nanos = variant.getTimestampNanos();
                    long epochMillis = Math.floorDiv(nanos, 1_000_000L);
                    int picosOfMilli = toIntExact(Math.floorMod(nanos, 1_000_000L) * 1_000L);
                    yield utf8Slice(DateTimes.formatTimestampWithTimeZone(9, epochMillis, picosOfMilli, UTC_KEY.getZoneId()));
                }
                case TIMESTAMP_NTZ_NANOS -> {
                    long nanos = variant.getTimestampNanos();
                    long epochMicros = Math.floorDiv(nanos, 1_000L);
                    int picosOfMicros = toIntExact(Math.floorMod(nanos, 1_000L) * 1_000L);
                    yield utf8Slice(DateTimes.formatTimestamp(9, epochMicros, picosOfMicros, UTC));
                }
                case UUID -> utf8Slice(variant.getUuid().toString());
                default -> throw new VariantCastException("Unsupported VARIANT primitive type for cast to VARCHAR: " + variant.primitiveType());
            };
            case SHORT_STRING -> variant.getString();
            default -> throw new VariantCastException("Unsupported VARIANT type for cast to VARCHAR: " + variant.basicType());
        };
    }

    public static Boolean asBoolean(Variant variant)
    {
        return switch (variant.basicType()) {
            case PRIMITIVE -> switch (variant.primitiveType()) {
                case NULL -> null;
                case BOOLEAN_TRUE -> true;
                case BOOLEAN_FALSE -> false;
                case STRING -> VarcharOperators.castToBoolean(variant.getString());
                case INT8 -> TinyintOperators.castToBoolean(variant.getByte());
                case INT16 -> SmallintOperators.castToBoolean(variant.getShort());
                case INT32 -> IntegerOperators.castToBoolean(variant.getInt());
                case INT64 -> BigintOperators.castToBoolean(variant.getLong());
                case DECIMAL4, DECIMAL8, DECIMAL16 -> variant.getDecimal().compareTo(BigDecimal.ZERO) != 0;
                case FLOAT -> DoubleOperators.castToBoolean(variant.getFloat());
                case DOUBLE -> DoubleOperators.castToBoolean(variant.getDouble());
                default -> throw new VariantCastException("Unsupported VARIANT primitive type for cast to BOOLEAN: " + variant.primitiveType());
            };
            case SHORT_STRING -> VarcharOperators.castToBoolean(variant.getString());
            default -> throw new VariantCastException("Unsupported VARIANT type for cast to BOOLEAN: " + variant.basicType());
        };
    }

    public static Long asTinyint(Variant variant)
    {
        return switch (variant.basicType()) {
            case PRIMITIVE -> switch (variant.primitiveType()) {
                case NULL -> null;
                case BOOLEAN_TRUE -> BooleanOperators.castToTinyint(true);
                case BOOLEAN_FALSE -> BooleanOperators.castToTinyint(false);
                case STRING -> VarcharOperators.castToTinyint(variant.getString());
                case INT8 -> (long) variant.getByte();
                case INT16 -> SmallintOperators.castToTinyint(variant.getShort());
                case INT32 -> IntegerOperators.castToTinyint(variant.getInt());
                case INT64 -> BigintOperators.castToTinyint(variant.getLong());
                case DECIMAL4, DECIMAL8, DECIMAL16 -> {
                    BigDecimal decimalValue = variant.getDecimal();
                    try {
                        yield (long) decimalValue.byteValueExact();
                    }
                    catch (ArithmeticException e) {
                        throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for tinyint: " + decimalValue, e);
                    }
                }
                case FLOAT -> DoubleOperators.castToTinyint(variant.getFloat());
                case DOUBLE -> DoubleOperators.castToTinyint(variant.getDouble());
                default -> throw new VariantCastException("Unsupported VARIANT primitive type for cast to TINYINT: " + variant.primitiveType());
            };
            case SHORT_STRING -> VarcharOperators.castToTinyint(variant.getString());
            default -> throw new VariantCastException("Unsupported VARIANT type for cast to TINYINT: " + variant.basicType());
        };
    }

    public static Long asSmallint(Variant variant)
    {
        return switch (variant.basicType()) {
            case PRIMITIVE -> switch (variant.primitiveType()) {
                case NULL -> null;
                case BOOLEAN_TRUE -> BooleanOperators.castToSmallint(true);
                case BOOLEAN_FALSE -> BooleanOperators.castToSmallint(false);
                case STRING -> VarcharOperators.castToSmallint(variant.getString());
                case INT8 -> (long) variant.getByte();
                case INT16 -> (long) variant.getShort();
                case INT32 -> IntegerOperators.castToSmallint(variant.getInt());
                case INT64 -> BigintOperators.castToSmallint(variant.getLong());
                case DECIMAL4, DECIMAL8, DECIMAL16 -> {
                    BigDecimal decimalValue = variant.getDecimal();
                    try {
                        yield (long) decimalValue.shortValueExact();
                    }
                    catch (ArithmeticException e) {
                        throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for smallint: " + decimalValue, e);
                    }
                }
                case FLOAT -> DoubleOperators.castToSmallint(variant.getFloat());
                case DOUBLE -> DoubleOperators.castToSmallint(variant.getDouble());
                default -> throw new VariantCastException("Unsupported VARIANT primitive type for cast to SMALLINT: " + variant.primitiveType());
            };
            case SHORT_STRING -> VarcharOperators.castToSmallint(variant.getString());
            default -> throw new VariantCastException("Unsupported VARIANT type for cast to SMALLINT: " + variant.basicType());
        };
    }

    public static Long asInteger(Variant variant)
    {
        return switch (variant.basicType()) {
            case PRIMITIVE -> switch (variant.primitiveType()) {
                case NULL -> null;
                case BOOLEAN_TRUE -> BooleanOperators.castToInteger(true);
                case BOOLEAN_FALSE -> BooleanOperators.castToInteger(false);
                case STRING -> VarcharOperators.castToInteger(variant.getString());
                case INT8 -> (long) variant.getByte();
                case INT16 -> (long) variant.getShort();
                case INT32 -> (long) variant.getInt();
                case INT64 -> BigintOperators.castToInteger(variant.getLong());
                case DECIMAL4, DECIMAL8, DECIMAL16 -> {
                    BigDecimal decimalValue = variant.getDecimal();
                    try {
                        yield (long) decimalValue.intValueExact();
                    }
                    catch (ArithmeticException e) {
                        throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for integer: " + decimalValue, e);
                    }
                }
                case FLOAT -> DoubleOperators.castToInteger(variant.getFloat());
                case DOUBLE -> DoubleOperators.castToInteger(variant.getDouble());
                default -> throw new VariantCastException("Unsupported VARIANT primitive type for cast to INTEGER: " + variant.primitiveType());
            };
            case SHORT_STRING -> VarcharOperators.castToInteger(variant.getString());
            default -> throw new VariantCastException("Unsupported VARIANT type for cast to INTEGER: " + variant.basicType());
        };
    }

    public static Long asBigint(Variant variant)
    {
        return switch (variant.basicType()) {
            case PRIMITIVE -> switch (variant.primitiveType()) {
                case NULL -> null;
                case BOOLEAN_TRUE -> BooleanOperators.castToBigint(true);
                case BOOLEAN_FALSE -> BooleanOperators.castToBigint(false);
                case STRING -> VarcharOperators.castToBigint(variant.getString());
                case INT8 -> (long) variant.getByte();
                case INT16 -> (long) variant.getShort();
                case INT32 -> (long) variant.getInt();
                case INT64 -> variant.getLong();
                case DECIMAL4, DECIMAL8, DECIMAL16 -> {
                    BigDecimal decimalValue = variant.getDecimal();
                    try {
                        yield decimalValue.longValueExact();
                    }
                    catch (ArithmeticException e) {
                        throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for bigint: " + decimalValue, e);
                    }
                }
                case FLOAT -> DoubleOperators.castToLong(variant.getFloat());
                case DOUBLE -> DoubleOperators.castToLong(variant.getDouble());
                default -> throw new VariantCastException("Unsupported VARIANT primitive type for cast to BIGINT: " + variant.primitiveType());
            };
            case SHORT_STRING -> VarcharOperators.castToBigint(variant.getString());
            default -> throw new VariantCastException("Unsupported VARIANT type for cast to BIGINT: " + variant.basicType());
        };
    }

    public static Long asReal(Variant variant)
    {
        return switch (variant.basicType()) {
            case PRIMITIVE -> switch (variant.primitiveType()) {
                case NULL -> null;
                case BOOLEAN_TRUE -> BooleanOperators.castToReal(true);
                case BOOLEAN_FALSE -> BooleanOperators.castToReal(false);
                case STRING -> VarcharOperators.castToFloat(variant.getString());
                case INT8 -> TinyintOperators.castToReal(variant.getByte());
                case INT16 -> SmallintOperators.castToReal(variant.getShort());
                case INT32 -> IntegerOperators.castToReal(variant.getInt());
                case INT64 -> BigintOperators.castToReal(variant.getLong());
                case DECIMAL4, DECIMAL8, DECIMAL16 -> (long) floatToRawIntBits(variant.getDecimal().floatValue());
                case FLOAT -> (long) floatToRawIntBits(variant.getFloat());
                case DOUBLE -> DoubleOperators.castToReal(variant.getDouble());
                default -> throw new VariantCastException("Unsupported VARIANT primitive type for cast to REAL: " + variant.primitiveType());
            };
            case SHORT_STRING -> VarcharOperators.castToFloat(variant.getString());
            default -> throw new VariantCastException("Unsupported VARIANT type for cast to REAL: " + variant.basicType());
        };
    }

    public static Double asDouble(Variant variant)
    {
        return switch (variant.basicType()) {
            case PRIMITIVE -> switch (variant.primitiveType()) {
                case NULL -> null;
                case BOOLEAN_TRUE -> BooleanOperators.castToDouble(true);
                case BOOLEAN_FALSE -> BooleanOperators.castToDouble(false);
                case STRING -> VarcharOperators.castToDouble(variant.getString());
                case INT8 -> TinyintOperators.castToDouble(variant.getByte());
                case INT16 -> SmallintOperators.castToDouble(variant.getShort());
                case INT32 -> IntegerOperators.castToDouble(variant.getInt());
                case INT64 -> BigintOperators.castToDouble(variant.getLong());
                case DECIMAL4, DECIMAL8, DECIMAL16 -> variant.getDecimal().doubleValue();
                case FLOAT -> (double) variant.getFloat();
                case DOUBLE -> variant.getDouble();
                default -> throw new VariantCastException("Unsupported VARIANT primitive type for cast to DOUBLE: " + variant.primitiveType());
            };
            case SHORT_STRING -> VarcharOperators.castToDouble(variant.getString());
            default -> throw new VariantCastException("Unsupported VARIANT type for cast to DOUBLE: " + variant.basicType());
        };
    }

    public static Long asShortDecimal(Variant variant, int precision, int scale)
    {
        BigDecimal bigDecimal = asJavaDecimal(variant, precision, scale);
        if (bigDecimal == null) {
            return null;
        }
        return bigDecimal.unscaledValue().longValue();
    }

    public static Int128 asLongDecimal(Variant variant, int precision, int scale)
    {
        BigDecimal bigDecimal = asJavaDecimal(variant, precision, scale);
        if (bigDecimal == null) {
            return null;
        }
        return Int128.valueOf(bigDecimal.unscaledValue());
    }

    private static BigDecimal asJavaDecimal(Variant variant, int precision, int scale)
    {
        BigDecimal bigDecimal = switch (variant.basicType()) {
            case PRIMITIVE -> switch (variant.primitiveType()) {
                case NULL -> null;
                case BOOLEAN_TRUE -> BigDecimal.ONE;
                case BOOLEAN_FALSE -> BigDecimal.ZERO;
                case STRING -> new BigDecimal(variant.getString().toStringUtf8());
                case INT8 -> BigDecimal.valueOf(variant.getByte());
                case INT16 -> BigDecimal.valueOf(variant.getShort());
                case INT32 -> BigDecimal.valueOf(variant.getInt());
                case INT64 -> BigDecimal.valueOf(variant.getLong());
                case DECIMAL4, DECIMAL8, DECIMAL16 -> variant.getDecimal();
                case FLOAT -> BigDecimal.valueOf(variant.getFloat());
                case DOUBLE -> BigDecimal.valueOf(variant.getDouble());
                default -> throw new VariantCastException("Unsupported VARIANT primitive type for cast to DECIMAL(%s,%s): %s".formatted(precision, scale, variant.primitiveType()));
            };
            case SHORT_STRING -> new BigDecimal(variant.getString().toStringUtf8());
            default -> throw new VariantCastException("Unsupported VARIANT type for cast to DECIMAL(%s,%s): %s".formatted(precision, scale, variant.basicType()));
        };
        if (bigDecimal == null) {
            return null;
        }
        bigDecimal = bigDecimal.setScale(scale, HALF_UP);
        if (bigDecimal.precision() > precision) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast input variant to DECIMAL(" + precision + "," + scale + ")");
        }
        return bigDecimal;
    }

    public static Long asDate(Variant variant)
    {
        return switch (variant.basicType()) {
            case PRIMITIVE -> switch (variant.primitiveType()) {
                case NULL -> null;
                case DATE -> (long) variant.getDate();
                case TIMESTAMP_UTC_MICROS, TIMESTAMP_NTZ_MICROS -> {
                    long micros = variant.getTimestampMicros();
                    long epochSeconds = Math.floorDiv(micros, 1_000_000L);
                    int nanoAdjustment = (int) Math.floorMod(micros, 1_000_000L) * 1_000;
                    yield Instant.ofEpochSecond(epochSeconds, nanoAdjustment)
                            .atZone(UTC)
                            .toLocalDate()
                            .toEpochDay();
                }
                case TIMESTAMP_UTC_NANOS, TIMESTAMP_NTZ_NANOS -> {
                    long nanos = variant.getTimestampNanos();
                    long epochSeconds = Math.floorDiv(nanos, 1_000_000_000L);
                    int nanoAdjustment = (int) Math.floorMod(nanos, 1_000_000_000L);
                    yield Instant.ofEpochSecond(epochSeconds, nanoAdjustment)
                            .atZone(UTC)
                            .toLocalDate()
                            .toEpochDay();
                }
                case STRING -> DateOperators.castFromVarchar(variant.getString());
                default -> throw new VariantCastException("Unsupported VARIANT primitive type for cast to DATE: " + variant.primitiveType());
            };
            case SHORT_STRING -> DateOperators.castFromVarchar(variant.getString());
            default -> throw new VariantCastException("Unsupported VARIANT type for cast to DATE: " + variant.basicType());
        };
    }

    public static Long asTime(Variant variant, int precision)
    {
        return switch (variant.basicType()) {
            case PRIMITIVE -> switch (variant.primitiveType()) {
                case NULL -> null;
                case TIME_NTZ_MICROS -> {
                    long timePicos = variant.getTimeMicros() * 1_000_000L;
                    // round can round up to a value equal to 24h, so we need to compute module 24h
                    yield round(timePicos, MAX_PRECISION - precision) % PICOSECONDS_PER_DAY;
                }
                case TIMESTAMP_UTC_MICROS, TIMESTAMP_NTZ_MICROS -> {
                    long micros = variant.getTimestampMicros() % MICROSECONDS_PER_DAY;
                    long timePicos = micros * 1_000_000L;
                    // round can round up to a value equal to 24h, so we need to compute module 24h
                    yield round(timePicos, MAX_PRECISION - precision) % PICOSECONDS_PER_DAY;
                }
                case TIMESTAMP_UTC_NANOS, TIMESTAMP_NTZ_NANOS -> {
                    long nanos = variant.getTimestampNanos() % NANOSECONDS_PER_DAY;
                    long timePicos = nanos * 1_000L;
                    // round can round up to a value equal to 24h, so we need to compute module 24h
                    yield round(timePicos, MAX_PRECISION - precision) % PICOSECONDS_PER_DAY;
                }
                case STRING -> TimeOperators.castFromVarchar(precision, variant.getString());
                default -> throw new VariantCastException("Unsupported VARIANT primitive type for cast to DATE: " + variant.primitiveType());
            };
            case SHORT_STRING -> TimeOperators.castFromVarchar(precision, variant.getString());
            default -> throw new VariantCastException("Unsupported VARIANT type for cast to DATE: " + variant.basicType());
        };
    }

    public static Long asShortTimestamp(Variant variant, int precision)
    {
        if (precision < 0 || precision > TimestampType.MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException("precision must be between 0 and " + TimestampType.MAX_SHORT_PRECISION);
        }

        return switch (variant.basicType()) {
            case PRIMITIVE -> switch (variant.primitiveType()) {
                case NULL -> null;
                case DATE -> TimeUnit.DAYS.toMicros(variant.getDate());
                case TIMESTAMP_UTC_MICROS, TIMESTAMP_NTZ_MICROS -> {
                    long micros = variant.getTimestampMicros();
                    if (precision == 6) {
                        yield micros;
                    }
                    yield round(micros, 6 - precision);
                }
                case TIMESTAMP_UTC_NANOS, TIMESTAMP_NTZ_NANOS -> {
                    long nanos = variant.getTimestampNanos();
                    // round is always required since the max precision is 6 (microseconds)
                    long roundedNanos = round(nanos, 9 - precision);
                    yield roundedNanos / 1_000;
                }
                case STRING -> VarcharToTimestampCast.castToShortTimestamp(precision, variant.getString().toStringUtf8());
                default -> throw new VariantCastException("Unsupported VARIANT primitive type for cast to TIMESTAMP(%d): %s".formatted(precision, variant.primitiveType()));
            };
            case SHORT_STRING -> VarcharToTimestampCast.castToShortTimestamp(precision, variant.getString().toStringUtf8());
            default -> throw new VariantCastException("Unsupported VARIANT type for cast to TIMESTAMP(%d): %s".formatted(precision, variant.basicType()));
        };
    }

    public static LongTimestamp asLongTimestamp(Variant variant, int precision)
    {
        if (precision <= TimestampType.MAX_SHORT_PRECISION || precision > TimestampType.MAX_PRECISION) {
            throw new IllegalArgumentException("precision must be between %d and %d".formatted(TimestampType.MAX_SHORT_PRECISION, TimestampType.MAX_PRECISION));
        }

        return switch (variant.basicType()) {
            case PRIMITIVE -> switch (variant.primitiveType()) {
                case NULL -> null;
                case DATE -> new LongTimestamp(TimeUnit.DAYS.toMicros(variant.getDate()), 0);
                case TIMESTAMP_UTC_MICROS, TIMESTAMP_NTZ_MICROS -> new LongTimestamp(variant.getTimestampMicros(), 0);
                case TIMESTAMP_UTC_NANOS, TIMESTAMP_NTZ_NANOS -> {
                    long nanos = variant.getTimestampNanos();
                    if (precision < 9) {
                        nanos = round(nanos, 9 - precision);
                    }
                    long micros = Math.floorDiv(nanos, 1_000L);
                    int picosOfMicro = toIntExact(Math.floorMod(nanos, 1_000L) * 1_000L);
                    yield new LongTimestamp(micros, picosOfMicro);
                }
                case STRING -> VarcharToTimestampCast.castToLongTimestamp(precision, variant.getString().toStringUtf8());
                default -> throw new VariantCastException("Unsupported VARIANT primitive type for cast to TIMESTAMP(%d): %s".formatted(precision, variant.primitiveType()));
            };
            case SHORT_STRING -> VarcharToTimestampCast.castToLongTimestamp(precision, variant.getString().toStringUtf8());
            default -> throw new VariantCastException("Unsupported VARIANT type for cast to TIMESTAMP(%d): %s".formatted(precision, variant.basicType()));
        };
    }

    public static Long asShortTimestampWithTimeZone(Variant variant, int precision)
    {
        if (precision < 0 || precision > TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException("precision must be between 0 and " + TimestampWithTimeZoneType.MAX_SHORT_PRECISION);
        }

        return switch (variant.basicType()) {
            case PRIMITIVE -> switch (variant.primitiveType()) {
                case NULL -> null;
                case DATE -> packDateTimeWithZone(TimeUnit.DAYS.toMillis(variant.getDate()), UTC_KEY);
                // round is always required as the max precision is 3 (milliseconds), and both micros and nanos have higher precision
                case TIMESTAMP_UTC_MICROS, TIMESTAMP_NTZ_MICROS -> packDateTimeWithZone(round(variant.getTimestampMicros(), 6 - precision) / 1_000, UTC_KEY);
                case TIMESTAMP_UTC_NANOS, TIMESTAMP_NTZ_NANOS -> packDateTimeWithZone(round(variant.getTimestampNanos(), 9 - precision) / 1_000_000, UTC_KEY);
                case STRING -> asShortTimestampWithTimeZone(variant.getString(), precision);
                default -> throw new VariantCastException("Unsupported VARIANT primitive type for cast to TIMESTAMP(%d) WITH TIME ZONE: %s".formatted(precision, variant.primitiveType()));
            };
            case SHORT_STRING -> asShortTimestampWithTimeZone(variant.getString(), precision);
            default -> throw new VariantCastException("Unsupported VARIANT type for cast to TIMESTAMP(%d) WITH TIME ZONE: %s".formatted(precision, variant.basicType()));
        };
    }

    private static long asShortTimestampWithTimeZone(Slice varchar, int precision)
    {
        return VarcharToTimestampWithTimeZoneCast.toShort(precision, varchar.toStringUtf8(), timezone -> timezone == null ? UTC : ZoneId.of(timezone));
    }

    public static LongTimestampWithTimeZone asLongTimestampWithTimeZone(Variant variant, int precision)
    {
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION || precision > TimestampWithTimeZoneType.MAX_PRECISION) {
            throw new IllegalArgumentException("precision must be between %d and %d".formatted(TimestampWithTimeZoneType.MAX_SHORT_PRECISION, TimestampWithTimeZoneType.MAX_PRECISION));
        }

        return switch (variant.basicType()) {
            case PRIMITIVE -> switch (variant.primitiveType()) {
                case NULL -> null;
                case DATE -> fromEpochMillisAndFraction(TimeUnit.DAYS.toMillis(variant.getDate()), 0, UTC_KEY);
                case TIMESTAMP_UTC_MICROS, TIMESTAMP_NTZ_MICROS -> {
                    long micros = variant.getTimestampMicros();
                    if (precision < 6) {
                        micros = round(micros, 6 - precision);
                    }
                    long millis = Math.floorDiv(micros, 1_000L);
                    int picosOfMillis = toIntExact(Math.floorMod(micros, 1_000L) * 1_000_000L);
                    yield fromEpochMillisAndFraction(millis, picosOfMillis, UTC_KEY);
                }
                case TIMESTAMP_UTC_NANOS, TIMESTAMP_NTZ_NANOS -> {
                    long nanos = variant.getTimestampNanos();
                    if (precision < 9) {
                        nanos = round(nanos, 9 - precision);
                    }
                    long millis = Math.floorDiv(nanos, 1_000_000L);
                    int picosOfMillis = toIntExact(Math.floorMod(nanos, 1_000_000L) * 1_000L);
                    yield fromEpochMillisAndFraction(millis, picosOfMillis, UTC_KEY);
                }
                case STRING -> asLongTimestampWithTimeZone(variant.getString(), precision);
                default -> throw new VariantCastException("Unsupported VARIANT primitive type for cast to TIMESTAMP(%d) WITH TIME ZONE: %s".formatted(precision, variant.primitiveType()));
            };
            case SHORT_STRING -> asLongTimestampWithTimeZone(variant.getString(), precision);
            default -> throw new VariantCastException("Unsupported VARIANT type for cast to TIMESTAMP(%d) WITH TIME ZONE: %s".formatted(precision, variant.basicType()));
        };
    }

    private static LongTimestampWithTimeZone asLongTimestampWithTimeZone(Slice varchar, int precision)
    {
        return VarcharToTimestampWithTimeZoneCast.toLong(precision, varchar.toStringUtf8(), timezone -> timezone == null ? UTC : ZoneId.of(timezone));
    }

    public static Slice asUuid(Variant variant)
    {
        return switch (variant.basicType()) {
            case PRIMITIVE -> switch (variant.primitiveType()) {
                case NULL -> null;
                case UUID -> variant.getUuidSlice();
                case STRING -> UuidOperators.castFromVarcharToUuid(variant.getString());
                default -> throw new VariantCastException("Unsupported VARIANT primitive type for cast to UUID: " + variant.primitiveType());
            };
            case SHORT_STRING -> UuidOperators.castFromVarcharToUuid(variant.getString());
            default -> throw new VariantCastException("Unsupported VARIANT type for cast to UUID: " + variant.basicType());
        };
    }

    public static Slice asVarbinary(Variant variant)
    {
        if (variant.isNull()) {
            return null;
        }
        if (variant.basicType() != PRIMITIVE || variant.primitiveType() != BINARY) {
            throw new VariantCastException("Unsupported VARIANT type for cast to VARBINARY: " + variant.basicType() + "/" + variant.primitiveType());
        }
        return variant.getBinary();
    }

    // given a VARIANT parser, write to the BlockBuilder
    public interface BlockBuilderAppender
    {
        void append(Variant variant, BlockBuilder blockBuilder);

        static BlockBuilderAppender createBlockBuilderAppender(Type type)
        {
            if (type instanceof BooleanType) {
                return new BooleanBlockBuilderAppender();
            }
            if (type instanceof TinyintType) {
                return new TinyintBlockBuilderAppender();
            }
            if (type instanceof SmallintType) {
                return new SmallintBlockBuilderAppender();
            }
            if (type instanceof IntegerType) {
                return new IntegerBlockBuilderAppender();
            }
            if (type instanceof BigintType) {
                return new BigintBlockBuilderAppender();
            }
            if (type instanceof RealType) {
                return new RealBlockBuilderAppender();
            }
            if (type instanceof DoubleType) {
                return new DoubleBlockBuilderAppender();
            }
            if (type instanceof DecimalType decimalType) {
                if (decimalType.isShort()) {
                    return new ShortDecimalBlockBuilderAppender(decimalType);
                }

                return new LongDecimalBlockBuilderAppender(decimalType);
            }
            if (type instanceof VarcharType) {
                return new VarcharBlockBuilderAppender(type);
            }
            if (type instanceof VarbinaryType) {
                return new VarbinaryBlockBuilderAppender(type);
            }
            if (type instanceof DateType) {
                return new DateBlockBuilderAppender();
            }
            if (type instanceof TimeType timeType) {
                return new TimeBlockBuilderAppender(timeType);
            }
            if (type instanceof TimestampType timestampType) {
                if (timestampType.isShort()) {
                    return new ShortTimestampBlockBuilderAppender(timestampType);
                }
                return new LongTimestampBlockBuilderAppender(timestampType);
            }
            if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
                if (timestampWithTimeZoneType.isShort()) {
                    return new ShortTimestampWithTimeZoneBlockBuilderAppender(timestampWithTimeZoneType);
                }
                return new LongTimestampWithTimeZoneBlockBuilderAppender(timestampWithTimeZoneType);
            }
            if (type instanceof UuidType) {
                return new UuidBlockBuilderAppender();
            }
            if (type instanceof VariantType) {
                return new VariantBlockBuilderAppender();
            }
            if (type instanceof JsonType) {
                return new JsonBlockBuilderAppender();
            }
            if (type instanceof ArrayType arrayType) {
                return new ArrayBlockBuilderAppender(createBlockBuilderAppender(arrayType.getElementType()));
            }
            if (type instanceof MapType mapType) {
                checkArgument(
                        mapType.getKeyType() instanceof VarcharType,
                        "Only maps with VARCHAR keys are supported for cast from VARIANT, but got: %s",
                        mapType);
                return new MapBlockBuilderAppender(createBlockBuilderAppender(mapType.getValueType()));
            }
            if (type instanceof RowType rowType) {
                List<Field> rowFields = rowType.getFields();
                BlockBuilderAppender[] fieldAppenders = new BlockBuilderAppender[rowFields.size()];
                for (int i = 0; i < fieldAppenders.length; i++) {
                    fieldAppenders[i] = createBlockBuilderAppender(rowFields.get(i).getType());
                }
                return new RowBlockBuilderAppender(fieldAppenders, getFieldNameToIndex(rowFields));
            }

            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Unsupported type: %s", type));
        }
    }

    private static class BooleanBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Boolean result = asBoolean(variant);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                BOOLEAN.writeBoolean(blockBuilder, result);
            }
        }
    }

    private static class TinyintBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Long result = asTinyint(variant);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                TINYINT.writeLong(blockBuilder, result);
            }
        }
    }

    private static class SmallintBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Long result = asSmallint(variant);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                SMALLINT.writeLong(blockBuilder, result);
            }
        }
    }

    private static class IntegerBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Long result = asInteger(variant);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                INTEGER.writeLong(blockBuilder, result);
            }
        }
    }

    private static class BigintBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Long result = asBigint(variant);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(blockBuilder, result);
            }
        }
    }

    private static class RealBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Long result = asReal(variant);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                REAL.writeLong(blockBuilder, result);
            }
        }
    }

    private static class DoubleBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Double result = asDouble(variant);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                DOUBLE.writeDouble(blockBuilder, result);
            }
        }
    }

    private record ShortDecimalBlockBuilderAppender(DecimalType type)
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Long result = asShortDecimal(variant, type.getPrecision(), type.getScale());

            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                type.writeLong(blockBuilder, result);
            }
        }
    }

    private record LongDecimalBlockBuilderAppender(DecimalType type)
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Int128 result = asLongDecimal(variant, type.getPrecision(), type.getScale());

            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                type.writeObject(blockBuilder, result);
            }
        }
    }

    private record VarcharBlockBuilderAppender(Type type)
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Slice result = asVarchar(variant);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                type.writeSlice(blockBuilder, result);
            }
        }
    }

    private record VarbinaryBlockBuilderAppender(Type type)
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Slice result = asVarbinary(variant);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                VARBINARY.writeSlice(blockBuilder, result);
            }
        }
    }

    private record DateBlockBuilderAppender()
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Long result = asDate(variant);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                DATE.writeLong(blockBuilder, result);
            }
        }
    }

    private record TimeBlockBuilderAppender(TimeType type)
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Long result = asTime(variant, type.getPrecision());
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                type.writeLong(blockBuilder, result);
            }
        }
    }

    private record ShortTimestampBlockBuilderAppender(TimestampType type)
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Long result = asShortTimestamp(variant, type.getPrecision());
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                type.writeLong(blockBuilder, result);
            }
        }
    }

    private record LongTimestampBlockBuilderAppender(TimestampType type)
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            LongTimestamp result = asLongTimestamp(variant, type.getPrecision());
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                type.writeObject(blockBuilder, result);
            }
        }
    }

    private record ShortTimestampWithTimeZoneBlockBuilderAppender(TimestampWithTimeZoneType type)
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Long result = asShortTimestampWithTimeZone(variant, type.getPrecision());
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                type.writeLong(blockBuilder, result);
            }
        }
    }

    private record LongTimestampWithTimeZoneBlockBuilderAppender(TimestampWithTimeZoneType type)
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            LongTimestampWithTimeZone result = asLongTimestampWithTimeZone(variant, type.getPrecision());
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                type.writeObject(blockBuilder, result);
            }
        }
    }

    private record UuidBlockBuilderAppender()
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Slice result = asUuid(variant);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                UUID.writeSlice(blockBuilder, result);
            }
        }
    }

    private record VariantBlockBuilderAppender()
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            VARIANT.writeObject(blockBuilder, variant);
        }
    }

    private record JsonBlockBuilderAppender()
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            Slice result = asJson(variant);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                JSON.writeSlice(blockBuilder, result);
            }
        }
    }

    private record ArrayBlockBuilderAppender(BlockBuilderAppender elementAppender)
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            if (variant.isNull()) {
                blockBuilder.appendNull();
                return;
            }

            if (variant.basicType() != Header.BasicType.ARRAY) {
                throw new VariantCastException("Expected a variant array, but got " + variant.basicType());
            }
            ((ArrayBlockBuilder) blockBuilder).buildEntry(elementBuilder ->
                    variant.arrayElements().forEach(element -> elementAppender.append(element, elementBuilder)));
        }
    }

    private record MapBlockBuilderAppender(BlockBuilderAppender valueAppender)
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            if (variant.isNull()) {
                blockBuilder.appendNull();
                return;
            }

            if (variant.basicType() != Header.BasicType.OBJECT) {
                throw new VariantCastException(format("Expected a variant object, but got %s", variant.basicType()));
            }

            MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) blockBuilder;
            Metadata metadata = variant.metadata();
            mapBlockBuilder.buildEntry((keyBuilder, valueBuilder) ->
                    variant.objectFields().forEach(fieldIdValue -> {
                        ((VariableWidthBlockBuilder) keyBuilder).writeEntry(metadata.get(fieldIdValue.fieldId()));
                        valueAppender.append(fieldIdValue.value(), valueBuilder);
                    }));
        }
    }

    private record RowBlockBuilderAppender(BlockBuilderAppender[] fieldAppenders, Optional<Map<String, Integer>> fieldNameToIndex)
            implements BlockBuilderAppender
    {
        @Override
        public void append(Variant variant, BlockBuilder blockBuilder)
        {
            if (variant.isNull()) {
                blockBuilder.appendNull();
                return;
            }

            if (variant.basicType() != Header.BasicType.OBJECT) {
                throw new VariantCastException("Expected an object, but got " + variant.basicType());
            }

            ((RowBlockBuilder) blockBuilder).buildEntry(fieldBuilders -> parseVariantToSingleRowBlock(variant, fieldBuilders, fieldAppenders, fieldNameToIndex));
        }
    }

    private static Optional<Map<String, Integer>> getFieldNameToIndex(List<Field> rowFields)
    {
        if (rowFields.getFirst().getName().isEmpty()) {
            return Optional.empty();
        }

        ImmutableMap.Builder<String, Integer> fieldNameToIndex = ImmutableMap.builderWithExpectedSize(rowFields.size());
        for (int i = 0; i < rowFields.size(); i++) {
            fieldNameToIndex.put(rowFields.get(i).getName().orElseThrow(), i);
        }
        return Optional.of(fieldNameToIndex.buildOrThrow());
    }

    private static void parseVariantToSingleRowBlock(
            Variant variant,
            List<BlockBuilder> fieldBuilders,
            BlockBuilderAppender[] fieldAppenders,
            Optional<Map<String, Integer>> fieldNameToIndex)
    {
        if (fieldNameToIndex.isEmpty()) {
            throw new VariantCastException("Cannot cast VARIANT object to anonymous row type. Row fields must have names.");
        }
        boolean[] fieldWritten = new boolean[fieldAppenders.length];

        Metadata metadata = variant.metadata();
        variant.objectFields().forEach(field -> {
            String fieldName = metadata.get(field.fieldId()).toStringUtf8().toLowerCase(Locale.ENGLISH);
            Integer fieldIndex = fieldNameToIndex.get().get(fieldName);
            if (fieldIndex != null) {
                if (fieldWritten[fieldIndex]) {
                    throw new VariantCastException("Duplicate field: " + fieldName);
                }
                fieldWritten[fieldIndex] = true;
                fieldAppenders[fieldIndex].append(field.value(), fieldBuilders.get(fieldIndex));
            }
        });

        for (int i = 0; i < fieldWritten.length; i++) {
            if (!fieldWritten[i]) {
                fieldBuilders.get(i).appendNull();
            }
        }
    }

    public static Slice asJson(Variant variant)
    {
        try {
            SliceOutput output = new DynamicSliceOutput(40);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.configure(ESCAPE_NON_ASCII.mappedFeature(), false);
                toJsonValue(jsonGenerator, variant);
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void toJsonValue(JsonGenerator jsonGenerator, Variant variant)
            throws IOException
    {
        switch (variant.basicType()) {
            case PRIMITIVE -> {
                switch (variant.primitiveType()) {
                    case NULL -> jsonGenerator.writeNull();
                    case BINARY -> {
                        Slice binary = variant.getBinary();
                        jsonGenerator.writeBinary(binary.byteArray(), binary.byteArrayOffset(), binary.length());
                    }
                    case STRING -> jsonGenerator.writeString(variant.getString().toStringUtf8());
                    case BOOLEAN_TRUE -> jsonGenerator.writeBoolean(true);
                    case BOOLEAN_FALSE -> jsonGenerator.writeBoolean(false);
                    case INT8 -> jsonGenerator.writeNumber(variant.getByte());
                    case INT16 -> jsonGenerator.writeNumber(variant.getShort());
                    case INT32 -> jsonGenerator.writeNumber(variant.getInt());
                    case INT64 -> jsonGenerator.writeNumber(variant.getLong());
                    case DECIMAL4, DECIMAL8, DECIMAL16 -> jsonGenerator.writeNumber(variant.getDecimal());
                    case FLOAT -> jsonGenerator.writeNumber(variant.getFloat());
                    case DOUBLE -> jsonGenerator.writeNumber(variant.getDouble());
                    case DATE -> jsonGenerator.writeString(DateOperators.castToVarchar(UNBOUNDED_LENGTH, variant.getDate()).toStringUtf8());
                    case TIMESTAMP_UTC_MICROS -> {
                        long micros = variant.getTimestampMicros();
                        long epochMillis = Math.floorDiv(micros, 1_000L);
                        int picosOfMilli = toIntExact(Math.floorMod(micros, 1_000L) * 1_000_000L);
                        jsonGenerator.writeString(DateTimes.formatTimestampWithTimeZone(6, epochMillis, picosOfMilli, UTC_KEY.getZoneId()));
                    }
                    case TIMESTAMP_NTZ_MICROS -> jsonGenerator.writeString(DateTimes.formatTimestamp(6, variant.getTimestampMicros(), 0, UTC));
                    // time types are not implemented in variant yet
                    case TIME_NTZ_MICROS -> throw new VariantCastException("Cannot cast VARIANT type TIME_NTZ_MICROS to JSON");
                    case TIMESTAMP_UTC_NANOS -> {
                        long nanos = variant.getTimestampNanos();
                        long epochMillis = Math.floorDiv(nanos, 1_000_000L);
                        int picosOfMilli = toIntExact(Math.floorMod(nanos, 1_000_000L) * 1_000L);
                        jsonGenerator.writeString(DateTimes.formatTimestampWithTimeZone(9, epochMillis, picosOfMilli, UTC_KEY.getZoneId()));
                    }
                    case TIMESTAMP_NTZ_NANOS -> {
                        long nanos = variant.getTimestampNanos();
                        long epochMicros = Math.floorDiv(nanos, 1_000L);
                        int picosOfMicros = toIntExact(Math.floorMod(nanos, 1_000L) * 1_000L);
                        jsonGenerator.writeString(DateTimes.formatTimestamp(9, epochMicros, picosOfMicros, UTC));
                    }
                    case UUID -> jsonGenerator.writeString(variant.getUuid().toString());
                }
            }
            case SHORT_STRING -> jsonGenerator.writeString(variant.getString().toStringUtf8());
            case ARRAY -> {
                jsonGenerator.writeStartArray();
                variant.arrayElements().forEach(element -> {
                    try {
                        toJsonValue(jsonGenerator, element);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                jsonGenerator.writeEndArray();
            }
            case OBJECT -> {
                Metadata metadata = variant.metadata();
                jsonGenerator.writeStartObject();
                variant.objectFields().forEach(fieldIdValue -> {
                    try {
                        String fieldName = metadata.get(fieldIdValue.fieldId()).toStringUtf8();
                        jsonGenerator.writeFieldName(fieldName);
                        toJsonValue(jsonGenerator, fieldIdValue.value());
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                jsonGenerator.writeEndObject();
            }
        }
    }
}
