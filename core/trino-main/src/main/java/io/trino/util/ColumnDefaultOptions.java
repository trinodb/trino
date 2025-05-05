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
package io.trino.util;

import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.analyzer.LiteralInterpreter;
import io.trino.sql.tree.Literal;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_PICOS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.lang.Math.floorMod;
import static java.util.Locale.ENGLISH;

public final class ColumnDefaultOptions
{
    private ColumnDefaultOptions() {}

    public static Constant evaluateLiteral(LiteralInterpreter literalInterpreter, Literal literal, Type type)
    {
        try {
            Object value = literalInterpreter.evaluate(literal, type);
            checkDefaultValue(value, type);
            return new Constant(value, type);
        }
        catch (RuntimeException e) {
            throw semanticException(INVALID_LITERAL, literal, e, "'%s' is not a valid %s literal", literal, type.getDisplayName().toUpperCase(ENGLISH));
        }
    }

    private static void checkDefaultValue(Object value, Type type)
    {
        if (value == null) {
            return;
        }
        checkArgument(Primitives.wrap(type.getJavaType()).isInstance(value), "Object '%s' does not match type %s", value, type.getJavaType());

        if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                checkArgument(!Decimals.overflows(((long) value), decimalType.getPrecision()), "Value too large");
            }
            else {
                checkArgument(!Decimals.overflows(((Int128) value), decimalType.getPrecision()), "Value too large");
            }
        }
        if (type instanceof CharType charType) {
            int targetLength = charType.getLength();
            Slice slice = (Slice) value;
            long actualLength = countCodePoints(slice);
            checkArgument(targetLength >= actualLength, "Cannot truncate characters when casting value '%s' to %s".formatted(slice.toStringUtf8(), type));
        }
        if (type instanceof VarcharType varcharType) {
            if (varcharType.isUnbounded()) {
                return;
            }
            int targetLength = varcharType.getBoundedLength();
            Slice slice = (Slice) value;
            long actualLength = countCodePoints(slice);
            checkArgument(targetLength >= actualLength, "Cannot truncate characters when casting value '%s' to %s".formatted(slice.toStringUtf8(), type));
        }
        if (type instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                Long epochMicros = (Long) value;
                int microOfSecond = floorMod(epochMicros, MICROSECONDS_PER_SECOND);
                checkArgument(microOfSecond % Math.pow(10, TIMESTAMP_MICROS.getPrecision() - timestampType.getPrecision()) == 0, "Value too large");
            }
            else {
                LongTimestamp longTimestamp = (LongTimestamp) value;
                int picosOfMicro = longTimestamp.getPicosOfMicro();
                checkArgument(picosOfMicro % Math.pow(10, TIMESTAMP_PICOS.getPrecision() - timestampType.getPrecision()) == 0, "Value too large");
            }
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            int precision = timestampWithTimeZoneType.getPrecision();
            if (timestampWithTimeZoneType.isShort()) {
                long epochMillis = unpackMillisUtc((long) value);
                checkArgument(epochMillis % Math.pow(10, TIMESTAMP_TZ_MILLIS.getPrecision() - precision) == 0, "Value too large");
            }
            else {
                LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) value;
                int picosOfMilli = timestamp.getPicosOfMilli();
                checkArgument(picosOfMilli % Math.pow(10, TIMESTAMP_TZ_PICOS.getPrecision() - precision) == 0, "Value too large");
            }
        }
    }
}
