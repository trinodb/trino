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
package io.trino.type;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TrinoNumber;

import java.math.BigDecimal;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.type.Reals.toReal;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public final class CharOperators
{
    private CharOperators() {}

    // fallible
    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean castToBoolean(@SqlType("char(x)") Slice value)
    {
        return VarcharOperators.castToBoolean(value);
    }

    // fallible
    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castToDouble(@SqlType("char(x)") Slice slice)
    {
        try {
            return Double.parseDouble(slice.toStringUtf8().trim());
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to DOUBLE", slice.toStringUtf8()));
        }
    }

    // fallible
    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.REAL)
    public static long castToReal(@SqlType("char(x)") Slice slice)
    {
        try {
            return toReal(Float.parseFloat(slice.toStringUtf8().trim()));
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to REAL", slice.toStringUtf8()));
        }
    }

    // fallible
    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToBigint(@SqlType("char(x)") Slice slice)
    {
        try {
            return Long.parseLong(slice.toStringUtf8().trim());
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to BIGINT", slice.toStringUtf8()));
        }
    }

    // fallible
    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType("char(x)") Slice slice)
    {
        try {
            return Integer.parseInt(slice.toStringUtf8().trim());
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to INT", slice.toStringUtf8()));
        }
    }

    // fallible
    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType("char(x)") Slice slice)
    {
        try {
            return Short.parseShort(slice.toStringUtf8().trim());
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to SMALLINT", slice.toStringUtf8()));
        }
    }

    // fallible
    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType("char(x)") Slice slice)
    {
        try {
            return Byte.parseByte(slice.toStringUtf8().trim());
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to TINYINT", slice.toStringUtf8()));
        }
    }

    // fallible
    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber castToNumber(@SqlType("char(x)") Slice slice)
    {
        String value = slice.toStringUtf8();
        try {
            return TrinoNumber.from(switch (value.trim()) {
                case "NaN" -> new TrinoNumber.NotANumber();
                case "+Infinity", "Infinity" -> new TrinoNumber.Infinity(false);
                case "-Infinity" -> new TrinoNumber.Infinity(true);
                case String trimmed -> new TrinoNumber.BigDecimalValue(new BigDecimal(trimmed));
            });
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to NUMBER", value), e);
        }
    }

    @LiteralParameters("x")
    @ScalarOperator(value = CAST, neverFails = true)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice castToBinary(@LiteralParameter("x") long x, @SqlType("char(x)") Slice slice)
    {
        return padSpaces(slice, toIntExact(x));
    }
}
