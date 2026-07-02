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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.function.Constraint;
import io.trino.spi.function.Description;
import io.trino.spi.function.InstanceMethod;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.StaticMethod;
import io.trino.spi.type.Int128;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TrinoNumber;

/**
 * Exposes the core math functions as SQL:2023 methods on numeric receivers, so
 * {@code value.abs()}, {@code value.round(2)}, {@code (2e0).sqrt()}, and
 * {@code double::pi()} resolve to the same implementations as the plain
 * functions in {@link MathFunctions}. Functions and methods resolve in separate
 * namespaces, so every existing call keeps working.
 *
 * <p>An instance method is matched on the exact base type of its receiver, so
 * each entry mirrors the receiver type of the corresponding {@code MathFunctions}
 * overload one-to-one. A double-only function such as {@code sqrt} is therefore
 * a method on {@code double} receivers only; use the plain function (or cast the
 * receiver) for other numeric types.
 */
public final class MathMethods
{
    private MathMethods() {}

    @Description("Absolute value")
    @ScalarFunction("abs")
    @InstanceMethod
    @SqlType(StandardTypes.TINYINT)
    public static long absTinyint(@SqlType(StandardTypes.TINYINT) long num)
    {
        return MathFunctions.absTinyint(num);
    }

    @Description("Absolute value")
    @ScalarFunction("abs")
    @InstanceMethod
    @SqlType(StandardTypes.SMALLINT)
    public static long absSmallint(@SqlType(StandardTypes.SMALLINT) long num)
    {
        return MathFunctions.absSmallint(num);
    }

    @Description("Absolute value")
    @ScalarFunction("abs")
    @InstanceMethod
    @SqlType(StandardTypes.INTEGER)
    public static long absInteger(@SqlType(StandardTypes.INTEGER) long num)
    {
        return MathFunctions.absInteger(num);
    }

    @Description("Absolute value")
    @ScalarFunction("abs")
    @InstanceMethod
    @SqlType(StandardTypes.BIGINT)
    public static long abs(@SqlType(StandardTypes.BIGINT) long num)
    {
        return MathFunctions.abs(num);
    }

    @Description("Absolute value")
    @ScalarFunction(value = "abs", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double abs(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.abs(num);
    }

    @Description("Absolute value")
    @ScalarFunction(value = "abs", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber abs(@SqlType(StandardTypes.NUMBER) TrinoNumber num)
    {
        return MathFunctions.abs(num);
    }

    @Description("Absolute value")
    @ScalarFunction(value = "abs", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.REAL)
    public static long absReal(@SqlType(StandardTypes.REAL) long num)
    {
        return MathFunctions.absReal(num);
    }

    @ScalarFunction(value = "abs", neverFails = true)
    @InstanceMethod
    @Description("Absolute value")
    public static final class Abs
    {
        private Abs() {}

        @LiteralParameters({"p", "s"})
        @SqlType("decimal(p, s)")
        public static long absShort(@SqlType("decimal(p, s)") long arg)
        {
            return MathFunctions.Abs.absShort(arg);
        }

        @LiteralParameters({"p", "s"})
        @SqlType("decimal(p, s)")
        public static Int128 absLong(@SqlType("decimal(p, s)") Int128 value)
        {
            return MathFunctions.Abs.absLong(value);
        }
    }

    @Description("Round up to nearest integer")
    @ScalarFunction(value = "ceiling", alias = "ceil", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.TINYINT)
    public static long ceilingTinyint(@SqlType(StandardTypes.TINYINT) long num)
    {
        return MathFunctions.ceilingTinyint(num);
    }

    @Description("Round up to nearest integer")
    @ScalarFunction(value = "ceiling", alias = "ceil", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.SMALLINT)
    public static long ceilingSmallint(@SqlType(StandardTypes.SMALLINT) long num)
    {
        return MathFunctions.ceilingSmallint(num);
    }

    @Description("Round up to nearest integer")
    @ScalarFunction(value = "ceiling", alias = "ceil", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.INTEGER)
    public static long ceilingInteger(@SqlType(StandardTypes.INTEGER) long num)
    {
        return MathFunctions.ceilingInteger(num);
    }

    @Description("Round up to nearest integer")
    @ScalarFunction(value = "ceiling", alias = "ceil", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.BIGINT)
    public static long ceiling(@SqlType(StandardTypes.BIGINT) long num)
    {
        return MathFunctions.ceiling(num);
    }

    @Description("Round up to nearest integer")
    @ScalarFunction(value = "ceiling", alias = "ceil", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double ceiling(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.ceiling(num);
    }

    @Description("Round up to nearest integer")
    @ScalarFunction(value = "ceiling", alias = "ceil", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.REAL)
    public static long ceilingReal(@SqlType(StandardTypes.REAL) long num)
    {
        return MathFunctions.ceilingReal(num);
    }

    @Description("Round up to nearest integer")
    @ScalarFunction(value = "ceiling", alias = "ceil", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber ceiling(@SqlType(StandardTypes.NUMBER) TrinoNumber num)
    {
        return MathFunctions.ceiling(num);
    }

    @ScalarFunction(value = "ceiling", alias = "ceil", neverFails = true)
    @InstanceMethod
    @Description("Round up to nearest integer")
    public static final class Ceiling
    {
        private Ceiling() {}

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "p - s + min(s, 1)")
        public static long ceilingShort(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") long num)
        {
            return MathFunctions.Ceiling.ceilingShort(numScale, num);
        }

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "p - s + min(s, 1)")
        public static Int128 ceilingLong(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Int128 num)
        {
            return MathFunctions.Ceiling.ceilingLong(numScale, num);
        }

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "p - s + min(s, 1)")
        public static long ceilingLongShort(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Int128 num)
        {
            return MathFunctions.Ceiling.ceilingLongShort(numScale, num);
        }
    }

    @Description("Round down to nearest integer")
    @ScalarFunction(value = "floor", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.TINYINT)
    public static long floorTinyint(@SqlType(StandardTypes.TINYINT) long num)
    {
        return MathFunctions.floorTinyint(num);
    }

    @Description("Round down to nearest integer")
    @ScalarFunction(value = "floor", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.SMALLINT)
    public static long floorSmallint(@SqlType(StandardTypes.SMALLINT) long num)
    {
        return MathFunctions.floorSmallint(num);
    }

    @Description("Round down to nearest integer")
    @ScalarFunction(value = "floor", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.INTEGER)
    public static long floorInteger(@SqlType(StandardTypes.INTEGER) long num)
    {
        return MathFunctions.floorInteger(num);
    }

    @Description("Round down to nearest integer")
    @ScalarFunction(value = "floor", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.BIGINT)
    public static long floor(@SqlType(StandardTypes.BIGINT) long num)
    {
        return MathFunctions.floor(num);
    }

    @Description("Round down to nearest integer")
    @ScalarFunction(value = "floor", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double floor(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.floor(num);
    }

    @Description("Round down to nearest integer")
    @ScalarFunction(value = "floor", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber floor(@SqlType(StandardTypes.NUMBER) TrinoNumber num)
    {
        return MathFunctions.floor(num);
    }

    @Description("Round down to nearest integer")
    @ScalarFunction(value = "floor", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.REAL)
    public static long floorReal(@SqlType(StandardTypes.REAL) long num)
    {
        return MathFunctions.floorReal(num);
    }

    @ScalarFunction(value = "floor", neverFails = true)
    @InstanceMethod
    @Description("Round down to nearest integer")
    public static final class Floor
    {
        private Floor() {}

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "p - s + min(s, 1)")
        public static long floorShort(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") long num)
        {
            return MathFunctions.Floor.floorShort(numScale, num);
        }

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "p - s + min(s, 1)")
        public static Int128 floorLong(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Int128 num)
        {
            return MathFunctions.Floor.floorLong(numScale, num);
        }

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "p - s + min(s, 1)")
        public static long floorLongShort(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Int128 num)
        {
            return MathFunctions.Floor.floorLongShort(numScale, num);
        }
    }

    @Description("Round to nearest integer")
    @ScalarFunction(value = "round", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.TINYINT)
    public static long roundTinyint(@SqlType(StandardTypes.TINYINT) long num)
    {
        return MathFunctions.roundTinyint(num);
    }

    @Description("Round to nearest integer")
    @ScalarFunction(value = "round", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.SMALLINT)
    public static long roundSmallint(@SqlType(StandardTypes.SMALLINT) long num)
    {
        return MathFunctions.roundSmallint(num);
    }

    @Description("Round to nearest integer")
    @ScalarFunction(value = "round", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.INTEGER)
    public static long roundInteger(@SqlType(StandardTypes.INTEGER) long num)
    {
        return MathFunctions.roundInteger(num);
    }

    @Description("Round to nearest integer")
    @ScalarFunction(value = "round", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.BIGINT)
    public static long round(@SqlType(StandardTypes.BIGINT) long num)
    {
        return MathFunctions.round(num);
    }

    @Description("Round to nearest integer")
    @ScalarFunction("round")
    @InstanceMethod
    @SqlType(StandardTypes.TINYINT)
    public static long roundTinyint(@SqlType(StandardTypes.TINYINT) long num, @SqlType(StandardTypes.INTEGER) long decimals)
    {
        return MathFunctions.roundTinyint(num, decimals);
    }

    @Description("Round to nearest integer")
    @ScalarFunction("round")
    @InstanceMethod
    @SqlType(StandardTypes.SMALLINT)
    public static long roundSmallint(@SqlType(StandardTypes.SMALLINT) long num, @SqlType(StandardTypes.INTEGER) long decimals)
    {
        return MathFunctions.roundSmallint(num, decimals);
    }

    @Description("Round to nearest integer")
    @ScalarFunction("round")
    @InstanceMethod
    @SqlType(StandardTypes.INTEGER)
    public static long roundInteger(@SqlType(StandardTypes.INTEGER) long num, @SqlType(StandardTypes.INTEGER) long decimals)
    {
        return MathFunctions.roundInteger(num, decimals);
    }

    @Description("Round to nearest integer")
    @ScalarFunction("round")
    @InstanceMethod
    @SqlType(StandardTypes.BIGINT)
    public static long round(@SqlType(StandardTypes.BIGINT) long num, @SqlType(StandardTypes.INTEGER) long decimals)
    {
        return MathFunctions.round(num, decimals);
    }

    @Description("Round to nearest integer")
    @ScalarFunction(value = "round", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double round(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.round(num);
    }

    @Description("Round to given number of decimal places")
    @ScalarFunction(value = "round", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.REAL)
    public static long roundReal(@SqlType(StandardTypes.REAL) long num)
    {
        return MathFunctions.roundReal(num);
    }

    @Description("Round to given number of decimal places")
    @ScalarFunction(value = "round", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double round(@SqlType(StandardTypes.DOUBLE) double num, @SqlType(StandardTypes.INTEGER) long decimals)
    {
        return MathFunctions.round(num, decimals);
    }

    @Description("Round to given number of decimal places")
    @ScalarFunction(value = "round", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.REAL)
    public static long roundReal(@SqlType(StandardTypes.REAL) long num, @SqlType(StandardTypes.INTEGER) long decimals)
    {
        return MathFunctions.roundReal(num, decimals);
    }

    @Description("Round to nearest integer")
    @ScalarFunction(value = "round", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber round(@SqlType(StandardTypes.NUMBER) TrinoNumber num)
    {
        return MathFunctions.round(num);
    }

    @Description("Round to given number of decimal places")
    @ScalarFunction(value = "round", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber round(@SqlType(StandardTypes.NUMBER) TrinoNumber num, @SqlType(StandardTypes.INTEGER) long decimals)
    {
        return MathFunctions.round(num, decimals);
    }

    @ScalarFunction(value = "round", neverFails = true)
    @InstanceMethod
    @Description("Round to nearest integer")
    public static final class Round
    {
        private Round() {}

        @LiteralParameters({"p", "s", "rp", "rs"})
        @SqlType("decimal(rp, rs)")
        @Constraint(variable = "rp", expression = "min(38, p - s + min(1, s))")
        @Constraint(variable = "rs", expression = "0")
        public static long roundShort(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") long num)
        {
            return MathFunctions.Round.roundShort(numScale, num);
        }

        @LiteralParameters({"p", "s", "rp", "rs"})
        @SqlType("decimal(rp, rs)")
        @Constraint(variable = "rp", expression = "min(38, p - s + min(1, s))")
        @Constraint(variable = "rs", expression = "0")
        public static Int128 roundLongLong(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Int128 num)
        {
            return MathFunctions.Round.roundLongLong(numScale, num);
        }

        @LiteralParameters({"p", "s", "rp", "rs"})
        @SqlType("decimal(rp, rs)")
        @Constraint(variable = "rp", expression = "min(38, p - s + min(1, s))")
        @Constraint(variable = "rs", expression = "0")
        public static long roundLongShort(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Int128 num)
        {
            return MathFunctions.Round.roundLongShort(numScale, num);
        }
    }

    @ScalarFunction("round")
    @InstanceMethod
    @Description("Round to given number of decimal places")
    public static final class RoundN
    {
        private RoundN() {}

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp, s)")
        @Constraint(variable = "rp", expression = "min(38, p + 1)")
        public static long roundNShort(
                @LiteralParameter("p") long numPrecision,
                @LiteralParameter("s") long numScale,
                @SqlType("decimal(p, s)") long num,
                @SqlType(StandardTypes.INTEGER) long decimals)
        {
            return MathFunctions.RoundN.roundNShort(numPrecision, numScale, num, decimals);
        }

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp, s)")
        @Constraint(variable = "rp", expression = "min(38, p + 1)")
        public static Int128 roundNLong(
                @LiteralParameter("s") long numScale,
                @LiteralParameter("rp") long resultPrecision,
                @SqlType("decimal(p, s)") Int128 num,
                @SqlType(StandardTypes.INTEGER) long decimals)
        {
            return MathFunctions.RoundN.roundNLong(numScale, resultPrecision, num, decimals);
        }

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp, s)")
        @Constraint(variable = "rp", expression = "min(38, p + 1)")
        public static Int128 roundNShortLong(
                @LiteralParameter("s") long numScale,
                @LiteralParameter("rp") long resultPrecision,
                @SqlType("decimal(p, s)") long num,
                @SqlType(StandardTypes.INTEGER) long decimals)
        {
            return MathFunctions.RoundN.roundNShortLong(numScale, resultPrecision, num, decimals);
        }
    }

    @Description("Round to integer by dropping digits after decimal point")
    @ScalarFunction(value = "truncate", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double truncate(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.truncate(num);
    }

    @Description("Round to integer by dropping digits after decimal point")
    @ScalarFunction(value = "truncate", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.REAL)
    public static long truncate(@SqlType(StandardTypes.REAL) long num)
    {
        return MathFunctions.truncate(num);
    }

    @Description("Round to integer by dropping digits after decimal point")
    @ScalarFunction(value = "truncate", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber truncate(@SqlType(StandardTypes.NUMBER) TrinoNumber num)
    {
        return MathFunctions.truncate(num);
    }

    @ScalarFunction(value = "truncate", neverFails = true)
    @InstanceMethod
    @Description("Round to integer by dropping digits after decimal point")
    public static final class Truncate
    {
        private Truncate() {}

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "max(1, p - s)")
        public static long truncateShort(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") long num)
        {
            return MathFunctions.Truncate.truncateShort(numScale, num);
        }

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "max(1, p - s)")
        public static Int128 truncateLong(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Int128 num)
        {
            return MathFunctions.Truncate.truncateLong(numScale, num);
        }

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "max(1, p - s)")
        public static long truncateLongShort(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Int128 num)
        {
            return MathFunctions.Truncate.truncateLongShort(numScale, num);
        }
    }

    @ScalarFunction(value = "truncate", neverFails = true)
    @InstanceMethod
    @Description("Round to integer by dropping given number of digits after decimal point")
    public static final class TruncateN
    {
        private TruncateN() {}

        @LiteralParameters({"p", "s"})
        @SqlType("decimal(p, s)")
        public static long truncateShort(
                @LiteralParameter("p") long numPrecision,
                @LiteralParameter("s") long numScale,
                @SqlType("decimal(p, s)") long num,
                @SqlType(StandardTypes.INTEGER) long roundScale)
        {
            return MathFunctions.TruncateN.truncateShort(numPrecision, numScale, num, roundScale);
        }

        @LiteralParameters({"p", "s"})
        @SqlType("decimal(p, s)")
        public static Int128 truncateLong(
                @LiteralParameter("p") long numPrecision,
                @LiteralParameter("s") long numScale,
                @SqlType("decimal(p, s)") Int128 num,
                @SqlType(StandardTypes.INTEGER) long roundScale)
        {
            return MathFunctions.TruncateN.truncateLong(numPrecision, numScale, num, roundScale);
        }
    }

    @Description("Signum")
    @ScalarFunction(value = "sign", neverFails = true)
    @InstanceMethod
    public static final class Sign
    {
        private Sign() {}

        @LiteralParameters({"p", "s"})
        @SqlType("decimal(1,0)")
        public static long signDecimalShort(@SqlType("decimal(p, s)") long num)
        {
            return MathFunctions.Sign.signDecimalShort(num);
        }

        @LiteralParameters({"p", "s"})
        @SqlType("decimal(1,0)")
        public static long signDecimalLong(@SqlType("decimal(p, s)") Int128 num)
        {
            return MathFunctions.Sign.signDecimalLong(num);
        }
    }

    @Description("Signum")
    @ScalarFunction(value = "sign", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.BIGINT)
    public static long sign(@SqlType(StandardTypes.BIGINT) long num)
    {
        return MathFunctions.sign(num);
    }

    @Description("Signum")
    @ScalarFunction(value = "sign", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.INTEGER)
    public static long signInteger(@SqlType(StandardTypes.INTEGER) long num)
    {
        return MathFunctions.signInteger(num);
    }

    @Description("Signum")
    @ScalarFunction(value = "sign", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.SMALLINT)
    public static long signSmallint(@SqlType(StandardTypes.SMALLINT) long num)
    {
        return MathFunctions.signSmallint(num);
    }

    @Description("Signum")
    @ScalarFunction(value = "sign", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.TINYINT)
    public static long signTinyint(@SqlType(StandardTypes.TINYINT) long num)
    {
        return MathFunctions.signTinyint(num);
    }

    @Description("Signum")
    @ScalarFunction(value = "sign", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double sign(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.sign(num);
    }

    @Description("Signum")
    @ScalarFunction(value = "sign", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.REAL)
    public static long signReal(@SqlType(StandardTypes.REAL) long num)
    {
        return MathFunctions.signReal(num);
    }

    @Description("Signum")
    @ScalarFunction(value = "sign", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber sign(@SqlType(StandardTypes.NUMBER) TrinoNumber num)
    {
        return MathFunctions.sign(num);
    }

    @Description("Square root")
    @ScalarFunction(value = "sqrt", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double sqrt(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.sqrt(num);
    }

    @Description("Cube root")
    @ScalarFunction(value = "cbrt", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double cbrt(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.cbrt(num);
    }

    @Description("Euler's number raised to the given power")
    @ScalarFunction(value = "exp", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double exp(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.exp(num);
    }

    @Description("Natural logarithm")
    @ScalarFunction(value = "ln", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double ln(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.ln(num);
    }

    @Description("Logarithm to given base")
    @ScalarFunction(value = "log", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double log(@SqlType(StandardTypes.DOUBLE) double base, @SqlType(StandardTypes.DOUBLE) double number)
    {
        return MathFunctions.log(base, number);
    }

    @Description("Logarithm to base 2")
    @ScalarFunction(value = "log2", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double log2(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.log2(num);
    }

    @Description("Logarithm to base 10")
    @ScalarFunction(value = "log10", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double log10(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.log10(num);
    }

    @Description("Value raised to the power of exponent")
    @ScalarFunction(value = "power", alias = "pow", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double power(@SqlType(StandardTypes.DOUBLE) double num, @SqlType(StandardTypes.DOUBLE) double exponent)
    {
        return MathFunctions.power(num, exponent);
    }

    @Description("Sine")
    @ScalarFunction(value = "sin", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double sin(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.sin(num);
    }

    @Description("Hyperbolic sine")
    @ScalarFunction(value = "sinh", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double sinh(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.sinh(num);
    }

    @Description("Cosine")
    @ScalarFunction(value = "cos", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double cos(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.cos(num);
    }

    @Description("Hyperbolic cosine")
    @ScalarFunction(value = "cosh", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double cosh(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.cosh(num);
    }

    @Description("Tangent")
    @ScalarFunction(value = "tan", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double tan(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.tan(num);
    }

    @Description("Hyperbolic tangent")
    @ScalarFunction(value = "tanh", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double tanh(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.tanh(num);
    }

    @Description("Arc sine")
    @ScalarFunction(value = "asin", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double asin(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.asin(num);
    }

    @Description("Arc cosine")
    @ScalarFunction(value = "acos", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double acos(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.acos(num);
    }

    @Description("Arc tangent")
    @ScalarFunction(value = "atan", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double atan(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.atan(num);
    }

    @Description("Arc tangent of given fraction")
    @ScalarFunction(value = "atan2", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double atan2(@SqlType(StandardTypes.DOUBLE) double num1, @SqlType(StandardTypes.DOUBLE) double num2)
    {
        return MathFunctions.atan2(num1, num2);
    }

    @Description("Converts an angle in radians to degrees")
    @ScalarFunction(value = "degrees", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double degrees(@SqlType(StandardTypes.DOUBLE) double radians)
    {
        return MathFunctions.degrees(radians);
    }

    @Description("Converts an angle in degrees to radians")
    @ScalarFunction(value = "radians", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double radians(@SqlType(StandardTypes.DOUBLE) double degrees)
    {
        return MathFunctions.radians(degrees);
    }

    @Description("Remainder of given quotient")
    @ScalarFunction(value = "mod", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.TINYINT)
    public static long modTinyint(@SqlType(StandardTypes.TINYINT) long num1, @SqlType(StandardTypes.TINYINT) long num2)
    {
        return MathFunctions.modTinyint(num1, num2);
    }

    @Description("Remainder of given quotient")
    @ScalarFunction(value = "mod", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.SMALLINT)
    public static long modSmallint(@SqlType(StandardTypes.SMALLINT) long num1, @SqlType(StandardTypes.SMALLINT) long num2)
    {
        return MathFunctions.modSmallint(num1, num2);
    }

    @Description("Remainder of given quotient")
    @ScalarFunction(value = "mod", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.INTEGER)
    public static long modInteger(@SqlType(StandardTypes.INTEGER) long num1, @SqlType(StandardTypes.INTEGER) long num2)
    {
        return MathFunctions.modInteger(num1, num2);
    }

    @Description("Remainder of given quotient")
    @ScalarFunction(value = "mod", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.BIGINT)
    public static long mod(@SqlType(StandardTypes.BIGINT) long num1, @SqlType(StandardTypes.BIGINT) long num2)
    {
        return MathFunctions.mod(num1, num2);
    }

    @Description("Remainder of given quotient")
    @ScalarFunction(value = "mod", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.DOUBLE)
    public static double mod(@SqlType(StandardTypes.DOUBLE) double num1, @SqlType(StandardTypes.DOUBLE) double num2)
    {
        return MathFunctions.mod(num1, num2);
    }

    @Description("Remainder of given quotient")
    @ScalarFunction(value = "mod", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.REAL)
    public static long modReal(@SqlType(StandardTypes.REAL) long num1, @SqlType(StandardTypes.REAL) long num2)
    {
        return MathFunctions.modReal(num1, num2);
    }

    @Description("Remainder of given quotient")
    @ScalarFunction(value = "mod", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.NUMBER)
    public static TrinoNumber mod(@SqlType(StandardTypes.NUMBER) TrinoNumber num1, @SqlType(StandardTypes.NUMBER) TrinoNumber num2)
    {
        return MathFunctions.mod(num1, num2);
    }

    @Description("Test if value is not-a-number")
    @ScalarFunction(value = "is_nan", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNaN(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.isNaN(num);
    }

    @Description("Test if value is not-a-number")
    @ScalarFunction(value = "is_nan", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNaNReal(@SqlType(StandardTypes.REAL) long value)
    {
        return MathFunctions.isNaNReal(value);
    }

    @Description("Test if value is not-a-number")
    @ScalarFunction(value = "is_nan", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNaN(@SqlType(StandardTypes.NUMBER) TrinoNumber num)
    {
        return MathFunctions.isNaN(num);
    }

    @Description("Test if value is finite")
    @ScalarFunction(value = "is_finite", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isFinite(@SqlType(StandardTypes.REAL) long num)
    {
        return MathFunctions.isFinite(num);
    }

    @Description("Test if value is finite")
    @ScalarFunction(value = "is_finite", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isFinite(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.isFinite(num);
    }

    @Description("Test if value is finite")
    @ScalarFunction(value = "is_finite", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isFinite(@SqlType(StandardTypes.NUMBER) TrinoNumber num)
    {
        return MathFunctions.isFinite(num);
    }

    @Description("Test if value is infinite")
    @ScalarFunction(value = "is_infinite", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isInfinite(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return MathFunctions.isInfinite(num);
    }

    @Description("Test if value is infinite")
    @ScalarFunction(value = "is_infinite", neverFails = true)
    @InstanceMethod
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isInfinite(@SqlType(StandardTypes.NUMBER) TrinoNumber num)
    {
        return MathFunctions.isInfinite(num);
    }

    @Description("The bucket number of a value given a lower and upper bound and the number of buckets")
    @ScalarFunction("width_bucket")
    @InstanceMethod
    @SqlType(StandardTypes.BIGINT)
    public static long widthBucket(@SqlType(StandardTypes.DOUBLE) double operand, @SqlType(StandardTypes.DOUBLE) double bound1, @SqlType(StandardTypes.DOUBLE) double bound2, @SqlType(StandardTypes.BIGINT) long bucketCount)
    {
        return MathFunctions.widthBucket(operand, bound1, bound2, bucketCount);
    }

    @Description("The bucket number of a value given an array of bins")
    @ScalarFunction("width_bucket")
    @InstanceMethod
    @SqlType(StandardTypes.BIGINT)
    public static long widthBucket(@SqlType(StandardTypes.DOUBLE) double operand, @SqlType("array(double)") Block bins)
    {
        return MathFunctions.widthBucket(operand, bins);
    }

    @Description("Convert a number to a string in the given base")
    @ScalarFunction("to_base")
    @InstanceMethod
    @SqlType("varchar(64)")
    public static Slice toBase(@SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long radix)
    {
        return MathFunctions.toBase(value, radix);
    }

    @Description("The constant Pi")
    @ScalarFunction(value = "pi", neverFails = true)
    @StaticMethod(StandardTypes.DOUBLE)
    @SqlType(StandardTypes.DOUBLE)
    public static double pi()
    {
        return MathFunctions.pi();
    }

    @Description("Euler's number")
    @ScalarFunction(value = "e", neverFails = true)
    @StaticMethod(StandardTypes.DOUBLE)
    @SqlType(StandardTypes.DOUBLE)
    public static double e()
    {
        return MathFunctions.e();
    }

    @Description("Constant representing not-a-number")
    @ScalarFunction(value = "nan", neverFails = true)
    @StaticMethod(StandardTypes.DOUBLE)
    @SqlType(StandardTypes.DOUBLE)
    public static double nan()
    {
        return MathFunctions.nan();
    }

    @Description("Infinity")
    @ScalarFunction(value = "infinity", neverFails = true)
    @StaticMethod(StandardTypes.DOUBLE)
    @SqlType(StandardTypes.DOUBLE)
    public static double infinity()
    {
        return MathFunctions.infinity();
    }

    @Description("Convert a string in the given base to a number")
    @ScalarFunction("from_base")
    @StaticMethod(StandardTypes.BIGINT)
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long fromBase(@SqlType("varchar(x)") Slice value, @SqlType(StandardTypes.BIGINT) long radix)
    {
        return MathFunctions.fromBase(value, radix);
    }
}
