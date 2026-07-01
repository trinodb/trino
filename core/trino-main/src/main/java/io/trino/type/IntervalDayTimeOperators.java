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

import com.google.common.collect.ImmutableList;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.PolymorphicScalarFunctionBuilder;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.Signature;
import io.trino.spi.type.IntervalField;
import io.trino.spi.type.NumericExpression;

import java.math.BigInteger;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.IntervalField.DAY;
import static io.trino.spi.type.IntervalField.HOUR;
import static io.trino.spi.type.IntervalField.MINUTE;
import static io.trino.spi.type.IntervalField.SECOND;
import static io.trino.spi.type.NumericExpression.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.DOUBLE;
import static io.trino.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TypeTemplates.numericVariable;
import static io.trino.spi.type.TypeTemplates.type;
import static io.trino.type.IntervalArithmetic.addMicros;
import static io.trino.type.IntervalArithmetic.subtractMicros;
import static io.trino.type.IntervalCasts.negatePicos;
import static io.trino.type.IntervalCasts.roundToLongInterval;
import static io.trino.type.IntervalCasts.truncateToField;
import static io.trino.type.IntervalDayTimeType.MAX_SHORT_FRACTIONAL_PRECISION;
import static io.trino.type.TypeCalculation.parseNumericExpression;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.negateExact;
import static java.lang.String.format;

/// Arithmetic over day-time intervals, regardless of qualifier.
///
/// Arithmetic dispatches on microsecond or picosecond storage independently of the qualifier.
/// Adding or subtracting two intervals yields the union of their fields (the most significant
/// start field and the least significant end field), while scaling by a number or negating
/// preserves the operand's qualifier.
public final class IntervalDayTimeOperators
{
    private static final BigInteger PICOS_PER_MICRO = BigInteger.valueOf(PICOSECONDS_PER_MICROSECOND);

    private IntervalDayTimeOperators() {}

    public static SqlScalarFunction[] intervalDayTimeOperators()
    {
        return new SqlScalarFunction[] {
                binary(ADD, "add"),
                binary(SUBTRACT, "subtract"),
                intervalScalar(MULTIPLY, BIGINT, "multiplyByBigint"),
                intervalScalar(MULTIPLY, DOUBLE, "multiplyByDouble"),
                scalarInterval(MULTIPLY, BIGINT, "bigintMultiply"),
                scalarInterval(MULTIPLY, DOUBLE, "doubleMultiply"),
                intervalScalar(DIVIDE, DOUBLE, "divideByDouble"),
                unary(NEGATION, "negate"),
        };
    }

    /// `interval <op> interval` whose result spans the union of the operand qualifiers and keeps the
    /// wider of the two fractional-seconds precisions.
    private static SqlScalarFunction binary(OperatorType operator, String method)
    {
        return new PolymorphicScalarFunctionBuilder(operator, IntervalDayTimeOperators.class)
                .signature(Signature.builder()
                        .numericVariable("r_start", parseNumericExpression("min(a_start, b_start)"))
                        .numericVariable("r_end", parseNumericExpression("max(a_end, b_end)"))
                        .numericVariable("r_precision", fieldMaxPrecision(parseNumericExpression("min(a_start, b_start)")))
                        .numericVariable("r_fractional", parseNumericExpression("max(a_fractional, b_fractional)"))
                        .argumentType(type(INTERVAL_DAY_TO_SECOND, numericVariable("a_start"), numericVariable("a_end"), numericVariable("a_precision"), numericVariable("a_fractional")))
                        .argumentType(type(INTERVAL_DAY_TO_SECOND, numericVariable("b_start"), numericVariable("b_end"), numericVariable("b_precision"), numericVariable("b_fractional")))
                        .returnType(type(INTERVAL_DAY_TO_SECOND, numericVariable("r_start"), numericVariable("r_end"), numericVariable("r_precision"), numericVariable("r_fractional")))
                        .build())
                .deterministic(true)
                .choice(choice -> choice.implementation(methodsGroup -> methodsGroup
                        .methods(method + "ShortShort", method + "ShortLong", method + "LongShort", method + "LongLong")))
                .build();
    }

    /// `interval <op> number`, preserving the interval's qualifier and fractional-seconds precision.
    private static SqlScalarFunction intervalScalar(OperatorType operator, String numericType, String method)
    {
        return new PolymorphicScalarFunctionBuilder(operator, IntervalDayTimeOperators.class)
                .signature(Signature.builder()
                        .numericVariable("r_precision", fieldMaxPrecision(new NumericExpression.Variable("start")))
                        .argumentType(type(INTERVAL_DAY_TO_SECOND, numericVariable("start"), numericVariable("end"), numericVariable("precision"), numericVariable("fractional")))
                        .argumentType(type(numericType))
                        .returnType(type(INTERVAL_DAY_TO_SECOND, numericVariable("start"), numericVariable("end"), numericVariable("r_precision"), numericVariable("fractional")))
                        .build())
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup
                                .methods(method + "Short")
                                .withExtraParameters(context -> numericType.equals(BIGINT)
                                        ? ImmutableList.of()
                                        : ImmutableList.of((long) ((IntervalDayTimeType) context.getReturnType()).getFractionalPrecision(), ((IntervalDayTimeType) context.getReturnType()).getEndField())))
                        .implementation(methodsGroup -> methodsGroup
                                .methods(method + "Long")
                                .withExtraParameters(context -> ImmutableList.of((long) ((IntervalDayTimeType) context.getReturnType()).getFractionalPrecision()))))
                .build();
    }

    /// `number <op> interval`, preserving the interval's qualifier and fractional-seconds precision.
    private static SqlScalarFunction scalarInterval(OperatorType operator, String numericType, String method)
    {
        return new PolymorphicScalarFunctionBuilder(operator, IntervalDayTimeOperators.class)
                .signature(Signature.builder()
                        .numericVariable("r_precision", fieldMaxPrecision(new NumericExpression.Variable("start")))
                        .argumentType(type(numericType))
                        .argumentType(type(INTERVAL_DAY_TO_SECOND, numericVariable("start"), numericVariable("end"), numericVariable("precision"), numericVariable("fractional")))
                        .returnType(type(INTERVAL_DAY_TO_SECOND, numericVariable("start"), numericVariable("end"), numericVariable("r_precision"), numericVariable("fractional")))
                        .build())
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup
                                .methods(method + "Short")
                                .withExtraParameters(context -> numericType.equals(BIGINT)
                                        ? ImmutableList.of()
                                        : ImmutableList.of((long) ((IntervalDayTimeType) context.getReturnType()).getFractionalPrecision(), ((IntervalDayTimeType) context.getReturnType()).getEndField())))
                        .implementation(methodsGroup -> methodsGroup
                                .methods(method + "Long")
                                .withExtraParameters(context -> ImmutableList.of((long) ((IntervalDayTimeType) context.getReturnType()).getFractionalPrecision()))))
                .build();
    }

    /// A unary operator preserving the interval's qualifier and fractional-seconds precision.
    private static SqlScalarFunction unary(OperatorType operator, String method)
    {
        return new PolymorphicScalarFunctionBuilder(operator, IntervalDayTimeOperators.class)
                .signature(Signature.builder()
                        .numericVariable("r_precision", fieldMaxPrecision(new NumericExpression.Variable("start")))
                        .argumentType(type(INTERVAL_DAY_TO_SECOND, numericVariable("start"), numericVariable("end"), numericVariable("precision"), numericVariable("fractional")))
                        .returnType(type(INTERVAL_DAY_TO_SECOND, numericVariable("start"), numericVariable("end"), numericVariable("r_precision"), numericVariable("fractional")))
                        .build())
                .deterministic(true)
                .choice(choice -> choice.implementation(methodsGroup -> methodsGroup
                        .methods(method + "Short", method + "Long")))
                .build();
    }

    /// `datetime - datetime`, producing a `day(9) to second` interval whose fractional-seconds precision
    /// is the datetime's (at least 6, so a microsecond-or-coarser datetime keeps the historical
    /// `second(6)` result). The implementation methods live in `clazz` and dispatch on whether the
    /// datetime and the resulting interval are short or long.
    public static SqlScalarFunction dateTimeDifference(Class<?> clazz, String dateTimeBaseName, String... methods)
    {
        return new PolymorphicScalarFunctionBuilder(SUBTRACT, clazz)
                .signature(Signature.builder()
                        .numericVariable("r_fractional", parseNumericExpression("max(p, 6)"))
                        .argumentType(type(dateTimeBaseName, numericVariable("p")))
                        .argumentType(type(dateTimeBaseName, numericVariable("p")))
                        .returnType(type(
                                INTERVAL_DAY_TO_SECOND,
                                new NumericExpression.Literal(DAY.code()),
                                new NumericExpression.Literal(SECOND.code()),
                                new NumericExpression.Literal(IntervalDayTimeType.maxLeadingPrecision(DAY)),
                                numericVariable("r_fractional")))
                        .build())
                .deterministic(true)
                .choice(choice -> choice.implementation(methodsGroup -> methodsGroup.methods(methods)))
                .build();
    }

    /// The leading precision of a derived interval: the field maximum of its leading field, which is
    /// sufficient to hold any value the result can take.
    private static NumericExpression fieldMaxPrecision(NumericExpression startField)
    {
        return new NumericExpression.Conditional(
                new NumericExpression.Comparison(LESS_THAN_OR_EQUAL, startField, new NumericExpression.Literal(DAY.code())),
                new NumericExpression.Literal(IntervalDayTimeType.maxLeadingPrecision(DAY)),
                new NumericExpression.Conditional(
                        new NumericExpression.Comparison(LESS_THAN_OR_EQUAL, startField, new NumericExpression.Literal(HOUR.code())),
                        new NumericExpression.Literal(IntervalDayTimeType.maxLeadingPrecision(HOUR)),
                        new NumericExpression.Conditional(
                                new NumericExpression.Comparison(LESS_THAN_OR_EQUAL, startField, new NumericExpression.Literal(MINUTE.code())),
                                new NumericExpression.Literal(IntervalDayTimeType.maxLeadingPrecision(MINUTE)),
                                new NumericExpression.Literal(IntervalDayTimeType.maxLeadingPrecision(SECOND)))));
    }

    @UsedByGeneratedCode
    public static long addShortShort(long left, long right)
    {
        return addMicros(left, right);
    }

    @UsedByGeneratedCode
    public static LongInterval addShortLong(long left, LongInterval right)
    {
        return new LongInterval(addMicros(left, right.getMicros()), right.getPicosOfMicro());
    }

    @UsedByGeneratedCode
    public static LongInterval addLongShort(LongInterval left, long right)
    {
        return new LongInterval(addMicros(left.getMicros(), right), left.getPicosOfMicro());
    }

    @UsedByGeneratedCode
    public static LongInterval addLongLong(LongInterval left, LongInterval right)
    {
        int picos = left.getPicosOfMicro() + right.getPicosOfMicro();
        long micros = addMicros(left.getMicros(), right.getMicros(), picos >= PICOSECONDS_PER_MICROSECOND);
        return new LongInterval(micros, picos % PICOSECONDS_PER_MICROSECOND);
    }

    @UsedByGeneratedCode
    public static long subtractShortShort(long left, long right)
    {
        return subtractMicros(left, right);
    }

    @UsedByGeneratedCode
    public static LongInterval subtractShortLong(long left, LongInterval right)
    {
        int picos = right.getPicosOfMicro();
        long micros = subtractMicros(left, right.getMicros(), picos > 0);
        if (picos > 0) {
            picos = PICOSECONDS_PER_MICROSECOND - picos;
        }
        return new LongInterval(micros, picos);
    }

    @UsedByGeneratedCode
    public static LongInterval subtractLongShort(LongInterval left, long right)
    {
        return new LongInterval(subtractMicros(left.getMicros(), right), left.getPicosOfMicro());
    }

    @UsedByGeneratedCode
    public static LongInterval subtractLongLong(LongInterval left, LongInterval right)
    {
        int picos = left.getPicosOfMicro() - right.getPicosOfMicro();
        long micros = subtractMicros(left.getMicros(), right.getMicros(), picos < 0);
        if (picos < 0) {
            picos += PICOSECONDS_PER_MICROSECOND;
        }
        return new LongInterval(micros, picos);
    }

    @UsedByGeneratedCode
    public static long multiplyByBigintShort(long left, long right)
    {
        return multiplyMicros(left, right);
    }

    @UsedByGeneratedCode
    public static LongInterval multiplyByBigintLong(LongInterval left, long right, long fractionalPrecision)
    {
        return multiplyByBigint(left, right);
    }

    @UsedByGeneratedCode
    public static long multiplyByDoubleShort(long left, double right, long fractionalPrecision, IntervalField endField)
    {
        return normalizeShort(multiplyMicrosByDouble(left, right), fractionalPrecision, endField);
    }

    @UsedByGeneratedCode
    public static LongInterval multiplyByDoubleLong(LongInterval left, double right, long fractionalPrecision)
    {
        return scaleByDouble(left, right, (int) fractionalPrecision);
    }

    @UsedByGeneratedCode
    public static long bigintMultiplyShort(long left, long right)
    {
        return multiplyMicros(left, right);
    }

    @UsedByGeneratedCode
    public static LongInterval bigintMultiplyLong(long left, LongInterval right, long fractionalPrecision)
    {
        return multiplyByBigint(right, left);
    }

    @UsedByGeneratedCode
    public static long doubleMultiplyShort(double left, long right, long fractionalPrecision, IntervalField endField)
    {
        return normalizeShort(multiplyMicrosByDouble(right, left), fractionalPrecision, endField);
    }

    @UsedByGeneratedCode
    public static LongInterval doubleMultiplyLong(double left, LongInterval right, long fractionalPrecision)
    {
        return scaleByDouble(right, left, (int) fractionalPrecision);
    }

    @UsedByGeneratedCode
    public static long divideByDoubleShort(long left, double right, long fractionalPrecision, IntervalField endField)
    {
        if (Double.isNaN(right) || right == 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Cannot divide by double %s", right));
        }
        return normalizeShort(left / right, fractionalPrecision, endField);
    }

    private static long normalizeShort(double micros, long fractionalPrecision, IntervalField endField)
    {
        checkScaledMicros(micros);
        if (endField != SECOND) {
            return truncateToField((long) micros, 0, endField);
        }
        long unit = (long) Math.pow(10, MAX_SHORT_FRACTIONAL_PRECISION - fractionalPrecision);
        return multiplyMicros(Math.round(micros / unit), unit);
    }

    @UsedByGeneratedCode
    public static LongInterval divideByDoubleLong(LongInterval left, double right, long fractionalPrecision)
    {
        if (Double.isNaN(right) || right == 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Cannot divide by double %s", right));
        }
        return fromScaledMicros(doubleMicros(left) / right, (int) fractionalPrecision);
    }

    private static long multiplyMicros(long left, long right)
    {
        try {
            return multiplyExact(left, right);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("interval day to second multiplication overflow: %s * %s", left, right), e);
        }
    }

    private static double multiplyMicrosByDouble(long micros, double factor)
    {
        if (Double.isNaN(factor)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot multiply by double NaN");
        }
        return micros * factor;
    }

    /// Multiplies a long interval by an integer, carrying the scaled sub-microsecond fraction. The
    /// picoseconds stay on the fractional-precision grid, so the result needs no rounding.
    private static LongInterval multiplyByBigint(LongInterval value, long factor)
    {
        if (value.getPicosOfMicro() == 0) {
            return new LongInterval(multiplyMicros(value.getMicros(), factor), 0);
        }
        // Both component products can overflow even when their sum, after normalization, fits.
        BigInteger totalPicos = BigInteger.valueOf(value.getMicros()).multiply(PICOS_PER_MICRO)
                .add(BigInteger.valueOf(value.getPicosOfMicro()))
                .multiply(BigInteger.valueOf(factor));
        BigInteger[] parts = totalPicos.divideAndRemainder(PICOS_PER_MICRO);
        if (parts[1].signum() < 0) {
            parts[0] = parts[0].subtract(BigInteger.ONE);
            parts[1] = parts[1].add(PICOS_PER_MICRO);
        }
        try {
            return new LongInterval(parts[0].longValueExact(), parts[1].intValueExact());
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "interval day to second multiplication overflow", e);
        }
    }

    /// Scales a long interval by a real factor, rounding the result to its fractional-seconds precision.
    /// The arithmetic goes through a double, so it carries the same imprecision as the short form.
    private static LongInterval scaleByDouble(LongInterval value, double factor, int fractionalPrecision)
    {
        if (Double.isNaN(factor)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot multiply by double NaN");
        }
        return fromScaledMicros(doubleMicros(value) * factor, fractionalPrecision);
    }

    private static double doubleMicros(LongInterval value)
    {
        return value.getMicros() + value.getPicosOfMicro() / (double) PICOSECONDS_PER_MICROSECOND;
    }

    private static LongInterval fromScaledMicros(double scaledMicros, int fractionalPrecision)
    {
        checkScaledMicros(scaledMicros);
        long wholeMicros = (long) Math.floor(scaledMicros);
        long picos = Math.round((scaledMicros - wholeMicros) * PICOSECONDS_PER_MICROSECOND);
        if (picos >= PICOSECONDS_PER_MICROSECOND) {
            wholeMicros = addMicros(wholeMicros, 1);
            picos -= PICOSECONDS_PER_MICROSECOND;
        }
        return roundToLongInterval(wholeMicros, picos, fractionalPrecision);
    }

    private static void checkScaledMicros(double micros)
    {
        // Long.MAX_VALUE rounds to 2^63 as a double, so the upper bound must be exclusive.
        if (!Double.isFinite(micros) || micros < -0x1.0p63 || micros >= 0x1.0p63) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "interval day to second scaling overflow: " + micros);
        }
    }

    @UsedByGeneratedCode
    public static long negateShort(long value)
    {
        try {
            return negateExact(value);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "interval day to second negation overflow: " + value, e);
        }
    }

    @UsedByGeneratedCode
    public static LongInterval negateLong(LongInterval value)
    {
        long[] negated = negatePicos(value.getMicros(), value.getPicosOfMicro());
        return new LongInterval(negated[0], (int) negated[1]);
    }
}
