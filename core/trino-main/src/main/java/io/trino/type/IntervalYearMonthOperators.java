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

import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.PolymorphicScalarFunctionBuilder;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.Signature;
import io.trino.spi.type.NumericExpression;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.IntervalField.MONTH;
import static io.trino.spi.type.IntervalField.YEAR;
import static io.trino.spi.type.NumericExpression.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.DOUBLE;
import static io.trino.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.trino.spi.type.TypeTemplates.numericVariable;
import static io.trino.spi.type.TypeTemplates.type;
import static io.trino.type.TypeCalculation.parseNumericExpression;
import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.negateExact;
import static java.lang.Math.subtractExact;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

/// Arithmetic over year-month intervals, regardless of qualifier.
///
/// Every year-month qualifier shares one physical representation (a signed count
/// of months), so the arithmetic is qualifier-agnostic. The result qualifier
/// follows the SQL specification: adding or subtracting two intervals yields the
/// union of their fields (the least significant start field and the most
/// significant end field), while scaling by a number or negating preserves the
/// operand's qualifier.
public final class IntervalYearMonthOperators
{
    private IntervalYearMonthOperators() {}

    public static SqlScalarFunction[] intervalYearMonthOperators()
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

    /// `interval <op> interval` whose result spans the union of the operand qualifiers.
    private static SqlScalarFunction binary(OperatorType operator, String method)
    {
        return new PolymorphicScalarFunctionBuilder(operator, IntervalYearMonthOperators.class)
                .signature(Signature.builder()
                        .numericVariable("r_start", parseNumericExpression("min(a_start, b_start)"))
                        .numericVariable("r_end", parseNumericExpression("max(a_end, b_end)"))
                        .numericVariable("r_precision", fieldMaxPrecision(parseNumericExpression("min(a_start, b_start)")))
                        .argumentType(type(INTERVAL_YEAR_TO_MONTH, numericVariable("a_start"), numericVariable("a_end"), numericVariable("a_precision")))
                        .argumentType(type(INTERVAL_YEAR_TO_MONTH, numericVariable("b_start"), numericVariable("b_end"), numericVariable("b_precision")))
                        .returnType(type(INTERVAL_YEAR_TO_MONTH, numericVariable("r_start"), numericVariable("r_end"), numericVariable("r_precision")))
                        .build())
                .deterministic(true)
                .choice(choice -> choice.implementation(methodsGroup -> methodsGroup.methods(method)))
                .build();
    }

    /// `interval <op> number`, preserving the interval's qualifier.
    private static SqlScalarFunction intervalScalar(OperatorType operator, String numericType, String method)
    {
        return new PolymorphicScalarFunctionBuilder(operator, IntervalYearMonthOperators.class)
                .signature(Signature.builder()
                        .numericVariable("r_precision", fieldMaxPrecision(new NumericExpression.Variable("start")))
                        .argumentType(type(INTERVAL_YEAR_TO_MONTH, numericVariable("start"), numericVariable("end"), numericVariable("precision")))
                        .argumentType(type(numericType))
                        .returnType(type(INTERVAL_YEAR_TO_MONTH, numericVariable("start"), numericVariable("end"), numericVariable("r_precision")))
                        .build())
                .deterministic(true)
                .choice(choice -> choice.implementation(methodsGroup -> methodsGroup.methods(method)))
                .build();
    }

    /// `number <op> interval`, preserving the interval's qualifier.
    private static SqlScalarFunction scalarInterval(OperatorType operator, String numericType, String method)
    {
        return new PolymorphicScalarFunctionBuilder(operator, IntervalYearMonthOperators.class)
                .signature(Signature.builder()
                        .numericVariable("r_precision", fieldMaxPrecision(new NumericExpression.Variable("start")))
                        .argumentType(type(numericType))
                        .argumentType(type(INTERVAL_YEAR_TO_MONTH, numericVariable("start"), numericVariable("end"), numericVariable("precision")))
                        .returnType(type(INTERVAL_YEAR_TO_MONTH, numericVariable("start"), numericVariable("end"), numericVariable("r_precision")))
                        .build())
                .deterministic(true)
                .choice(choice -> choice.implementation(methodsGroup -> methodsGroup.methods(method)))
                .build();
    }

    /// A unary operator preserving the interval's qualifier.
    private static SqlScalarFunction unary(OperatorType operator, String method)
    {
        return new PolymorphicScalarFunctionBuilder(operator, IntervalYearMonthOperators.class)
                .signature(Signature.builder()
                        .numericVariable("r_precision", fieldMaxPrecision(new NumericExpression.Variable("start")))
                        .argumentType(type(INTERVAL_YEAR_TO_MONTH, numericVariable("start"), numericVariable("end"), numericVariable("precision")))
                        .returnType(type(INTERVAL_YEAR_TO_MONTH, numericVariable("start"), numericVariable("end"), numericVariable("r_precision")))
                        .build())
                .deterministic(true)
                .choice(choice -> choice.implementation(methodsGroup -> methodsGroup.methods(method)))
                .build();
    }

    /// The leading precision of a derived interval: the field maximum of its leading field, which is
    /// sufficient to hold any value the result can take.
    private static NumericExpression fieldMaxPrecision(NumericExpression startField)
    {
        return new NumericExpression.Conditional(
                new NumericExpression.Comparison(LESS_THAN_OR_EQUAL, startField, new NumericExpression.Literal(YEAR.code())),
                new NumericExpression.Literal(IntervalYearMonthType.maxLeadingPrecision(YEAR)),
                new NumericExpression.Literal(IntervalYearMonthType.maxLeadingPrecision(MONTH)));
    }

    @UsedByGeneratedCode
    public static long add(long left, long right)
    {
        try {
            return addExact(toIntExact(left), toIntExact(right));
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("interval year to month addition overflow: %s + %s", left, right), e);
        }
    }

    @UsedByGeneratedCode
    public static long subtract(long left, long right)
    {
        try {
            return subtractExact(toIntExact(left), toIntExact(right));
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("interval year to month subtraction overflow: %s - %s", left, right), e);
        }
    }

    @UsedByGeneratedCode
    public static long multiplyByBigint(long left, long right)
    {
        try {
            return toIntExact(multiplyExact(left, right));
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("interval year to month multiplication overflow: %s * %s", left, right), e);
        }
    }

    @UsedByGeneratedCode
    public static long multiplyByDouble(long left, double right)
    {
        if (Double.isNaN(right)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot multiply by double NaN");
        }
        return (long) (left * right);
    }

    @UsedByGeneratedCode
    public static long bigintMultiply(long left, long right)
    {
        try {
            return toIntExact(multiplyExact(left, right));
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("interval year to month multiplication overflow: %s * %s", left, right), e);
        }
    }

    @UsedByGeneratedCode
    public static long doubleMultiply(double left, long right)
    {
        if (Double.isNaN(left)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot multiply by double NaN");
        }
        return (long) (left * right);
    }

    @UsedByGeneratedCode
    public static long divideByDouble(long left, double right)
    {
        if (Double.isNaN(right) || right == 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Cannot divide by double %s", right));
        }
        return (long) (left / right);
    }

    @UsedByGeneratedCode
    public static long negate(long value)
    {
        try {
            return negateExact(toIntExact(value));
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "interval year to month negation overflow: " + value, e);
        }
    }
}
