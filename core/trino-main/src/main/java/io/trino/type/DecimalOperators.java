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
import io.trino.metadata.PolymorphicScalarFunctionBuilder.SpecializeContext;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.Signature;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TypeSignature;

import java.util.List;

import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MODULUS;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.Int128Math.add;
import static io.trino.spi.type.Int128Math.divideRoundUp;
import static io.trino.spi.type.Int128Math.multiply;
import static io.trino.spi.type.Int128Math.negateExact;
import static io.trino.spi.type.Int128Math.remainder;
import static io.trino.spi.type.Int128Math.rescale;
import static io.trino.spi.type.Int128Math.subtract;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static java.lang.Integer.max;
import static java.lang.Long.signum;
import static java.lang.Math.abs;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class DecimalOperators
{
    public static final SqlScalarFunction DECIMAL_ADD_OPERATOR = decimalAddOperator();
    public static final SqlScalarFunction DECIMAL_SUBTRACT_OPERATOR = decimalSubtractOperator();
    public static final SqlScalarFunction DECIMAL_MULTIPLY_OPERATOR = decimalMultiplyOperator();
    public static final SqlScalarFunction DECIMAL_DIVIDE_OPERATOR = decimalDivideOperator();
    public static final SqlScalarFunction DECIMAL_MODULUS_OPERATOR = decimalModulusOperator();

    private DecimalOperators()
    {
    }

    private static SqlScalarFunction decimalAddOperator()
    {
        TypeSignature decimalLeftSignature = new TypeSignature("decimal", typeVariable("a_precision"), typeVariable("a_scale"));
        TypeSignature decimalRightSignature = new TypeSignature("decimal", typeVariable("b_precision"), typeVariable("b_scale"));
        TypeSignature decimalResultSignature = new TypeSignature("decimal", typeVariable("r_precision"), typeVariable("r_scale"));

        Signature signature = Signature.builder()
                .operatorType(ADD)
                .longVariable("r_precision", "min(38, max(a_precision - a_scale, b_precision - b_scale) + max(a_scale, b_scale) + 1)")
                .longVariable("r_scale", "max(a_scale, b_scale)")
                .argumentType(decimalLeftSignature)
                .argumentType(decimalRightSignature)
                .returnType(decimalResultSignature)
                .build();
        return new PolymorphicScalarFunctionBuilder(DecimalOperators.class)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup
                                .methods("addShortShortShort")
                                .withExtraParameters(DecimalOperators::calculateShortRescaleParameters))
                        .implementation(methodsGroup -> methodsGroup
                                .methods("addShortShortLong", "addLongLongLong", "addShortLongLong", "addLongShortLong")
                                .withExtraParameters(DecimalOperators::calculateLongRescaleParameters)))
                .build();
    }

    @UsedByGeneratedCode
    public static long addShortShortShort(long a, long b, long aRescale, long bRescale)
    {
        return a * aRescale + b * bRescale;
    }

    @UsedByGeneratedCode
    public static Int128 addShortShortLong(long a, long b, int rescale, boolean left)
    {
        return internalAddLongLongLong(a >> 63, a, b >> 63, b, rescale, left);
    }

    @UsedByGeneratedCode
    public static Int128 addLongLongLong(Int128 a, Int128 b, int rescale, boolean left)
    {
        return internalAddLongLongLong(a.getHigh(), a.getLow(), b.getHigh(), b.getLow(), rescale, left);
    }

    @UsedByGeneratedCode
    public static Int128 addShortLongLong(long a, Int128 b, int rescale, boolean left)
    {
        return addLongShortLong(b, a, rescale, !left);
    }

    @UsedByGeneratedCode
    public static Int128 addLongShortLong(Int128 a, long b, int rescale, boolean rescaleLeft)
    {
        return internalAddLongLongLong(a.getHigh(), a.getLow(), b >> 63, b, rescale, rescaleLeft);
    }

    private static Int128 internalAddLongLongLong(long leftHigh, long leftLow, long rightHigh, long rightLow, int rescale, boolean rescaleLeft)
    {
        // TODO: specialize implementation for rescale == 0 vs rescale > 0, and rescale left vs rescale right to avoid branch pollution
        try {
            long[] result = new long[2];
            if (rescaleLeft) {
                rescale(leftHigh, leftLow, rescale, result, 0);
                add(result[0], result[1], rightHigh, rightLow, result, 0);
            }
            else {
                rescale(rightHigh, rightLow, rescale, result, 0);
                add(leftHigh, leftLow, result[0], result[1], result, 0);
            }

            if (Decimals.overflows(result[0], result[1])) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
            }

            return Int128.valueOf(result);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    private static SqlScalarFunction decimalSubtractOperator()
    {
        TypeSignature decimalLeftSignature = new TypeSignature("decimal", typeVariable("a_precision"), typeVariable("a_scale"));
        TypeSignature decimalRightSignature = new TypeSignature("decimal", typeVariable("b_precision"), typeVariable("b_scale"));
        TypeSignature decimalResultSignature = new TypeSignature("decimal", typeVariable("r_precision"), typeVariable("r_scale"));

        Signature signature = Signature.builder()
                .operatorType(SUBTRACT)
                .longVariable("r_precision", "min(38, max(a_precision - a_scale, b_precision - b_scale) + max(a_scale, b_scale) + 1)")
                .longVariable("r_scale", "max(a_scale, b_scale)")
                .argumentType(decimalLeftSignature)
                .argumentType(decimalRightSignature)
                .returnType(decimalResultSignature)
                .build();
        return new PolymorphicScalarFunctionBuilder(DecimalOperators.class)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup
                                .methods("subtractShortShortShort")
                                .withExtraParameters(DecimalOperators::calculateShortRescaleParameters))
                        .implementation(methodsGroup -> methodsGroup
                                .methods("subtractShortShortLong", "subtractLongLongLong", "subtractShortLongLong", "subtractLongShortLong")
                                .withExtraParameters(DecimalOperators::calculateLongRescaleParameters)))
                .build();
    }

    @UsedByGeneratedCode
    public static long subtractShortShortShort(long a, long b, long aRescale, long bRescale)
    {
        return a * aRescale - b * bRescale;
    }

    @UsedByGeneratedCode
    public static Int128 subtractShortShortLong(long a, long b, int rescale, boolean left)
    {
        return internalSubtractLongLongLong(a >> 63, a, b >> 63, b, rescale, left);
    }

    @UsedByGeneratedCode
    public static Int128 subtractLongLongLong(Int128 a, Int128 b, int rescale, boolean left)
    {
        return internalSubtractLongLongLong(a.getHigh(), a.getLow(), b.getHigh(), b.getLow(), rescale, left);
    }

    @UsedByGeneratedCode
    public static Int128 subtractShortLongLong(long a, Int128 b, int rescale, boolean left)
    {
        return internalSubtractLongLongLong(a >> 63, a, b.getHigh(), b.getLow(), rescale, left);
    }

    @UsedByGeneratedCode
    public static Int128 subtractLongShortLong(Int128 a, long b, int rescale, boolean left)
    {
        return internalSubtractLongLongLong(a.getHigh(), a.getLow(), b >> 63, b, rescale, left);
    }

    private static Int128 internalSubtractLongLongLong(long leftHigh, long leftLow, long rightHigh, long rightLow, int rescale, boolean rescaleLeft)
    {
        try {
            long[] result = new long[2];
            if (rescaleLeft) {
                rescale(leftHigh, leftLow, rescale, result, 0);
                subtract(result[0], result[1], rightHigh, rightLow, result, 0);
            }
            else {
                rescale(rightHigh, rightLow, rescale, result, 0);
                subtract(leftHigh, leftLow, result[0], result[1], result, 0);
            }

            if (Decimals.overflows(result[0], result[1])) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
            }

            return Int128.valueOf(result);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    private static SqlScalarFunction decimalMultiplyOperator()
    {
        TypeSignature decimalLeftSignature = new TypeSignature("decimal", typeVariable("a_precision"), typeVariable("a_scale"));
        TypeSignature decimalRightSignature = new TypeSignature("decimal", typeVariable("b_precision"), typeVariable("b_scale"));
        TypeSignature decimalResultSignature = new TypeSignature("decimal", typeVariable("r_precision"), typeVariable("r_scale"));

        Signature signature = Signature.builder()
                .operatorType(MULTIPLY)
                .longVariable("r_precision", "min(38, a_precision + b_precision)")
                .longVariable("r_scale", "a_scale + b_scale")
                .argumentType(decimalLeftSignature)
                .argumentType(decimalRightSignature)
                .returnType(decimalResultSignature)
                .build();
        return new PolymorphicScalarFunctionBuilder(DecimalOperators.class)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup
                                .methods("multiplyShortShortShort", "multiplyShortShortLong", "multiplyLongLongLong", "multiplyShortLongLong", "multiplyLongShortLong")))
                .build();
    }

    @UsedByGeneratedCode
    public static long multiplyShortShortShort(long a, long b)
    {
        return a * b;
    }

    @UsedByGeneratedCode
    public static Int128 multiplyShortShortLong(long a, long b)
    {
        try {
            return multiply(a, b);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Int128 multiplyLongLongLong(Int128 a, Int128 b)
    {
        try {
            Int128 result = multiply(a, b);

            if (Decimals.overflows(result)) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
            }

            return result;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Int128 multiplyShortLongLong(long a, Int128 b)
    {
        return multiplyLongShortLong(b, a);
    }

    @UsedByGeneratedCode
    public static Int128 multiplyLongShortLong(Int128 a, long b)
    {
        try {
            Int128 result = multiply(a, b);

            if (Decimals.overflows(result)) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
            }

            return result;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    private static SqlScalarFunction decimalDivideOperator()
    {
        TypeSignature decimalLeftSignature = new TypeSignature("decimal", typeVariable("a_precision"), typeVariable("a_scale"));
        TypeSignature decimalRightSignature = new TypeSignature("decimal", typeVariable("b_precision"), typeVariable("b_scale"));
        TypeSignature decimalResultSignature = new TypeSignature("decimal", typeVariable("r_precision"), typeVariable("r_scale"));

        // we extend target precision by b_scale. This is upper bound on how much division result will grow.
        // pessimistic case is a / 0.0000001
        // if scale of divisor is greater than scale of dividend we extend scale further as we
        // want result scale to be maximum of scales of divisor and dividend.
        Signature signature = Signature.builder()
                .operatorType(DIVIDE)
                .longVariable("r_precision", "min(38, a_precision + b_scale + max(b_scale - a_scale, 0))")
                .longVariable("r_scale", "max(a_scale, b_scale)")
                .argumentType(decimalLeftSignature)
                .argumentType(decimalRightSignature)
                .returnType(decimalResultSignature)
                .build();
        return new PolymorphicScalarFunctionBuilder(DecimalOperators.class)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup
                                .methods("divideShortShortShort", "divideShortLongShort", "divideLongShortShort", "divideShortShortLong", "divideLongLongLong", "divideShortLongLong", "divideLongShortLong")
                                .withExtraParameters(DecimalOperators::divideRescaleFactor)))
                .build();
    }

    private static List<Object> divideRescaleFactor(PolymorphicScalarFunctionBuilder.SpecializeContext context)
    {
        DecimalType returnType = (DecimalType) context.getReturnType();
        int dividendScale = toIntExact(requireNonNull(context.getLiteral("a_scale"), "a_scale is null"));
        int divisorScale = toIntExact(requireNonNull(context.getLiteral("b_scale"), "b_scale is null"));
        int resultScale = returnType.getScale();
        int rescaleFactor = resultScale - dividendScale + divisorScale;
        return ImmutableList.of(rescaleFactor);
    }

    @UsedByGeneratedCode
    public static long divideShortShortShort(long dividend, long divisor, int rescaleFactor)
    {
        if (divisor == 0) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }

        if (dividend == 0) {
            return 0;
        }

        int resultSignum = signum(dividend) * signum(divisor);

        long unsignedDividend = abs(dividend);
        long unsignedDivisor = abs(divisor);

        long rescaledUnsignedDividend = unsignedDividend * longTenToNth(rescaleFactor);
        long quotient = rescaledUnsignedDividend / unsignedDivisor;
        long remainder = rescaledUnsignedDividend - (quotient * unsignedDivisor);

        if (Long.compareUnsigned(remainder * 2, unsignedDivisor) >= 0) {
            quotient++;
        }

        return resultSignum * quotient;
    }

    @UsedByGeneratedCode
    public static long divideShortLongShort(long dividend, Int128 divisor, int rescaleFactor)
    {
        if (divisor.isZero()) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return divideRoundUp(dividend >> 63, dividend, rescaleFactor, divisor.getHigh(), divisor.getLow(), 0).toLongExact();
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static long divideLongShortShort(Int128 dividend, long divisor, int rescaleFactor)
    {
        if (divisor == 0) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return divideRoundUp(dividend.getHigh(), dividend.getLow(), rescaleFactor, divisor >> 63, divisor, 0).toLongExact();
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Int128 divideShortShortLong(long dividend, long divisor, int rescaleFactor)
    {
        if (divisor == 0) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            Int128 result = divideRoundUp(dividend >> 63, dividend, rescaleFactor, divisor >> 63, divisor, 0);
            if (Decimals.overflows(result)) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
            }
            return result;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Int128 divideLongLongLong(Int128 dividend, Int128 divisor, int rescaleFactor)
    {
        if (divisor.isZero()) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            Int128 result = divideRoundUp(dividend.getHigh(), dividend.getLow(), rescaleFactor, divisor.getHigh(), divisor.getLow(), 0);
            if (Decimals.overflows(result)) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
            }
            return result;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Int128 divideShortLongLong(long dividend, Int128 divisor, int rescaleFactor)
    {
        if (divisor.isZero()) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            Int128 result = divideRoundUp(dividend >> 63, dividend, rescaleFactor, divisor.getHigh(), divisor.getLow(), 0);
            if (Decimals.overflows(result)) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
            }
            return result;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Int128 divideLongShortLong(Int128 dividend, long divisor, int rescaleFactor)
    {
        if (divisor == 0) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            Int128 result = divideRoundUp(dividend.getHigh(), dividend.getLow(), rescaleFactor, divisor >> 63, divisor, 0);
            if (Decimals.overflows(result)) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
            }
            return result;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    private static SqlScalarFunction decimalModulusOperator()
    {
        Signature signature = modulusSignatureBuilder()
                .operatorType(MODULUS)
                .build();
        return modulusScalarFunction(signature);
    }

    public static SqlScalarFunction modulusScalarFunction(Signature signature)
    {
        return new PolymorphicScalarFunctionBuilder(DecimalOperators.class)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup
                                .methods("modulusShortShortShort", "modulusLongLongLong", "modulusShortLongLong", "modulusShortLongShort", "modulusLongShortShort", "modulusLongShortLong")
                                .withExtraParameters(DecimalOperators::modulusRescaleParameters)))
                .build();
    }

    public static Signature.Builder modulusSignatureBuilder()
    {
        TypeSignature decimalLeftSignature = new TypeSignature("decimal", typeVariable("a_precision"), typeVariable("a_scale"));
        TypeSignature decimalRightSignature = new TypeSignature("decimal", typeVariable("b_precision"), typeVariable("b_scale"));
        TypeSignature decimalResultSignature = new TypeSignature("decimal", typeVariable("r_precision"), typeVariable("r_scale"));

        return Signature.builder()
                .longVariable("r_precision", "min(b_precision - b_scale, a_precision - a_scale) + max(a_scale, b_scale)")
                .longVariable("r_scale", "max(a_scale, b_scale)")
                .argumentType(decimalLeftSignature)
                .argumentType(decimalRightSignature)
                .returnType(decimalResultSignature);
    }

    private static List<Object> calculateShortRescaleParameters(SpecializeContext context)
    {
        long aRescale = longTenToNth(rescaleFactor(context.getLiteral("a_scale"), context.getLiteral("b_scale")));
        long bRescale = longTenToNth(rescaleFactor(context.getLiteral("b_scale"), context.getLiteral("a_scale")));
        return ImmutableList.of(aRescale, bRescale);
    }

    private static List<Object> calculateLongRescaleParameters(SpecializeContext context)
    {
        long aScale = context.getLiteral("a_scale");
        long bScale = context.getLiteral("b_scale");
        int aRescale = rescaleFactor(aScale, bScale);
        int bRescale = rescaleFactor(bScale, aScale);

        int rescale;
        boolean left;
        if (aRescale == 0) {
            rescale = bRescale;
            left = false;
        }
        else if (bRescale == 0) {
            rescale = aRescale;
            left = true;
        }
        else {
            throw new IllegalStateException();
        }
        return ImmutableList.of(rescale, left);
    }

    private static List<Object> modulusRescaleParameters(PolymorphicScalarFunctionBuilder.SpecializeContext context)
    {
        int dividendScale = toIntExact(requireNonNull(context.getLiteral("a_scale"), "a_scale is null"));
        int divisorScale = toIntExact(requireNonNull(context.getLiteral("b_scale"), "b_scale is null"));
        int dividendRescaleFactor = rescaleFactor(dividendScale, divisorScale);
        int divisorRescaleFactor = rescaleFactor(divisorScale, dividendScale);
        return ImmutableList.of(dividendRescaleFactor, divisorRescaleFactor);
    }

    private static int rescaleFactor(long fromScale, long toScale)
    {
        return max(0, (int) toScale - (int) fromScale);
    }

    @UsedByGeneratedCode
    public static long modulusShortShortShort(long dividend, long divisor, int dividendRescaleFactor, int divisorRescaleFactor)
    {
        if (divisor == 0) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return remainder(dividend >> 63, dividend, dividendRescaleFactor, divisor >> 63, divisor, divisorRescaleFactor).toLongExact();
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static long modulusShortLongShort(long dividend, Int128 divisor, int dividendRescaleFactor, int divisorRescaleFactor)
    {
        if (divisor.isZero()) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return remainder(dividend >> 63, dividend, dividendRescaleFactor, divisor.getHigh(), divisor.getLow(), divisorRescaleFactor).toLongExact();
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static long modulusLongShortShort(Int128 dividend, long divisor, int dividendRescaleFactor, int divisorRescaleFactor)
    {
        if (divisor == 0) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return remainder(dividend.getHigh(), dividend.getLow(), dividendRescaleFactor, divisor >> 63, divisor, divisorRescaleFactor).toLongExact();
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Int128 modulusShortLongLong(long dividend, Int128 divisor, int dividendRescaleFactor, int divisorRescaleFactor)
    {
        if (divisor.isZero()) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return remainder(dividend >> 63, dividend, dividendRescaleFactor, divisor.getHigh(), divisor.getLow(), divisorRescaleFactor);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Int128 modulusLongShortLong(Int128 dividend, long divisor, int dividendRescaleFactor, int divisorRescaleFactor)
    {
        if (divisor == 0) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return remainder(dividend.getHigh(), dividend.getLow(), dividendRescaleFactor, divisor >> 63, divisor, divisorRescaleFactor);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Int128 modulusLongLongLong(Int128 dividend, Int128 divisor, int dividendRescaleFactor, int divisorRescaleFactor)
    {
        if (divisor.isZero()) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return remainder(dividend.getHigh(), dividend.getLow(), dividendRescaleFactor, divisor.getHigh(), divisor.getLow(), divisorRescaleFactor);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @ScalarOperator(NEGATION)
    public static final class Negation
    {
        @LiteralParameters({"p", "s"})
        @SqlType("decimal(p, s)")
        public static long negate(@SqlType("decimal(p, s)") long arg)
        {
            return -arg;
        }

        @LiteralParameters({"p", "s"})
        @SqlType("decimal(p, s)")
        public static Int128 negate(@SqlType("decimal(p, s)") Int128 value)
        {
            return negateExact(value);
        }
    }
}
