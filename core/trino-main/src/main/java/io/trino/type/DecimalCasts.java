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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.PolymorphicScalarFunctionBuilder;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DecimalConversions;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Int128Math;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import io.trino.util.JsonCastException;

import java.io.IOException;
import java.math.BigDecimal;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.scalar.JsonOperators.JSON_FACTORY;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Decimals.isShortDecimal;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.Int128.ZERO;
import static io.trino.spi.type.Int128Math.multiply;
import static io.trino.spi.type.Int128Math.rescale;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static io.trino.spi.type.VarcharType.UNBOUNDED_LENGTH;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.JsonUtil.createJsonGenerator;
import static io.trino.util.JsonUtil.createJsonParser;
import static io.trino.util.JsonUtil.currentTokenAsLongDecimal;
import static io.trino.util.JsonUtil.currentTokenAsShortDecimal;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class DecimalCasts
{
    public static final SqlScalarFunction DECIMAL_TO_BOOLEAN_CAST = castFunctionFromDecimalTo(BOOLEAN.getTypeSignature(), "shortDecimalToBoolean", "longDecimalToBoolean");
    public static final SqlScalarFunction BOOLEAN_TO_DECIMAL_CAST = castFunctionToDecimalFrom(BOOLEAN.getTypeSignature(), "booleanToShortDecimal", "booleanToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_BIGINT_CAST = castFunctionFromDecimalTo(BIGINT.getTypeSignature(), "shortDecimalToBigint", "longDecimalToBigint");
    public static final SqlScalarFunction BIGINT_TO_DECIMAL_CAST = castFunctionToDecimalFrom(BIGINT.getTypeSignature(), "bigintToShortDecimal", "bigintToLongDecimal");
    public static final SqlScalarFunction INTEGER_TO_DECIMAL_CAST = castFunctionToDecimalFrom(INTEGER.getTypeSignature(), "integerToShortDecimal", "integerToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_INTEGER_CAST = castFunctionFromDecimalTo(INTEGER.getTypeSignature(), "shortDecimalToInteger", "longDecimalToInteger");
    public static final SqlScalarFunction SMALLINT_TO_DECIMAL_CAST = castFunctionToDecimalFrom(SMALLINT.getTypeSignature(), "smallintToShortDecimal", "smallintToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_SMALLINT_CAST = castFunctionFromDecimalTo(SMALLINT.getTypeSignature(), "shortDecimalToSmallint", "longDecimalToSmallint");
    public static final SqlScalarFunction TINYINT_TO_DECIMAL_CAST = castFunctionToDecimalFrom(TINYINT.getTypeSignature(), "tinyintToShortDecimal", "tinyintToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_TINYINT_CAST = castFunctionFromDecimalTo(TINYINT.getTypeSignature(), "shortDecimalToTinyint", "longDecimalToTinyint");
    public static final SqlScalarFunction DECIMAL_TO_DOUBLE_CAST = castFunctionFromDecimalTo(DOUBLE.getTypeSignature(), "shortDecimalToDouble", "longDecimalToDouble");
    public static final SqlScalarFunction DOUBLE_TO_DECIMAL_CAST = castFunctionToDecimalFrom(DOUBLE.getTypeSignature(), "doubleToShortDecimal", "doubleToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_REAL_CAST = castFunctionFromDecimalTo(REAL.getTypeSignature(), "shortDecimalToReal", "longDecimalToReal");
    public static final SqlScalarFunction REAL_TO_DECIMAL_CAST = castFunctionToDecimalFrom(REAL.getTypeSignature(), "realToShortDecimal", "realToLongDecimal");
    public static final SqlScalarFunction VARCHAR_TO_DECIMAL_CAST = castFunctionToDecimalFrom(new TypeSignature("varchar", typeVariable("x")), "varcharToShortDecimal", "varcharToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_JSON_CAST = castFunctionFromDecimalTo(JSON.getTypeSignature(), "shortDecimalToJson", "longDecimalToJson");
    public static final SqlScalarFunction JSON_TO_DECIMAL_CAST = castFunctionToDecimalFromBuilder(JSON.getTypeSignature(), true, "jsonToShortDecimal", "jsonToLongDecimal");

    private static SqlScalarFunction castFunctionFromDecimalTo(TypeSignature to, String... methodNames)
    {
        Signature signature = Signature.builder()
                .operatorType(CAST)
                .argumentTypes(new TypeSignature("decimal", typeVariable("precision"), typeVariable("scale")))
                .returnType(to)
                .build();
        return new PolymorphicScalarFunctionBuilder(DecimalCasts.class)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup
                                .methods(methodNames)
                                .withExtraParameters((context) -> {
                                    long precision = context.getLiteral("precision");
                                    long scale = context.getLiteral("scale");
                                    Object tenToScale;
                                    if (isShortDecimal(context.getParameterTypes().get(0))) {
                                        tenToScale = longTenToNth(DecimalConversions.intScale(scale));
                                    }
                                    else {
                                        tenToScale = Int128Math.POWERS_OF_TEN[DecimalConversions.intScale(scale)];
                                    }
                                    return ImmutableList.of(precision, scale, tenToScale);
                                })))
                .build();
    }

    private static SqlScalarFunction castFunctionToDecimalFrom(TypeSignature from, String... methodNames)
    {
        return castFunctionToDecimalFromBuilder(from, false, methodNames);
    }

    private static SqlScalarFunction castFunctionToDecimalFromBuilder(TypeSignature from, boolean nullableResult, String... methodNames)
    {
        Signature signature = Signature.builder()
                .operatorType(CAST)
                .argumentTypes(from)
                .returnType(new TypeSignature("decimal", typeVariable("precision"), typeVariable("scale")))
                .build();
        return new PolymorphicScalarFunctionBuilder(DecimalCasts.class)
                .signature(signature)
                .nullableResult(nullableResult)
                .deterministic(true)
                .choice(choice -> choice
                        .returnConvention(nullableResult ? NULLABLE_RETURN : FAIL_ON_NULL)
                        .implementation(methodsGroup -> methodsGroup
                                .methods(methodNames)
                                .withExtraParameters((context) -> {
                                    DecimalType resultType = (DecimalType) context.getReturnType();
                                    Object tenToScale;
                                    if (isShortDecimal(resultType)) {
                                        tenToScale = longTenToNth(resultType.getScale());
                                    }
                                    else {
                                        tenToScale = Int128Math.POWERS_OF_TEN[resultType.getScale()];
                                    }
                                    return ImmutableList.of(resultType.getPrecision(), resultType.getScale(), tenToScale);
                                }))).build();
    }

    public static final SqlScalarFunction DECIMAL_TO_VARCHAR_CAST = new PolymorphicScalarFunctionBuilder(DecimalCasts.class)
            .signature(Signature.builder()
                    .operatorType(CAST)
                    .argumentTypes(new TypeSignature("decimal", typeVariable("precision"), typeVariable("scale")))
                    .returnType(new TypeSignature("varchar", typeVariable("x")))
                    .build())
            .deterministic(true)
            .choice(choice -> choice
                    .implementation(methodsGroup -> methodsGroup
                            .methods("shortDecimalToVarchar", "longDecimalToVarchar")
                            .withExtraParameters((context) -> {
                                long scale = context.getLiteral("scale");
                                VarcharType resultType = (VarcharType) context.getReturnType();
                                long length;
                                if (resultType.isUnbounded()) {
                                    length = UNBOUNDED_LENGTH;
                                }
                                else {
                                    length = resultType.getBoundedLength();
                                }
                                return ImmutableList.of(scale, length);
                            })))
            .build();

    private DecimalCasts() {}

    @UsedByGeneratedCode
    public static boolean shortDecimalToBoolean(long decimal, long precision, long scale, long tenToScale)
    {
        return decimal != 0;
    }

    @UsedByGeneratedCode
    public static boolean longDecimalToBoolean(Int128 decimal, long precision, long scale, Int128 tenToScale)
    {
        return !decimal.isZero();
    }

    @UsedByGeneratedCode
    public static long booleanToShortDecimal(boolean value, long precision, long scale, long tenToScale)
    {
        return value ? tenToScale : 0;
    }

    @UsedByGeneratedCode
    public static Int128 booleanToLongDecimal(boolean value, long precision, long scale, Int128 tenToScale)
    {
        return value ? tenToScale : ZERO;
    }

    @UsedByGeneratedCode
    public static long shortDecimalToBigint(long decimal, long precision, long scale, long tenToScale)
    {
        // this rounds the decimal value to the nearest integral value
        if (decimal >= 0) {
            return (decimal + tenToScale / 2) / tenToScale;
        }
        return -((-decimal + tenToScale / 2) / tenToScale);
    }

    @UsedByGeneratedCode
    public static long longDecimalToBigint(Int128 decimal, long precision, long scale, Int128 tenToScale)
    {
        try {
            return rescale(decimal, DecimalConversions.intScale(-scale)).toLongExact();
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to BIGINT", Decimals.toString(decimal, DecimalConversions.intScale(scale))));
        }
    }

    @UsedByGeneratedCode
    public static long bigintToShortDecimal(long value, long precision, long scale, long tenToScale)
    {
        try {
            long decimal = multiplyExact(value, tenToScale);
            if (overflows(decimal, DecimalConversions.intScale(precision))) {
                throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast BIGINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast BIGINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static Int128 bigintToLongDecimal(long value, long precision, long scale, Int128 tenToScale)
    {
        try {
            Int128 result = multiply(tenToScale, value);
            if (Decimals.overflows(result, (int) precision)) {
                throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast BIGINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return result;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast BIGINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static long shortDecimalToInteger(long decimal, long precision, long scale, long tenToScale)
    {
        // this rounds the decimal value to the nearest integral value
        long longResult = (decimal + tenToScale / 2) / tenToScale;
        if (decimal < 0) {
            longResult = -((-decimal + tenToScale / 2) / tenToScale);
        }

        try {
            return toIntExact(longResult);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to INTEGER", longResult));
        }
    }

    @UsedByGeneratedCode
    public static long longDecimalToInteger(Int128 decimal, long precision, long scale, Int128 tenToScale)
    {
        try {
            return toIntExact(rescale(decimal, DecimalConversions.intScale(-scale)).toLongExact());
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to INTEGER", Decimals.toString(decimal, DecimalConversions.intScale(scale))));
        }
    }

    @UsedByGeneratedCode
    public static long integerToShortDecimal(long value, long precision, long scale, long tenToScale)
    {
        try {
            long decimal = multiplyExact(value, tenToScale);
            if (overflows(decimal, DecimalConversions.intScale(precision))) {
                throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast INTEGER '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast INTEGER '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static Int128 integerToLongDecimal(long value, long precision, long scale, Int128 tenToScale)
    {
        try {
            Int128 result = multiply(tenToScale, value);
            if (Decimals.overflows(result, (int) precision)) {
                throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast INTEGER '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return result;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast INTEGER '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static long shortDecimalToSmallint(long decimal, long precision, long scale, long tenToScale)
    {
        // this rounds the decimal value to the nearest integral value
        long longResult = (decimal + tenToScale / 2) / tenToScale;
        if (decimal < 0) {
            longResult = -((-decimal + tenToScale / 2) / tenToScale);
        }

        try {
            return Shorts.checkedCast(longResult);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to SMALLINT", longResult));
        }
    }

    @UsedByGeneratedCode
    public static long longDecimalToSmallint(Int128 decimal, long precision, long scale, Int128 tenToScale)
    {
        try {
            Int128 decimal1 = rescale(decimal, DecimalConversions.intScale(-scale));
            return Shorts.checkedCast(decimal1.toLongExact());
        }
        catch (ArithmeticException | IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to SMALLINT", Decimals.toString(decimal, DecimalConversions.intScale(scale))));
        }
    }

    @UsedByGeneratedCode
    public static long smallintToShortDecimal(long value, long precision, long scale, long tenToScale)
    {
        try {
            long decimal = multiplyExact(value, tenToScale);
            if (overflows(decimal, DecimalConversions.intScale(precision))) {
                throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast SMALLINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast SMALLINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static Int128 smallintToLongDecimal(long value, long precision, long scale, Int128 tenToScale)
    {
        try {
            Int128 result = multiply(tenToScale, value);
            if (Decimals.overflows(result, (int) precision)) {
                throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast SMALLINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return result;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast SMALLINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static long shortDecimalToTinyint(long decimal, long precision, long scale, long tenToScale)
    {
        // this rounds the decimal value to the nearest integral value
        long longResult = (decimal + tenToScale / 2) / tenToScale;
        if (decimal < 0) {
            longResult = -((-decimal + tenToScale / 2) / tenToScale);
        }

        try {
            return SignedBytes.checkedCast(longResult);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to TINYINT", longResult));
        }
    }

    @UsedByGeneratedCode
    public static long longDecimalToTinyint(Int128 decimal, long precision, long scale, Int128 tenToScale)
    {
        try {
            return SignedBytes.checkedCast(rescale(decimal, DecimalConversions.intScale(-scale)).toLongExact());
        }
        catch (ArithmeticException | IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to TINYINT", Decimals.toString(decimal, DecimalConversions.intScale(scale))));
        }
    }

    @UsedByGeneratedCode
    public static long tinyintToShortDecimal(long value, long precision, long scale, long tenToScale)
    {
        try {
            long decimal = multiplyExact(value, tenToScale);
            if (overflows(decimal, DecimalConversions.intScale(precision))) {
                throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast TINYINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast TINYINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static Int128 tinyintToLongDecimal(long value, long precision, long scale, Int128 tenToScale)
    {
        try {
            Int128 result = multiply(tenToScale, value);
            if (Decimals.overflows(result, (int) precision)) {
                throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast TINYINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return result;
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast TINYINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static double shortDecimalToDouble(long decimal, long precision, long scale, long tenToScale)
    {
        return ((double) decimal) / tenToScale;
    }

    @UsedByGeneratedCode
    public static double longDecimalToDouble(Int128 decimal, long precision, long scale, Int128 tenToScale)
    {
        return DecimalConversions.longDecimalToDouble(decimal, scale);
    }

    @UsedByGeneratedCode
    public static long shortDecimalToReal(long decimal, long precision, long scale, long tenToScale)
    {
        return DecimalConversions.shortDecimalToReal(decimal, tenToScale);
    }

    @UsedByGeneratedCode
    public static long longDecimalToReal(Int128 decimal, long precision, long scale, Int128 tenToScale)
    {
        return DecimalConversions.longDecimalToReal(decimal, scale);
    }

    @UsedByGeneratedCode
    public static long doubleToShortDecimal(double value, long precision, long scale, long tenToScale)
    {
        return DecimalConversions.doubleToShortDecimal(value, precision, scale);
    }

    @UsedByGeneratedCode
    public static Int128 doubleToLongDecimal(double value, long precision, long scale, Int128 tenToScale)
    {
        return DecimalConversions.doubleToLongDecimal(value, precision, scale);
    }

    @UsedByGeneratedCode
    public static long realToShortDecimal(long value, long precision, long scale, long tenToScale)
    {
        return DecimalConversions.realToShortDecimal(value, precision, scale);
    }

    @UsedByGeneratedCode
    public static Int128 realToLongDecimal(long value, long precision, long scale, Int128 tenToScale)
    {
        return DecimalConversions.realToLongDecimal(value, precision, scale);
    }

    @UsedByGeneratedCode
    public static Slice shortDecimalToVarchar(long decimal, long scale, long varcharLength)
    {
        String stringValue = Decimals.toString(decimal, DecimalConversions.intScale(scale));
        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= varcharLength) {
            return utf8Slice(stringValue);
        }

        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s cannot be represented as varchar(%s)", stringValue, varcharLength));
    }

    @UsedByGeneratedCode
    public static Slice longDecimalToVarchar(Int128 decimal, long scale, long varcharLength)
    {
        String stringValue = Decimals.toString(decimal, DecimalConversions.intScale(scale));
        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= varcharLength) {
            return utf8Slice(stringValue);
        }

        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s cannot be represented as varchar(%s)", stringValue, varcharLength));
    }

    @UsedByGeneratedCode
    public static long varcharToShortDecimal(Slice value, long precision, long scale, long tenToScale)
    {
        BigDecimal result;
        String stringValue = value.toString(UTF_8);
        try {
            result = new BigDecimal(stringValue).setScale(DecimalConversions.intScale(scale), HALF_UP);
        }
        catch (NumberFormatException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s). Value is not a number.", stringValue, precision, scale));
        }

        if (overflows(result, precision)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s). Value too large.", stringValue, precision, scale));
        }

        return result.unscaledValue().longValue();
    }

    @UsedByGeneratedCode
    public static Int128 varcharToLongDecimal(Slice value, long precision, long scale, Int128 tenToScale)
    {
        BigDecimal result;
        String stringValue = value.toString(UTF_8);
        try {
            result = new BigDecimal(stringValue).setScale(DecimalConversions.intScale(scale), HALF_UP);
        }
        catch (NumberFormatException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s). Value is not a number.", stringValue, precision, scale));
        }

        if (overflows(result, precision)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s). Value too large.", stringValue, precision, scale));
        }

        return Int128.valueOf(result.unscaledValue());
    }

    @UsedByGeneratedCode
    public static Slice shortDecimalToJson(long decimal, long precision, long scale, long tenToScale)
    {
        return decimalToJson(BigDecimal.valueOf(decimal, DecimalConversions.intScale(scale)));
    }

    @UsedByGeneratedCode
    public static Slice longDecimalToJson(Int128 decimal, long precision, long scale, Int128 tenToScale)
    {
        return decimalToJson(new BigDecimal(decimal.toBigInteger(), DecimalConversions.intScale(scale)));
    }

    private static Slice decimalToJson(BigDecimal bigDecimal)
    {
        try {
            SliceOutput dynamicSliceOutput = new DynamicSliceOutput(32);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, dynamicSliceOutput)) {
                jsonGenerator.writeNumber(bigDecimal);
            }
            return dynamicSliceOutput.slice();
        }
        catch (IOException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%f' to %s", bigDecimal, StandardTypes.JSON));
        }
    }

    @UsedByGeneratedCode
    public static Int128 jsonToLongDecimal(Slice json, long precision, long scale, Int128 tenToScale)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            Int128 result = currentTokenAsLongDecimal(parser, intPrecision(precision), DecimalConversions.intScale(scale));
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to DECIMAL(%s,%s)", precision, scale); // check no trailing token
            return result;
        }
        catch (IOException | NumberFormatException | JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to DECIMAL(%s,%s)", json.toStringUtf8(), precision, scale), e);
        }
    }

    @UsedByGeneratedCode
    public static Long jsonToShortDecimal(Slice json, long precision, long scale, long tenToScale)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            Long result = currentTokenAsShortDecimal(parser, intPrecision(precision), DecimalConversions.intScale(scale));
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to DECIMAL(%s,%s)", precision, scale); // check no trailing token
            return result;
        }
        catch (IOException | NumberFormatException | JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to DECIMAL(%s,%s)", json.toStringUtf8(), precision, scale), e);
        }
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static int intPrecision(long precision)
    {
        return (int) precision;
    }
}
