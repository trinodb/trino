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
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import static io.trino.spi.function.OperatorType.SATURATED_FLOOR_CAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Int128Math.POWERS_OF_TEN;
import static io.trino.spi.type.Int128Math.floorDiv;
import static io.trino.spi.type.Int128Math.multiply;
import static io.trino.spi.type.Int128Math.negate;
import static io.trino.spi.type.Int128Math.subtract;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static java.lang.Math.toIntExact;

public final class DecimalSaturatedFloorCasts
{
    private DecimalSaturatedFloorCasts() {}

    public static final SqlScalarFunction DECIMAL_TO_DECIMAL_SATURATED_FLOOR_CAST = new PolymorphicScalarFunctionBuilder(DecimalSaturatedFloorCasts.class)
            .signature(Signature.builder()
                    .operatorType(SATURATED_FLOOR_CAST)
                    .argumentTypes(new TypeSignature("decimal", typeVariable("source_precision"), typeVariable("source_scale")))
                    .returnType(new TypeSignature("decimal", typeVariable("result_precision"), typeVariable("result_scale")))
                    .build())
            .deterministic(true)
            .choice(choice -> choice
                    .implementation(methodsGroup -> methodsGroup
                            .methods("shortDecimalToShortDecimal", "shortDecimalToLongDecimal", "longDecimalToShortDecimal", "longDecimalToLongDecimal")
                            .withExtraParameters((context) -> {
                                int sourcePrecision = toIntExact(context.getLiteral("source_precision"));
                                int sourceScale = toIntExact(context.getLiteral("source_scale"));
                                int resultPrecision = toIntExact(context.getLiteral("result_precision"));
                                int resultScale = toIntExact(context.getLiteral("result_scale"));
                                return ImmutableList.of(sourcePrecision, sourceScale, resultPrecision, resultScale);
                            })))
            .build();

    @UsedByGeneratedCode
    public static long shortDecimalToShortDecimal(long value, int sourcePrecision, int sourceScale, int resultPrecision, int resultScale)
    {
        return saturatedCast(Int128.valueOf(value), sourceScale, resultPrecision, resultScale).toLongExact();
    }

    @UsedByGeneratedCode
    public static Int128 shortDecimalToLongDecimal(long value, int sourcePrecision, int sourceScale, int resultPrecision, int resultScale)
    {
        return saturatedCast(Int128.valueOf(value), sourceScale, resultPrecision, resultScale);
    }

    @UsedByGeneratedCode
    public static long longDecimalToShortDecimal(Int128 value, int sourcePrecision, int sourceScale, int resultPrecision, int resultScale)
    {
        return saturatedCast(value, sourceScale, resultPrecision, resultScale).toLongExact();
    }

    @UsedByGeneratedCode
    public static Int128 longDecimalToLongDecimal(Int128 value, int sourcePrecision, int sourceScale, int resultPrecision, int resultScale)
    {
        return saturatedCast(value, sourceScale, resultPrecision, resultScale);
    }

    private static Int128 saturatedCast(Int128 value, int sourceScale, int resultPrecision, int resultScale)
    {
        int scale = resultScale - sourceScale;
        if (scale > 0) {
            value = multiply(value, POWERS_OF_TEN[scale]);
        }
        else if (scale < 0) {
            value = floorDiv(value, POWERS_OF_TEN[-scale]);
        }

        Int128 maxUnscaledValue = subtract(POWERS_OF_TEN[resultPrecision], Int128.ONE);
        if (value.compareTo(maxUnscaledValue) > 0) {
            return maxUnscaledValue;
        }
        Int128 minUnscaledValue = negate(maxUnscaledValue);
        if (value.compareTo(minUnscaledValue) < 0) {
            return minUnscaledValue;
        }
        return value;
    }

    public static final SqlScalarFunction DECIMAL_TO_BIGINT_SATURATED_FLOOR_CAST = decimalToGenericIntegerTypeSaturatedFloorCast(BIGINT, Long.MIN_VALUE, Long.MAX_VALUE);
    public static final SqlScalarFunction DECIMAL_TO_INTEGER_SATURATED_FLOOR_CAST = decimalToGenericIntegerTypeSaturatedFloorCast(INTEGER, Integer.MIN_VALUE, Integer.MAX_VALUE);
    public static final SqlScalarFunction DECIMAL_TO_SMALLINT_SATURATED_FLOOR_CAST = decimalToGenericIntegerTypeSaturatedFloorCast(SMALLINT, Short.MIN_VALUE, Short.MAX_VALUE);
    public static final SqlScalarFunction DECIMAL_TO_TINYINT_SATURATED_FLOOR_CAST = decimalToGenericIntegerTypeSaturatedFloorCast(TINYINT, Byte.MIN_VALUE, Byte.MAX_VALUE);

    private static SqlScalarFunction decimalToGenericIntegerTypeSaturatedFloorCast(Type type, long minValue, long maxValue)
    {
        return new PolymorphicScalarFunctionBuilder(DecimalSaturatedFloorCasts.class)
                .signature(Signature.builder()
                        .operatorType(SATURATED_FLOOR_CAST)
                        .argumentTypes(new TypeSignature("decimal", typeVariable("source_precision"), typeVariable("source_scale")))
                        .returnType(type.getTypeSignature())
                        .build())
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup
                                .methods("shortDecimalToGenericIntegerType", "longDecimalToGenericIntegerType")
                                .withExtraParameters((context) -> {
                                    int sourceScale = toIntExact(context.getLiteral("source_scale"));
                                    return ImmutableList.of(sourceScale, minValue, maxValue);
                                })))
                .build();
    }

    @UsedByGeneratedCode
    public static long shortDecimalToGenericIntegerType(long value, int sourceScale, long minValue, long maxValue)
    {
        return saturatedCast(Int128.valueOf(value), sourceScale, minValue, maxValue);
    }

    @UsedByGeneratedCode
    public static long longDecimalToGenericIntegerType(Int128 value, int sourceScale, long minValue, long maxValue)
    {
        return saturatedCast(value, sourceScale, minValue, maxValue);
    }

    private static long saturatedCast(Int128 value, int sourceScale, long minValue, long maxValue)
    {
        if (sourceScale > 0) {
            value = floorDiv(value, POWERS_OF_TEN[sourceScale]);
        }

        if (value.compareTo(Int128.valueOf(maxValue)) > 0) {
            return maxValue;
        }
        if (value.compareTo(Int128.valueOf(minValue)) < 0) {
            return minValue;
        }
        return value.toLongExact();
    }

    public static final SqlScalarFunction BIGINT_TO_DECIMAL_SATURATED_FLOOR_CAST = genericIntegerTypeToDecimalSaturatedFloorCast(BIGINT);
    public static final SqlScalarFunction INTEGER_TO_DECIMAL_SATURATED_FLOOR_CAST = genericIntegerTypeToDecimalSaturatedFloorCast(INTEGER);
    public static final SqlScalarFunction SMALLINT_TO_DECIMAL_SATURATED_FLOOR_CAST = genericIntegerTypeToDecimalSaturatedFloorCast(SMALLINT);
    public static final SqlScalarFunction TINYINT_TO_DECIMAL_SATURATED_FLOOR_CAST = genericIntegerTypeToDecimalSaturatedFloorCast(TINYINT);

    private static SqlScalarFunction genericIntegerTypeToDecimalSaturatedFloorCast(Type integerType)
    {
        return new PolymorphicScalarFunctionBuilder(DecimalSaturatedFloorCasts.class)
                .signature(Signature.builder()
                        .operatorType(SATURATED_FLOOR_CAST)
                        .argumentTypes(integerType.getTypeSignature())
                        .returnType(new TypeSignature("decimal", typeVariable("result_precision"), typeVariable("result_scale")))
                        .build())
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup
                                .methods("genericIntegerTypeToShortDecimal", "genericIntegerTypeToLongDecimal")
                                .withExtraParameters((context) -> {
                                    int resultPrecision = toIntExact(context.getLiteral("result_precision"));
                                    int resultScale = toIntExact(context.getLiteral("result_scale"));
                                    return ImmutableList.of(resultPrecision, resultScale);
                                })))
                .build();
    }

    @UsedByGeneratedCode
    public static long genericIntegerTypeToShortDecimal(long value, int resultPrecision, int resultScale)
    {
        return saturatedCast(Int128.valueOf(value), 0, resultPrecision, resultScale).toLongExact();
    }

    @UsedByGeneratedCode
    public static Int128 genericIntegerTypeToLongDecimal(long value, int resultPrecision, int resultScale)
    {
        return saturatedCast(Int128.valueOf(value), 0, resultPrecision, resultScale);
    }
}
