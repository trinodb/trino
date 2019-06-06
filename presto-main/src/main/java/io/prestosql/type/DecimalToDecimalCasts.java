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
package io.prestosql.type;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.type.DecimalConversions;
import io.prestosql.spi.type.DecimalType;

import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.Signature.withVariadicBound;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.Decimals.longTenToNth;
import static io.prestosql.spi.type.StandardTypes.DECIMAL;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;

public final class DecimalToDecimalCasts
{
    public static final Signature SIGNATURE = Signature.builder()
            .kind(SCALAR)
            .operatorType(CAST)
            .typeVariableConstraints(withVariadicBound("F", DECIMAL), withVariadicBound("T", DECIMAL))
            .argumentTypes(parseTypeSignature("F"))
            .returnType(parseTypeSignature("T"))
            .build();

    // TODO: filtering mechanism could be used to return NoOp method when only precision is increased
    public static final SqlScalarFunction DECIMAL_TO_DECIMAL_CAST = SqlScalarFunction.builder(DecimalConversions.class)
            .signature(SIGNATURE)
            .deterministic(true)
            .choice(choice -> choice
                    .implementation(methodsGroup -> methodsGroup
                            .methods("shortToShortCast")
                            .withExtraParameters((context) -> {
                                DecimalType argumentType = (DecimalType) context.getType("F");
                                DecimalType resultType = (DecimalType) context.getType("T");
                                long rescale = longTenToNth(Math.abs(resultType.getScale() - argumentType.getScale()));
                                return ImmutableList.of(
                                        argumentType.getPrecision(), argumentType.getScale(),
                                        resultType.getPrecision(), resultType.getScale(),
                                        rescale, rescale / 2);
                            }))
                    .implementation(methodsGroup -> methodsGroup
                            .methods("shortToLongCast", "longToShortCast", "longToLongCast")
                            .withExtraParameters((context) -> {
                                DecimalType argumentType = (DecimalType) context.getType("F");
                                DecimalType resultType = (DecimalType) context.getType("T");
                                return ImmutableList.of(
                                        argumentType.getPrecision(), argumentType.getScale(),
                                        resultType.getPrecision(), resultType.getScale());
                            })))
            .build();

    private DecimalToDecimalCasts() {}
}
