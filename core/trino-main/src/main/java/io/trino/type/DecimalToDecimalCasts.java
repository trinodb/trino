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
import io.trino.metadata.PolymorphicScalarFunctionBuilder;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.function.Signature;
import io.trino.spi.type.DecimalConversions;
import io.trino.spi.type.DecimalType;

import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.parseTypeTemplate;

public final class DecimalToDecimalCasts
{
    public static final Signature SIGNATURE = Signature.builder()
            .argumentType(parseTypeTemplate("decimal(from_precision,from_scale)", Set.of(), Set.of("from_precision", "from_scale")))
            .returnType(parseTypeTemplate("decimal(to_precision,to_scale)", Set.of(), Set.of("to_precision", "to_scale")))
            .build();

    // TODO: filtering mechanism could be used to return NoOp method when only precision is increased
    public static final SqlScalarFunction DECIMAL_TO_DECIMAL_CAST = new PolymorphicScalarFunctionBuilder(CAST, DecimalConversions.class)
            .signature(SIGNATURE)
            .deterministic(true)
            .neverFails(boundSignature -> {
                DecimalType source = (DecimalType) getOnlyElement(boundSignature.getArgumentTypes());
                DecimalType target = (DecimalType) boundSignature.getReturnType();
                // When target scale shrinks, rounding can push the result up by one integer digit (e.g. 99.9 → 100).
                int requiredIntegerDigits = source.getPrecision() - source.getScale()
                        + (target.getScale() < source.getScale() ? 1 : 0);
                return target.getPrecision() - target.getScale() >= requiredIntegerDigits;
            })
            .choice(choice -> choice
                    .implementation(methodsGroup -> methodsGroup
                            .methods("shortToShortCast")
                            .withExtraParameters(context -> {
                                DecimalType argumentType = (DecimalType) context.getParameterTypes().get(0);
                                DecimalType resultType = (DecimalType) context.getReturnType();
                                long rescale = longTenToNth(Math.abs(resultType.getScale() - argumentType.getScale()));
                                return ImmutableList.of(
                                        argumentType.getPrecision(),
                                        argumentType.getScale(),
                                        resultType.getPrecision(),
                                        resultType.getScale(),
                                        rescale,
                                        rescale / 2);
                            }))
                    .implementation(methodsGroup -> methodsGroup
                            .methods("shortToLongCast", "longToShortCast", "longToLongCast")
                            .withExtraParameters(context -> {
                                DecimalType argumentType = (DecimalType) context.getParameterTypes().get(0);
                                DecimalType resultType = (DecimalType) context.getReturnType();
                                return ImmutableList.of(
                                        argumentType.getPrecision(),
                                        argumentType.getScale(),
                                        resultType.getPrecision(),
                                        resultType.getScale());
                            })))
            .build();

    private DecimalToDecimalCasts() {}
}
