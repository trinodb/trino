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
package io.trino.plugin.varada.expression.rewrite.coordinator.varadatonative;

import io.airlift.log.Logger;
import io.trino.matching.Pattern;
import io.trino.plugin.varada.expression.VaradaConstant;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;

import java.util.function.BiFunction;

interface ExpressionRewriter<T extends VaradaExpression>
{
    Logger logger = Logger.get(ExpressionRewriter.class);

    Pattern<T> getPattern();

    default Domain convertConstantToDomain(VaradaConstant varadaConstant, BiFunction<Type, Object, Range> rangeBiFunction)
    {
        Type type = varadaConstant.getType();
        try {
            Object value = varadaConstant.getValue();
            Range range = rangeBiFunction.apply(type, value);
            return Domain.create(ValueSet.ofRanges(range), false);
        }
        catch (Exception e) {
            logger.error("failed to get create domain from constant  varadaConstant=%s, type=%s", varadaConstant, type);
            throw new UnsupportedOperationException(e);
        }
    }
}
