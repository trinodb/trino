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

import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaConstant;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.warp.gen.stats.VaradaStatsPushdownPredicates;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;

import java.util.function.BiFunction;

abstract class BaseOperatorRewriter
        implements ExpressionRewriter<VaradaCall>
{
    static final BiFunction<Type, Object, Range> EQUAL_FUNCTION = Range::equal;
    static final BiFunction<Type, Object, Range> GREATER_THAN_FUNCTION = Range::greaterThan;
    static final BiFunction<Type, Object, Range> GREATER_THAN_OR_EQUAL_FUNCTION = Range::greaterThanOrEqual;
    static final BiFunction<Type, Object, Range> LESS_THAN_FUNCTION = Range::lessThan;
    static final BiFunction<Type, Object, Range> LESS_THAN_OR_EQUAL_FUNCTION = Range::lessThanOrEqual;
    final NativeExpressionRulesHandler nativeExpressionRulesHandler;
    final VaradaStatsPushdownPredicates varadaStatsPushdownPredicates;

    BaseOperatorRewriter(NativeExpressionRulesHandler nativeExpressionRulesHandler,
            VaradaStatsPushdownPredicates varadaStatsPushdownPredicates)
    {
        this.nativeExpressionRulesHandler = nativeExpressionRulesHandler;
        this.varadaStatsPushdownPredicates = varadaStatsPushdownPredicates;
    }

    boolean greaterThan(VaradaExpression varadaExpression, RewriteContext rewriteContext)
    {
        return convert(varadaExpression,
                rewriteContext,
                GREATER_THAN_FUNCTION);
    }

    boolean greaterThanOrEqual(VaradaExpression varadaExpression, RewriteContext rewriteContext)
    {
        return convert(varadaExpression,
                rewriteContext,
                GREATER_THAN_OR_EQUAL_FUNCTION);
    }

    boolean equal(VaradaExpression varadaExpression, RewriteContext rewriteContext)
    {
        VaradaConstant varadaConstant = ((VaradaConstant) varadaExpression.getChildren().get(1));
        Type constantType = varadaConstant.getType();
        if (constantType == BooleanType.BOOLEAN && varadaConstant.getValue() == Boolean.FALSE) {
            rewriteContext.customStats().compute("unsupported_functions_native", (key, value) -> value == null ? 1L : value + 1);
            varadaStatsPushdownPredicates.incunsupported_functions_native();
            return false;
        }
        return convert(varadaExpression,
                rewriteContext,
                EQUAL_FUNCTION);
    }

    boolean lessThanOrEqual(VaradaExpression varadaExpression, RewriteContext rewriteContext)
    {
        return convert(varadaExpression,
                rewriteContext,
                LESS_THAN_OR_EQUAL_FUNCTION);
    }

    boolean lessThan(VaradaExpression varadaExpression, RewriteContext rewriteContext)
    {
        return convert(varadaExpression,
                rewriteContext,
                LESS_THAN_FUNCTION);
    }

    abstract boolean convert(VaradaExpression varadaExpression,
            RewriteContext rewriteContext,
            BiFunction<Type, Object, Range> rangeBiFunction);
}
