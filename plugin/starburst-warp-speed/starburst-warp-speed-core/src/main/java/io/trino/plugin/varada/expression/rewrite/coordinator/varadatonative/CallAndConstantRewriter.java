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

import io.trino.matching.Pattern;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaConstant;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.rewrite.ExpressionPatterns;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.stats.VaradaStatsPushdownPredicates;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.Type;

import java.util.function.BiFunction;

import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argument;
import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argumentCount;
import static io.trino.plugin.varada.expression.rewrite.coordinator.varadatonative.VariableRewriter.calcPredicateType;

class CallAndConstantRewriter
        extends BaseOperatorRewriter
{
    private static final Pattern<VaradaCall> PATTERN = ExpressionPatterns.call()
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(x -> x instanceof VaradaCall))
            .with(argument(1).matching(x -> x instanceof VaradaConstant));

    CallAndConstantRewriter(NativeExpressionRulesHandler nativeExpressionRulesHandler,
            VaradaStatsPushdownPredicates varadaStatsPushdownPredicates)
    {
        super(nativeExpressionRulesHandler, varadaStatsPushdownPredicates);
    }

    @Override
    public Pattern<VaradaCall> getPattern()
    {
        return PATTERN;
    }

    @Override
    boolean convert(VaradaExpression varadaExpression,
            RewriteContext rewriteContext,
            BiFunction<Type, Object, Range> rangeBiFunction)
    {
        if (rewriteContext.nativeExpressionBuilder().getDomain() != null) {
            varadaStatsPushdownPredicates.incunsupported_functions_composite();
            //currently, not supported complex expression. etc: where (ceil(c1) > 5) = false
            return false;
        }
        VaradaConstant varadaConstant = ((VaradaConstant) varadaExpression.getChildren().get(1));
        Type constantType = varadaConstant.getType();
        String functionName = ((VaradaCall) varadaExpression).getFunctionName();
        PredicateType predicateType = calcPredicateType(constantType, functionName);

        Domain domain = convertConstantToDomain(varadaConstant, rangeBiFunction);
        NativeExpression.Builder nativeExpressionBuilder = rewriteContext.nativeExpressionBuilder();
        nativeExpressionBuilder.domain(domain)
                .predicateType(predicateType)
                .collectNulls(domain.isNullAllowed());
        return nativeExpressionRulesHandler.rewrite(varadaExpression.getChildren().get(0), rewriteContext);
    }
}
