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
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.rewrite.ExpressionPatterns;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.stats.VaradaStatsPushdownPredicates;
import io.trino.spi.predicate.Domain;

import java.util.List;

import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argumentCount;

class AndOrRewriter
        implements ExpressionRewriter<VaradaCall>
{
    private static final Pattern<VaradaCall> PATTERN = ExpressionPatterns.call()
            .with(argumentCount().matching(x -> x >= 2));
    private final NativeExpressionRulesHandler nativeExpressionRulesHandler;
    private final VaradaStatsPushdownPredicates varadaStatsPushdownPredicates;

    AndOrRewriter(NativeExpressionRulesHandler nativeExpressionRulesHandler, VaradaStatsPushdownPredicates varadaStatsPushdownPredicates)
    {
        this.nativeExpressionRulesHandler = nativeExpressionRulesHandler;
        this.varadaStatsPushdownPredicates = varadaStatsPushdownPredicates;
    }

    @Override
    public Pattern<VaradaCall> getPattern()
    {
        return PATTERN;
    }

    boolean or(VaradaExpression varadaExpression, RewriteContext parentContext)
    {
        boolean isValid = true;
        NativeExpression.Builder parentNativeBuilder = parentContext.nativeExpressionBuilder();
        for (VaradaExpression child : varadaExpression.getChildren()) {
            RewriteContext childContext = new RewriteContext(NativeExpression.builder(),
                    parentContext.columnType(),
                    parentContext.unsupportedNativeFunctions(),
                    parentContext.customStats());
            boolean valid = nativeExpressionRulesHandler.rewrite(child, childContext);
            if (!valid) {
                isValid = false;
                break;
            }
            NativeExpression.Builder childNativeBuilder = childContext.nativeExpressionBuilder();
            if (parentNativeBuilder.getFunctionType() == null) {
                parentNativeBuilder.functionType(childNativeBuilder.getFunctionType())
                        .predicateType(childNativeBuilder.getPredicateType())
                        .collectNulls(childNativeBuilder.getCollectNulls())
                        .functionParams(childNativeBuilder.getFunctionParams())
                        .domain(childNativeBuilder.getDomain());
            }
            else if (parentNativeBuilder.getFunctionType() != childNativeBuilder.getFunctionType() ||
                    !parentNativeBuilder.getFunctionParams().equals(childNativeBuilder.getFunctionParams())) {
                isValid = false;
                break;
            }
            else {
                Domain newDomain = Domain.union(List.of(parentNativeBuilder.getDomain(),
                        childNativeBuilder.getDomain()));
                buildNativeBuilder(parentNativeBuilder, childNativeBuilder, newDomain);
            }
        }
        return isValid;
    }

    boolean and(VaradaExpression varadaExpression, RewriteContext parentContext)
    {
        boolean isValid = false;
        NativeExpression.Builder parentNativeBuilder = parentContext.nativeExpressionBuilder();
        for (VaradaExpression child : varadaExpression.getChildren()) {
            RewriteContext childContext = new RewriteContext(NativeExpression.builder(),
                    parentContext.columnType(),
                    parentContext.unsupportedNativeFunctions(),
                    parentContext.customStats());
            boolean valid = nativeExpressionRulesHandler.rewrite(child, childContext);
            if (!valid) {
                continue;
            }
            NativeExpression.Builder childNativeBuilder = childContext.nativeExpressionBuilder();
            if (parentNativeBuilder.getFunctionType() == null) {
                parentNativeBuilder.functionType(childNativeBuilder.getFunctionType())
                        .predicateType(childNativeBuilder.getPredicateType())
                        .collectNulls(childNativeBuilder.getCollectNulls())
                        .functionParams(childNativeBuilder.getFunctionParams())
                        .domain(childNativeBuilder.getDomain());
                isValid = true;
            }
            else if (FunctionType.FUNCTION_TYPE_TRANSFORMED == parentNativeBuilder.getFunctionType() &&
                    parentNativeBuilder.getFunctionType() == childNativeBuilder.getFunctionType()) {
                Domain newDomain = parentNativeBuilder.getDomain().union(childNativeBuilder.getDomain());
                buildNativeBuilder(parentNativeBuilder, childNativeBuilder, newDomain);
            }
            else if (parentNativeBuilder.getFunctionType() != childNativeBuilder.getFunctionType() ||
                    !parentNativeBuilder.getFunctionParams().equals(childNativeBuilder.getFunctionParams())) {
                logger.debug("getting mixed expressions under AND, we give up expression=%s", child);
            }
            else {
                Domain newDomain = parentNativeBuilder.getDomain().intersect(childNativeBuilder.getDomain());
                buildNativeBuilder(parentNativeBuilder, childNativeBuilder, newDomain);
            }
        }
        if (!isValid) {
            varadaStatsPushdownPredicates.incunsupported_functions_native();
        }
        return isValid;
    }

    private void buildNativeBuilder(NativeExpression.Builder parentNativeBuilder, NativeExpression.Builder childNativeBuilder, Domain newDomain)
    {
        PredicateType descendantPredicateType = childNativeBuilder.getPredicateType();
        if (descendantPredicateType == PredicateType.PREDICATE_TYPE_RANGES ||
                descendantPredicateType == PredicateType.PREDICATE_TYPE_STRING_RANGES) {
            parentNativeBuilder.predicateType(descendantPredicateType);
        }
        if (childNativeBuilder.getCollectNulls()) {
            parentNativeBuilder.collectNulls(true);
        }
        parentNativeBuilder.domain(newDomain);
    }
}
