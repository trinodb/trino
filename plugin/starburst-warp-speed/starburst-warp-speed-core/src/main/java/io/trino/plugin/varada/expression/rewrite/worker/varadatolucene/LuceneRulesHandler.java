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
package io.trino.plugin.varada.expression.rewrite.worker.varadatolucene;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import io.trino.matching.Pattern;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;

import java.util.Set;
import java.util.function.BiFunction;

import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.CONTAINS;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.START_WITH;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;

public class LuceneRulesHandler
{
    private SetMultimap<String, FunctionRewriter> luceneRules;

    public LuceneRulesHandler() {}

    public void init(DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        GeneralRewriter luceneGeneralRewriter = new GeneralRewriter(dispatcherProxiedConnectorTransformer);
        EqualityRewriter equalityRewriter = new EqualityRewriter(this);
        LogicalOperatorRewriter luceneLogicalOperatorRewriter = new LogicalOperatorRewriter(this);
        NotRewriter notRewriter = new NotRewriter(this);
        InRewriter inRewriter = new InRewriter();
        LuceneVariableRewriter variableRewriter = new LuceneVariableRewriter();
        EqualityValueRewriter equalityValueRewriter = new EqualityValueRewriter();
        luceneRules = HashMultimap.create();
        luceneRules.put(IS_NULL_FUNCTION_NAME.getName(), new FunctionRewriter(variableRewriter.getPattern(), variableRewriter::handleIsNull));
        luceneRules.put(IN_PREDICATE_FUNCTION_NAME.getName(), new FunctionRewriter(inRewriter.getPattern(), inRewriter::handleIn));
        luceneRules.put(NOT_FUNCTION_NAME.getName(), new FunctionRewriter(notRewriter.getPattern(), notRewriter::handleNotLike));
        luceneRules.put(LIKE_FUNCTION_NAME.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::handleLike));
        luceneRules.put(START_WITH.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::handleStartsWith));
        luceneRules.put(EQUAL_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(equalityValueRewriter.getPattern(), equalityValueRewriter::handleEqual));
        luceneRules.put(OR_FUNCTION_NAME.getName(), new FunctionRewriter(luceneLogicalOperatorRewriter.getPattern(), luceneLogicalOperatorRewriter::handleOr));
        luceneRules.put(AND_FUNCTION_NAME.getName(), new FunctionRewriter(luceneLogicalOperatorRewriter.getPattern(), luceneLogicalOperatorRewriter::handleAnd));
        luceneRules.put(CONTAINS.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::handleContains));
        luceneRules.put(NOT_EQUAL_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(equalityRewriter.getPattern(), equalityRewriter::handleNotEqual));
        luceneRules.put(NOT_EQUAL_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::handleNotEqual));
        luceneRules.put(EQUAL_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::handleEqual));
        luceneRules.put(EQUAL_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(equalityRewriter.getPattern(), equalityRewriter::handleEqual));
        luceneRules.put(LESS_THAN_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::handleLessThan));
        luceneRules.put(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::handleLessThanOrEqual));
        luceneRules.put(GREATER_THAN_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::greateThan));
        luceneRules.put(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::greatThanOrEqual));
    }

    public boolean rewrite(VaradaExpression varadaExpression,
            LuceneRewriteContext context)
    {
        if (!(varadaExpression instanceof VaradaCall)) {
            return false;
        }
        String functionName = ((VaradaCall) varadaExpression).getFunctionName();
        Set<FunctionRewriter> varadaExpressionRules = luceneRules.get(functionName);
        boolean isValid = false;
        for (FunctionRewriter rule : varadaExpressionRules) {
            if (rule.pattern().matches(varadaExpression, null)) {
                isValid = rule.rewriteCallback().apply(varadaExpression, context);
                break;
            }
        }
        return isValid;
    }

    private record FunctionRewriter(
            @SuppressWarnings("unused") Pattern<VaradaCall> pattern,
            @SuppressWarnings("unused") BiFunction<VaradaExpression, LuceneRewriteContext, Boolean> rewriteCallback) {}
}
