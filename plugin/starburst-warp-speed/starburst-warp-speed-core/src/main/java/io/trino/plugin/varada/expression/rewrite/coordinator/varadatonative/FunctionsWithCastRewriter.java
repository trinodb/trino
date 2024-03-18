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
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.rewrite.ExpressionPatterns;
import io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;

import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argument;
import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argumentCount;
import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.functionName;

public class FunctionsWithCastRewriter
        implements ExpressionRewriter<VaradaCall>
{
    private static final Pattern<VaradaCall> PATTERN = ExpressionPatterns.call()
            .with(functionName().matching(functionName -> SupportedFunctions.DATE_FUNCTIONS.contains(new FunctionName(functionName))))
            .with(argumentCount().equalTo(1))
            .with(argument(0).matching(x -> x instanceof VaradaCall varadaCall && varadaCall.getFunctionName().equals(StandardFunctions.CAST_FUNCTION_NAME.getName())));
    private final NativeExpressionRulesHandler nativeExpressionRulesHandler;

    public FunctionsWithCastRewriter(NativeExpressionRulesHandler nativeExpressionRulesHandler)
    {
        this.nativeExpressionRulesHandler = nativeExpressionRulesHandler;
    }

    @Override
    public Pattern<VaradaCall> getPattern()
    {
        return PATTERN;
    }

    public boolean dayOfWeek(VaradaExpression varadaExpression, RewriteContext parentContext)
    {
        VaradaCall castFunction = (VaradaCall) varadaExpression.getChildren().get(0);
        parentContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_DAY_OF_WEEK);
        return nativeExpressionRulesHandler.rewrite(castFunction, parentContext);
    }

    public boolean day(VaradaExpression varadaExpression, RewriteContext parentContext)
    {
        parentContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_DAY);
        VaradaCall castFunction = (VaradaCall) varadaExpression.getChildren().get(0);
        return nativeExpressionRulesHandler.rewrite(castFunction, parentContext);
    }

    public boolean dayOfYear(VaradaExpression varadaExpression, RewriteContext parentContext)
    {
        parentContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_DAY_OF_YEAR);
        VaradaCall castFunction = (VaradaCall) varadaExpression.getChildren().get(0);
        return nativeExpressionRulesHandler.rewrite(castFunction, parentContext);
    }

    public boolean week(VaradaExpression varadaExpression, RewriteContext parentContext)
    {
        parentContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_WEEK);
        VaradaCall castFunction = (VaradaCall) varadaExpression.getChildren().get(0);
        return nativeExpressionRulesHandler.rewrite(castFunction, parentContext);
    }

    public boolean yearOfWeek(VaradaExpression varadaExpression, RewriteContext parentContext)
    {
        parentContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_YEAR_OF_WEEK);
        VaradaCall castFunction = (VaradaCall) varadaExpression.getChildren().get(0);
        return nativeExpressionRulesHandler.rewrite(castFunction, parentContext);
    }
}
