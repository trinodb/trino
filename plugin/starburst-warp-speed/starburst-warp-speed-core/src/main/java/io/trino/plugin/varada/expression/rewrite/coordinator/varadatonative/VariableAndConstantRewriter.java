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
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaConstant;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaPrimitiveConstant;
import io.trino.plugin.varada.expression.VaradaSliceConstant;
import io.trino.plugin.varada.expression.VaradaVariable;
import io.trino.plugin.varada.expression.rewrite.ExpressionPatterns;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.stats.VaradaStatsPushdownPredicates;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.function.BiFunction;

import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argument;
import static io.trino.plugin.varada.expression.rewrite.ExpressionPatterns.argumentCount;
import static io.trino.plugin.varada.expression.rewrite.coordinator.varadatonative.VariableRewriter.calcPredicateType;
import static io.trino.plugin.varada.type.TypeUtils.isWarmBasicSupported;

class VariableAndConstantRewriter
        extends BaseOperatorRewriter
{
    private static final Pattern<VaradaCall> PATTERN = ExpressionPatterns.call()
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(x -> x instanceof VaradaVariable))
            .with(argument(1).matching(x -> x instanceof VaradaConstant));

    VariableAndConstantRewriter(
            NativeExpressionRulesHandler nativeExpressionRulesHandler,
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
    boolean convert(VaradaExpression varadaExpression, RewriteContext rewriteContext, BiFunction<Type, Object, Range> rangeBiFunction)
    {
        if (rewriteContext.nativeExpressionBuilder().getDomain() != null) {
            varadaStatsPushdownPredicates.incunsupported_functions_composite();
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
                .functionType(FunctionType.FUNCTION_TYPE_NONE)
                .collectNulls(domain.isNullAllowed());
        return true;
    }

    public boolean elementAt(VaradaExpression varadaExpression, RewriteContext rewriteContext)
    {
        boolean res = false;
        if (varadaExpression.getChildren().get(0).getType() instanceof MapType mapType && isWarmBasicSupported(mapType.getValueType())) {
            VaradaConstant varadaConstant = (VaradaConstant) varadaExpression.getChildren().get(1);
            NativeExpression.Builder nativeExpressionBuilder = rewriteContext.nativeExpressionBuilder();
            nativeExpressionBuilder
                    .functionType(FunctionType.FUNCTION_TYPE_TRANSFORMED)
                    .transformedColumn(new TransformFunction(TransformFunction.TransformType.ELEMENT_AT, List.of(varadaConstant)));
            res = true;
        }
        return res;
    }

    public boolean jsonExtractScalar(VaradaExpression varadaExpression, RewriteContext rewriteContext)
    {
        VaradaSliceConstant varadaSliceConstant = (VaradaSliceConstant) varadaExpression.getChildren()
                .stream()
                .filter(varadaExpr -> varadaExpr instanceof VaradaSliceConstant)
                .findFirst()
                .orElseThrow();
        rewriteContext
                .nativeExpressionBuilder()
                .functionType(FunctionType.FUNCTION_TYPE_TRANSFORMED)
                .transformedColumn(new TransformFunction(TransformFunction.TransformType.JSON_EXTRACT_SCALAR,
                        List.of(new VaradaPrimitiveConstant(varadaSliceConstant.getValue().toStringUtf8(), VarcharType.VARCHAR))));
        return true;
    }
}
