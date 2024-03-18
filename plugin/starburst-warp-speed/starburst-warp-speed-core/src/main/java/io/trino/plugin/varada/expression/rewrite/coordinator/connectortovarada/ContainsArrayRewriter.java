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
package io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionPatterns;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.CONTAINS;
import static io.trino.spi.type.BooleanType.BOOLEAN;

class ContainsArrayRewriter
        implements ConnectorExpressionRule<Call, VaradaExpression>

{
    public ContainsArrayRewriter()
    {
    }

    private static final Pattern<Call> PATTERN = ConnectorExpressionPatterns.call()
            .with(ConnectorExpressionPatterns.argumentCount().equalTo(2))
            .with(type().equalTo(BOOLEAN))
            .with(ConnectorExpressionPatterns.functionName().equalTo(CONTAINS))
            .with(ConnectorExpressionPatterns.argument(0).matching(x -> x instanceof Variable variable &&
                    variable.getType() instanceof ArrayType arrayType &&
                            (arrayType.getElementType() instanceof VarcharType || arrayType.getElementType() instanceof CharType)))
            .with(ConnectorExpressionPatterns.argument(1).matching(x -> x instanceof Constant));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<VaradaExpression> rewrite(Call expression, Captures captures, RewriteContext<VaradaExpression> context)
    {
        Optional<VaradaExpression> variable = context.defaultRewrite(expression.getChildren().get(0));
        Optional<VaradaExpression> constant = context.defaultRewrite(expression.getChildren().get(1));
        Optional<VaradaExpression> res = Optional.empty();
        if (variable.isPresent() && constant.isPresent()) {
            res = Optional.of(new VaradaCall(expression.getFunctionName().getName(), List.of(variable.get(), constant.get()), BOOLEAN));
        }
        return res;
    }
}
