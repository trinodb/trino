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
import io.trino.spi.expression.ConnectorExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;

public class OrRewriter
        implements ConnectorExpressionRule<Call, VaradaExpression>
{
    private static final Pattern<Call> PATTERN = ConnectorExpressionPatterns.call()
            .with(ConnectorExpressionPatterns.functionName().equalTo(OR_FUNCTION_NAME));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<VaradaExpression> rewrite(Call expression, Captures captures, RewriteContext<VaradaExpression> context)
    {
        List<VaradaExpression> children = new ArrayList<>();
        for (ConnectorExpression connectorExpression : expression.getArguments()) {
            Optional<VaradaExpression> varadaExpression = context.defaultRewrite(connectorExpression);
            if (varadaExpression.isEmpty()) {
                children.clear();
                break;
            }
            else {
                children.add(varadaExpression.get());
            }
        }
        if (children.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new VaradaCall(OR_FUNCTION_NAME.getName(), children, expression.getType()));
    }
}
