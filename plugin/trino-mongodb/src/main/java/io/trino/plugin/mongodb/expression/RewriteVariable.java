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
package io.trino.plugin.mongodb.expression;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.mongodb.MongoColumnHandle;
import io.trino.spi.expression.Variable;

import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;

public class RewriteVariable
        implements ConnectorExpressionRule<Variable, FilterExpression>
{
    @Override
    public Pattern<Variable> getPattern()
    {
        return variable();
    }

    @Override
    public Optional<FilterExpression> rewrite(Variable variable, Captures captures, RewriteContext<FilterExpression> context)
    {
        MongoColumnHandle columnHandle = (MongoColumnHandle) context.getAssignment(variable.getName());
        String expressionVariable = toExpressionVariable(columnHandle.getQualifiedName());
        return Optional.of(new FilterExpression(expressionVariable, FilterExpression.ExpressionType.VARIABLE, Optional.of(
                new ExpressionInfo(expressionVariable, columnHandle.getType()))));
    }

    private static String toExpressionVariable(String variable)
    {
        return "$" + variable;
    }
}
