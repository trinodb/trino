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
package io.trino.plugin.jdbc.expression;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.spi.expression.Variable;

import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;

public class RewriteVariable
        implements ConnectorExpressionRule<Variable, String>
{
    @Override
    public Pattern<Variable> getPattern()
    {
        return variable();
    }

    @Override
    public Optional<String> rewrite(Variable variable, Captures captures, RewriteContext<String> context)
    {
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(variable.getName());
        return Optional.of(context.getIdentifierQuote().apply(columnHandle.getColumnName()));
    }
}
