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
package io.trino.plugin.base.expression;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verifyNotNull;
import static java.util.Objects.requireNonNull;

public interface ConnectorExpressionRule<ExpressionType extends ConnectorExpression, Result>
{
    Pattern<ExpressionType> getPattern();

    Optional<Result> rewrite(ExpressionType expression, Captures captures, RewriteContext<Result> context);

    interface RewriteContext<Result>
    {
        default ColumnHandle getAssignment(String name)
        {
            requireNonNull(name, "name is null");
            ColumnHandle columnHandle = getAssignments().get(name);
            verifyNotNull(columnHandle, "No assignment for %s", name);
            return columnHandle;
        }

        Map<String, ColumnHandle> getAssignments();

        ConnectorSession getSession();

        Optional<Result> defaultRewrite(ConnectorExpression expression);
    }
}
