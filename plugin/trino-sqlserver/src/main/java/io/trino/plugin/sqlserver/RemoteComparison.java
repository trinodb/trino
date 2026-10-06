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
package io.trino.plugin.sqlserver;

import io.trino.plugin.base.expression.ConnectorExpressionRule.RewriteContext;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;

final class RemoteComparison
{
    private RemoteComparison() {}

    /**
     * Whether the connector pushes down a comparison over the values in the expression. varchar is never pushed
     * down, because SQL Server compares it with PAD SPACE semantics while Trino compares it with NO PAD. char is
     * pushed down only when the column collation is classified as case-sensitive, which still allows it to be
     * accent-, width- or kana-insensitive and so to match values that Trino treats as different.
     */
    static boolean isSupportedComparison(ConnectorExpression expression, RewriteContext<?> context)
    {
        Type type = expression.getType();
        if (type instanceof VarcharType) {
            return false;
        }
        if (type instanceof CharType && expression instanceof Variable variable && !isCaseSensitive(variable, context)) {
            return false;
        }
        return expression.getChildren().stream().allMatch(child -> isSupportedComparison(child, context));
    }

    private static boolean isCaseSensitive(Variable variable, RewriteContext<?> context)
    {
        return ((JdbcColumnHandle) context.getAssignment(variable.getName())).getJdbcTypeHandle().caseSensitivity().orElse(CASE_INSENSITIVE) == CASE_SENSITIVE;
    }
}
