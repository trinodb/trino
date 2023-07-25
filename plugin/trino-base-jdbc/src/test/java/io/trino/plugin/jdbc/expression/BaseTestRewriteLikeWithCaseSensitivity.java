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

import com.google.common.collect.ImmutableList;
import io.trino.matching.Match;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;

import java.sql.Types;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseTestRewriteLikeWithCaseSensitivity
{
    protected abstract ConnectorExpressionRule<Call, ParameterizedExpression> getRewrite();

    protected Optional<ParameterizedExpression> apply(Call expression)
    {
        Optional<Match> match = getRewrite().getPattern().match(expression).collect(toOptional());
        if (match.isEmpty()) {
            return Optional.empty();
        }
        return getRewrite().rewrite(expression, match.get().captures(), new ConnectorExpressionRule.RewriteContext<>()
        {
            @Override
            public Map<String, ColumnHandle> getAssignments()
            {
                return Map.of("case_insensitive_value", new JdbcColumnHandle("case_insensitive_value", JDBC_BIGINT, VARCHAR),
                        "case_sensitive_value", new JdbcColumnHandle("case_sensitive_value", new JdbcTypeHandle(Types.VARCHAR, Optional.of("varchar"), Optional.of(10), Optional.empty(), Optional.empty(), Optional.of(CASE_SENSITIVE)), VARCHAR));
            }

            @Override
            public ConnectorSession getSession()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<ParameterizedExpression> defaultRewrite(ConnectorExpression expression)
            {
                if (expression instanceof Variable) {
                    String name = ((Variable) expression).getName();
                    return Optional.of(new ParameterizedExpression("\"" + name.replace("\"", "\"\"") + "\"", ImmutableList.of(new QueryParameter(expression.getType(), Optional.of(name)))));
                }
                return Optional.empty();
            }
        });
    }

    protected void assertNoRewrite(Call expression)
    {
        Optional<ParameterizedExpression> rewritten = apply(expression);
        assertThat(rewritten).isEmpty();
    }
}
