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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.BooleanType;
import org.testng.annotations.Test;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRewriteLikeAndEscapeWithCaseSensitivity
{
    @Test
    public void testRewriteLikeCallInvalidNumberOfArguments()
    {
        RewriteLikeAndEscapeWithCaseSensitivity rewrite = new RewriteLikeAndEscapeWithCaseSensitivity();
        Call expression = new Call(
                BooleanType.BOOLEAN,
                new FunctionName("$like"),
                List.of(new Variable("case_sensitive_value", VARCHAR)));

        Optional<ParameterizedExpression> rewritten = apply(rewrite, expression);
        assertThat(rewritten).isEmpty();
    }

    @Test
    public void testRewriteLikeCallInvalidTypeValue()
    {
        RewriteLikeAndEscapeWithCaseSensitivity rewrite = new RewriteLikeAndEscapeWithCaseSensitivity();
        Call expression = new Call(
                BooleanType.BOOLEAN,
                new FunctionName("$like"),
                List.of(
                        new Variable("case_sensitive_value", BIGINT),
                        new Variable("pattern", VARCHAR)));

        Optional<ParameterizedExpression> rewritten = apply(rewrite, expression);
        assertThat(rewritten).isEmpty();
    }

    @Test
    public void testRewriteLikeCallInvalidTypePattern()
    {
        RewriteLikeAndEscapeWithCaseSensitivity rewrite = new RewriteLikeAndEscapeWithCaseSensitivity();
        Call expression = new Call(
                BooleanType.BOOLEAN,
                new FunctionName("$like"),
                List.of(
                        new Variable("case_sensitive_value", VARCHAR),
                        new Variable("pattern", BIGINT)));

        Optional<ParameterizedExpression> rewritten = apply(rewrite, expression);
        assertThat(rewritten).isEmpty();
    }

    @Test
    public void testRewriteLikeCallInvalidTypeEscape()
    {
        RewriteLikeAndEscapeWithCaseSensitivity rewrite = new RewriteLikeAndEscapeWithCaseSensitivity();
        Call expression = new Call(
                BooleanType.BOOLEAN,
                new FunctionName("$like"),
                List.of(
                        new Variable("case_sensitive_value", VARCHAR),
                        new Variable("pattern", VARCHAR),
                        new Variable("escape", BIGINT)));

        Optional<ParameterizedExpression> rewritten = apply(rewrite, expression);
        assertThat(rewritten).isEmpty();
    }

    @Test
    public void testRewriteLikeCallOnCaseInsensitiveValue()
    {
        RewriteLikeAndEscapeWithCaseSensitivity rewrite = new RewriteLikeAndEscapeWithCaseSensitivity();
        Call expression = new Call(
                BooleanType.BOOLEAN,
                new FunctionName("$like"),
                List.of(
                        new Variable("case_insensitive_value", VARCHAR),
                        new Variable("pattern", VARCHAR)));

        Optional<ParameterizedExpression> rewritten = apply(rewrite, expression);
        assertThat(rewritten).isEmpty();
    }

    @Test
    public void testRewriteLikeEscapeCallOnCaseInsensitiveValue()
    {
        RewriteLikeAndEscapeWithCaseSensitivity rewrite = new RewriteLikeAndEscapeWithCaseSensitivity();
        Call expression = new Call(
                BooleanType.BOOLEAN,
                new FunctionName("$like"),
                List.of(
                        new Variable("case_insensitive_value", VARCHAR),
                        new Variable("pattern", VARCHAR),
                        new Variable("escape", VARCHAR)));

        Optional<ParameterizedExpression> rewritten = apply(rewrite, expression);
        assertThat(rewritten).isEmpty();
    }

    @Test
    public void testRewriteLikeCallOnCaseSensitiveValue()
    {
        RewriteLikeAndEscapeWithCaseSensitivity rewrite = new RewriteLikeAndEscapeWithCaseSensitivity();
        Call expression = new Call(
                BooleanType.BOOLEAN,
                new FunctionName("$like"),
                List.of(
                        new Variable("case_sensitive_value", VARCHAR),
                        new Variable("pattern", VARCHAR)));

        ParameterizedExpression rewritten = apply(rewrite, expression).orElseThrow();
        assertThat(rewritten.expression()).isEqualTo("\"case_sensitive_value\" LIKE \"pattern\"");
        assertThat(rewritten.parameters()).isEmpty();
    }

    @Test
    public void testRewriteLikeEscapeCallOnCaseSensitiveValue()
    {
        RewriteLikeAndEscapeWithCaseSensitivity rewrite = new RewriteLikeAndEscapeWithCaseSensitivity();
        Call expression = new Call(
                BooleanType.BOOLEAN,
                new FunctionName("$like"),
                List.of(
                        new Variable("case_sensitive_value", VARCHAR),
                        new Variable("pattern", VARCHAR),
                        new Variable("escape", VARCHAR)));

        ParameterizedExpression rewritten = apply(rewrite, expression).orElseThrow();
        assertThat(rewritten.expression()).isEqualTo("\"case_sensitive_value\" LIKE \"pattern\" ESCAPE \"escape\"");
        assertThat(rewritten.parameters()).isEmpty();
    }

    private static Optional<ParameterizedExpression> apply(RewriteLikeAndEscapeWithCaseSensitivity rewrite, Call expression)
    {
        Optional<Match> match = rewrite.getPattern().match(expression).collect(toOptional());
        if (match.isEmpty()) {
            return Optional.empty();
        }
        return rewrite.rewrite(expression, match.get().captures(), new ConnectorExpressionRule.RewriteContext<>()
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
                    return Optional.of(new ParameterizedExpression("\"" + ((Variable) expression).getName().replace("\"", "\"\"") + "\"", ImmutableList.of()));
                }
                return Optional.empty();
            }
        });
    }
}
