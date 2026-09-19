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
package io.trino.plugin.trino;

import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.testing.connector.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestTrinoExpressionRewriter
{
    private static final JdbcTypeHandle BIGINT_TYPE_HANDLE = new JdbcTypeHandle(
            Types.BIGINT,
            Optional.of("bigint"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    private final ConnectorExpressionRewriter<ParameterizedExpression> rewriter =
            TrinoClient.createConnectorExpressionRewriter(name -> "\"" + name + "\"");

    @Test
    void testIdenticalComparisonRewrite()
    {
        ParameterizedExpression rewritten = rewrite(
                new Call(
                        BOOLEAN,
                        StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME,
                        List.of(
                                new Variable("left_key", BIGINT),
                                new Variable("right_key", BIGINT))),
                Map.of(
                        "left_key", column("left_key"),
                        "right_key", column("right_key")));

        assertThat(rewritten.expression()).isEqualTo("(\"left_key\") IS NOT DISTINCT FROM (\"right_key\")");
        assertThat(rewritten.parameters()).isEmpty();
    }

    @Test
    void testArithmeticComparisonRewrite()
    {
        ConnectorExpression arithmetic = new Call(
                BIGINT,
                StandardFunctions.SUBTRACT_FUNCTION_NAME,
                List.of(
                        new Call(
                                BIGINT,
                                StandardFunctions.ADD_FUNCTION_NAME,
                                List.of(
                                        new Variable("nationkey", BIGINT),
                                        new Variable("custkey", BIGINT))),
                        new Constant(3L, BIGINT)));

        ParameterizedExpression rewritten = rewrite(
                new Call(
                        BOOLEAN,
                        StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                        List.of(
                                arithmetic,
                                new Constant(0L, BIGINT))),
                Map.of(
                        "nationkey", column("nationkey"),
                        "custkey", column("custkey")));

        assertThat(rewritten.expression()).isEqualTo("(((\"nationkey\" + \"custkey\") - ?)) = (?)");
        assertThat(rewritten.parameters()).hasSize(2);
        assertThat(rewritten.parameters().get(0).getValue()).contains(3L);
        assertThat(rewritten.parameters().get(1).getValue()).contains(0L);
    }

    private ParameterizedExpression rewrite(ConnectorExpression expression, Map<String, JdbcColumnHandle> assignments)
    {
        return rewriter.rewrite(SESSION, expression, Map.copyOf(assignments))
                .orElseThrow(() -> new AssertionError("Expected expression to be rewritten: " + expression));
    }

    private static JdbcColumnHandle column(String name)
    {
        return new JdbcColumnHandle(name, BIGINT_TYPE_HANDLE, BIGINT);
    }
}
