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
import io.trino.plugin.base.expression.ConnectorExpressionRule.RewriteContext;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGenericRewrite
{
    @Test
    public void testRewriteCall()
    {
        GenericRewrite rewrite = new GenericRewrite(Map.of(), "add(foo: decimal(p, s), bar: bigint): decimal(rp, rs)", "foo + bar::decimal(rp,rs)");
        ConnectorExpression expression = new Call(
                createDecimalType(21, 2),
                new FunctionName("add"),
                List.of(
                        new Variable("first", createDecimalType(10, 2)),
                        new Variable("second", BIGINT)));

        ParameterizedExpression rewritten = apply(rewrite, expression).orElseThrow();
        assertThat(rewritten.expression()).isEqualTo("(\"first\") + (\"second\")::decimal(21,2)");
        assertThat(rewritten.parameters()).isEqualTo(List.of());
    }

    @Test
    public void testRewriteCallWithTypeClass()
    {
        Map<String, Set<String>> typeClasses = Map.of("integer_class", Set.of("integer", "bigint"));
        GenericRewrite rewrite = new GenericRewrite(typeClasses, "add(foo: integer_class, bar: bigint): integer_class", "foo + bar");

        assertThat(apply(rewrite, new Call(
                BIGINT,
                new FunctionName("add"),
                List.of(
                        new Variable("first", INTEGER),
                        new Variable("second", BIGINT))))
                .orElseThrow().expression())
                .isEqualTo("(\"first\") + (\"second\")");

        // argument type not in class
        assertThat(apply(rewrite, new Call(
                BIGINT,
                new FunctionName("add"),
                List.of(
                        new Variable("first", DOUBLE),
                        new Variable("second", BIGINT)))))
                .isEmpty();

        // result type not in class
        assertThat(apply(rewrite, new Call(
                DOUBLE,
                new FunctionName("add"),
                List.of(
                        new Variable("first", INTEGER),
                        new Variable("second", BIGINT)))))
                .isEmpty();
    }

    private static Optional<ParameterizedExpression> apply(GenericRewrite rewrite, ConnectorExpression expression)
    {
        Optional<Match> match = rewrite.getPattern().match(expression).collect(toOptional());
        if (match.isEmpty()) {
            return Optional.empty();
        }
        return rewrite.rewrite(expression, match.get().captures(), new RewriteContext<>()
        {
            @Override
            public Map<String, ColumnHandle> getAssignments()
            {
                throw new UnsupportedOperationException();
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
