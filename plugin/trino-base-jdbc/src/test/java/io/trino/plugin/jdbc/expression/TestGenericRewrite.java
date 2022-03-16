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

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGenericRewrite
{
    @Test
    public void testRewriteCall()
    {
        GenericRewrite rewrite = new GenericRewrite("add(foo: decimal(p, s), bar: bigint): decimal(rp, rs)", "foo + bar::decimal(rp,rs)");
        ConnectorExpression expression = new Call(
                createDecimalType(21, 2),
                new FunctionName("add"),
                List.of(
                        new Variable("first", createDecimalType(10, 2)),
                        new Variable("second", BIGINT)));

        Optional<String> rewritten = apply(rewrite, expression);
        assertThat(rewritten).hasValue("(\"first\") + (\"second\")::decimal(21,2)");
    }

    private static Optional<String> apply(GenericRewrite rewrite, ConnectorExpression expression)
    {
        Match match = rewrite.getPattern().match(expression).collect(onlyElement());
        return rewrite.rewrite(expression, match.captures(), new RewriteContext<>()
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
            public Optional<String> defaultRewrite(ConnectorExpression expression1)
            {
                if (expression1 instanceof Variable) {
                    return Optional.of("\"" + ((Variable) expression1).getName().replace("\"", "\"\"") + "\"");
                }
                return Optional.empty();
            }
        });
    }
}
