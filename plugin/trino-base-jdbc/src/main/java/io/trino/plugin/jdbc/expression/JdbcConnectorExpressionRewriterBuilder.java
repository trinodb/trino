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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.expression.ConnectorExpressionRule;

import java.util.function.Function;

public class JdbcConnectorExpressionRewriterBuilder
{
    public static JdbcConnectorExpressionRewriterBuilder newBuilder()
    {
        return new JdbcConnectorExpressionRewriterBuilder();
    }

    private ImmutableSet.Builder<ConnectorExpressionRule<?, String>> rules = ImmutableSet.builder();

    private JdbcConnectorExpressionRewriterBuilder() {}

    public JdbcConnectorExpressionRewriterBuilder addStandardRules(Function<String, String> identifierQuote)
    {
        add(new RewriteVariable(identifierQuote));
        add(new RewriteVarcharConstant());
        add(new RewriteExactNumericConstant());
        add(new RewriteOr());

        return this;
    }

    public JdbcConnectorExpressionRewriterBuilder add(ConnectorExpressionRule<?, String> rule)
    {
        rules.add(rule);
        return this;
    }

    public ExpressionMapping<JdbcConnectorExpressionRewriterBuilder> map(String expressionPattern)
    {
        return new ExpressionMapping<>()
        {
            @Override
            public JdbcConnectorExpressionRewriterBuilder to(String rewritePattern)
            {
                rules.add(new GenericRewrite(expressionPattern, rewritePattern));
                return JdbcConnectorExpressionRewriterBuilder.this;
            }
        };
    }

    public ConnectorExpressionRewriter<String> build()
    {
        return new ConnectorExpressionRewriter<>(rules.build());
    }

    public interface ExpressionMapping<Continuation>
    {
        Continuation to(String rewritePattern);
    }
}
