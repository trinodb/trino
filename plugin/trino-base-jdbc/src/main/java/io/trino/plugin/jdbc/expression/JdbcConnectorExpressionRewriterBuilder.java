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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.connector.ConnectorSession;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class JdbcConnectorExpressionRewriterBuilder
{
    public static JdbcConnectorExpressionRewriterBuilder newBuilder()
    {
        return new JdbcConnectorExpressionRewriterBuilder();
    }

    private final ImmutableSet.Builder<ConnectorExpressionRule<?, ParameterizedExpression>> rules = ImmutableSet.builder();
    private final Map<String, Set<String>> typeClasses = new HashMap<>();

    private JdbcConnectorExpressionRewriterBuilder() {}

    public JdbcConnectorExpressionRewriterBuilder addStandardRules(Function<String, String> identifierQuote)
    {
        add(new RewriteVariable(identifierQuote));
        add(new RewriteVarcharConstant());
        add(new RewriteExactNumericConstant());
        add(new RewriteAnd());
        add(new RewriteOr());

        return this;
    }

    public JdbcConnectorExpressionRewriterBuilder add(ConnectorExpressionRule<?, ParameterizedExpression> rule)
    {
        rules.add(rule);
        return this;
    }

    public JdbcConnectorExpressionRewriterBuilder withTypeClass(String typeClass, Set<String> typeNames)
    {
        requireNonNull(typeClass, "typeClass is null");
        checkArgument(!typeNames.isEmpty(), "No typeNames");
        checkState(!typeClasses.containsKey(typeClass), "typeClass already defined");
        typeClasses.put(typeClass, ImmutableSet.copyOf(typeNames));
        return this;
    }

    public ExpectSourceExpression when(Predicate<ConnectorSession> condition)
    {
        return new GenericRewriteBuilder()
                .when(condition);
    }

    public ExpectRewriteTarget map(String expressionPattern)
    {
        return new GenericRewriteBuilder()
                .map(expressionPattern);
    }

    public ConnectorExpressionRewriter<ParameterizedExpression> build()
    {
        return new ConnectorExpressionRewriter<>(rules.build());
    }

    public interface ExpectSourceExpression
    {
        ExpectRewriteTarget map(String expressionPattern);
    }

    public interface ExpectRewriteTarget
    {
        JdbcConnectorExpressionRewriterBuilder to(String rewritePattern);
    }

    private class GenericRewriteBuilder
            implements ExpectSourceExpression, ExpectRewriteTarget
    {
        private Predicate<ConnectorSession> condition = session -> true;
        private String expressionPattern;

        GenericRewriteBuilder when(Predicate<ConnectorSession> condition)
        {
            this.condition = requireNonNull(condition, "condition is null");
            return this;
        }

        @Override
        public ExpectRewriteTarget map(String expressionPattern)
        {
            this.expressionPattern = requireNonNull(expressionPattern, "expressionPattern is null");
            return this;
        }

        @Override
        public JdbcConnectorExpressionRewriterBuilder to(String rewritePattern)
        {
            rules.add(new GenericRewrite(ImmutableMap.copyOf(typeClasses), condition, expressionPattern, rewritePattern));
            return JdbcConnectorExpressionRewriterBuilder.this;
        }
    }
}
