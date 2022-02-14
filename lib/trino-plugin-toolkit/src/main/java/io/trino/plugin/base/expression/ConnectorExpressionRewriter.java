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

import com.google.common.collect.ImmutableSet;
import io.trino.matching.Capture;
import io.trino.matching.Match;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule.RewriteContext;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static java.util.Objects.requireNonNull;

public final class ConnectorExpressionRewriter<Result>
{
    private final Function<String, String> identifierQuote;
    private final Set<ConnectorExpressionRule<?, Result>> rules;

    public ConnectorExpressionRewriter(Function<String, String> identifierQuote, Set<ConnectorExpressionRule<?, Result>> rules)
    {
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
        this.rules = ImmutableSet.copyOf(requireNonNull(rules, "rules is null"));
    }

    public Optional<Result> rewrite(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        requireNonNull(session, "session is null");
        requireNonNull(expression, "expression is null");
        requireNonNull(assignments, "assignments is null");

        RewriteContext<Result> context = new RewriteContext<>()
        {
            @Override
            public Map<String, ColumnHandle> getAssignments()
            {
                return assignments;
            }

            @Override
            public Function<String, String> getIdentifierQuote()
            {
                return identifierQuote;
            }

            @Override
            public ConnectorSession getSession()
            {
                return session;
            }

            @Override
            public Optional<Result> defaultRewrite(ConnectorExpression expression)
            {
                return rewrite(expression, this);
            }
        };

        return rewrite(expression, context);
    }

    private Optional<Result> rewrite(ConnectorExpression expression, RewriteContext<Result> context)
    {
        for (ConnectorExpressionRule<?, Result> rule : rules) {
            Optional<Result> rewritten = rewrite(rule, expression, context);
            if (rewritten.isPresent()) {
                return rewritten;
            }
        }

        return Optional.empty();
    }

    private <ExpressionType extends ConnectorExpression> Optional<Result> rewrite(
            ConnectorExpressionRule<ExpressionType, Result> rule,
            ConnectorExpression expression,
            RewriteContext<Result> context)
    {
        Capture<ExpressionType> expressionCapture = newCapture();
        Pattern<ExpressionType> pattern = rule.getPattern().capturedAs(expressionCapture);
        Iterator<Match> matches = pattern.match(expression, context).iterator();
        while (matches.hasNext()) {
            Match match = matches.next();
            ExpressionType capturedExpression = match.capture(expressionCapture);
            verify(capturedExpression == expression);
            Optional<Result> rewritten = rule.rewrite(capturedExpression, match.captures(), context);
            if (rewritten.isPresent()) {
                return rewritten;
            }
        }
        return Optional.empty();
    }
}
