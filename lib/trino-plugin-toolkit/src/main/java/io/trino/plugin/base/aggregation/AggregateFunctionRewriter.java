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
package io.trino.plugin.base.aggregation;

import com.google.common.collect.ImmutableSet;
import io.trino.matching.Match;
import io.trino.plugin.base.aggregation.AggregateFunctionRule.RewriteContext;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class AggregateFunctionRewriter<AggregationResult, ExpressionResult>
{
    private final ConnectorExpressionRewriter<ExpressionResult> connectorExpressionRewriter;
    private final Set<AggregateFunctionRule<AggregationResult, ExpressionResult>> rules;

    public AggregateFunctionRewriter(ConnectorExpressionRewriter<ExpressionResult> connectorExpressionRewriter, Set<AggregateFunctionRule<AggregationResult, ExpressionResult>> rules)
    {
        this.connectorExpressionRewriter = requireNonNull(connectorExpressionRewriter, "connectorExpressionRewriter is null");
        this.rules = ImmutableSet.copyOf(requireNonNull(rules, "rules is null"));
    }

    public Optional<AggregationResult> rewrite(ConnectorSession session, AggregateFunction aggregateFunction, Map<String, ColumnHandle> assignments)
    {
        requireNonNull(aggregateFunction, "aggregateFunction is null");
        requireNonNull(assignments, "assignments is null");

        RewriteContext<ExpressionResult> context = new RewriteContext<>()
        {
            @Override
            public Map<String, ColumnHandle> getAssignments()
            {
                return assignments;
            }

            @Override
            public ConnectorSession getSession()
            {
                return session;
            }

            @Override
            public Optional<ExpressionResult> rewriteExpression(ConnectorExpression expression)
            {
                return connectorExpressionRewriter.rewrite(session, expression, assignments);
            }
        };

        for (AggregateFunctionRule<AggregationResult, ExpressionResult> rule : rules) {
            Iterator<Match> matches = rule.getPattern().match(aggregateFunction, context).iterator();
            while (matches.hasNext()) {
                Match match = matches.next();
                Optional<AggregationResult> rewritten = rule.rewrite(aggregateFunction, match.captures(), context);
                if (rewritten.isPresent()) {
                    return rewritten;
                }
            }
        }

        return Optional.empty();
    }
}
