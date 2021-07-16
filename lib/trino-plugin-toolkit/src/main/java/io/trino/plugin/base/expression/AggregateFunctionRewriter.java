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
import io.trino.matching.Match;
import io.trino.plugin.base.expression.AggregateFunctionRule.RewriteContext;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class AggregateFunctionRewriter<Result>
{
    private final Function<String, String> identifierQuote;
    private final Set<AggregateFunctionRule<Result>> rules;

    public AggregateFunctionRewriter(Function<String, String> identifierQuote, Set<AggregateFunctionRule<Result>> rules)
    {
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
        this.rules = ImmutableSet.copyOf(requireNonNull(rules, "rules is null"));
    }

    public Optional<Result> rewrite(ConnectorSession session, AggregateFunction aggregateFunction, Map<String, ColumnHandle> assignments)
    {
        requireNonNull(aggregateFunction, "aggregateFunction is null");
        requireNonNull(assignments, "assignments is null");

        RewriteContext context = new RewriteContext()
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
        };

        for (AggregateFunctionRule<Result> rule : rules) {
            Iterator<Match> matches = rule.getPattern().match(aggregateFunction, context).iterator();
            while (matches.hasNext()) {
                Match match = matches.next();
                Optional<Result> rewritten = rule.rewrite(aggregateFunction, match.captures(), context);
                if (rewritten.isPresent()) {
                    return rewritten;
                }
            }
        }

        return Optional.empty();
    }
}
