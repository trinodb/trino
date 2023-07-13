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
package io.trino.sql.planner.assertions;

import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.tree.Expression;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static java.util.Objects.requireNonNull;

final class CorrelatedJoinMatcher
        implements Matcher
{
    private final Expression filter;

    CorrelatedJoinMatcher(Expression filter)
    {
        this.filter = requireNonNull(filter, "filter is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        // However this is used for CorrelatedJoinNode only
        return true;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        if (!(node instanceof CorrelatedJoinNode correlatedJoinNode)) {
            throw new IllegalStateException("This is a detailed matcher for CorrelatedJoinNode, got: " + node);
        }
        Expression filter = correlatedJoinNode.getFilter();
        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);
        DynamicFilters.ExtractResult extractResult = extractDynamicFilters(filter);
        return new MatchResult(verifier.process(combineConjuncts(metadata, extractResult.getStaticConjuncts()), filter));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("filter", filter)
                .toString();
    }
}
