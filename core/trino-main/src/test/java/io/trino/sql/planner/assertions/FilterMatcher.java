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
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static java.util.Objects.requireNonNull;

final class FilterMatcher
        implements Matcher
{
    private final Expression predicate;
    private final Optional<Expression> dynamicFilter;

    FilterMatcher(Expression predicate, Optional<Expression> dynamicFilter)
    {
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof FilterNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        FilterNode filterNode = (FilterNode) node;
        Expression filterPredicate = filterNode.getPredicate();
        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);

        if (dynamicFilter.isPresent()) {
            return new MatchResult(verifier.process(filterPredicate, combineConjuncts(predicate, dynamicFilter.get())));
        }

        DynamicFilters.ExtractResult extractResult = extractDynamicFilters(filterPredicate);
        return new MatchResult(verifier.process(combineConjuncts(extractResult.getStaticConjuncts()), predicate));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("predicate", predicate)
                .toString();
    }
}
