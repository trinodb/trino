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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;

public class AggregationMatcher
        implements Matcher
{
    private final PlanMatchPattern.GroupingSetDescriptor groupingSets;
    private final List<String> masks;
    private final List<String> preGroupedSymbols;
    private final Optional<Symbol> groupId;
    private final Step step;

    public AggregationMatcher(PlanMatchPattern.GroupingSetDescriptor groupingSets, List<String> preGroupedSymbols, List<String> masks, Optional<Symbol> groupId, Step step)
    {
        this.groupingSets = groupingSets;
        this.masks = masks;
        this.preGroupedSymbols = preGroupedSymbols;
        this.groupId = groupId;
        this.step = step;
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof AggregationNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        AggregationNode aggregationNode = (AggregationNode) node;

        if (groupId.isPresent() != aggregationNode.getGroupIdSymbol().isPresent()) {
            return NO_MATCH;
        }

        if (!matches(groupingSets.getGroupingKeys(), aggregationNode.getGroupingKeys(), symbolAliases)) {
            return NO_MATCH;
        }

        if (groupingSets.getGroupingSetCount() != aggregationNode.getGroupingSetCount()) {
            return NO_MATCH;
        }

        if (!groupingSets.getGlobalGroupingSets().equals(aggregationNode.getGlobalGroupingSets())) {
            return NO_MATCH;
        }

        Set<Symbol> actualMasks = aggregationNode.getAggregations().values().stream()
                .filter(aggregation -> aggregation.getMask().isPresent())
                .map(aggregation -> aggregation.getMask().get())
                .collect(toImmutableSet());

        Set<Symbol> expectedMasks = masks.stream()
                .map(name -> new Symbol(symbolAliases.get(name).getName()))
                .collect(toImmutableSet());

        if (!actualMasks.equals(expectedMasks)) {
            return NO_MATCH;
        }

        if (step != aggregationNode.getStep()) {
            return NO_MATCH;
        }

        if (!matches(preGroupedSymbols, aggregationNode.getPreGroupedSymbols(), symbolAliases)) {
            return NO_MATCH;
        }

        if (!preGroupedSymbols.isEmpty() && !aggregationNode.isStreamable()) {
            return NO_MATCH;
        }

        return match();
    }

    static boolean matches(Collection<String> expectedAliases, Collection<Symbol> actualSymbols, SymbolAliases symbolAliases)
    {
        if (expectedAliases.size() != actualSymbols.size()) {
            return false;
        }

        List<Symbol> expectedSymbols = expectedAliases
                .stream()
                .map(alias -> new Symbol(symbolAliases.get(alias).getName()))
                .collect(toImmutableList());
        for (Symbol symbol : expectedSymbols) {
            if (!actualSymbols.contains(symbol)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("groupingSets", groupingSets)
                .add("preGroupedSymbols", preGroupedSymbols)
                .add("masks", masks)
                .add("groupId", groupId)
                .add("step", step)
                .toString();
    }
}
