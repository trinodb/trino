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
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.UnnestNode.Mapping;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static java.util.Objects.requireNonNull;

final class UnnestMatcher
        implements Matcher
{
    private final List<String> replicateSymbols;
    private final List<PlanMatchPattern.UnnestMapping> unnestMappings;
    private final Optional<String> ordinalitySymbol;
    private final JoinNode.Type type;
    private final Optional<Expression> filter;

    public UnnestMatcher(List<String> replicateSymbols, List<PlanMatchPattern.UnnestMapping> unnestMappings, Optional<String> ordinalitySymbol, JoinNode.Type type, Optional<Expression> filter)
    {
        this.replicateSymbols = requireNonNull(replicateSymbols, "replicateSymbols is null");
        this.unnestMappings = requireNonNull(unnestMappings, "unnestMappings is null");
        this.ordinalitySymbol = requireNonNull(ordinalitySymbol, "ordinalitySymbol is null");
        this.type = requireNonNull(type, "type is null");
        this.filter = requireNonNull(filter, "filter is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        if (!(node instanceof UnnestNode unnestNode)) {
            return false;
        }

        return unnestNode.getJoinType() == type;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        UnnestNode unnestNode = (UnnestNode) node;

        if (unnestNode.getReplicateSymbols().size() != replicateSymbols.size()) {
            return NO_MATCH;
        }
        if (!replicateSymbols.stream()
                .map(symbolAliases::get)
                .map(Symbol::from)
                .collect(toImmutableList())
                .equals(unnestNode.getReplicateSymbols())) {
            return NO_MATCH;
        }

        if (unnestNode.getMappings().size() != unnestMappings.size()) {
            return NO_MATCH;
        }

        if (!IntStream.range(0, unnestMappings.size()).boxed().allMatch(index -> {
            Mapping nodeMapping = unnestNode.getMappings().get(index);
            PlanMatchPattern.UnnestMapping patternMapping = unnestMappings.get(index);
            return nodeMapping.getInput().toSymbolReference().equals(symbolAliases.get(patternMapping.getInput())) &&
                    patternMapping.getOutputs().size() == nodeMapping.getOutputs().size();
        })) {
            return NO_MATCH;
        }

        if (ordinalitySymbol.isPresent() != unnestNode.getOrdinalitySymbol().isPresent()) {
            return NO_MATCH;
        }

        if (!type.equals(unnestNode.getJoinType())) {
            return NO_MATCH;
        }

        if (filter.isPresent() != unnestNode.getFilter().isPresent()) {
            return NO_MATCH;
        }
        if (filter.isEmpty()) {
            return MatchResult.match();
        }
        if (!new ExpressionVerifier(symbolAliases).process(unnestNode.getFilter().get(), filter.get())) {
            return NO_MATCH;
        }

        return MatchResult.match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("type", type)
                .add("replicateSymbols", replicateSymbols)
                .add("unnestMappings", unnestMappings)
                .add("ordinalitySymbol", ordinalitySymbol.orElse(null))
                .add("filter", filter.orElse(null))
                .toString();
    }
}
