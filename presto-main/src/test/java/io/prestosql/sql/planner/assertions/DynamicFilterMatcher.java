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
package io.prestosql.sql.planner.assertions;

import com.google.common.base.Joiner;
import io.prestosql.Session;
import io.prestosql.cost.StatsProvider;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.tree.Expression;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.sql.DynamicFilters.extractDynamicFilters;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static java.util.Objects.requireNonNull;

public class DynamicFilterMatcher
        implements Matcher
{
    private final Metadata metadata;

    // LEFT_SYMBOL -> RIGHT_SYMBOL
    private final Map<SymbolAlias, SymbolAlias> expectedDynamicFilters;
    private final Map<String, String> joinExpectedMappings;
    private final Map<String, String> filterExpectedMappings;
    private final Optional<Expression> expectedStaticFilter;

    private JoinNode joinNode;
    private SymbolAliases symbolAliases;
    private FilterNode filterNode;

    public DynamicFilterMatcher(Metadata metadata, Map<SymbolAlias, SymbolAlias> expectedDynamicFilters, Optional<Expression> expectedStaticFilter)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.expectedDynamicFilters = requireNonNull(expectedDynamicFilters, "expectedDynamicFilters is null");
        this.joinExpectedMappings = expectedDynamicFilters.values().stream()
                .collect(toImmutableMap(rightSymbol -> rightSymbol.toString() + "_alias", SymbolAlias::toString));
        this.filterExpectedMappings = expectedDynamicFilters.entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().toString(), entry -> entry.getValue().toString() + "_alias"));
        this.expectedStaticFilter = requireNonNull(expectedStaticFilter, "expectedStaticFilter is null");
    }

    public MatchResult match(Metadata metadata, JoinNode joinNode, SymbolAliases symbolAliases)
    {
        checkState(this.joinNode == null, "joinNode must be null at this point");
        this.joinNode = joinNode;
        this.symbolAliases = symbolAliases;
        return new MatchResult(match(metadata));
    }

    public MatchResult match(Metadata metadata, FilterNode filterNode, SymbolAliases symbolAliases)
    {
        checkState(this.filterNode == null, "filterNode must be null at this point");
        this.filterNode = filterNode;
        this.symbolAliases = symbolAliases;

        boolean staticFilterMatches = expectedStaticFilter.map(filter -> {
            ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);
            Expression staticFilter = combineConjuncts(metadata, extractDynamicFilters(metadata, filterNode.getPredicate()).getStaticConjuncts());
            return verifier.process(staticFilter, filter);
        }).orElse(true);

        return new MatchResult(match(metadata) && staticFilterMatches);
    }

    private boolean match(Metadata metadata)
    {
        checkState(symbolAliases != null, "symbolAliases is null");

        // both nodes must be provided to do the matching
        if (filterNode == null || joinNode == null) {
            return true;
        }

        Map<String, Symbol> idToProbeSymbolMap = extractDynamicFilters(metadata, filterNode.getPredicate())
                .getDynamicConjuncts().stream()
                .collect(toImmutableMap(DynamicFilters.Descriptor::getId, filter -> Symbol.from(filter.getInput())));
        Map<String, Symbol> idToBuildSymbolMap = joinNode.getDynamicFilters();

        if (idToProbeSymbolMap == null) {
            return false;
        }

        if (idToProbeSymbolMap.size() != expectedDynamicFilters.size()) {
            return false;
        }

        Map<Symbol, Symbol> actual = new HashMap<>();
        for (Map.Entry<String, Symbol> idToProbeSymbol : idToProbeSymbolMap.entrySet()) {
            String id = idToProbeSymbol.getKey();
            Symbol probe = idToProbeSymbol.getValue();
            Symbol build = idToBuildSymbolMap.get(id);
            if (build == null) {
                return false;
            }
            actual.put(probe, build);
        }

        Map<Symbol, Symbol> expected = expectedDynamicFilters.entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().toSymbol(symbolAliases), entry -> entry.getValue().toSymbol(symbolAliases)));

        return expected.equals(actual);
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof FilterNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        if (!(node instanceof FilterNode)) {
            return new MatchResult(false);
        }
        return match(metadata, (FilterNode) node, symbolAliases);
    }

    public Map<String, String> getJoinExpectedMappings()
    {
        return joinExpectedMappings;
    }

    @Override
    public String toString()
    {
        String predicate = Joiner.on(" AND ")
                .join(filterExpectedMappings.entrySet().stream()
                        .map(entry -> entry.getKey() + " = " + entry.getValue())
                        .collect(toImmutableList()));
        return toStringHelper(this)
                .add("dynamicPredicate", predicate)
                .add("staticPredicate", expectedStaticFilter)
                .toString();
    }
}
