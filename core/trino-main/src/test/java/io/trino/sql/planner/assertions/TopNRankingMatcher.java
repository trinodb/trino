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
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.TopNRankingNode.RankingType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static java.util.Objects.requireNonNull;

public class TopNRankingMatcher
        implements Matcher
{
    private final Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification;
    private final Optional<SymbolAlias> rankingSymbol;
    private final Optional<RankingType> rankingType;
    private final Optional<Integer> maxRankingPerPartition;
    private final Optional<Boolean> partial;
    private final Optional<Optional<SymbolAlias>> hashSymbol;

    private TopNRankingMatcher(
            Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification,
            Optional<SymbolAlias> rankingSymbol,
            Optional<RankingType> rankingType,
            Optional<Integer> maxRankingPerPartition,
            Optional<Boolean> partial,
            Optional<Optional<SymbolAlias>> hashSymbol)
    {
        this.specification = requireNonNull(specification, "specification is null");
        this.rankingSymbol = requireNonNull(rankingSymbol, "rankingSymbol is null");
        this.rankingType = requireNonNull(rankingType, "rankingType is null");
        this.maxRankingPerPartition = requireNonNull(maxRankingPerPartition, "maxRankingPerPartition is null");
        this.partial = requireNonNull(partial, "partial is null");
        this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof TopNRankingNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        TopNRankingNode topNRankingNode = (TopNRankingNode) node;

        if (specification.isPresent()) {
            DataOrganizationSpecification expected = specification.get().getExpectedValue(symbolAliases);
            if (!expected.equals(topNRankingNode.getSpecification())) {
                return NO_MATCH;
            }
        }

        if (rankingSymbol.isPresent()) {
            Symbol expected = rankingSymbol.get().toSymbol(symbolAliases);
            if (!expected.equals(topNRankingNode.getRankingSymbol())) {
                return NO_MATCH;
            }
        }

        if (rankingType.isPresent()) {
            if (!rankingType.get().equals(topNRankingNode.getRankingType())) {
                return NO_MATCH;
            }
        }

        if (maxRankingPerPartition.isPresent()) {
            if (!maxRankingPerPartition.get().equals(topNRankingNode.getMaxRankingPerPartition())) {
                return NO_MATCH;
            }
        }

        if (partial.isPresent()) {
            if (!partial.get().equals(topNRankingNode.isPartial())) {
                return NO_MATCH;
            }
        }

        if (hashSymbol.isPresent()) {
            Optional<Symbol> expected = hashSymbol.get().map(alias -> alias.toSymbol(symbolAliases));
            if (!expected.equals(topNRankingNode.getHashSymbol())) {
                return NO_MATCH;
            }
        }

        return match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("specification", specification)
                .add("rankingSymbol", rankingSymbol)
                .add("rankingType", rankingType)
                .add("maxRankingPerPartition", maxRankingPerPartition)
                .add("partial", partial)
                .add("hashSymbol", hashSymbol)
                .toString();
    }

    public static class Builder
    {
        private final PlanMatchPattern source;
        private Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification = Optional.empty();
        private Optional<SymbolAlias> rankingSymbol = Optional.empty();
        private Optional<RankingType> rankingType = Optional.empty();
        private Optional<Integer> maxRankingPerPartition = Optional.empty();
        private Optional<Boolean> partial = Optional.empty();
        private Optional<Optional<SymbolAlias>> hashSymbol = Optional.empty();

        Builder(PlanMatchPattern source)
        {
            this.source = requireNonNull(source, "source is null");
        }

        public Builder specification(List<String> partitionBy, List<String> orderBy, Map<String, SortOrder> orderings)
        {
            this.specification = Optional.of(PlanMatchPattern.specification(partitionBy, orderBy, orderings));
            return this;
        }

        public Builder rankingSymbol(SymbolAlias rankingSymbol)
        {
            this.rankingSymbol = Optional.of(requireNonNull(rankingSymbol, "rankingSymbol is null"));
            return this;
        }

        public Builder rankingType(RankingType rankingType)
        {
            this.rankingType = Optional.of(requireNonNull(rankingType, "rankingType is null"));
            return this;
        }

        public Builder maxRankingPerPartition(int maxRankingPerPartition)
        {
            this.maxRankingPerPartition = Optional.of(maxRankingPerPartition);
            return this;
        }

        public Builder partial(boolean partial)
        {
            this.partial = Optional.of(partial);
            return this;
        }

        public Builder hashSymbol(Optional<SymbolAlias> hashSymbol)
        {
            this.hashSymbol = Optional.of(requireNonNull(hashSymbol, "hashSymbol is null"));
            return this;
        }

        PlanMatchPattern build()
        {
            return node(TopNRankingNode.class, source).with(
                    new TopNRankingMatcher(
                            specification,
                            rankingSymbol,
                            rankingType,
                            maxRankingPerPartition,
                            partial,
                            hashSymbol));
        }
    }
}
