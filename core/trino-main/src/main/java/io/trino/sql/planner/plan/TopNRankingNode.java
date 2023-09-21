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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.Immutable;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.concat;
import static java.util.Objects.requireNonNull;

@Immutable
public final class TopNRankingNode
        extends PlanNode
{
    public enum RankingType
    {
        ROW_NUMBER,
        RANK,
        DENSE_RANK
    }

    private final PlanNode source;
    private final DataOrganizationSpecification specification;
    private final RankingType rankingType;
    private final Symbol rankingSymbol;
    private final int maxRankingPerPartition;
    private final boolean partial;
    private final Optional<Symbol> hashSymbol;

    @JsonCreator
    public TopNRankingNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("specification") DataOrganizationSpecification specification,
            @JsonProperty("rankingType") RankingType rankingType,
            @JsonProperty("rankingSymbol") Symbol rankingSymbol,
            @JsonProperty("maxRankingPerPartition") int maxRankingPerPartition,
            @JsonProperty("partial") boolean partial,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(specification, "specification is null");
        checkArgument(specification.getOrderingScheme().isPresent(), "specification orderingScheme is absent");
        requireNonNull(rankingType, "rankingType is null");
        requireNonNull(rankingSymbol, "rankingSymbol is null");
        checkArgument(maxRankingPerPartition > 0, "maxRankingPerPartition must be > 0");
        requireNonNull(hashSymbol, "hashSymbol is null");

        this.source = source;
        this.specification = specification;
        this.rankingType = rankingType;
        this.rankingSymbol = rankingSymbol;
        this.maxRankingPerPartition = maxRankingPerPartition;
        this.partial = partial;
        this.hashSymbol = hashSymbol;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        if (!partial) {
            return ImmutableList.copyOf(concat(source.getOutputSymbols(), ImmutableList.of(rankingSymbol)));
        }
        return ImmutableList.copyOf(source.getOutputSymbols());
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public DataOrganizationSpecification getSpecification()
    {
        return specification;
    }

    public List<Symbol> getPartitionBy()
    {
        return specification.getPartitionBy();
    }

    public OrderingScheme getOrderingScheme()
    {
        return specification.getOrderingScheme().get();
    }

    @JsonProperty
    public RankingType getRankingType()
    {
        return rankingType;
    }

    @JsonProperty
    public Symbol getRankingSymbol()
    {
        return rankingSymbol;
    }

    @JsonProperty
    public int getMaxRankingPerPartition()
    {
        return maxRankingPerPartition;
    }

    @JsonProperty
    public boolean isPartial()
    {
        return partial;
    }

    @JsonProperty
    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTopNRanking(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new TopNRankingNode(getId(), Iterables.getOnlyElement(newChildren), specification, rankingType, rankingSymbol, maxRankingPerPartition, partial, hashSymbol);
    }
}
