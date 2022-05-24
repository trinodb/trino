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

package io.trino.sql.planner.planprinter.anonymize;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.planprinter.TypedSymbol;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.trino.sql.planner.plan.SemiJoinNode.DistributionType;
import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.anonymize;
import static java.util.Objects.requireNonNull;

public class SemiJoinNodeRepresentation
        extends AnonymizedNodeRepresentation
{
    private final Symbol sourceJoinSymbol;
    private final Symbol filteringSourceJoinSymbol;
    private final Optional<Symbol> sourceHashSymbol;
    private final Optional<Symbol> filteringSourceHashSymbol;
    private final Optional<DistributionType> distributionType;

    @JsonCreator
    public SemiJoinNodeRepresentation(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputLayout") List<TypedSymbol> outputLayout,
            @JsonProperty("sources") List<AnonymizedNodeRepresentation> sources,
            @JsonProperty("sourceJoinSymbol") Symbol sourceJoinSymbol,
            @JsonProperty("filteringSourceJoinSymbol") Symbol filteringSourceJoinSymbol,
            @JsonProperty("sourceHashSymbol") Optional<Symbol> sourceHashSymbol,
            @JsonProperty("filteringSourceHashSymbol") Optional<Symbol> filteringSourceHashSymbol,
            @JsonProperty("distributionType") Optional<DistributionType> distributionType)
    {
        super(id, outputLayout, sources);
        this.sourceJoinSymbol = requireNonNull(sourceJoinSymbol, "sourceJoinSymbol is null");
        this.filteringSourceJoinSymbol = requireNonNull(filteringSourceJoinSymbol, "filteringSourceJoinSymbol is null");
        this.sourceHashSymbol = requireNonNull(sourceHashSymbol, "sourceHashSymbol is null");
        this.filteringSourceHashSymbol = requireNonNull(filteringSourceHashSymbol, "filteringSourceHashSymbol is null");
        this.distributionType = requireNonNull(distributionType, "distributionType is null");
    }

    @JsonProperty
    public Symbol getSourceJoinSymbol()
    {
        return sourceJoinSymbol;
    }

    @JsonProperty
    public Symbol getFilteringSourceJoinSymbol()
    {
        return filteringSourceJoinSymbol;
    }

    @JsonProperty("sourceHashSymbol")
    public Optional<Symbol> getSourceHashSymbol()
    {
        return sourceHashSymbol;
    }

    @JsonProperty("filteringSourceHashSymbol")
    public Optional<Symbol> getFilteringSourceHashSymbol()
    {
        return filteringSourceHashSymbol;
    }

    @JsonProperty
    public Optional<SemiJoinNode.DistributionType> getDistributionType()
    {
        return distributionType;
    }

    public static SemiJoinNodeRepresentation fromPlanNode(SemiJoinNode node, TypeProvider typeProvider, List<AnonymizedNodeRepresentation> sources)
    {
        return new SemiJoinNodeRepresentation(
                node.getId(),
                anonymize(node.getOutputSymbols(), typeProvider),
                sources,
                anonymize(node.getSourceJoinSymbol()),
                anonymize(node.getFilteringSourceJoinSymbol()),
                node.getSourceHashSymbol().map(AnonymizationUtils::anonymize),
                node.getFilteringSourceHashSymbol().map(AnonymizationUtils::anonymize),
                node.getDistributionType());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SemiJoinNodeRepresentation)) {
            return false;
        }
        SemiJoinNodeRepresentation that = (SemiJoinNodeRepresentation) o;
        return getId().equals(that.getId())
                && getOutputLayout().equals(that.getOutputLayout())
                && getSources().equals(that.getSources())
                && sourceJoinSymbol.equals(that.sourceJoinSymbol)
                && filteringSourceJoinSymbol.equals(that.filteringSourceJoinSymbol)
                && sourceHashSymbol.equals(that.sourceHashSymbol)
                && filteringSourceHashSymbol.equals(that.filteringSourceHashSymbol)
                && distributionType.equals(that.distributionType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                getId(),
                getOutputLayout(),
                getSources(),
                sourceJoinSymbol,
                filteringSourceJoinSymbol,
                sourceHashSymbol,
                filteringSourceHashSymbol,
                distributionType);
    }
}
