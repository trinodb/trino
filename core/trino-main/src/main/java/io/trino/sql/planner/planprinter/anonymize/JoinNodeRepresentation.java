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
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.planprinter.TypedSymbol;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.plan.JoinNode.DistributionType;
import static io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import static io.trino.sql.planner.plan.JoinNode.Type;
import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.anonymize;
import static java.util.Objects.requireNonNull;

public class JoinNodeRepresentation
        extends AnonymizedNodeRepresentation
{
    private final Type type;
    private final List<EquiJoinClause> criteria;
    private final boolean maySkipOutputDuplicates;
    private final Optional<DistributionType> distributionType;

    @JsonCreator
    public JoinNodeRepresentation(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputLayout") List<TypedSymbol> outputLayout,
            @JsonProperty("sources") List<AnonymizedNodeRepresentation> sources,
            @JsonProperty("type") Type type,
            @JsonProperty("criteria") List<EquiJoinClause> criteria,
            @JsonProperty("maySkipOutputDuplicates") boolean maySkipOutputDuplicates,
            @JsonProperty("distributionType") Optional<DistributionType> distributionType)
    {
        super(id, outputLayout, sources);
        this.type = requireNonNull(type, "type is null");
        this.criteria = requireNonNull(criteria, "criteria is null");
        this.maySkipOutputDuplicates = maySkipOutputDuplicates;
        this.distributionType = requireNonNull(distributionType, "distributionType is null");
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public List<EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @JsonProperty
    public boolean isMaySkipOutputDuplicates()
    {
        return maySkipOutputDuplicates;
    }

    @JsonProperty
    public Optional<DistributionType> getDistributionType()
    {
        return distributionType;
    }

    public static JoinNodeRepresentation fromPlanNode(JoinNode node, TypeProvider typeProvider, List<AnonymizedNodeRepresentation> sources)
    {
        return new JoinNodeRepresentation(
                node.getId(),
                anonymize(node.getOutputSymbols(), typeProvider),
                sources,
                node.getType(),
                node.getCriteria().stream()
                        .map(clause -> new JoinNode.EquiJoinClause(anonymize(clause.getLeft()), anonymize(clause.getRight())))
                        .collect(toImmutableList()),
                node.isMaySkipOutputDuplicates(),
                node.getDistributionType());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JoinNodeRepresentation that = (JoinNodeRepresentation) o;
        return getId().equals(that.getId())
                && getOutputLayout().equals(that.getOutputLayout())
                && getSources().equals(that.getSources())
                && maySkipOutputDuplicates == that.maySkipOutputDuplicates
                && type == that.type
                && criteria.equals(that.criteria)
                && distributionType.equals(that.distributionType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getId(), getOutputLayout(), getSources(), type, criteria, maySkipOutputDuplicates, distributionType);
    }
}
