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
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.planprinter.TypedSymbol;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.anonymize;
import static java.util.Objects.requireNonNull;

public class RemoteSourceNodeRepresentation
        extends AnonymizedNodeRepresentation
{
    private final List<PlanFragmentId> sourceFragmentIds;
    private final Optional<Map<Symbol, SortOrder>> orderings;
    private final ExchangeNode.Type exchangeType;

    @JsonCreator
    public RemoteSourceNodeRepresentation(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputLayout") List<TypedSymbol> outputLayout,
            @JsonProperty("sourceFragmentIds") List<PlanFragmentId> sourceFragmentIds,
            @JsonProperty("orderings") Optional<Map<Symbol, SortOrder>> orderings,
            @JsonProperty("exchangeType") ExchangeNode.Type exchangeType)
    {
        super(id, outputLayout, ImmutableList.of());
        this.sourceFragmentIds = requireNonNull(sourceFragmentIds, "sourceFragmentIds is null");
        this.orderings = requireNonNull(orderings, "orderings is null");
        this.exchangeType = requireNonNull(exchangeType, "exchangeType is null");
    }

    @JsonProperty
    public List<PlanFragmentId> getSourceFragmentIds()
    {
        return sourceFragmentIds;
    }

    @JsonProperty
    public Optional<Map<Symbol, SortOrder>> getOrderings()
    {
        return orderings;
    }

    @JsonProperty
    public ExchangeNode.Type getExchangeType()
    {
        return exchangeType;
    }

    public static RemoteSourceNodeRepresentation fromPlanNode(RemoteSourceNode node, TypeProvider typeProvider)
    {
        return new RemoteSourceNodeRepresentation(
                node.getId(),
                anonymize(node.getOutputSymbols(), typeProvider),
                node.getSourceFragmentIds(),
                node.getOrderingScheme().map(AnonymizationUtils::anonymize),
                node.getExchangeType());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RemoteSourceNodeRepresentation)) {
            return false;
        }
        RemoteSourceNodeRepresentation that = (RemoteSourceNodeRepresentation) o;
        return getId().equals(that.getId())
                && getOutputLayout().equals(that.getOutputLayout())
                && getSources().equals(that.getSources())
                && sourceFragmentIds.equals(that.sourceFragmentIds)
                && orderings.equals(that.orderings)
                && exchangeType == that.exchangeType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getId(), getOutputLayout(), getSources(), sourceFragmentIds, orderings, exchangeType);
    }
}
