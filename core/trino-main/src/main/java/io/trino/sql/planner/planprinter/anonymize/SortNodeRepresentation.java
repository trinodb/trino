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
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.planprinter.TypedSymbol;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.anonymize;
import static java.util.Objects.requireNonNull;

public class SortNodeRepresentation
        extends AnonymizedNodeRepresentation
{
    private final Map<Symbol, SortOrder> orderings;
    private final boolean partial;

    @JsonCreator
    public SortNodeRepresentation(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputLayout") List<TypedSymbol> outputLayout,
            @JsonProperty("sources") List<AnonymizedNodeRepresentation> sources,
            @JsonProperty("orderings") Map<Symbol, SortOrder> orderings,
            @JsonProperty("partial") boolean partial)
    {
        super(id, outputLayout, sources);
        this.orderings = requireNonNull(orderings, "orderings is null");
        this.partial = partial;
    }

    @JsonProperty
    public Map<Symbol, SortOrder> getOrderings()
    {
        return orderings;
    }

    @JsonProperty
    public boolean isPartial()
    {
        return partial;
    }

    public static SortNodeRepresentation fromPlanNode(SortNode node, TypeProvider typeProvider, List<AnonymizedNodeRepresentation> sources)
    {
        return new SortNodeRepresentation(
                node.getId(),
                anonymize(node.getOutputSymbols(), typeProvider),
                sources,
                anonymize(node.getOrderingScheme()),
                node.isPartial());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SortNodeRepresentation)) {
            return false;
        }
        SortNodeRepresentation that = (SortNodeRepresentation) o;
        return getId().equals(that.getId())
                && getOutputLayout().equals(that.getOutputLayout())
                && getSources().equals(that.getSources())
                && partial == that.partial
                && orderings.equals(that.orderings);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getId(), getOutputLayout(), getSources(), orderings, partial);
    }
}
