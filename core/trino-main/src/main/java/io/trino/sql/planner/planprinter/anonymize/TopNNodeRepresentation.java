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
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.planprinter.TypedSymbol;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.trino.sql.planner.plan.TopNNode.Step;
import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.anonymize;
import static java.util.Objects.requireNonNull;

public class TopNNodeRepresentation
        extends AnonymizedNodeRepresentation
{
    private final long count;
    private final Map<Symbol, SortOrder> orderings;
    private final Step step;

    @JsonCreator
    public TopNNodeRepresentation(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputLayout") List<TypedSymbol> outputLayout,
            @JsonProperty("sources") List<AnonymizedNodeRepresentation> sources,
            @JsonProperty("count") long count,
            @JsonProperty("orderings") Map<Symbol, SortOrder> orderings,
            @JsonProperty("step") Step step)
    {
        super(id, outputLayout, sources);
        this.count = count;
        this.orderings = requireNonNull(orderings, "orderings is null");
        this.step = requireNonNull(step, "step is null");
    }

    @JsonProperty
    public long getCount()
    {
        return count;
    }

    @JsonProperty
    public Map<Symbol, SortOrder> getOrderings()
    {
        return orderings;
    }

    @JsonProperty
    public Step getStep()
    {
        return step;
    }

    public static TopNNodeRepresentation fromPlanNode(TopNNode node, TypeProvider typeProvider, List<AnonymizedNodeRepresentation> sources)
    {
        return new TopNNodeRepresentation(
                node.getId(),
                anonymize(node.getOutputSymbols(), typeProvider),
                sources,
                node.getCount(),
                anonymize(node.getOrderingScheme()),
                node.getStep());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TopNNodeRepresentation)) {
            return false;
        }
        TopNNodeRepresentation that = (TopNNodeRepresentation) o;
        return getId().equals(that.getId())
                && getOutputLayout().equals(that.getOutputLayout())
                && getSources().equals(that.getSources())
                && count == that.count
                && orderings.equals(that.orderings)
                && step == that.step;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getId(), getOutputLayout(), getSources(), count, orderings, step);
    }
}
