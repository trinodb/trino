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
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.planprinter.TypedSymbol;

import java.util.List;
import java.util.Objects;

import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.anonymize;
import static java.util.Objects.requireNonNull;

public class DefaultNodeRepresentation
        extends AnonymizedNodeRepresentation
{
    private final String name;

    @JsonCreator
    public DefaultNodeRepresentation(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("name") String name,
            @JsonProperty("outputLayout") List<TypedSymbol> outputLayout,
            @JsonProperty("sources") List<AnonymizedNodeRepresentation> sources)
    {
        super(id, outputLayout, sources);
        this.name = requireNonNull(name, "name is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    public static DefaultNodeRepresentation fromPlanNode(PlanNode node, TypeProvider provider, List<AnonymizedNodeRepresentation> sources)
    {
        return new DefaultNodeRepresentation(
                node.getId(),
                node.getClass().getSimpleName(),
                anonymize(node.getOutputSymbols(), provider),
                sources);
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
        DefaultNodeRepresentation that = (DefaultNodeRepresentation) o;
        return getId().equals(that.getId())
                && getOutputLayout().equals(that.getOutputLayout())
                && getSources().equals(that.getSources())
                && name.equals(that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getId(), getOutputLayout(), getSources(), name);
    }
}
