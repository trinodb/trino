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
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.planprinter.TypedSymbol;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.sql.ExpressionUtils.unResolveFunctions;
import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.anonymize;
import static java.util.Objects.requireNonNull;

public class ProjectNodeRepresentation
        extends AnonymizedNodeRepresentation
{
    private final Map<Symbol, String> assignments;

    @JsonCreator
    public ProjectNodeRepresentation(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputLayout") List<TypedSymbol> outputLayout,
            @JsonProperty("sources") List<AnonymizedNodeRepresentation> sources,
            @JsonProperty("assignments") Map<Symbol, String> assignments)
    {
        super(id, outputLayout, sources);
        this.assignments = requireNonNull(assignments, "assignments is null");
    }

    @JsonProperty
    public Map<Symbol, String> getAssignments()
    {
        return assignments;
    }

    public static ProjectNodeRepresentation fromPlanNode(ProjectNode node, TypeProvider typeProvider, List<AnonymizedNodeRepresentation> sources)
    {
        return new ProjectNodeRepresentation(
                node.getId(),
                anonymize(node.getOutputSymbols(), typeProvider),
                sources,
                node.getAssignments().entrySet().stream()
                        .collect(toImmutableMap(
                                entry -> anonymize(entry.getKey()),
                                entry -> anonymize(unResolveFunctions(entry.getValue())).toString())));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ProjectNodeRepresentation)) {
            return false;
        }
        ProjectNodeRepresentation that = (ProjectNodeRepresentation) o;
        return getId().equals(that.getId())
                && getOutputLayout().equals(that.getOutputLayout())
                && getSources().equals(that.getSources())
                && assignments.equals(that.assignments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getId(), getOutputLayout(), getSources(), assignments);
    }
}
