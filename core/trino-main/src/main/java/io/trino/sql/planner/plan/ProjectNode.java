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
import io.trino.sql.planner.Symbol;

import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public class ProjectNode
        extends PlanNode
{
    private final PlanNode source;
    private final Assignments assignments;

    // TODO: pass in the "assignments" and the "outputs" separately (i.e., get rid if the symbol := symbol idiom)
    @JsonCreator
    public ProjectNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("assignments") Assignments assignments)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(assignments, "assignments is null");

        this.source = source;
        this.assignments = assignments;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.copyOf(assignments.outputs());
    }

    @JsonProperty
    public Assignments getAssignments()
    {
        return assignments;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    public boolean isIdentity()
    {
        return assignments.isIdentity();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitProject(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new ProjectNode(getId(), Iterables.getOnlyElement(newChildren), assignments);
    }
}
