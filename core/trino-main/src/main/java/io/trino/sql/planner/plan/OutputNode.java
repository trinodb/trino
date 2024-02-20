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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class OutputNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<String> columnNames;
    private final List<Symbol> outputs; // column name = symbol

    @JsonCreator
    public OutputNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("columns") List<String> columnNames,
            @JsonProperty("outputs") List<Symbol> outputs)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(columnNames, "columnNames is null");
        requireNonNull(outputs, "outputs is null");
        checkArgument(columnNames.size() == outputs.size(), "columnNames and assignments sizes don't match");

        this.source = source;
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.outputs = ImmutableList.copyOf(outputs);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    @JsonProperty("outputs")
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @JsonProperty("columns")
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitOutput(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new OutputNode(getId(), Iterables.getOnlyElement(newChildren), columnNames, outputs);
    }
}
