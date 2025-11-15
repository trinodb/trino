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
    private final List<String> catalogNames;
    private final List<String> schemaNames;
    private final List<String> tableNames;
    private final List<String> columnNames;
    private final List<String> columnLabels;
    private final List<Symbol> outputs; // column name = symbol

    @JsonCreator
    public OutputNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("catalogs") List<String> catalogNames,
            @JsonProperty("schemas") List<String> schemaNames,
            @JsonProperty("tables") List<String> tableNames,
            @JsonProperty("columns") List<String> columnNames,
            @JsonProperty("labels") List<String> columnLabels,
            @JsonProperty("outputs") List<Symbol> outputs)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(catalogNames, "catalogNames is null");
        requireNonNull(schemaNames, "schemaNames is null");
        requireNonNull(tableNames, "tableNames is null");
        requireNonNull(columnNames, "columnNames is null");
        requireNonNull(columnLabels, "columnLabels is null");
        requireNonNull(outputs, "outputs is null");
        checkArgument(columnNames.size() == catalogNames.size(), "columnNames and catalogNames sizes don't match");
        checkArgument(columnNames.size() == schemaNames.size(), "columnNames and schemaNames sizes don't match");
        checkArgument(columnNames.size() == tableNames.size(), "columnNames and tableNames sizes don't match");
        checkArgument(columnNames.size() == columnLabels.size(), "columnNames and columnLabels sizes don't match");
        checkArgument(columnNames.size() == outputs.size(), "columnNames and assignments sizes don't match");

        this.source = source;
        this.catalogNames = ImmutableList.copyOf(catalogNames);
        this.schemaNames = ImmutableList.copyOf(schemaNames);
        this.tableNames = ImmutableList.copyOf(tableNames);
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.columnLabels = ImmutableList.copyOf(columnLabels);
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

    @JsonProperty("catalogs")
    public List<String> getCatalogNames()
    {
        return catalogNames;
    }

    @JsonProperty("schemas")
    public List<String> getSchemaNames()
    {
        return schemaNames;
    }

    @JsonProperty("tables")
    public List<String> getTableNames()
    {
        return tableNames;
    }

    @JsonProperty("columns")
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty("labels")
    public List<String> getColumnLabels()
    {
        return columnLabels;
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
        return new OutputNode(getId(), Iterables.getOnlyElement(newChildren), catalogNames, schemaNames, tableNames, columnNames, columnLabels, outputs);
    }
}
