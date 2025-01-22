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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.IndexHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.SymbolUtils.containsAll;
import static java.util.Objects.requireNonNull;

public class IndexSourceNode
        extends PlanNode
{
    private final IndexHandle indexHandle;
    private final TableHandle tableHandle;
    private final Set<Symbol> lookupSymbols;
    private final List<Symbol> outputSymbols;
    private final Map<Symbol, ColumnHandle> assignments; // symbol -> column

    @JsonCreator
    public IndexSourceNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("indexHandle") IndexHandle indexHandle,
            @JsonProperty("tableHandle") TableHandle tableHandle,
            @JsonProperty("lookupSymbols") Set<Symbol> lookupSymbols,
            @JsonProperty("outputSymbols") List<Symbol> outputSymbols,
            @JsonProperty("assignments") Map<Symbol, ColumnHandle> assignments)
    {
        super(id);
        this.indexHandle = requireNonNull(indexHandle, "indexHandle is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.lookupSymbols = ImmutableSet.copyOf(requireNonNull(lookupSymbols, "lookupSymbols is null"));
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputSymbols, "outputSymbols is null"));
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        checkArgument(!lookupSymbols.isEmpty(), "lookupSymbols is empty");
        checkArgument(!outputSymbols.isEmpty(), "outputSymbols is empty");
        checkArgument(assignments.keySet().containsAll(lookupSymbols), "Assignments do not include all lookup symbols");
        checkArgument(containsAll(outputSymbols, lookupSymbols), "Lookup symbols need to be part of the output symbols");
    }

    @JsonProperty
    public IndexHandle getIndexHandle()
    {
        return indexHandle;
    }

    @JsonProperty
    public TableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public Set<Symbol> getLookupSymbols()
    {
        return lookupSymbols;
    }

    @Override
    @JsonProperty
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @JsonProperty
    public Map<Symbol, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitIndexSource(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return this;
    }
}
