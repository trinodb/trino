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
import com.google.errorprone.annotations.Immutable;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class ChooseAlternativeNode
        extends PlanNode
{
    private final List<PlanNode> alternatives;

    private final FilteredTableScan originalTableScan;

    @JsonCreator
    public ChooseAlternativeNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("sources") List<PlanNode> alternatives,
            @JsonProperty("originalTableScan") FilteredTableScan originalTableScan)
    {
        super(id);

        requireNonNull(alternatives, "alternatives is null");
        checkArgument(alternatives.size() > 1, "Expected at least two alternative");
        checkArgument(sameOutputSymbols(alternatives), "All alternatives should have the same output symbols");
        this.alternatives = ImmutableList.copyOf(alternatives);

        this.originalTableScan = requireNonNull(originalTableScan, "originalTableScan is null");
    }

    private boolean sameOutputSymbols(List<PlanNode> alternatives)
    {
        List<Symbol> outputSymbols = alternatives.get(0).getOutputSymbols();
        for (int i = 1; i < alternatives.size(); i++) {
            if (!outputSymbols.equals(alternatives.get(i).getOutputSymbols())) {
                return false;
            }
        }
        return true;
    }

    @JsonProperty
    public FilteredTableScan getOriginalTableScan()
    {
        return originalTableScan;
    }

    @JsonProperty
    @Override
    public List<PlanNode> getSources()
    {
        return alternatives;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        // all alternatives must have the same output symbols but can theoretically differ on the order.
        // any order would work here, so we pick the order from the first alternative.
        // this is consistent with LocalExecutionPlanner.Visitor.visitChooseAlternativeNode
        return alternatives.get(0).getOutputSymbols();
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == alternatives.size(), "expected newChildren to contain %s nodes", alternatives.size());
        return new ChooseAlternativeNode(getId(), newChildren, originalTableScan);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitChooseAlternativeNode(this, context);
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
        ChooseAlternativeNode chooseAlternativeNode = (ChooseAlternativeNode) o;
        return Objects.equals(alternatives, chooseAlternativeNode.alternatives) &&
                Objects.equals(originalTableScan, chooseAlternativeNode.originalTableScan);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(alternatives, originalTableScan);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("alternatives", alternatives)
                .add("originalTableScan", originalTableScan)
                .toString();
    }

    public record FilteredTableScan(TableScanNode tableScanNode, Optional<Expression> filterPredicate)
    {
        public FilteredTableScan
        {
            requireNonNull(tableScanNode, "tableScanNode is null");
            requireNonNull(filterPredicate, "filterPredicate is null");
        }

        public TableHandle tableHandle()
        {
            return tableScanNode.getTable();
        }

        public Map<Symbol, ColumnHandle> assignments()
        {
            return tableScanNode.getAssignments();
        }
    }
}
