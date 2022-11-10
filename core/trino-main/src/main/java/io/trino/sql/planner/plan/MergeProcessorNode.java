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
import io.trino.sql.planner.Symbol;

import java.util.List;

import static io.trino.sql.planner.plan.TableWriterNode.MergeTarget;
import static java.util.Objects.requireNonNull;

/**
 * The node processes the result of the Searched CASE and RIGHT JOIN
 * derived from a MERGE statement.
 */
public class MergeProcessorNode
        extends PlanNode
{
    private final PlanNode source;
    private final MergeTarget target;
    private final Symbol rowIdSymbol;
    private final Symbol mergeRowSymbol;
    private final List<Symbol> dataColumnSymbols;
    private final List<Symbol> redistributionColumnSymbols;
    private final List<Symbol> outputs;

    @JsonCreator
    public MergeProcessorNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("target") MergeTarget target,
            @JsonProperty("rowIdSymbol") Symbol rowIdSymbol,
            @JsonProperty("mergeRowSymbol") Symbol mergeRowSymbol,
            @JsonProperty("dataColumnSymbols") List<Symbol> dataColumnSymbols,
            @JsonProperty("redistributionColumnSymbols") List<Symbol> redistributionColumnSymbols,
            @JsonProperty("outputs") List<Symbol> outputs)
    {
        super(id);

        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
        this.mergeRowSymbol = requireNonNull(mergeRowSymbol, "mergeRowSymbol is null");
        this.rowIdSymbol = requireNonNull(rowIdSymbol, "rowIdSymbol is null");
        this.dataColumnSymbols = requireNonNull(dataColumnSymbols, "dataColumnSymbols is null");
        this.redistributionColumnSymbols = requireNonNull(redistributionColumnSymbols, "redistributionColumnSymbols is null");
        this.outputs = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public MergeTarget getTarget()
    {
        return target;
    }

    @JsonProperty
    public Symbol getMergeRowSymbol()
    {
        return mergeRowSymbol;
    }

    @JsonProperty
    public Symbol getRowIdSymbol()
    {
        return rowIdSymbol;
    }

    @JsonProperty
    public List<Symbol> getDataColumnSymbols()
    {
        return dataColumnSymbols;
    }

    @JsonProperty
    public List<Symbol> getRedistributionColumnSymbols()
    {
        return redistributionColumnSymbols;
    }

    @JsonProperty("outputs")
    @Override
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitMergeProcessor(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new MergeProcessorNode(getId(), Iterables.getOnlyElement(newChildren), target, rowIdSymbol, mergeRowSymbol, dataColumnSymbols, redistributionColumnSymbols, outputs);
    }
}
