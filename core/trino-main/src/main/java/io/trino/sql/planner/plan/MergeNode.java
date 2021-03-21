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
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.TableWriterNode.MergeTarget;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class MergeNode
        extends PlanNode
{
    private final PlanNode source;
    private final MergeTarget target;
    private final Optional<PlanNodeId> tableScanId;
    private final List<Symbol> projectedSymbols;
    private final Optional<PartitioningScheme> partitioningScheme;
    private final List<Symbol> outputs;

    @JsonCreator
    public MergeNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("target") MergeTarget target,
            @JsonProperty("tableScanId") Optional<PlanNodeId> tableScanId,
            @JsonProperty("projectedSymbols") List<Symbol> projectedSymbols,
            @JsonProperty("partitioningScheme") Optional<PartitioningScheme> partitioningScheme,
            @JsonProperty("outputs") List<Symbol> outputs)
    {
        super(id);

        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
        this.tableScanId = requireNonNull(tableScanId, "tableScanId is null");
        this.projectedSymbols = requireNonNull(projectedSymbols, "projectedSymbols is null");
        this.partitioningScheme = requireNonNull(partitioningScheme, "partitioningScheme is null");
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
    public Optional<PlanNodeId> getTableScanId()
    {
        return tableScanId;
    }

    @JsonProperty
    public List<Symbol> getProjectedSymbols()
    {
        return projectedSymbols;
    }

    @JsonProperty
    public Optional<PartitioningScheme> getPartitioningScheme()
    {
        return partitioningScheme;
    }

    /**
     * Aggregate information about updated data
     */
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
        return visitor.visitMerge(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new MergeNode(getId(), Iterables.getOnlyElement(newChildren), target, tableScanId, projectedSymbols, partitioningScheme, outputs);
    }
}
