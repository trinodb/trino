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
 * A node that supports connectors that morph update operations into delete
 * and insert operations, if required by the connector's
 * {@link io.trino.spi.connector.RowChangeParadigm}.
 */
public class DeleteAndInsertNode
        extends PlanNode
{
    private final PlanNode source;
    private final MergeTarget target;
    private final List<Symbol> outputs;

    @JsonCreator
    public DeleteAndInsertNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("target") MergeTarget target,
            @JsonProperty("outputs") List<Symbol> outputs)
    {
        super(id);

        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
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
        return visitor.visitDeleteAndInsert(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new DeleteAndInsertNode(getId(), Iterables.getOnlyElement(newChildren), target, outputs);
    }
}
