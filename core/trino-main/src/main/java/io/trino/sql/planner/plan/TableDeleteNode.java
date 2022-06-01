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
import io.trino.metadata.TableHandle;
import io.trino.sql.planner.Symbol;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class TableDeleteNode
        extends PlanNode
{
    private final TableHandle target;
    private final Symbol output;

    @JsonCreator
    public TableDeleteNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("target") TableHandle target,
            @JsonProperty("output") Symbol output)
    {
        super(id);
        this.target = requireNonNull(target, "target is null");
        this.output = requireNonNull(output, "output is null");
    }

    @JsonProperty
    public TableHandle getTarget()
    {
        return target;
    }

    @JsonProperty
    public Symbol getOutput()
    {
        return output;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.of(output);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableDelete(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren should be empty");
        return this;
    }
}
