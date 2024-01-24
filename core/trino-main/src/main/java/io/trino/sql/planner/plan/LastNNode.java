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

import javax.annotation.concurrent.Immutable;

import java.util.List;

@Immutable
public class LastNNode
        extends PlanNode
{
    private final PlanNode source;
    private final int count;

    @JsonCreator
    public LastNNode(@JsonProperty("id") PlanNodeId id,
                     @JsonProperty("source") PlanNode source,
                     @JsonProperty("count") int count)
    {
        super(id);
        this.source = source;
        this.count = count;
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

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return source.getOutputSymbols();
    }

    @JsonProperty("count")
    public int getCount()
    {
        return count;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitLastN(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new LastNNode(getId(), Iterables.getOnlyElement(newChildren), count);
    }
}
