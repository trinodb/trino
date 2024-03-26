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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.trino.sql.planner.Symbol;

import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public final class GroupReference
        implements PlanNode
{
    private final PlanNodeId id;
    private final int groupId;
    private final List<Symbol> outputs;

    public GroupReference(PlanNodeId id, int groupId, List<Symbol> outputs)
    {
        this.id = requireNonNull(id, "id is null");
        this.groupId = groupId;
        this.outputs = ImmutableList.copyOf(outputs);
    }

    public int getGroupId()
    {
        return groupId;
    }

    @Override
    public PlanNodeId id()
    {
        return id;
    }

    @Override
    public List<PlanNode> sources()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitGroupReference(this, context);
    }

    @Override
    public List<Symbol> outputSymbols()
    {
        return outputs;
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        throw new UnsupportedOperationException();
    }
}
