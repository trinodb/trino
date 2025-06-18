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
public class DistinctLimitNode
        extends PlanNode
{
    private final PlanNode source;
    private final long limit;
    private final boolean partial;
    private final List<Symbol> distinctSymbols;

    @JsonCreator
    public DistinctLimitNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("limit") long limit,
            @JsonProperty("partial") boolean partial,
            @JsonProperty("distinctSymbols") List<Symbol> distinctSymbols)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        checkArgument(limit >= 0, "limit must be greater than or equal to zero");
        this.limit = limit;
        this.partial = partial;
        this.distinctSymbols = ImmutableList.copyOf(distinctSymbols);
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

    @JsonProperty
    public long getLimit()
    {
        return limit;
    }

    @JsonProperty
    public boolean isPartial()
    {
        return partial;
    }

    @JsonProperty
    public List<Symbol> getDistinctSymbols()
    {
        return distinctSymbols;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return distinctSymbols;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitDistinctLimit(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new DistinctLimitNode(getId(), Iterables.getOnlyElement(newChildren), limit, partial, distinctSymbols);
    }
}
