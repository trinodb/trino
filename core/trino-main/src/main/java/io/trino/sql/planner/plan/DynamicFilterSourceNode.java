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
import com.google.errorprone.annotations.Immutable;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class DynamicFilterSourceNode
        extends PlanNode
{
    private final PlanNode source;
    private final Map<DynamicFilterId, Symbol> dynamicFilters;

    @JsonCreator
    public DynamicFilterSourceNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("dynamicFilters") Map<DynamicFilterId, Symbol> dynamicFilters)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        this.dynamicFilters = ImmutableMap.copyOf(requireNonNull(dynamicFilters, "dynamicFilters is null"));
        Set<Symbol> outputSymbols = ImmutableSet.copyOf(source.getOutputSymbols());
        checkArgument(!outputSymbols.isEmpty(), "outputSymbols is empty");
        checkArgument(outputSymbols.containsAll(dynamicFilters.values()), "Dynamic filter symbols need to be part of the output symbols");
    }

    @Override
    @JsonProperty
    public List<Symbol> getOutputSymbols()
    {
        return source.getOutputSymbols();
    }

    @JsonProperty
    public Map<DynamicFilterId, Symbol> getDynamicFilters()
    {
        return dynamicFilters;
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitDynamicFilterSource(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1, "expected newChildren to contain 1 node");
        return new DynamicFilterSourceNode(getId(), newChildren.get(0), dynamicFilters);
    }
}
