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
import io.trino.cache.CommonPlanAdaptation.PlanSignatureWithPredicate;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Immutable
public class LoadCachedDataPlanNode
        extends PlanNode
{
    private final PlanSignatureWithPredicate planSignature;
    /**
     * Dynamic filter disjuncts from all common subplans.
     */
    private final Expression dynamicFilterDisjuncts;
    private final Map<CacheColumnId, ColumnHandle> commonColumnHandles;
    private final List<Symbol> outputSymbols;

    @JsonCreator
    public LoadCachedDataPlanNode(
            @JsonProperty PlanNodeId id,
            @JsonProperty PlanSignatureWithPredicate planSignature,
            @JsonProperty Expression dynamicFilterDisjuncts,
            @JsonProperty Map<CacheColumnId, ColumnHandle> commonColumnHandles,
            @JsonProperty List<Symbol> outputSymbols)
    {
        super(id);
        this.planSignature = requireNonNull(planSignature, "planSignature is null");
        this.dynamicFilterDisjuncts = requireNonNull(dynamicFilterDisjuncts, "dynamicFilterDisjuncts is null");
        this.commonColumnHandles = requireNonNull(commonColumnHandles, "commonColumnHandles is null");
        this.outputSymbols = requireNonNull(outputSymbols, "outputSymbols is null");
    }

    @JsonProperty
    public PlanSignatureWithPredicate getPlanSignature()
    {
        return planSignature;
    }

    @JsonProperty
    public Expression getDynamicFilterDisjuncts()
    {
        return dynamicFilterDisjuncts;
    }

    @JsonProperty
    public Map<CacheColumnId, ColumnHandle> getCommonColumnHandles()
    {
        return commonColumnHandles;
    }

    @Override
    @JsonProperty
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitLoadCachedDataPlanNode(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new LoadCachedDataPlanNode(
                getId(),
                planSignature,
                dynamicFilterDisjuncts,
                commonColumnHandles,
                outputSymbols);
    }
}
