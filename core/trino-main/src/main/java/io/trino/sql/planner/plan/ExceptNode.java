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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ListMultimap;
import com.google.errorprone.annotations.Immutable;
import io.trino.sql.planner.Symbol;

import java.util.List;

@Immutable
public class ExceptNode
        extends SetOperationNode
{
    private final boolean distinct;

    public ExceptNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("sources") List<PlanNode> sources,
            @JsonProperty("outputToInputs") ListMultimap<Symbol, Symbol> outputToInputs,
            @JsonProperty("outputs") List<Symbol> outputs,
            @JsonProperty("distinct") boolean distinct)
    {
        super(id, sources, outputToInputs, outputs);
        this.distinct = distinct;
    }

    @JsonProperty
    public boolean isDistinct()
    {
        return distinct;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitExcept(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new ExceptNode(getId(), newChildren, getSymbolMapping(), getOutputSymbols(), isDistinct());
    }
}
