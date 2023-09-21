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
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class LimitNode
        extends PlanNode
{
    private final PlanNode source;
    private final long count;
    private final Optional<OrderingScheme> tiesResolvingScheme;
    private final boolean partial;
    private final List<Symbol> preSortedInputs;

    public LimitNode(PlanNodeId id, PlanNode source, long count, boolean partial)
    {
        this(id, source, count, Optional.empty(), partial, ImmutableList.of());
    }

    @JsonCreator
    public LimitNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("count") long count,
            @JsonProperty("tiesResolvingScheme") Optional<OrderingScheme> tiesResolvingScheme,
            @JsonProperty("partial") boolean partial,
            @JsonProperty("requiresPreSortedInputs") List<Symbol> preSortedInputs)
    {
        super(id);
        this.partial = partial;

        requireNonNull(source, "source is null");
        checkArgument(count >= 0, "count must be greater than or equal to zero");
        requireNonNull(tiesResolvingScheme, "tiesResolvingScheme is null");
        requireNonNull(preSortedInputs, "requiresPreSortedInputs is null");

        this.source = source;
        this.count = count;
        this.tiesResolvingScheme = tiesResolvingScheme;
        this.preSortedInputs = ImmutableList.copyOf(preSortedInputs);
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
    public long getCount()
    {
        return count;
    }

    public boolean isWithTies()
    {
        return tiesResolvingScheme.isPresent();
    }

    @JsonProperty
    public Optional<OrderingScheme> getTiesResolvingScheme()
    {
        return tiesResolvingScheme;
    }

    @JsonProperty
    public boolean isPartial()
    {
        return partial;
    }

    public boolean requiresPreSortedInputs()
    {
        return !preSortedInputs.isEmpty();
    }

    @JsonProperty
    public List<Symbol> getPreSortedInputs()
    {
        return preSortedInputs;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return source.getOutputSymbols();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitLimit(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new LimitNode(getId(), Iterables.getOnlyElement(newChildren), count, tiesResolvingScheme, isPartial(), preSortedInputs);
    }
}
