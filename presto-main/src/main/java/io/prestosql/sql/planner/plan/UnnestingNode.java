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
package io.prestosql.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.prestosql.sql.planner.Symbol;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.sql.planner.plan.JoinNode.Type.FULL;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.JoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.JoinNode.Type.RIGHT;
import static java.util.Objects.requireNonNull;

@Immutable
public class UnnestingNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<Symbol> replicateSymbols;
    private final Map<Symbol, List<Symbol>> unnestSymbols;
    private final Optional<Symbol> ordinalitySymbol;
    private final Optional<Symbol> rightIdSymbol;
    private final Optional<Symbol> markerSymbol;
    private final JoinNode.Type joinType;
    private final boolean onTrue;

    @JsonCreator
    public UnnestingNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("replicateSymbols") List<Symbol> replicateSymbols,
            @JsonProperty("unnestSymbols") Map<Symbol, List<Symbol>> unnestSymbols,
            @JsonProperty("ordinalitySymbol") Optional<Symbol> ordinalitySymbol,
            @JsonProperty("rightIdSymbol") Optional<Symbol> rightIdSymbol,
            @JsonProperty("markerSymbol") Optional<Symbol> markerSymbol,
            @JsonProperty("joinType") JoinNode.Type joinType,
            @JsonProperty("onTrue") boolean onTrue)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        this.replicateSymbols = ImmutableList.copyOf(requireNonNull(replicateSymbols, "replicateSymbols is null"));
        checkArgument(source.getOutputSymbols().containsAll(replicateSymbols), "Source does not contain all replicateSymbols");
        requireNonNull(unnestSymbols, "unnestSymbols is null");
        checkArgument(!unnestSymbols.isEmpty(), "unnestSymbols is empty");
        ImmutableMap.Builder<Symbol, List<Symbol>> builder = ImmutableMap.builder();
        for (Map.Entry<Symbol, List<Symbol>> entry : unnestSymbols.entrySet()) {
            builder.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        this.unnestSymbols = builder.build();
        this.ordinalitySymbol = requireNonNull(ordinalitySymbol, "ordinalitySymbol is null");
        this.rightIdSymbol = requireNonNull(rightIdSymbol, "rightIdSymbol is null");
        this.markerSymbol = requireNonNull(markerSymbol, "markerSymbol is null");
        this.joinType = requireNonNull(joinType, "type is null");
        this.onTrue = onTrue;
        if (onTrue || joinType == INNER) {
            checkArgument(!rightIdSymbol.isPresent(), "redundant rightId symbol: " + rightIdSymbol);
            checkArgument(!markerSymbol.isPresent(), "redundant rightId symbol: " + markerSymbol);
        }
        else {
            if (joinType == RIGHT || joinType == FULL) {
                checkArgument(rightIdSymbol.isPresent(), "missing rightId symbol for %s type", joinType.name());
            }
            if (joinType == RIGHT || joinType == FULL || joinType == LEFT) {
                checkArgument(markerSymbol.isPresent(), "missing marker symbol for %s type", joinType.name());
            }
        }
        if (joinType == LEFT) {
            checkArgument(!rightIdSymbol.isPresent(), "redundant rightId symbol: " + rightIdSymbol);
        }
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.<Symbol>builder()
                .addAll(replicateSymbols)
                .addAll(Iterables.concat(unnestSymbols.values()));
        ordinalitySymbol.ifPresent(outputSymbolsBuilder::add);
        rightIdSymbol.ifPresent(outputSymbolsBuilder::add);
        markerSymbol.ifPresent(outputSymbolsBuilder::add);
        return outputSymbolsBuilder.build();
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public List<Symbol> getReplicateSymbols()
    {
        return replicateSymbols;
    }

    @JsonProperty
    public Map<Symbol, List<Symbol>> getUnnestSymbols()
    {
        return unnestSymbols;
    }

    @JsonProperty
    public Optional<Symbol> getOrdinalitySymbol()
    {
        return ordinalitySymbol;
    }

    @JsonProperty
    public Optional<Symbol> getRightIdSymbol()
    {
        return rightIdSymbol;
    }

    @JsonProperty
    public Optional<Symbol> getMarkerSymbol()
    {
        return markerSymbol;
    }

    @JsonProperty
    public JoinNode.Type getJoinType()
    {
        return joinType;
    }

    @JsonProperty
    public boolean isOnTrue()
    {
        return onTrue;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitUnnesting(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new UnnestingNode(getId(), Iterables.getOnlyElement(newChildren), replicateSymbols, unnestSymbols, ordinalitySymbol, rightIdSymbol, markerSymbol, joinType, onTrue);
    }
}
