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
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.Immutable;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_PRECEDING;
import static io.trino.sql.planner.plan.WindowFrameType.RANGE;
import static java.util.Objects.requireNonNull;

@Immutable
public class WindowNode
        extends PlanNode
{
    private final PlanNode source;
    private final Set<Symbol> prePartitionedInputs;
    private final DataOrganizationSpecification specification;
    private final int preSortedOrderPrefix;
    private final Map<Symbol, Function> windowFunctions;
    private final Optional<Symbol> hashSymbol;

    @JsonCreator
    public WindowNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("specification") DataOrganizationSpecification specification,
            @JsonProperty("windowFunctions") Map<Symbol, Function> windowFunctions,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol,
            @JsonProperty("prePartitionedInputs") Set<Symbol> prePartitionedInputs,
            @JsonProperty("preSortedOrderPrefix") int preSortedOrderPrefix)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(specification, "specification is null");
        requireNonNull(windowFunctions, "windowFunctions is null");
        requireNonNull(hashSymbol, "hashSymbol is null");
        requireNonNull(prePartitionedInputs, "prePartitionedInputs is null");
        // Make the defensive copy eagerly, so it can be used for both the validation checks and assigned directly to the field afterwards
        prePartitionedInputs = ImmutableSet.copyOf(prePartitionedInputs);

        ImmutableSet<Symbol> partitionBy = ImmutableSet.copyOf(specification.partitionBy());
        Optional<OrderingScheme> orderingScheme = specification.orderingScheme();
        checkArgument(partitionBy.containsAll(prePartitionedInputs), "prePartitionedInputs must be contained in partitionBy");
        checkArgument(preSortedOrderPrefix == 0 || (orderingScheme.isPresent() && preSortedOrderPrefix <= orderingScheme.get().orderBy().size()), "Cannot have sorted more symbols than those requested");
        checkArgument(preSortedOrderPrefix == 0 || partitionBy.equals(prePartitionedInputs), "preSortedOrderPrefix can only be greater than zero if all partition symbols are pre-partitioned");

        this.source = source;
        this.prePartitionedInputs = prePartitionedInputs;
        this.specification = specification;
        this.windowFunctions = ImmutableMap.copyOf(windowFunctions);
        this.hashSymbol = hashSymbol;
        this.preSortedOrderPrefix = preSortedOrderPrefix;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.copyOf(concat(source.getOutputSymbols(), windowFunctions.keySet()));
    }

    public Set<Symbol> getCreatedSymbols()
    {
        return ImmutableSet.copyOf(windowFunctions.keySet());
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public DataOrganizationSpecification getSpecification()
    {
        return specification;
    }

    public List<Symbol> getPartitionBy()
    {
        return specification.partitionBy();
    }

    public Optional<OrderingScheme> getOrderingScheme()
    {
        return specification.orderingScheme();
    }

    @JsonProperty
    public Map<Symbol, Function> getWindowFunctions()
    {
        return windowFunctions;
    }

    public List<Frame> getFrames()
    {
        return windowFunctions.values().stream()
                .map(WindowNode.Function::getFrame)
                .collect(toImmutableList());
    }

    @JsonProperty
    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    @JsonProperty
    public Set<Symbol> getPrePartitionedInputs()
    {
        return prePartitionedInputs;
    }

    @JsonProperty
    public int getPreSortedOrderPrefix()
    {
        return preSortedOrderPrefix;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitWindow(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new WindowNode(getId(), Iterables.getOnlyElement(newChildren), specification, windowFunctions, hashSymbol, prePartitionedInputs, preSortedOrderPrefix);
    }

    @Immutable
    public static class Frame
    {
        public static final Frame DEFAULT_FRAME = new WindowNode.Frame(
                RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                Optional.empty(),
                CURRENT_ROW,
                Optional.empty(),
                Optional.empty());

        private final WindowFrameType type;
        private final FrameBoundType startType;
        private final Optional<Symbol> startValue;
        private final Optional<Symbol> sortKeyCoercedForFrameStartComparison;
        private final FrameBoundType endType;
        private final Optional<Symbol> endValue;
        private final Optional<Symbol> sortKeyCoercedForFrameEndComparison;

        @JsonCreator
        public Frame(
                @JsonProperty("type") WindowFrameType type,
                @JsonProperty("startType") FrameBoundType startType,
                @JsonProperty("startValue") Optional<Symbol> startValue,
                @JsonProperty("sortKeyCoercedForFrameStartComparison") Optional<Symbol> sortKeyCoercedForFrameStartComparison,
                @JsonProperty("endType") FrameBoundType endType,
                @JsonProperty("endValue") Optional<Symbol> endValue,
                @JsonProperty("sortKeyCoercedForFrameEndComparison") Optional<Symbol> sortKeyCoercedForFrameEndComparison)
        {
            this.startType = requireNonNull(startType, "startType is null");
            this.startValue = requireNonNull(startValue, "startValue is null");
            this.sortKeyCoercedForFrameStartComparison = requireNonNull(sortKeyCoercedForFrameStartComparison, "sortKeyCoercedForFrameStartComparison is null");
            this.endType = requireNonNull(endType, "endType is null");
            this.endValue = requireNonNull(endValue, "endValue is null");
            this.sortKeyCoercedForFrameEndComparison = requireNonNull(sortKeyCoercedForFrameEndComparison, "sortKeyCoercedForFrameEndComparison is null");
            this.type = requireNonNull(type, "type is null");

            if (startValue.isPresent()) {
                if (type == RANGE) {
                    checkArgument(sortKeyCoercedForFrameStartComparison.isPresent(), "for frame of type RANGE, sortKeyCoercedForFrameStartComparison must be present if startValue is present");
                }
            }

            if (endValue.isPresent()) {
                if (type == RANGE) {
                    checkArgument(sortKeyCoercedForFrameEndComparison.isPresent(), "for frame of type RANGE, sortKeyCoercedForFrameEndComparison must be present if endValue is present");
                }
            }
        }

        @JsonProperty
        public WindowFrameType getType()
        {
            return type;
        }

        @JsonProperty
        public FrameBoundType getStartType()
        {
            return startType;
        }

        @JsonProperty
        public Optional<Symbol> getStartValue()
        {
            return startValue;
        }

        @JsonProperty
        public Optional<Symbol> getSortKeyCoercedForFrameStartComparison()
        {
            return sortKeyCoercedForFrameStartComparison;
        }

        @JsonProperty
        public FrameBoundType getEndType()
        {
            return endType;
        }

        @JsonProperty
        public Optional<Symbol> getEndValue()
        {
            return endValue;
        }

        @JsonProperty
        public Optional<Symbol> getSortKeyCoercedForFrameEndComparison()
        {
            return sortKeyCoercedForFrameEndComparison;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Frame frame = (Frame) o;
            return type == frame.type &&
                    startType == frame.startType &&
                    Objects.equals(startValue, frame.startValue) &&
                    Objects.equals(sortKeyCoercedForFrameStartComparison, frame.sortKeyCoercedForFrameStartComparison) &&
                    endType == frame.endType &&
                    Objects.equals(endValue, frame.endValue) &&
                    Objects.equals(sortKeyCoercedForFrameEndComparison, frame.sortKeyCoercedForFrameEndComparison);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, startType, startValue, sortKeyCoercedForFrameStartComparison, endType, endValue, sortKeyCoercedForFrameEndComparison);
        }
    }

    @Immutable
    public static final class Function
    {
        private final ResolvedFunction resolvedFunction;
        private final List<Expression> arguments;
        private final Optional<OrderingScheme> orderingScheme;
        private final Frame frame;
        private final boolean ignoreNulls;
        private final boolean distinct;

        @JsonCreator
        public Function(
                @JsonProperty("resolvedFunction") ResolvedFunction resolvedFunction,
                @JsonProperty("arguments") List<Expression> arguments,
                @JsonProperty("orderingScheme") Optional<OrderingScheme> orderingScheme,
                @JsonProperty("frame") Frame frame,
                @JsonProperty("ignoreNulls") boolean ignoreNulls,
                @JsonProperty("distinct") boolean distinct)
        {
            this.resolvedFunction = requireNonNull(resolvedFunction, "resolvedFunction is null");
            this.arguments = requireNonNull(arguments, "arguments is null");
            this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
            this.frame = requireNonNull(frame, "frame is null");
            this.ignoreNulls = ignoreNulls;
            this.distinct = distinct;
        }

        @JsonProperty
        public ResolvedFunction getResolvedFunction()
        {
            return resolvedFunction;
        }

        @JsonProperty
        public List<Expression> getArguments()
        {
            return arguments;
        }

        @JsonProperty
        public Optional<OrderingScheme> getOrderingScheme()
        {
            return orderingScheme;
        }

        @JsonProperty
        public Frame getFrame()
        {
            return frame;
        }

        @JsonProperty
        public boolean isIgnoreNulls()
        {
            return ignoreNulls;
        }

        @JsonProperty
        public boolean isDistinct()
        {
            return distinct;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(resolvedFunction, arguments, orderingScheme, frame, ignoreNulls, distinct);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Function other = (Function) obj;
            return Objects.equals(this.resolvedFunction, other.resolvedFunction) &&
                    Objects.equals(this.arguments, other.arguments) &&
                    Objects.equals(this.orderingScheme, other.orderingScheme) &&
                    Objects.equals(this.frame, other.frame) &&
                    this.ignoreNulls == other.ignoreNulls &&
                    this.distinct == other.distinct;
        }
    }
}
