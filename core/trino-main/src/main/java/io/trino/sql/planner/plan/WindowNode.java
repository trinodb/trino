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
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.WindowFrame;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static io.trino.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static io.trino.sql.tree.WindowFrame.Type.RANGE;
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

        ImmutableSet<Symbol> partitionBy = ImmutableSet.copyOf(specification.getPartitionBy());
        Optional<OrderingScheme> orderingScheme = specification.getOrderingScheme();
        checkArgument(partitionBy.containsAll(prePartitionedInputs), "prePartitionedInputs must be contained in partitionBy");
        checkArgument(preSortedOrderPrefix == 0 || (orderingScheme.isPresent() && preSortedOrderPrefix <= orderingScheme.get().getOrderBy().size()), "Cannot have sorted more symbols than those requested");
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
        return specification.getPartitionBy();
    }

    public Optional<OrderingScheme> getOrderingScheme()
    {
        return specification.getOrderingScheme();
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
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        private final WindowFrame.Type type;
        private final FrameBound.Type startType;
        private final Optional<Symbol> startValue;
        private final Optional<Symbol> sortKeyCoercedForFrameStartComparison;
        private final FrameBound.Type endType;
        private final Optional<Symbol> endValue;
        private final Optional<Symbol> sortKeyCoercedForFrameEndComparison;

        // This information is only used for printing the plan.
        private final Optional<Expression> originalStartValue;
        private final Optional<Expression> originalEndValue;

        @JsonCreator
        public Frame(
                @JsonProperty("type") WindowFrame.Type type,
                @JsonProperty("startType") FrameBound.Type startType,
                @JsonProperty("startValue") Optional<Symbol> startValue,
                @JsonProperty("sortKeyCoercedForFrameStartComparison") Optional<Symbol> sortKeyCoercedForFrameStartComparison,
                @JsonProperty("endType") FrameBound.Type endType,
                @JsonProperty("endValue") Optional<Symbol> endValue,
                @JsonProperty("sortKeyCoercedForFrameEndComparison") Optional<Symbol> sortKeyCoercedForFrameEndComparison,
                @JsonProperty("originalStartValue") Optional<Expression> originalStartValue,
                @JsonProperty("originalEndValue") Optional<Expression> originalEndValue)
        {
            this.startType = requireNonNull(startType, "startType is null");
            this.startValue = requireNonNull(startValue, "startValue is null");
            this.sortKeyCoercedForFrameStartComparison = requireNonNull(sortKeyCoercedForFrameStartComparison, "sortKeyCoercedForFrameStartComparison is null");
            this.endType = requireNonNull(endType, "endType is null");
            this.endValue = requireNonNull(endValue, "endValue is null");
            this.sortKeyCoercedForFrameEndComparison = requireNonNull(sortKeyCoercedForFrameEndComparison, "sortKeyCoercedForFrameEndComparison is null");
            this.type = requireNonNull(type, "type is null");
            this.originalStartValue = requireNonNull(originalStartValue, "originalStartValue is null");
            this.originalEndValue = requireNonNull(originalEndValue, "originalEndValue is null");

            if (startValue.isPresent()) {
                checkArgument(originalStartValue.isPresent(), "originalStartValue must be present if startValue is present");
                if (type == RANGE) {
                    checkArgument(sortKeyCoercedForFrameStartComparison.isPresent(), "for frame of type RANGE, sortKeyCoercedForFrameStartComparison must be present if startValue is present");
                }
            }

            if (endValue.isPresent()) {
                checkArgument(originalEndValue.isPresent(), "originalEndValue must be present if endValue is present");
                if (type == RANGE) {
                    checkArgument(sortKeyCoercedForFrameEndComparison.isPresent(), "for frame of type RANGE, sortKeyCoercedForFrameEndComparison must be present if endValue is present");
                }
            }
        }

        @JsonProperty
        public WindowFrame.Type getType()
        {
            return type;
        }

        @JsonProperty
        public FrameBound.Type getStartType()
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
        public FrameBound.Type getEndType()
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

        @JsonProperty
        public Optional<Expression> getOriginalStartValue()
        {
            return originalStartValue;
        }

        @JsonProperty
        public Optional<Expression> getOriginalEndValue()
        {
            return originalEndValue;
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
        private final Frame frame;
        private final boolean ignoreNulls;

        @JsonCreator
        public Function(
                @JsonProperty("resolvedFunction") ResolvedFunction resolvedFunction,
                @JsonProperty("arguments") List<Expression> arguments,
                @JsonProperty("frame") Frame frame,
                @JsonProperty("ignoreNulls") boolean ignoreNulls)
        {
            this.resolvedFunction = requireNonNull(resolvedFunction, "resolvedFunction is null");
            this.arguments = requireNonNull(arguments, "arguments is null");
            this.frame = requireNonNull(frame, "frame is null");
            this.ignoreNulls = ignoreNulls;
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
        public Frame getFrame()
        {
            return frame;
        }

        @JsonProperty
        public boolean isIgnoreNulls()
        {
            return ignoreNulls;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(resolvedFunction, arguments, frame, ignoreNulls);
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
                    Objects.equals(this.frame, other.frame) &&
                    this.ignoreNulls == other.ignoreNulls;
        }
    }
}
