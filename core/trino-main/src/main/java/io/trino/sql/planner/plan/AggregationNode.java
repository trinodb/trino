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
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.type.FunctionType;

import javax.annotation.concurrent.Immutable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static java.util.Objects.requireNonNull;

@Immutable
public class AggregationNode
        extends PlanNode
{
    private final PlanNode source;
    private final Map<Symbol, Aggregation> aggregations;
    private final GroupingSetDescriptor groupingSets;
    private final List<Symbol> preGroupedSymbols;
    private final Step step;
    private final Optional<Symbol> hashSymbol;
    private final Optional<Symbol> groupIdSymbol;
    private final List<Symbol> outputs;

    public static AggregationNode singleAggregation(
            PlanNodeId id,
            PlanNode source,
            Map<Symbol, Aggregation> aggregations,
            GroupingSetDescriptor groupingSets)
    {
        return new AggregationNode(id, source, aggregations, groupingSets, ImmutableList.of(), SINGLE, Optional.empty(), Optional.empty());
    }

    @JsonCreator
    public AggregationNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("aggregations") Map<Symbol, Aggregation> aggregations,
            @JsonProperty("groupingSets") GroupingSetDescriptor groupingSets,
            @JsonProperty("preGroupedSymbols") List<Symbol> preGroupedSymbols,
            @JsonProperty("step") Step step,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol,
            @JsonProperty("groupIdSymbol") Optional<Symbol> groupIdSymbol)
    {
        super(id);

        this.source = source;
        this.aggregations = ImmutableMap.copyOf(requireNonNull(aggregations, "aggregations is null"));
        aggregations.values().forEach(aggregation -> aggregation.verifyArguments(step));

        requireNonNull(groupingSets, "groupingSets is null");
        groupIdSymbol.ifPresent(symbol -> checkArgument(groupingSets.getGroupingKeys().contains(symbol), "Grouping columns does not contain groupId column"));
        this.groupingSets = groupingSets;

        this.groupIdSymbol = requireNonNull(groupIdSymbol);

        boolean noOrderBy = aggregations.values().stream()
                .map(Aggregation::getOrderingScheme)
                .noneMatch(Optional::isPresent);
        checkArgument(noOrderBy || step == SINGLE, "ORDER BY does not support distributed aggregation");

        this.step = step;
        this.hashSymbol = hashSymbol;

        requireNonNull(preGroupedSymbols, "preGroupedSymbols is null");
        checkArgument(preGroupedSymbols.isEmpty() || groupingSets.getGroupingKeys().containsAll(preGroupedSymbols), "Pre-grouped symbols must be a subset of the grouping keys");
        this.preGroupedSymbols = ImmutableList.copyOf(preGroupedSymbols);

        ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
        outputs.addAll(groupingSets.getGroupingKeys());
        hashSymbol.ifPresent(outputs::add);
        outputs.addAll(aggregations.keySet());

        this.outputs = outputs.build();
    }

    public List<Symbol> getGroupingKeys()
    {
        return groupingSets.getGroupingKeys();
    }

    @JsonProperty("groupingSets")
    public GroupingSetDescriptor getGroupingSets()
    {
        return groupingSets;
    }

    /**
     * @return true if the aggregation collapses all rows into a single global group (e.g., as a result of a GROUP BY () query).
     * Otherwise, false.
     */
    public boolean hasSingleGlobalAggregation()
    {
        return hasEmptyGroupingSet() && getGroupingSetCount() == 1;
    }

    /**
     * @return whether this node should produce default output in case of no input pages.
     * For example for query:
     * <p>
     * SELECT count(*) FROM nation WHERE nationkey < 0
     * <p>
     * A default output of "0" is expected to be produced by FINAL aggregation operator.
     */
    public boolean hasDefaultOutput()
    {
        return hasEmptyGroupingSet() && (step.isOutputPartial() || step == SINGLE);
    }

    public boolean hasEmptyGroupingSet()
    {
        return !groupingSets.getGlobalGroupingSets().isEmpty();
    }

    public boolean hasNonEmptyGroupingSet()
    {
        return groupingSets.getGroupingSetCount() > groupingSets.getGlobalGroupingSets().size();
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @JsonProperty
    public Map<Symbol, Aggregation> getAggregations()
    {
        return aggregations;
    }

    @JsonProperty("preGroupedSymbols")
    public List<Symbol> getPreGroupedSymbols()
    {
        return preGroupedSymbols;
    }

    public int getGroupingSetCount()
    {
        return groupingSets.getGroupingSetCount();
    }

    public Set<Integer> getGlobalGroupingSets()
    {
        return groupingSets.getGlobalGroupingSets();
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty("step")
    public Step getStep()
    {
        return step;
    }

    @JsonProperty("hashSymbol")
    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    @JsonProperty("groupIdSymbol")
    public Optional<Symbol> getGroupIdSymbol()
    {
        return groupIdSymbol;
    }

    public boolean hasOrderings()
    {
        return aggregations.values().stream()
                .map(Aggregation::getOrderingScheme)
                .anyMatch(Optional::isPresent);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitAggregation(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return builderFrom(this)
                .setSource(Iterables.getOnlyElement(newChildren))
                .build();
    }

    public boolean producesDistinctRows()
    {
        return aggregations.isEmpty() &&
                !groupingSets.getGroupingKeys().isEmpty() &&
                outputs.size() == groupingSets.getGroupingKeys().size() &&
                outputs.containsAll(new HashSet<>(groupingSets.getGroupingKeys()));
    }

    public boolean isDecomposable(Session session, Metadata metadata)
    {
        boolean hasOrderBy = getAggregations().values().stream()
                .map(Aggregation::getOrderingScheme)
                .anyMatch(Optional::isPresent);

        boolean hasDistinct = getAggregations().values().stream()
                .anyMatch(Aggregation::isDistinct);

        boolean decomposableFunctions = getAggregations().values().stream()
                .map(Aggregation::getResolvedFunction)
                .map(resolvedFunction -> metadata.getAggregationFunctionMetadata(session, resolvedFunction))
                .allMatch(AggregationFunctionMetadata::isDecomposable);

        return !hasOrderBy && !hasDistinct && decomposableFunctions;
    }

    public boolean hasSingleNodeExecutionPreference(Session session, Metadata metadata)
    {
        // There are two kinds of aggregations the have single node execution preference:
        //
        // 1. aggregations with only empty grouping sets like
        //
        // SELECT count(*) FROM lineitem;
        //
        // there is no need for distributed aggregation. Single node FINAL aggregation will suffice,
        // since all input have to be aggregated into one line output.
        //
        // 2. aggregations that must produce default output and are not decomposable, we cannot distribute them.
        return (hasEmptyGroupingSet() && !hasNonEmptyGroupingSet()) || (hasDefaultOutput() && !isDecomposable(session, metadata));
    }

    public boolean isStreamable()
    {
        return ImmutableSet.copyOf(preGroupedSymbols).equals(ImmutableSet.copyOf(groupingSets.getGroupingKeys()))
                && groupingSets.getGroupingSetCount() == 1
                && groupingSets.getGlobalGroupingSets().isEmpty();
    }

    public static GroupingSetDescriptor globalAggregation()
    {
        return singleGroupingSet(ImmutableList.of());
    }

    public static GroupingSetDescriptor singleGroupingSet(List<Symbol> groupingKeys)
    {
        Set<Integer> globalGroupingSets;
        if (groupingKeys.isEmpty()) {
            globalGroupingSets = ImmutableSet.of(0);
        }
        else {
            globalGroupingSets = ImmutableSet.of();
        }

        return new GroupingSetDescriptor(groupingKeys, 1, globalGroupingSets);
    }

    public static GroupingSetDescriptor groupingSets(List<Symbol> groupingKeys, int groupingSetCount, Set<Integer> globalGroupingSets)
    {
        return new GroupingSetDescriptor(groupingKeys, groupingSetCount, globalGroupingSets);
    }

    public static class GroupingSetDescriptor
    {
        private final List<Symbol> groupingKeys;
        private final int groupingSetCount;
        private final Set<Integer> globalGroupingSets;

        @JsonCreator
        public GroupingSetDescriptor(
                @JsonProperty("groupingKeys") List<Symbol> groupingKeys,
                @JsonProperty("groupingSetCount") int groupingSetCount,
                @JsonProperty("globalGroupingSets") Set<Integer> globalGroupingSets)
        {
            requireNonNull(globalGroupingSets, "globalGroupingSets is null");
            checkArgument(groupingSetCount > 0, "grouping set count must be larger than 0");
            checkArgument(globalGroupingSets.size() <= groupingSetCount, "list of empty global grouping sets must be no larger than grouping set count");
            requireNonNull(groupingKeys, "groupingKeys is null");
            if (groupingKeys.isEmpty()) {
                checkArgument(!globalGroupingSets.isEmpty(), "no grouping keys implies at least one global grouping set, but none provided");
            }

            this.groupingKeys = ImmutableList.copyOf(groupingKeys);
            this.groupingSetCount = groupingSetCount;
            this.globalGroupingSets = ImmutableSet.copyOf(globalGroupingSets);
        }

        @JsonProperty
        public List<Symbol> getGroupingKeys()
        {
            return groupingKeys;
        }

        @JsonProperty
        public int getGroupingSetCount()
        {
            return groupingSetCount;
        }

        @JsonProperty
        public Set<Integer> getGlobalGroupingSets()
        {
            return globalGroupingSets;
        }
    }

    public enum Step
    {
        PARTIAL(true, true),
        FINAL(false, false),
        INTERMEDIATE(false, true),
        SINGLE(true, false);

        private final boolean inputRaw;
        private final boolean outputPartial;

        Step(boolean inputRaw, boolean outputPartial)
        {
            this.inputRaw = inputRaw;
            this.outputPartial = outputPartial;
        }

        public boolean isInputRaw()
        {
            return inputRaw;
        }

        public boolean isOutputPartial()
        {
            return outputPartial;
        }

        public static Step partialOutput(Step step)
        {
            if (step.isInputRaw()) {
                return Step.PARTIAL;
            }
            return Step.INTERMEDIATE;
        }

        public static Step partialInput(Step step)
        {
            if (step.isOutputPartial()) {
                return Step.INTERMEDIATE;
            }
            return Step.FINAL;
        }
    }

    public static class Aggregation
    {
        private final ResolvedFunction resolvedFunction;
        private final List<Expression> arguments;
        private final boolean distinct;
        private final Optional<Symbol> filter;
        private final Optional<OrderingScheme> orderingScheme;
        private final Optional<Symbol> mask;

        @JsonCreator
        public Aggregation(
                @JsonProperty("resolvedFunction") ResolvedFunction resolvedFunction,
                @JsonProperty("arguments") List<Expression> arguments,
                @JsonProperty("distinct") boolean distinct,
                @JsonProperty("filter") Optional<Symbol> filter,
                @JsonProperty("orderingScheme") Optional<OrderingScheme> orderingScheme,
                @JsonProperty("mask") Optional<Symbol> mask)
        {
            this.resolvedFunction = requireNonNull(resolvedFunction, "resolvedFunction is null");
            this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
            for (Expression argument : arguments) {
                checkArgument(argument instanceof SymbolReference || argument instanceof LambdaExpression,
                        "argument must be symbol or lambda expression: %s", argument.getClass().getSimpleName());
            }
            this.distinct = distinct;
            this.filter = requireNonNull(filter, "filter is null");
            this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
            this.mask = requireNonNull(mask, "mask is null");
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
        public boolean isDistinct()
        {
            return distinct;
        }

        @JsonProperty
        public Optional<Symbol> getFilter()
        {
            return filter;
        }

        @JsonProperty
        public Optional<OrderingScheme> getOrderingScheme()
        {
            return orderingScheme;
        }

        @JsonProperty
        public Optional<Symbol> getMask()
        {
            return mask;
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
            Aggregation that = (Aggregation) o;
            return distinct == that.distinct &&
                    Objects.equals(resolvedFunction, that.resolvedFunction) &&
                    Objects.equals(arguments, that.arguments) &&
                    Objects.equals(filter, that.filter) &&
                    Objects.equals(orderingScheme, that.orderingScheme) &&
                    Objects.equals(mask, that.mask);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(resolvedFunction, arguments, distinct, filter, orderingScheme, mask);
        }

        private void verifyArguments(Step step)
        {
            int expectedArgumentCount;
            if (step == SINGLE || step == Step.PARTIAL) {
                expectedArgumentCount = resolvedFunction.getSignature().getArgumentTypes().size();
            }
            else {
                // Intermediate and final steps get the intermediate value and the lambda functions
                expectedArgumentCount = 1 + (int) resolvedFunction.getSignature().getArgumentTypes().stream()
                        .filter(FunctionType.class::isInstance)
                        .count();
            }

            checkArgument(
                    expectedArgumentCount == arguments.size(),
                    "%s aggregation function %s has %s arguments, but %s arguments were provided to function call",
                    step,
                    resolvedFunction.getSignature(),
                    expectedArgumentCount,
                    arguments.size());
        }
    }

    public static Builder builderFrom(AggregationNode node)
    {
        return new Builder(node);
    }

    public static class Builder
    {
        private PlanNodeId id;
        private PlanNode source;
        private Map<Symbol, Aggregation> aggregations;
        private GroupingSetDescriptor groupingSets;
        private List<Symbol> preGroupedSymbols;
        private Step step;
        private Optional<Symbol> hashSymbol;
        private Optional<Symbol> groupIdSymbol;

        public Builder(AggregationNode node)
        {
            requireNonNull(node, "node is null");
            this.id = node.getId();
            this.source = node.getSource();
            this.aggregations = node.getAggregations();
            this.groupingSets = node.getGroupingSets();
            this.preGroupedSymbols = node.getPreGroupedSymbols();
            this.step = node.getStep();
            this.hashSymbol = node.getHashSymbol();
            this.groupIdSymbol = node.getGroupIdSymbol();
        }

        public Builder setId(PlanNodeId id)
        {
            this.id = requireNonNull(id, "id is null");
            return this;
        }

        public Builder setSource(PlanNode source)
        {
            this.source = requireNonNull(source, "source is null");
            return this;
        }

        public Builder setAggregations(Map<Symbol, Aggregation> aggregations)
        {
            this.aggregations = requireNonNull(aggregations, "aggregations is null");
            return this;
        }

        public Builder setGroupingSets(GroupingSetDescriptor groupingSets)
        {
            this.groupingSets = requireNonNull(groupingSets, "groupingSets is null");
            return this;
        }

        public Builder setPreGroupedSymbols(List<Symbol> preGroupedSymbols)
        {
            this.preGroupedSymbols = requireNonNull(preGroupedSymbols, "preGroupedSymbols is null");
            return this;
        }

        public Builder setStep(Step step)
        {
            this.step = requireNonNull(step, "step is null");
            return this;
        }

        public Builder setHashSymbol(Optional<Symbol> hashSymbol)
        {
            this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
            return this;
        }

        public Builder setGroupIdSymbol(Optional<Symbol> groupIdSymbol)
        {
            this.groupIdSymbol = requireNonNull(groupIdSymbol, "groupIdSymbol is null");
            return this;
        }

        public AggregationNode build()
        {
            return new AggregationNode(
                    id,
                    source,
                    aggregations,
                    groupingSets,
                    preGroupedSymbols,
                    step,
                    hashSymbol,
                    groupIdSymbol);
        }
    }
}
