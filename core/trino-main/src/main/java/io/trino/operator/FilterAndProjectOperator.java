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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProcessorMetrics;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.WorkProcessorOperatorAdapter.createAdapterOperatorFactory;
import static io.trino.operator.project.MergePages.mergePages;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static java.util.Objects.requireNonNull;

public class FilterAndProjectOperator
        implements WorkProcessorOperator
{
    private static final Set<ComparisonOperator> SUPPORTED_RUNTIME_CONSTRAINT_COMPARISONS = Set.of(EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL);
    private final WorkProcessor<Page> pages;
    private final PageProcessorMetrics metrics = new PageProcessorMetrics();

    private FilterAndProjectOperator(
            OperatorContext operatorContext,
            WorkProcessor<Page> sourcePages,
            PageProcessor pageProcessor,
            List<Type> types,
            DataSize minOutputPageSize,
            int minOutputPageRowCount)
    {
        LocalMemoryContext operatorMemoryContext = operatorContext.newLocalUserMemoryContext(FilterAndProjectOperator.class.getSimpleName());
        AggregatedMemoryContext localAggregatedMemoryContext = newSimpleAggregatedMemoryContext();
        LocalMemoryContext outputMemoryContext = localAggregatedMemoryContext.newLocalMemoryContext(FilterAndProjectOperator.class.getSimpleName());
        ConnectorSession connectorSession = operatorContext.getSession().toConnectorSession();

        this.pages = sourcePages
                .flatMap(page -> pageProcessor.createWorkProcessor(
                        connectorSession,
                        outputMemoryContext,
                        metrics,
                        SourcePage.create(page)))
                .transformProcessor(processor -> mergePages(types, minOutputPageSize.toBytes(), minOutputPageRowCount, processor, localAggregatedMemoryContext))
                .blocking(() -> operatorMemoryContext.setBytes(localAggregatedMemoryContext.getBytes()));
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    @Override
    public Metrics getMetrics()
    {
        return metrics.getMetrics();
    }

    public static OperatorFactory createOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            Supplier<PageProcessor> processor,
            List<Type> types,
            DataSize minOutputPageSize,
            int minOutputPageRowCount)
    {
        return createOperatorFactory(
                operatorId,
                planNodeId,
                processor,
                types,
                types.stream().map(_ -> OptionalInt.empty()).collect(toImmutableList()),
                ImmutableList.of(),
                minOutputPageSize,
                minOutputPageRowCount);
    }

    public static OperatorFactory createOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            Supplier<PageProcessor> processor,
            List<Type> types,
            List<OptionalInt> inputChannels,
            DataSize minOutputPageSize,
            int minOutputPageRowCount)
    {
        return createOperatorFactory(operatorId, planNodeId, processor, types, inputChannels, ImmutableList.of(), minOutputPageSize, minOutputPageRowCount);
    }

    public static OperatorFactory createOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            Supplier<PageProcessor> processor,
            List<Type> types,
            List<OptionalInt> inputChannels,
            List<Integer> requiredTrueInputChannels,
            DataSize minOutputPageSize,
            int minOutputPageRowCount)
    {
        return createOperatorFactory(operatorId, planNodeId, processor, types, inputChannels, inputChannels.stream().map(_ -> Optional.<Type>empty()).toList(), requiredTrueInputChannels, minOutputPageSize, minOutputPageRowCount);
    }

    public static OperatorFactory createOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            Supplier<PageProcessor> processor,
            List<Type> types,
            List<OptionalInt> inputChannels,
            List<Optional<Type>> inputTypes,
            List<Integer> requiredTrueInputChannels,
            DataSize minOutputPageSize,
            int minOutputPageRowCount)
    {
        return createAdapterOperatorFactory(new Factory(
                operatorId,
                planNodeId,
                processor,
                types,
                inputChannels,
                inputTypes,
                requiredTrueInputChannels.stream().map(RuntimeConstraintRequest::requireTrue).toList(),
                minOutputPageSize,
                minOutputPageRowCount));
    }

    public static OperatorFactory createOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            Supplier<PageProcessor> processor,
            List<Type> types,
            List<OptionalInt> inputChannels,
            List<Optional<Type>> inputTypes,
            Optional<Expression> filter,
            Map<Symbol, Integer> inputLayout,
            DataSize minOutputPageSize,
            int minOutputPageRowCount)
    {
        List<RuntimeConstraintRequest> inputRuntimeConstraints = filter.stream()
                .flatMap(expression -> extractConjuncts(expression).stream())
                .filter(Reference.class::isInstance)
                .map(Reference.class::cast)
                .map(Symbol::from)
                .map(inputLayout::get)
                .filter(Objects::nonNull)
                .map(RuntimeConstraintRequest::requireTrue)
                .toList();
        inputRuntimeConstraints = Stream.concat(
                        inputRuntimeConstraints.stream(),
                        filter.stream().flatMap(expression -> extractComparisonDemands(expression, inputLayout).stream()))
                .toList();
        return createAdapterOperatorFactory(new Factory(
                operatorId,
                planNodeId,
                processor,
                types,
                inputChannels,
                inputTypes,
                inputRuntimeConstraints,
                minOutputPageSize,
                minOutputPageRowCount));
    }

    private static List<RuntimeConstraintRequest> extractComparisonDemands(Expression filter, Map<Symbol, Integer> inputLayout)
    {
        ImmutableList.Builder<RuntimeConstraintRequest> comparisons = ImmutableList.builder();
        for (Expression conjunct : extractConjuncts(filter)) {
            Comparison comparison = matchComparison(conjunct);
            if (comparison == null) {
                continue;
            }
            ComparisonOperator operator = comparison.operator();
            boolean nullAllowed = operator == IDENTICAL;
            if (nullAllowed) {
                if (comparison.left().type().equals(REAL) || comparison.right().type().equals(REAL) || comparison.left().type().equals(DOUBLE) || comparison.right().type().equals(DOUBLE)) {
                    continue;
                }
                operator = EQUAL;
            }
            else if (!SUPPORTED_RUNTIME_CONSTRAINT_COMPARISONS.contains(operator)) {
                continue;
            }
            Reference left = sourceReference(comparison.left());
            Reference right = sourceReference(comparison.right());
            if (left == null || right == null) {
                continue;
            }
            Integer leftChannel = inputLayout.get(Symbol.from(left));
            Integer rightChannel = inputLayout.get(Symbol.from(right));
            if (leftChannel != null && rightChannel != null && !leftChannel.equals(rightChannel)) {
                comparisons.add(RuntimeConstraintRequest.comparisonDemand(leftChannel, rightChannel, operator, nullAllowed));
            }
        }
        return comparisons.build();
    }

    private static Reference sourceReference(Expression expression)
    {
        return switch (expression) {
            case Reference reference -> reference;
            case Cast(Reference reference, _, _) -> reference;
            default -> null;
        };
    }

    private static class Factory
            implements WorkProcessorOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Supplier<PageProcessor> processor;
        private final List<Type> types;
        private final List<OptionalInt> inputChannels;
        private final List<Optional<Type>> inputTypes;
        private final List<RuntimeConstraintRequest> inputRuntimeConstraints;
        private final DataSize minOutputPageSize;
        private final int minOutputPageRowCount;
        private boolean closed;

        private Factory(
                int operatorId,
                PlanNodeId planNodeId,
                Supplier<PageProcessor> processor,
                List<Type> types,
                List<OptionalInt> inputChannels,
                List<Optional<Type>> inputTypes,
                List<RuntimeConstraintRequest> inputRuntimeConstraints,
                DataSize minOutputPageSize,
                int minOutputPageRowCount)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.processor = requireNonNull(processor, "processor is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.inputChannels = ImmutableList.copyOf(requireNonNull(inputChannels, "inputChannels is null"));
            this.inputTypes = ImmutableList.copyOf(requireNonNull(inputTypes, "inputTypes is null"));
            checkArgument(inputChannels.size() == inputTypes.size(), "inputChannels and inputTypes have different sizes");
            this.inputRuntimeConstraints = ImmutableList.copyOf(requireNonNull(inputRuntimeConstraints, "inputRuntimeConstraints is null"));
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
        }

        @Override
        public WorkProcessorOperator create(OperatorContext operatorContext, WorkProcessor<Page> sourcePages)
        {
            checkState(!closed, "Factory is already closed");
            return new FilterAndProjectOperator(
                    operatorContext,
                    sourcePages,
                    processor.get(),
                    types,
                    minOutputPageSize,
                    minOutputPageRowCount);
        }

        @Override
        public void propagateRuntimeConstraint(
                RuntimeConstraintRequest request,
                Consumer<RuntimeConstraintRequest> input,
                RuntimeConstraintWiringContext context)
        {
            if (!request.channelsMatch(channel -> channel < inputChannels.size() && inputChannels.get(channel).isPresent())) {
                context.stop(getOperatorType(), request);
                return;
            }
            if (request.isComparisonDemand()) {
                input.accept(request.mapChannels(channel -> inputChannels.get(channel).orElseThrow()));
                return;
            }
            int inputChannel = inputChannels.get(request.channel()).orElseThrow();
            RuntimeConstraintRequest mapped = inputTypes.get(request.channel())
                    .map(type -> request.withChannelAndTargetType(inputChannel, type))
                    .orElseGet(() -> request.withChannel(inputChannel));
            input.accept(context.mapConstraint(getOperatorType(), request, mapped));
        }

        @Override
        public List<RuntimeConstraintRequest> getInputRuntimeConstraints()
        {
            return inputRuntimeConstraints;
        }

        @Override
        public int getOperatorId()
        {
            return operatorId;
        }

        @Override
        public PlanNodeId getPlanNodeId()
        {
            return planNodeId;
        }

        @Override
        public String getOperatorType()
        {
            return FilterAndProjectOperator.class.getSimpleName();
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public WorkProcessorOperatorFactory duplicate()
        {
            return new Factory(operatorId, planNodeId, processor, types, inputChannels, inputTypes, inputRuntimeConstraints, minOutputPageSize, minOutputPageRowCount);
        }
    }
}
