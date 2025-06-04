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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.operator.WorkProcessor.Transformation;
import io.trino.operator.WorkProcessor.TransformationState;
import io.trino.operator.window.FrameInfo;
import io.trino.operator.window.Partitioner;
import io.trino.operator.window.PartitionerSupplier;
import io.trino.operator.window.PatternRecognitionPartitioner;
import io.trino.operator.window.WindowPartition;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.WindowFunction;
import io.trino.spi.type.Type;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterators.peekingIterator;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.MoreFutures.checkSuccess;
import static io.trino.operator.PositionSearcher.findEndPosition;
import static io.trino.operator.WorkProcessor.TransformationState.needsMoreData;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.sql.planner.plan.FrameBoundType.FOLLOWING;
import static io.trino.sql.planner.plan.FrameBoundType.PRECEDING;
import static io.trino.sql.planner.plan.WindowFrameType.RANGE;
import static io.trino.util.MergeSortedPages.mergeSortedPages;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class WindowOperator
        implements Operator
{
    public static class WindowOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final List<Integer> outputChannels;
        private final List<WindowFunctionDefinition> windowFunctionDefinitions;
        private final List<Integer> partitionChannels;
        private final List<Integer> preGroupedChannels;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final int preSortedChannelPrefix;
        private final int expectedPositions;
        private boolean closed;
        private final PagesIndex.Factory pagesIndexFactory;
        private final boolean spillEnabled;
        private final SpillerFactory spillerFactory;
        private final OrderingCompiler orderingCompiler;
        private final List<Type> measureTypes;
        private final PartitionerSupplier partitionerSupplier;

        public WindowOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<WindowFunctionDefinition> windowFunctionDefinitions,
                List<Integer> partitionChannels,
                List<Integer> preGroupedChannels,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                int preSortedChannelPrefix,
                int expectedPositions,
                PagesIndex.Factory pagesIndexFactory,
                boolean spillEnabled,
                SpillerFactory spillerFactory,
                OrderingCompiler orderingCompiler,
                List<Type> measureTypes,
                PartitionerSupplier partitionerSupplier)
        {
            requireNonNull(sourceTypes, "sourceTypes is null");
            requireNonNull(planNodeId, "planNodeId is null");
            requireNonNull(outputChannels, "outputChannels is null");
            requireNonNull(windowFunctionDefinitions, "windowFunctionDefinitions is null");
            requireNonNull(partitionChannels, "partitionChannels is null");
            requireNonNull(preGroupedChannels, "preGroupedChannels is null");
            checkArgument(partitionChannels.containsAll(preGroupedChannels), "preGroupedChannels must be a subset of partitionChannels");
            requireNonNull(sortChannels, "sortChannels is null");
            requireNonNull(sortOrder, "sortOrder is null");
            requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            requireNonNull(spillerFactory, "spillerFactory is null");
            requireNonNull(orderingCompiler, "orderingCompiler is null");
            checkArgument(sortChannels.size() == sortOrder.size(), "Must have same number of sort channels as sort orders");
            checkArgument(preSortedChannelPrefix <= sortChannels.size(), "Cannot have more pre-sorted channels than specified sorted channels");
            checkArgument(preSortedChannelPrefix == 0 || ImmutableSet.copyOf(preGroupedChannels).equals(ImmutableSet.copyOf(partitionChannels)), "preSortedChannelPrefix can only be greater than zero if all partition channels are pre-grouped");
            requireNonNull(measureTypes, "measureTypes is null");
            requireNonNull(partitionerSupplier, "partitionerSupplier is null");

            this.pagesIndexFactory = pagesIndexFactory;
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.sourceTypes = ImmutableList.copyOf(sourceTypes);
            this.outputChannels = ImmutableList.copyOf(outputChannels);
            this.windowFunctionDefinitions = ImmutableList.copyOf(windowFunctionDefinitions);
            this.partitionChannels = ImmutableList.copyOf(partitionChannels);
            this.preGroupedChannels = ImmutableList.copyOf(preGroupedChannels);
            this.sortChannels = ImmutableList.copyOf(sortChannels);
            this.sortOrder = ImmutableList.copyOf(sortOrder);
            this.preSortedChannelPrefix = preSortedChannelPrefix;
            this.expectedPositions = expectedPositions;
            this.spillEnabled = spillEnabled;
            this.spillerFactory = spillerFactory;
            this.orderingCompiler = orderingCompiler;
            this.measureTypes = measureTypes;
            this.partitionerSupplier = partitionerSupplier;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, WindowOperator.class.getSimpleName());
            return new WindowOperator(
                    operatorContext,
                    sourceTypes,
                    outputChannels,
                    windowFunctionDefinitions,
                    partitionChannels,
                    preGroupedChannels,
                    sortChannels,
                    sortOrder,
                    preSortedChannelPrefix,
                    expectedPositions,
                    pagesIndexFactory,
                    spillEnabled,
                    spillerFactory,
                    orderingCompiler,
                    measureTypes,
                    partitionerSupplier.get(operatorContext.aggregateUserMemoryContext()));
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new WindowOperatorFactory(
                    operatorId,
                    planNodeId,
                    sourceTypes,
                    outputChannels,
                    windowFunctionDefinitions,
                    partitionChannels,
                    preGroupedChannels,
                    sortChannels,
                    sortOrder,
                    preSortedChannelPrefix,
                    expectedPositions,
                    pagesIndexFactory,
                    spillEnabled,
                    spillerFactory,
                    orderingCompiler,
                    measureTypes,
                    partitionerSupplier);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> outputTypes;
    private final int[] outputChannels;
    private final List<WindowFunction> windowFunctions;
    private final List<FrameInfo> frames;
    private final WindowInfo.DriverWindowInfoBuilder windowInfo;
    private final AtomicReference<WindowInfo> driverWindowInfo = new AtomicReference<>(WindowInfo.emptyInfo());

    private final Optional<SpillablePagesToPagesIndexes> spillablePagesToPagesIndexes;

    private final WorkProcessor<Page> outputPages;
    private final PageBuffer pageBuffer = new PageBuffer();

    private final Partitioner partitioner;

    public WindowOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> windowFunctionDefinitions,
            List<Integer> partitionChannels,
            List<Integer> preGroupedChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            int preSortedChannelPrefix,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory,
            boolean spillEnabled,
            SpillerFactory spillerFactory,
            OrderingCompiler orderingCompiler,
            List<Type> measureTypes,
            Partitioner partitioner)
    {
        requireNonNull(operatorContext, "operatorContext is null");
        requireNonNull(outputChannels, "outputChannels is null");
        requireNonNull(windowFunctionDefinitions, "windowFunctionDefinitions is null");
        checkArgument(
                windowFunctionDefinitions.stream().allMatch(definition -> definition.getFrameInfo().isPresent()) ||
                        windowFunctionDefinitions.stream().allMatch(definition -> definition.getFrameInfo().isEmpty()),
                "FrameInfo must be equally present or empty for all window functions");
        requireNonNull(partitionChannels, "partitionChannels is null");
        requireNonNull(preGroupedChannels, "preGroupedChannels is null");
        checkArgument(partitionChannels.containsAll(preGroupedChannels), "preGroupedChannels must be a subset of partitionChannels");
        requireNonNull(sortChannels, "sortChannels is null");
        requireNonNull(sortOrder, "sortOrder is null");
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
        requireNonNull(spillerFactory, "spillerFactory is null");
        checkArgument(sortChannels.size() == sortOrder.size(), "Must have same number of sort channels as sort orders");
        checkArgument(preSortedChannelPrefix <= sortChannels.size(), "Cannot have more pre-sorted channels than specified sorted channels");
        checkArgument(preSortedChannelPrefix == 0 || ImmutableSet.copyOf(preGroupedChannels).equals(ImmutableSet.copyOf(partitionChannels)), "preSortedChannelPrefix can only be greater than zero if all partition channels are pre-grouped");
        requireNonNull(measureTypes, "measureTypes is null");
        requireNonNull(partitioner, "partitioner is null");
        checkArgument(
                windowFunctionDefinitions.stream().noneMatch(definition -> definition.getFrameInfo().isEmpty()) || partitioner instanceof PatternRecognitionPartitioner,
                "Missing FrameInfo for a window function outside pattern recognition context");

        this.operatorContext = operatorContext;
        this.outputChannels = Ints.toArray(outputChannels);
        this.windowFunctions = windowFunctionDefinitions.stream()
                .map(WindowFunctionDefinition::createWindowFunction)
                .collect(toImmutableList());
        if (windowFunctionDefinitions.stream().anyMatch(definition -> definition.getFrameInfo().isPresent())) {
            this.frames = windowFunctionDefinitions.stream()
                    .map(functionDefinition -> functionDefinition.getFrameInfo().get())
                    .collect(toImmutableList());
        }
        else {
            this.frames = ImmutableList.of();
        }

        ImmutableList.Builder<Type> outputTypes = ImmutableList.builder();
        outputTypes.addAll(outputChannels.stream()
                .map(sourceTypes::get)
                .collect(Collectors.toList()));
        outputTypes.addAll(measureTypes);
        outputTypes.addAll(windowFunctionDefinitions.stream()
                .map(WindowFunctionDefinition::getType)
                .collect(Collectors.toList()));
        this.outputTypes = outputTypes.build();

        List<Integer> unGroupedPartitionChannels = partitionChannels.stream()
                .filter(channel -> !preGroupedChannels.contains(channel))
                .collect(toImmutableList());
        List<Integer> preSortedChannels = sortChannels.stream()
                .limit(preSortedChannelPrefix)
                .collect(toImmutableList());

        List<Integer> unGroupedOrderChannels = ImmutableList.copyOf(concat(unGroupedPartitionChannels, sortChannels));
        List<SortOrder> unGroupedOrdering = ImmutableList.copyOf(concat(nCopies(unGroupedPartitionChannels.size(), ASC_NULLS_LAST), sortOrder));

        List<Integer> orderChannels;
        List<SortOrder> ordering;
        if (preSortedChannelPrefix > 0) {
            // This already implies that set(preGroupedChannels) == set(partitionChannels) (enforced with checkArgument)
            orderChannels = ImmutableList.copyOf(Iterables.skip(sortChannels, preSortedChannelPrefix));
            ordering = ImmutableList.copyOf(Iterables.skip(sortOrder, preSortedChannelPrefix));
        }
        else {
            // Otherwise, we need to sort by the unGroupedPartitionChannels and all original sort channels
            orderChannels = unGroupedOrderChannels;
            ordering = unGroupedOrdering;
        }

        PagesIndexWithHashStrategies inMemoryPagesIndexWithHashStrategies = new PagesIndexWithHashStrategies(
                pagesIndexFactory,
                sourceTypes,
                expectedPositions,
                preGroupedChannels,
                unGroupedPartitionChannels,
                preSortedChannels,
                sortChannels,
                windowFunctionDefinitions);

        if (spillEnabled) {
            PagesIndexWithHashStrategies mergedPagesIndexWithHashStrategies = new PagesIndexWithHashStrategies(
                    pagesIndexFactory,
                    sourceTypes,
                    expectedPositions,
                    // merged pages are grouped on all partition channels
                    partitionChannels,
                    ImmutableList.of(),
                    // merged pages are pre sorted on all sort channels
                    sortChannels,
                    sortChannels,
                    windowFunctionDefinitions);

            List<Type> unGroupedOrderTypes = unGroupedOrderChannels.stream()
                    .map(sourceTypes::get)
                    .collect(toImmutableList());
            this.spillablePagesToPagesIndexes = Optional.of(new SpillablePagesToPagesIndexes(
                    inMemoryPagesIndexWithHashStrategies,
                    mergedPagesIndexWithHashStrategies,
                    sourceTypes,
                    orderChannels,
                    ordering,
                    spillerFactory,
                    orderingCompiler.compilePageWithPositionComparator(unGroupedOrderTypes, unGroupedOrderChannels, unGroupedOrdering)));

            this.outputPages = pageBuffer.pages()
                    .flatTransform(spillablePagesToPagesIndexes.get())
                    .flatMap(this::pagesIndexToWindowPartitions)
                    .transform(new WindowPartitionsToOutputPages());
        }
        else {
            this.spillablePagesToPagesIndexes = Optional.empty();
            this.outputPages = pageBuffer.pages()
                    .transform(new PagesToPagesIndexes(inMemoryPagesIndexWithHashStrategies, orderChannels, ordering))
                    .flatMap(this::pagesIndexToWindowPartitions)
                    .transform(new WindowPartitionsToOutputPages());
        }

        windowInfo = new WindowInfo.DriverWindowInfoBuilder();
        operatorContext.setInfoSupplier(driverWindowInfo::get);

        this.partitioner = partitioner;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        pageBuffer.finish();
    }

    @Override
    public boolean isFinished()
    {
        return outputPages.isFinished();
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        // We can block e.g. because of self-triggered spill
        if (outputPages.isBlocked()) {
            return outputPages.getBlockedFuture();
        }

        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return pageBuffer.isEmpty() && !pageBuffer.isFinished();
    }

    @Override
    public void addInput(Page page)
    {
        pageBuffer.add(page);
    }

    @Override
    public Page getOutput()
    {
        if (!outputPages.process()) {
            return null;
        }

        if (outputPages.isFinished()) {
            return null;
        }

        return outputPages.getResult();
    }

    @Override
    public ListenableFuture<Void> startMemoryRevoke()
    {
        return spillablePagesToPagesIndexes.get().spill();
    }

    @Override
    public void finishMemoryRevoke()
    {
        spillablePagesToPagesIndexes.get().finishRevokeMemory();
    }

    private static class PagesIndexWithHashStrategies
    {
        final PagesIndex pagesIndex;
        final PagesHashStrategy preGroupedPartitionHashStrategy;
        final PagesHashStrategy unGroupedPartitionHashStrategy;
        final PagesHashStrategy preSortedPartitionHashStrategy;
        final PagesHashStrategy peerGroupHashStrategy;
        final int[] preGroupedPartitionChannels;
        final Map<FrameBoundKey, PagesIndexComparator> frameBoundComparators;

        PagesIndexWithHashStrategies(
                PagesIndex.Factory pagesIndexFactory,
                List<Type> sourceTypes,
                int expectedPositions,
                List<Integer> preGroupedPartitionChannels,
                List<Integer> unGroupedPartitionChannels,
                List<Integer> preSortedChannels,
                List<Integer> sortChannels,
                List<WindowFunctionDefinition> windowFunctionDefinitions)
        {
            this.pagesIndex = pagesIndexFactory.newPagesIndex(sourceTypes, expectedPositions);
            this.preGroupedPartitionHashStrategy = pagesIndex.createPagesHashStrategy(preGroupedPartitionChannels, OptionalInt.empty());
            this.unGroupedPartitionHashStrategy = pagesIndex.createPagesHashStrategy(unGroupedPartitionChannels, OptionalInt.empty());
            this.preSortedPartitionHashStrategy = pagesIndex.createPagesHashStrategy(preSortedChannels, OptionalInt.empty());
            this.peerGroupHashStrategy = pagesIndex.createPagesHashStrategy(sortChannels, OptionalInt.empty());
            this.preGroupedPartitionChannels = Ints.toArray(preGroupedPartitionChannels);
            this.frameBoundComparators = createFrameBoundComparators(pagesIndex, windowFunctionDefinitions);
        }
    }

    /**
     * Create comparators necessary for seeking frame start or frame end for window functions with frame type RANGE.
     * Whenever a frame bound is specified as RANGE X PRECEDING or RANGE X FOLLOWING,
     * a dedicated comparator is created to compare sort key values with expected frame bound values.
     */
    private static Map<FrameBoundKey, PagesIndexComparator> createFrameBoundComparators(PagesIndex pagesIndex, List<WindowFunctionDefinition> windowFunctionDefinitions)
    {
        ImmutableMap.Builder<FrameBoundKey, PagesIndexComparator> builder = ImmutableMap.builder();

        for (int i = 0; i < windowFunctionDefinitions.size(); i++) {
            Optional<FrameInfo> frame = windowFunctionDefinitions.get(i).getFrameInfo();
            if (frame.isPresent() && frame.get().getType() == RANGE) {
                FrameInfo frameInfo = frame.get();
                if (frameInfo.getStartType() == PRECEDING || frameInfo.getStartType() == FOLLOWING) {
                    PagesIndexComparator comparator = pagesIndex.createChannelComparator(frameInfo.getSortKeyChannelForStartComparison(), frameInfo.getStartChannel());
                    builder.put(new FrameBoundKey(i, FrameBoundKey.Type.START), comparator);
                }
                if (frameInfo.getEndType() == PRECEDING || frameInfo.getEndType() == FOLLOWING) {
                    PagesIndexComparator comparator = pagesIndex.createChannelComparator(frameInfo.getSortKeyChannelForEndComparison(), frameInfo.getEndChannel());
                    builder.put(new FrameBoundKey(i, FrameBoundKey.Type.END), comparator);
                }
            }
        }

        return builder.buildOrThrow();
    }

    public static class FrameBoundKey
    {
        private final int functionIndex;
        private final Type type;

        public enum Type
        {
            START,
            END
        }

        public FrameBoundKey(int functionIndex, Type type)
        {
            this.functionIndex = functionIndex;
            this.type = requireNonNull(type, "type is null");
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
            FrameBoundKey that = (FrameBoundKey) o;
            return functionIndex == that.functionIndex &&
                    type == that.type;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(functionIndex, type);
        }
    }

    private class PagesToPagesIndexes
            implements Transformation<Page, PagesIndexWithHashStrategies>
    {
        final PagesIndexWithHashStrategies pagesIndexWithHashStrategies;
        final List<Integer> orderChannels;
        final List<SortOrder> ordering;
        final LocalMemoryContext memoryContext;

        boolean resetPagesIndex;
        int pendingInputPosition;

        PagesToPagesIndexes(
                PagesIndexWithHashStrategies pagesIndexWithHashStrategies,
                List<Integer> orderChannels,
                List<SortOrder> ordering)
        {
            this.pagesIndexWithHashStrategies = pagesIndexWithHashStrategies;
            this.orderChannels = orderChannels;
            this.ordering = ordering;
            this.memoryContext = operatorContext.aggregateUserMemoryContext().newLocalMemoryContext(PagesToPagesIndexes.class.getSimpleName());
        }

        @Override
        public TransformationState<PagesIndexWithHashStrategies> process(Page pendingInput)
        {
            if (resetPagesIndex) {
                pagesIndexWithHashStrategies.pagesIndex.clear();
                updateMemoryUsage();
                resetPagesIndex = false;
            }

            boolean finishing = pendingInput == null;
            if (finishing && pagesIndexWithHashStrategies.pagesIndex.getPositionCount() == 0) {
                memoryContext.close();
                return TransformationState.finished();
            }

            if (!finishing) {
                pendingInputPosition = updatePagesIndex(pagesIndexWithHashStrategies, pendingInput, pendingInputPosition, Optional.empty());
                updateMemoryUsage();
            }

            // If we have unused input or are finishing, then we have buffered a full group
            if (finishing || pendingInputPosition < pendingInput.getPositionCount()) {
                sortPagesIndexIfNecessary(pagesIndexWithHashStrategies, orderChannels, ordering);
                resetPagesIndex = true;
                return TransformationState.ofResult(pagesIndexWithHashStrategies, false);
            }

            pendingInputPosition = 0;
            return TransformationState.needsMoreData();
        }

        void updateMemoryUsage()
        {
            memoryContext.setBytes(pagesIndexWithHashStrategies.pagesIndex.getEstimatedSize().toBytes());
        }
    }

    private WorkProcessor<WindowPartition> pagesIndexToWindowPartitions(PagesIndexWithHashStrategies pagesIndexWithHashStrategies)
    {
        PagesIndex pagesIndex = pagesIndexWithHashStrategies.pagesIndex;

        // pagesIndex contains the full grouped & sorted data for one or more partitions

        windowInfo.addIndex(pagesIndex);

        return WorkProcessor.create(new WorkProcessor.Process<>()
        {
            int partitionStart;

            @Override
            public ProcessState<WindowPartition> process()
            {
                if (partitionStart == pagesIndex.getPositionCount()) {
                    return ProcessState.finished();
                }

                int partitionEnd = findGroupEnd(pagesIndex, pagesIndexWithHashStrategies.unGroupedPartitionHashStrategy, partitionStart);

                WindowPartition partition = partitioner.createPartition(
                        pagesIndex,
                        partitionStart,
                        partitionEnd,
                        outputChannels,
                        windowFunctions,
                        frames,
                        pagesIndexWithHashStrategies.peerGroupHashStrategy,
                        pagesIndexWithHashStrategies.frameBoundComparators,
                        operatorContext.aggregateUserMemoryContext());

                windowInfo.addPartition(partition);
                partitionStart = partitionEnd;
                return ProcessState.ofResult(partition);
            }
        });
    }

    private class WindowPartitionsToOutputPages
            implements Transformation<WindowPartition, Page>
    {
        final PageBuilder pageBuilder;

        WindowPartitionsToOutputPages()
        {
            pageBuilder = new PageBuilder(outputTypes);
        }

        @Override
        public TransformationState<Page> process(WindowPartition partition)
        {
            boolean finishing = partition == null;
            if (finishing) {
                if (pageBuilder.isEmpty()) {
                    return TransformationState.finished();
                }

                // Output the remaining page if we have anything buffered
                Page page = pageBuilder.build();
                pageBuilder.reset();
                return TransformationState.ofResult(page, false);
            }

            while (!pageBuilder.isFull() && partition.hasNext()) {
                partition.processNextRow(pageBuilder);
            }
            if (!pageBuilder.isFull()) {
                return needsMoreData();
            }

            Page page = pageBuilder.build();
            pageBuilder.reset();
            return TransformationState.ofResult(page, !partition.hasNext());
        }
    }

    private class SpillablePagesToPagesIndexes
            implements Transformation<Page, WorkProcessor<PagesIndexWithHashStrategies>>
    {
        final PagesIndexWithHashStrategies inMemoryPagesIndexWithHashStrategies;
        final PagesIndexWithHashStrategies mergedPagesIndexWithHashStrategies;
        final List<Type> sourceTypes;
        final List<Integer> orderChannels;
        final List<SortOrder> ordering;
        final LocalMemoryContext localRevocableMemoryContext;
        final LocalMemoryContext localUserMemoryContext;
        final SpillerFactory spillerFactory;
        final PageWithPositionComparator pageWithPositionComparator;

        boolean spillingWhenConvertingRevocableMemory;
        boolean resetPagesIndex;
        int pendingInputPosition;

        Optional<Page> currentSpillGroupRowPage;
        Optional<Spiller> spiller;
        // Spill can be trigger by Driver, by us or both. `spillInProgress` is not empty when spill was triggered but not `finishMemoryRevoke()` yet
        Optional<ListenableFuture<Void>> spillInProgress = Optional.empty();

        SpillablePagesToPagesIndexes(
                PagesIndexWithHashStrategies inMemoryPagesIndexWithHashStrategies,
                PagesIndexWithHashStrategies mergedPagesIndexWithHashStrategies,
                List<Type> sourceTypes,
                List<Integer> orderChannels,
                List<SortOrder> ordering,
                SpillerFactory spillerFactory,
                PageWithPositionComparator pageWithPositionComparator)
        {
            this.inMemoryPagesIndexWithHashStrategies = inMemoryPagesIndexWithHashStrategies;
            this.mergedPagesIndexWithHashStrategies = mergedPagesIndexWithHashStrategies;
            this.sourceTypes = sourceTypes;
            this.orderChannels = orderChannels;
            this.ordering = ordering;
            this.localUserMemoryContext = operatorContext.aggregateUserMemoryContext().newLocalMemoryContext(SpillablePagesToPagesIndexes.class.getSimpleName());
            this.localRevocableMemoryContext = operatorContext.aggregateRevocableMemoryContext().newLocalMemoryContext(SpillablePagesToPagesIndexes.class.getSimpleName());
            this.spillerFactory = spillerFactory;
            this.pageWithPositionComparator = pageWithPositionComparator;

            this.currentSpillGroupRowPage = Optional.empty();
            this.spiller = Optional.empty();
        }

        @Override
        public TransformationState<WorkProcessor<PagesIndexWithHashStrategies>> process(Page pendingInput)
        {
            if (spillingWhenConvertingRevocableMemory) {
                // Spill could already be finished by Driver (via WindowOperator#finishMemoryRevoke), but finishRevokeMemory will take care of that
                finishRevokeMemory();
                spillingWhenConvertingRevocableMemory = false;
                return fullGroupBuffered();
            }

            if (resetPagesIndex) {
                inMemoryPagesIndexWithHashStrategies.pagesIndex.clear();
                currentSpillGroupRowPage = Optional.empty();

                closeSpiller();

                updateMemoryUsage(false);
                resetPagesIndex = false;
            }

            boolean finishing = pendingInput == null;
            if (finishing && inMemoryPagesIndexWithHashStrategies.pagesIndex.getPositionCount() == 0 && spiller.isEmpty()) {
                localRevocableMemoryContext.close();
                localUserMemoryContext.close();
                return TransformationState.finished();
            }

            if (!finishing) {
                pendingInputPosition = updatePagesIndex(inMemoryPagesIndexWithHashStrategies, pendingInput, pendingInputPosition, currentSpillGroupRowPage);
            }

            // If we have unused input or are finishing, then we have buffered a full group
            if (finishing || pendingInputPosition < pendingInput.getPositionCount()) {
                return fullGroupBuffered();
            }

            updateMemoryUsage(true);
            pendingInputPosition = 0;
            return needsMoreData();
        }

        void closeSpiller()
        {
            spiller.ifPresent(Spiller::close);
            spiller = Optional.empty();
        }

        void clearIndexes()
        {
            inMemoryPagesIndexWithHashStrategies.pagesIndex.clear();
            mergedPagesIndexWithHashStrategies.pagesIndex.clear();
        }

        TransformationState<WorkProcessor<PagesIndexWithHashStrategies>> fullGroupBuffered()
        {
            // Convert revocable memory to user memory as inMemoryPagesIndexWithHashStrategies holds on to memory so we no longer can revoke
            if (localRevocableMemoryContext.getBytes() > 0) {
                long currentRevocableBytes = localRevocableMemoryContext.getBytes();
                localRevocableMemoryContext.setBytes(0);
                if (!localUserMemoryContext.trySetBytes(localUserMemoryContext.getBytes() + currentRevocableBytes)) {
                    // TODO: this might fail (even though we have just released memory), but we don't
                    // have a proper way to atomically convert memory reservations
                    localRevocableMemoryContext.setBytes(currentRevocableBytes);
                    spillingWhenConvertingRevocableMemory = true;
                    return TransformationState.blocked(spill());
                }
            }

            sortPagesIndexIfNecessary(inMemoryPagesIndexWithHashStrategies, orderChannels, ordering);
            resetPagesIndex = true;
            return TransformationState.ofResult(unspill(), false);
        }

        ListenableFuture<Void> spill()
        {
            if (spillInProgress.isPresent()) {
                // Spill can be triggered first in SpillablePagesToPagesIndexes#process(..) and then by Driver (via WindowOperator#startMemoryRevoke)
                return spillInProgress.get();
            }

            if (localRevocableMemoryContext.getBytes() == 0) {
                // This must be stale revoke request
                spillInProgress = Optional.of(immediateVoidFuture());
                return spillInProgress.get();
            }

            if (spiller.isEmpty()) {
                spiller = Optional.of(spillerFactory.create(
                        sourceTypes,
                        operatorContext.getSpillContext(),
                        operatorContext.newAggregateUserMemoryContext()));
            }

            verify(inMemoryPagesIndexWithHashStrategies.pagesIndex.getPositionCount() > 0);
            sortPagesIndexIfNecessary(inMemoryPagesIndexWithHashStrategies, orderChannels, ordering);
            PeekingIterator<Page> sortedPages = peekingIterator(inMemoryPagesIndexWithHashStrategies.pagesIndex.getSortedPages());
            Page anyPage = sortedPages.peek();
            verify(anyPage.getPositionCount() != 0, "PagesIndex.getSortedPages returned an empty page");
            currentSpillGroupRowPage = Optional.of(anyPage.getSingleValuePage(/* any */0));
            spillInProgress = Optional.of(spiller.get().spill(sortedPages));

            return spillInProgress.get();
        }

        void finishRevokeMemory()
        {
            if (spillInProgress.isEmpty()) {
                // Same spill iteration can be finished first by Driver (via WindowOperator#finishMemoryRevoke) and then by SpillablePagesToPagesIndexes#process(..)
                return;
            }

            checkSuccess(spillInProgress.get(), "spilling failed");
            spillInProgress = Optional.empty();

            // No memory to reclaim
            if (localRevocableMemoryContext.getBytes() == 0) {
                return;
            }

            inMemoryPagesIndexWithHashStrategies.pagesIndex.clear();
            updateMemoryUsage(false);
        }

        WorkProcessor<PagesIndexWithHashStrategies> unspill()
        {
            if (spiller.isEmpty()) {
                return WorkProcessor.fromIterable(ImmutableList.of(inMemoryPagesIndexWithHashStrategies));
            }

            List<WorkProcessor<Page>> sortedStreams = ImmutableList.<WorkProcessor<Page>>builder()
                    .addAll(spiller.get().getSpills().stream()
                            .map(WorkProcessor::fromIterator)
                            .collect(toImmutableList()))
                    .add(WorkProcessor.fromIterator(inMemoryPagesIndexWithHashStrategies.pagesIndex.getSortedPages()))
                    .build();

            WorkProcessor<Page> mergedPages = mergeSortedPages(
                    sortedStreams,
                    pageWithPositionComparator,
                    sourceTypes,
                    operatorContext.aggregateUserMemoryContext(),
                    operatorContext.getDriverContext().getYieldSignal());

            return mergedPages.transform(new PagesToPagesIndexes(mergedPagesIndexWithHashStrategies, ImmutableList.of(), ImmutableList.of()));
        }

        void updateMemoryUsage(boolean revocablePagesIndex)
        {
            long pagesIndexBytes = inMemoryPagesIndexWithHashStrategies.pagesIndex.getEstimatedSize().toBytes();
            if (revocablePagesIndex) {
                verify(inMemoryPagesIndexWithHashStrategies.pagesIndex.getPositionCount() > 0);
                localUserMemoryContext.setBytes(0);
                localRevocableMemoryContext.setBytes(pagesIndexBytes);
            }
            else {
                localRevocableMemoryContext.setBytes(0L);
                localUserMemoryContext.setBytes(pagesIndexBytes);
            }
        }
    }

    private int updatePagesIndex(PagesIndexWithHashStrategies pagesIndexWithHashStrategies, Page page, int startPosition, Optional<Page> currentSpillGroupRowPage)
    {
        checkArgument(page.getPositionCount() > startPosition);

        // TODO: Fix pagesHashStrategy to allow specifying channels for comparison, it currently requires us to rearrange the right side blocks in consecutive channel order
        Page preGroupedPage = page.getColumns(pagesIndexWithHashStrategies.preGroupedPartitionChannels);

        PagesIndex pagesIndex = pagesIndexWithHashStrategies.pagesIndex;
        PagesHashStrategy preGroupedPartitionHashStrategy = pagesIndexWithHashStrategies.preGroupedPartitionHashStrategy;
        if (currentSpillGroupRowPage.isPresent()) {
            if (!preGroupedPartitionHashStrategy.rowIdenticalToRow(0, currentSpillGroupRowPage.get().getColumns(pagesIndexWithHashStrategies.preGroupedPartitionChannels), startPosition, preGroupedPage)) {
                return startPosition;
            }
        }

        if (pagesIndex.getPositionCount() == 0 || pagesIndex.positionIdenticalToRow(preGroupedPartitionHashStrategy, 0, startPosition, preGroupedPage)) {
            // Find the position where the pre-grouped columns change
            int groupEnd = findGroupEnd(preGroupedPage, preGroupedPartitionHashStrategy, startPosition);

            // Add the section of the page that contains values for the current group
            pagesIndex.addPage(page.getRegion(startPosition, groupEnd - startPosition));

            if (page.getPositionCount() - groupEnd > 0) {
                // Save the remaining page, which may contain multiple partitions
                return groupEnd;
            }
            // Page fully consumed
            return page.getPositionCount();
        }
        // We had previous results buffered, but the remaining page starts with new group values
        return startPosition;
    }

    private void sortPagesIndexIfNecessary(PagesIndexWithHashStrategies pagesIndexWithHashStrategies, List<Integer> orderChannels, List<SortOrder> ordering)
    {
        if (pagesIndexWithHashStrategies.pagesIndex.getPositionCount() > 1 && !orderChannels.isEmpty()) {
            int startPosition = 0;
            while (startPosition < pagesIndexWithHashStrategies.pagesIndex.getPositionCount()) {
                int endPosition = findGroupEnd(pagesIndexWithHashStrategies.pagesIndex, pagesIndexWithHashStrategies.preSortedPartitionHashStrategy, startPosition);
                pagesIndexWithHashStrategies.pagesIndex.sort(orderChannels, ordering, startPosition, endPosition);
                startPosition = endPosition;
            }
        }
    }

    // Assumes input grouped on relevant pagesHashStrategy columns
    private static int findGroupEnd(Page page, PagesHashStrategy pagesHashStrategy, int startPosition)
    {
        checkArgument(page.getPositionCount() > 0, "Must have at least one position");
        checkPositionIndex(startPosition, page.getPositionCount(), "startPosition out of bounds");

        return findEndPosition(startPosition, page.getPositionCount(), (firstPosition, secondPosition) -> pagesHashStrategy.rowIdenticalToRow(firstPosition, page, secondPosition, page));
    }

    // Assumes input grouped on relevant pagesHashStrategy columns
    private static int findGroupEnd(PagesIndex pagesIndex, PagesHashStrategy pagesHashStrategy, int startPosition)
    {
        checkArgument(pagesIndex.getPositionCount() > 0, "Must have at least one position");
        checkPositionIndex(startPosition, pagesIndex.getPositionCount(), "startPosition out of bounds");

        return findEndPosition(startPosition, pagesIndex.getPositionCount(), (firstPosition, secondPosition) -> pagesIndex.positionIdenticalToPosition(pagesHashStrategy, firstPosition, secondPosition));
    }

    @Override
    public void close()
    {
        driverWindowInfo.set(new WindowInfo(ImmutableList.of(windowInfo.build())));
        spillablePagesToPagesIndexes.ifPresent(SpillablePagesToPagesIndexes::clearIndexes);
        spillablePagesToPagesIndexes.ifPresent(SpillablePagesToPagesIndexes::closeSpiller);
    }
}
