/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering;

import com.google.common.annotations.VisibleForTesting;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeOperators;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.starburstdata.trino.plugins.dynamicfiltering.SelectedPositions.EMPTY;
import static com.starburstdata.trino.plugins.dynamicfiltering.SelectedPositions.positionsList;
import static com.starburstdata.trino.plugins.dynamicfiltering.SelectedPositions.positionsRange;
import static com.starburstdata.trino.plugins.dynamicfiltering.TupleDomainFilterUtils.createTupleDomainFilters;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class DynamicPageFilter
{
    static final BlockFilter[] NONE_BLOCK_FILTER = new BlockFilter[] {new NoneBlockFilter()};

    private final DynamicFilter dynamicFilter;
    private final Map<ColumnHandle, Integer> channelIndexes;
    private final TypeOperators typeOperators;
    private final IsolatedBlockFilterFactory isolatedBlockFilterFactory;
    private final Executor executor;

    private final AtomicReference<BlockFilter[]> currentFilter = new AtomicReference<>();

    @Nullable
    @GuardedBy("this")
    private TupleDomain<ColumnHandle> collectedDynamicFilter = TupleDomain.all();

    private final CompletableFuture<?> isBlocked;

    public DynamicPageFilter(
            DynamicFilter dynamicFilter,
            List<ColumnHandle> columns,
            TypeOperators typeOperators,
            IsolatedBlockFilterFactory isolatedBlockFilterFactory,
            Executor executor)
    {
        requireNonNull(columns, "columns is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.channelIndexes = IntStream.range(0, columns.size())
                .boxed()
                .collect(toImmutableMap(columns::get, identity()));
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.isolatedBlockFilterFactory = requireNonNull(isolatedBlockFilterFactory, "isolatedBlockFilterFactory is null");
        this.executor = requireNonNull(executor, "executorService is null");
        this.isBlocked = new CompletableFuture<>();
        executor.execute(this::collectDynamicFilterAsync);
    }

    public CompletableFuture<?> isBlocked()
    {
        return unmodifiableFuture(isBlocked);
    }

    public boolean isAwaitable()
    {
        return !isBlocked.isDone();
    }

    public Optional<BlockFilter[]> getBlockFilters()
    {
        return Optional.ofNullable(currentFilter.get());
    }

    private void collectDynamicFilterAsync()
    {
        if (!dynamicFilter.isAwaitable()) {
            updatePageFilter();
            isBlocked.complete(null);
            return;
        }

        toListenableFuture(dynamicFilter.isBlocked()).addListener(this::collectDynamicFilterAsync, executor);
        // Update page filter since we might miss a DF update between the isAwaitable check
        // and getting the next future from isBlocked
        updatePageFilter();
    }

    private synchronized void updatePageFilter()
    {
        boolean dynamicFilterComplete = !dynamicFilter.isAwaitable();
        TupleDomain<ColumnHandle> currentDynamicFilter = dynamicFilter.getCurrentPredicate();
        if (collectedDynamicFilter == null || currentDynamicFilter.equals(collectedDynamicFilter)) {
            if (dynamicFilterComplete) {
                // dynamicFilter completed with an ALL update
                // save on memory as we have already created the complete page filter
                collectedDynamicFilter = null;
            }
            return;
        }
        collectedDynamicFilter = currentDynamicFilter;
        createPageFilter(currentDynamicFilter, channelIndexes, typeOperators, isolatedBlockFilterFactory)
                .ifPresent(currentFilter::set);
        if (dynamicFilterComplete) {
            // save on memory as we have already created the complete page filter
            collectedDynamicFilter = null;
        }
    }

    @VisibleForTesting
    static Optional<BlockFilter[]> createPageFilter(
            TupleDomain<ColumnHandle> tupleDomain,
            Map<ColumnHandle, Integer> channelIndexes,
            TypeOperators typeOperators,
            IsolatedBlockFilterFactory isolatedBlockFilterFactory)
    {
        if (tupleDomain.isAll()) {
            return Optional.empty();
        }
        if (tupleDomain.isNone()) {
            return Optional.of(NONE_BLOCK_FILTER);
        }

        Map<ColumnHandle, TupleDomainFilter> columnFilters = createTupleDomainFilters(tupleDomain, typeOperators);
        if (columnFilters.isEmpty()) {
            return Optional.empty();
        }

        BlockFilter[] channelBlockFilters = new BlockFilter[columnFilters.size()];
        int index = 0;
        for (Map.Entry<ColumnHandle, TupleDomainFilter> entry : columnFilters.entrySet()) {
            TupleDomainFilter filter = entry.getValue();
            int channelIndex = requireNonNull(channelIndexes.get(entry.getKey()));
            channelBlockFilters[index] = isolatedBlockFilterFactory.createIsolatedBlockFilter(filter, channelIndex);
            index++;
        }
        return Optional.of(channelBlockFilters);
    }

    private static class NoneBlockFilter
            implements BlockFilter
    {
        @Override
        public SelectedPositions filter(Block block, SelectedPositions selectedPositions)
        {
            return EMPTY;
        }

        @Override
        public boolean[] selectedPositionsMask(Block block)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getChannelIndex()
        {
            throw new UnsupportedOperationException();
        }
    }

    // Needs to be public for class loader isolation to work
    public interface BlockFilter
    {
        SelectedPositions filter(Block block, SelectedPositions selectedPositions);

        // Used to generate a mask of selected positions for dictionaries
        boolean[] selectedPositionsMask(Block block);

        int getChannelIndex();
    }

    // Needs to be public for class loader isolation to work
    public static class InternalBlockFilter
            implements BlockFilter
    {
        private final TupleDomainFilter filter;
        private final int channelIndex;

        public InternalBlockFilter(TupleDomainFilter filter, int channelIndex)
        {
            this.filter = requireNonNull(filter, "filter is null");
            this.channelIndex = channelIndex;
        }

        @Override
        public SelectedPositions filter(Block block, SelectedPositions selectedPositions)
        {
            if (block instanceof RunLengthEncodedBlock) {
                return filterRleBlock((RunLengthEncodedBlock) block, selectedPositions);
            }
            return filterNonRleBlock(block, selectedPositions);
        }

        @Override
        public boolean[] selectedPositionsMask(Block block)
        {
            int positionCount = block.getPositionCount();
            boolean[] selectionMask = new boolean[positionCount];
            boolean nullAllowed = filter.isNullAllowed();
            if (block.mayHaveNull()) {
                for (int position = 0; position < positionCount; position++) {
                    selectionMask[position] = block.isNull(position)
                            ? nullAllowed
                            : filter.testContains(block, position);
                }
            }
            else {
                for (int position = 0; position < positionCount; position++) {
                    selectionMask[position] = filter.testContains(block, position);
                }
            }
            return selectionMask;
        }

        @Override
        public int getChannelIndex()
        {
            return channelIndex;
        }

        private SelectedPositions filterNonRleBlock(Block block, SelectedPositions selectedPositions)
        {
            if (!block.mayHaveNull()) {
                // Block is guaranteed to be non-null
                if (selectedPositions.isList()) {
                    return filterBlockWithoutNulls(block, selectedPositions);
                }
                return filterBlockWithoutNulls(block, selectedPositions.size());
            }

            if (selectedPositions.isList()) {
                return filterBlockWithNulls(block, filter.isNullAllowed(), selectedPositions);
            }
            return filterBlockWithNulls(block, filter.isNullAllowed(), selectedPositions.size());
        }

        private SelectedPositions filterRleBlock(RunLengthEncodedBlock block, SelectedPositions positions)
        {
            boolean nullAllowed = filter.isNullAllowed();
            if (positions.isEmpty()) {
                return positions;
            }
            if (block.isNull(0)) {
                return nullAllowed ? positions : EMPTY;
            }
            return filter.testContains(block.getValue(), 0) ? positions : EMPTY;
        }

        private SelectedPositions filterBlockWithoutNulls(Block block, SelectedPositions selectedPositions)
        {
            int[] positions = selectedPositions.getPositions();
            int positionCount = selectedPositions.size();
            int outputPositionsCount = 0;
            int[] outputPositions = new int[positionCount];
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                // extra copying to avoid branch
                outputPositions[outputPositionsCount] = position;
                outputPositionsCount += filter.testContains(block, position) ? 1 : 0;
            }
            return positionsList(outputPositions, outputPositionsCount);
        }

        private SelectedPositions filterBlockWithoutNulls(Block block, int positionCount)
        {
            int outputPositionsCount = 0;
            int[] outputPositions = new int[positionCount];
            for (int position = 0; position < positionCount; position++) {
                // extra copying to avoid branch
                outputPositions[outputPositionsCount] = position;
                outputPositionsCount += filter.testContains(block, position) ? 1 : 0;
            }
            // full range was selected
            if (outputPositionsCount == positionCount) {
                return positionsRange(outputPositionsCount);
            }
            return positionsList(outputPositions, outputPositionsCount);
        }

        private SelectedPositions filterBlockWithNulls(Block block, boolean nullAllowed, SelectedPositions selectedPositions)
        {
            int[] positions = selectedPositions.getPositions();
            int positionCount = selectedPositions.size();
            int outputPositionsCount = 0;
            int[] outputPositions = new int[positionCount];
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                // extra copying to avoid branch
                outputPositions[outputPositionsCount] = position;
                outputPositionsCount += block.isNull(position)
                        ? (nullAllowed ? 1 : 0)
                        : (filter.testContains(block, position) ? 1 : 0);
            }
            return positionsList(outputPositions, outputPositionsCount);
        }

        private SelectedPositions filterBlockWithNulls(Block block, boolean nullAllowed, int positionCount)
        {
            int[] outputPositions = new int[positionCount];
            int outputPositionsCount = 0;
            for (int position = 0; position < positionCount; position++) {
                // extra copying to avoid branch
                outputPositions[outputPositionsCount] = position;
                outputPositionsCount += block.isNull(position)
                        ? (nullAllowed ? 1 : 0)
                        : (filter.testContains(block, position) ? 1 : 0);
            }
            // full range was selected
            if (outputPositionsCount == positionCount) {
                return positionsRange(outputPositionsCount);
            }
            return positionsList(outputPositions, outputPositionsCount);
        }
    }
}
