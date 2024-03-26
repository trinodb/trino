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
package io.trino.operator.dynamicfiltering;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeOperators;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static io.trino.operator.dynamicfiltering.TupleDomainFilterUtils.createTupleDomainFilters;
import static io.trino.operator.project.SelectedPositions.positionsList;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static java.util.Objects.requireNonNull;

public class DynamicPageFilter
{
    static final List<BlockFilter> NONE_BLOCK_FILTER = ImmutableList.of(new NoneBlockFilter());

    private final DynamicFilter dynamicFilter;
    private final Map<ColumnHandle, Integer> channelIndexes;
    private final TypeOperators typeOperators;
    private final IsolatedBlockFilterFactory isolatedBlockFilterFactory;
    private final Executor executor;

    private final AtomicReference<List<BlockFilter>> currentFilter = new AtomicReference<>();

    @Nullable
    @GuardedBy("this")
    private TupleDomain<ColumnHandle> collectedDynamicFilter = TupleDomain.all();

    private final CompletableFuture<?> isBlocked;

    public DynamicPageFilter(
            DynamicFilter dynamicFilter,
            Map<ColumnHandle, Integer> channelIndexes,
            TypeOperators typeOperators,
            IsolatedBlockFilterFactory isolatedBlockFilterFactory,
            Executor executor)
    {
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.channelIndexes = ImmutableMap.copyOf(requireNonNull(channelIndexes, "channelIndexes is null"));
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

    public Optional<List<BlockFilter>> getBlockFilters()
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
    static Optional<List<BlockFilter>> createPageFilter(
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

        ImmutableList.Builder<BlockFilter> channelBlockFilters = ImmutableList.builderWithExpectedSize(columnFilters.size());
        for (Map.Entry<ColumnHandle, TupleDomainFilter> entry : columnFilters.entrySet()) {
            TupleDomainFilter filter = entry.getValue();
            int channelIndex = requireNonNull(channelIndexes.get(entry.getKey()));
            channelBlockFilters.add(isolatedBlockFilterFactory.createIsolatedBlockFilter(filter, channelIndex));
        }
        return Optional.of(channelBlockFilters.build());
    }

    private static class NoneBlockFilter
            implements BlockFilter
    {
        @Override
        public SelectedPositions filter(Block block, SelectedPositions selectedPositions)
        {
            return BlockFilter.EMPTY;
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
        SelectedPositions EMPTY = positionsRange(0, 0);

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
                return nullAllowed ? positions : BlockFilter.EMPTY;
            }
            return filter.testContains(block.getValue(), 0) ? positions : BlockFilter.EMPTY;
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
            return positionsList(outputPositions, 0, outputPositionsCount);
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
                return positionsRange(0, outputPositionsCount);
            }
            return positionsList(outputPositions, 0, outputPositionsCount);
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
            return positionsList(outputPositions, 0, outputPositionsCount);
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
                return positionsRange(0, outputPositionsCount);
            }
            return positionsList(outputPositions, 0, outputPositionsCount);
        }
    }
}
