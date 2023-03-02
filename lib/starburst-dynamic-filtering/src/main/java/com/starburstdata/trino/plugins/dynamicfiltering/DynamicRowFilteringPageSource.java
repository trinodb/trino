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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static com.starburstdata.trino.plugins.dynamicfiltering.DictionaryAwarePageFilter.BlockFilterStats;
import static com.starburstdata.trino.plugins.dynamicfiltering.DictionaryAwarePageFilter.SelectedPositionsWithStats;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class DynamicRowFilteringPageSource
        implements ConnectorPageSource
{
    public static final String FILTER_INPUT_POSITIONS = "filterInputPositions";
    public static final String FILTER_OUTPUT_POSITIONS = "filterOutputPositions";
    public static final String ROW_FILTERING_TIME_MILLIS = "rowFilteringTimeMillis";

    static final Page EMPTY_PAGE = new Page(0);

    private final ConnectorPageSource delegate;
    private final DynamicPageFilter dynamicPageFilter;
    private final long blockingTimeoutNanos;
    private final long startNanos;
    private final FilterProfiler profiler;

    private final DictionaryAwarePageFilter filter;

    private long rowFilteringTimeNanos;

    public DynamicRowFilteringPageSource(
            ConnectorPageSource delegate,
            double dynamicRowFilterSelectivityThreshold,
            Duration blockingTimeout,
            List<ColumnHandle> columns,
            DynamicPageFilter dynamicPageFilter)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.blockingTimeoutNanos = (long) requireNonNull(blockingTimeout, "blockingTimeout is null").getValue(NANOSECONDS);
        this.dynamicPageFilter = requireNonNull(dynamicPageFilter, "dynamicPageFilter is null");
        this.startNanos = System.nanoTime();
        this.profiler = new FilterProfiler(dynamicRowFilterSelectivityThreshold, columns.size());
        this.filter = new DictionaryAwarePageFilter(columns.size());
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        // Delegate ConnectorPageSource should implement getCompletedPositions
        // to ensure that physical input positions stats are accurate
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        Page dataPage = delegate.getNextPage();
        if (dataPage == null || dataPage.getPositionCount() == 0) {
            return dataPage;
        }
        Optional<DynamicPageFilter.BlockFilter[]> blockFilters = dynamicPageFilter.getBlockFilters();
        if (blockFilters.isEmpty()) {
            return dataPage;
        }
        if (blockFilters.get() == DynamicPageFilter.NONE_BLOCK_FILTER) {
            profiler.record(dataPage.getPositionCount(), 0, ImmutableList.of(), filter);
            return EMPTY_PAGE;
        }

        DynamicPageFilter.BlockFilter[] effectiveFilters = blockFilters.get();
        int length = 0;
        for (DynamicPageFilter.BlockFilter blockFilter : effectiveFilters) {
            if (profiler.isChannelFilterEffective(blockFilter.getChannelIndex())) {
                effectiveFilters[length++] = blockFilter;
            }
        }
        if (length == 0) {
            return dataPage;
        }

        long start = System.nanoTime();
        int inputPositions = dataPage.getPositionCount();
        SelectedPositionsWithStats positionsWithStats = filter.filterPage(dataPage, effectiveFilters, length);
        SelectedPositions selectedPositions = positionsWithStats.getPositions();
        int selectedPositionsCount = selectedPositions.size();
        profiler.record(inputPositions, selectedPositionsCount, positionsWithStats.getBlockFilterStats(), filter);

        Page resultPage = dataPage;
        if (selectedPositionsCount == 0) {
            resultPage = EMPTY_PAGE;
        }
        else if (selectedPositionsCount < inputPositions) {
            if (selectedPositions.isList()) {
                Block[] blocks = new Block[dataPage.getChannelCount()];
                for (int channel = 0; channel < blocks.length; channel++) {
                    Block block = dataPage.getBlock(channel);
                    if (block.isLoaded()) {
                        blocks[channel] = getOrCopyPositions(block, selectedPositions);
                    }
                    else {
                        blocks[channel] = new LazyBlock(selectedPositionsCount, () -> {
                            Block loadedBlock;
                            if (block instanceof LazyBlock) {
                                // load top-level block only
                                loadedBlock = ((LazyBlock) block).getBlock();
                            }
                            else {
                                loadedBlock = block;
                            }
                            return getOrCopyPositions(loadedBlock, selectedPositions);
                        });
                    }
                }
                resultPage = new Page(blocks);
            }
            else {
                resultPage = dataPage.getRegion(0, selectedPositions.size());
            }
        }
        rowFilteringTimeNanos += System.nanoTime() - start;
        return resultPage;
    }

    private Block getOrCopyPositions(Block block, SelectedPositions selectedPositions)
    {
        if (block instanceof DictionaryBlock) {
            return block.getPositions(selectedPositions.getPositions(), 0, selectedPositions.size());
        }
        else {
            // Copy positions for non-dictionary blocks. If we used Block#getPositions instead,
            // then non-dictionary blocks would be converted to (masking) dictionaries. It might
            // happen that non-dictionary input blocks are interleaved with dictionary input blocks
            // which share same dictionary. In such case, using Block#getPositions for non-dictionary
            // input blocks might confuse downstream dictionary-aware operators that keep track of
            // dictionaries which are shared across input blocks.
            // TODO: revise as part of https://starburstdata.atlassian.net/browse/SEP-6818
            return block.copyPositions(selectedPositions.getPositions(), 0, selectedPositions.size());
        }
    }

    @Override
    public Metrics getMetrics()
    {
        // Don't add custom metrics to scan operator if there hasn't been any filtering
        if (profiler.getFilterInputPositions() == 0) {
            return delegate.getMetrics();
        }
        return delegate.getMetrics().mergeWith(
                new Metrics(ImmutableMap.of(
                        FILTER_INPUT_POSITIONS, new LongCount(profiler.getFilterInputPositions()),
                        FILTER_OUTPUT_POSITIONS, new LongCount(profiler.getFilterOutputPositions()),
                        ROW_FILTERING_TIME_MILLIS, new LongCount(NANOSECONDS.toMillis(rowFilteringTimeNanos)))));
    }

    @Override
    public long getMemoryUsage()
    {
        return delegate.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        // Block until one of below conditions is met:
        // 1. Timeout after waiting for the configured time
        // 2. Creation of final DynamicPageFilter
        long timeLeft = blockingTimeoutNanos - (System.nanoTime() - startNanos);
        if (timeLeft > 0 && dynamicPageFilter.isAwaitable()) {
            return dynamicPageFilter.isBlocked()
                    .completeOnTimeout(null, timeLeft, NANOSECONDS)
                    .thenCompose(ignored -> delegate.isBlocked());
        }
        return delegate.isBlocked();
    }

    private static class FilterProfiler
    {
        private static final int MIN_SAMPLE_POSITIONS = 2047;

        private final double selectivityThreshold;
        private long filterInputPositions;
        private long filterOutputPositions;
        private final long[] channelInputPositions;
        private final long[] channelOutputPositions;
        private final IntOpenHashSet ineffectiveFilterChannels = new IntOpenHashSet();

        public FilterProfiler(double selectivityThreshold, int columnsCount)
        {
            this.selectivityThreshold = selectivityThreshold;
            this.channelInputPositions = new long[columnsCount];
            this.channelOutputPositions = new long[columnsCount];
        }

        void record(
                int inputPositions,
                int selectedPositions,
                List<BlockFilterStats> blockFiltersStats,
                DictionaryAwarePageFilter filter)
        {
            filterInputPositions += inputPositions;
            filterOutputPositions += selectedPositions;
            for (BlockFilterStats blockFilterStats : blockFiltersStats) {
                int channelIndex = blockFilterStats.getChannelIndex();
                channelOutputPositions[channelIndex] += blockFilterStats.getOutputPositions();
                channelInputPositions[channelIndex] += blockFilterStats.getInputPositions();

                if (channelInputPositions[channelIndex] < MIN_SAMPLE_POSITIONS) {
                    continue;
                }
                if (channelOutputPositions[channelIndex] > (selectivityThreshold * channelInputPositions[channelIndex])) {
                    ineffectiveFilterChannels.add(channelIndex);
                    filter.cleanUp(channelIndex);
                }
            }
        }

        public boolean isChannelFilterEffective(int channelIndex)
        {
            return !ineffectiveFilterChannels.contains(channelIndex);
        }

        public long getFilterInputPositions()
        {
            return filterInputPositions;
        }

        public long getFilterOutputPositions()
        {
            return filterOutputPositions;
        }
    }
}
