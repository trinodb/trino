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

import io.trino.spi.Page;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DictionaryAwarePageFilter
{
    // BlockFilter is a shared structure which cannot contain mutable state.
    // Therefore, we use this class per split to save results from previously processed dictionary
    private final DictionaryAwareBlockFilter[] dictionaryAwareBlockFilters;

    public DictionaryAwarePageFilter(int columnsCount)
    {
        this.dictionaryAwareBlockFilters = new DictionaryAwareBlockFilter[columnsCount];
    }

    public SelectedPositionsWithStats filterPage(Page page, DynamicPageFilter.BlockFilter[] blockFilters, int length)
    {
        SelectedPositions positions = SelectedPositions.positionsRange(page.getPositionCount());
        List<BlockFilterStats> blockFilterStats = new ArrayList<>(length);
        for (int index = 0; index < length; index++) {
            DynamicPageFilter.BlockFilter blockFilter = blockFilters[index];
            int channelIndex = blockFilter.getChannelIndex();
            DictionaryAwareBlockFilter dictionaryAwareBlockFilter = dictionaryAwareBlockFilters[channelIndex];
            if (dictionaryAwareBlockFilter == null) {
                dictionaryAwareBlockFilter = new DictionaryAwareBlockFilter();
                dictionaryAwareBlockFilters[channelIndex] = dictionaryAwareBlockFilter;
            }

            int inputPositions = positions.size();
            positions = dictionaryAwareBlockFilter.filter(page.getBlock(channelIndex), blockFilter, positions);
            blockFilterStats.add(new BlockFilterStats(
                    inputPositions,
                    positions.size(),
                    blockFilter.getChannelIndex()));

            if (positions.isEmpty()) {
                return new SelectedPositionsWithStats(SelectedPositions.EMPTY, blockFilterStats);
            }
        }
        return new SelectedPositionsWithStats(positions, blockFilterStats);
    }

    public void cleanUp(int channelIndex)
    {
        dictionaryAwareBlockFilters[channelIndex].cleanUp();
    }

    static class SelectedPositionsWithStats
    {
        private final SelectedPositions positions;
        private final List<BlockFilterStats> blockFilterStats;

        private SelectedPositionsWithStats(SelectedPositions positions, List<BlockFilterStats> blockFilterStats)
        {
            this.positions = requireNonNull(positions, "positions is null");
            this.blockFilterStats = requireNonNull(blockFilterStats, "blockFilterStats is null");
        }

        SelectedPositions getPositions()
        {
            return positions;
        }

        List<BlockFilterStats> getBlockFilterStats()
        {
            return blockFilterStats;
        }
    }

    static class BlockFilterStats
    {
        private final int inputPositions;
        private final int outputPositions;
        private final int channelIndex;

        BlockFilterStats(int inputPositions, int outputPositions, int channelIndex)
        {
            this.inputPositions = inputPositions;
            this.outputPositions = outputPositions;
            this.channelIndex = channelIndex;
        }

        int getInputPositions()
        {
            return inputPositions;
        }

        int getOutputPositions()
        {
            return outputPositions;
        }

        int getChannelIndex()
        {
            return channelIndex;
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
            BlockFilterStats that = (BlockFilterStats) o;
            return inputPositions == that.inputPositions
                    && outputPositions == that.outputPositions
                    && channelIndex == that.channelIndex;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(inputPositions, outputPositions, channelIndex);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("inputPositions", inputPositions)
                    .add("outputPositions", outputPositions)
                    .add("channelIndex", channelIndex)
                    .toString();
        }
    }
}
