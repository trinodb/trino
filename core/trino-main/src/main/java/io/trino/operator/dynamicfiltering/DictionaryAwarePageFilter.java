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

import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.operator.dynamicfiltering.DynamicPageFilter.BlockFilter.EMPTY;
import static io.trino.operator.project.SelectedPositions.positionsRange;
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
        SelectedPositions positions = positionsRange(0, page.getPositionCount());
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
                return new SelectedPositionsWithStats(EMPTY, blockFilterStats);
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
