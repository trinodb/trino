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
package io.trino.operator.join.unspilled;

import com.google.common.primitives.Ints;
import io.trino.operator.join.LookupSource;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

/**
 * This class eagerly calculates all join positions and stores them in an array
 * PageJoiner is responsible for ensuring that only the first position is processed for RLE with no or single build row match
 */
public class JoinProbe
{
    public static class JoinProbeFactory
    {
        private final int[] probeOutputChannels;
        private final int[] probeJoinChannels;
        private final int probeHashChannel; // only valid when >= 0
        private final boolean hasFilter;

        public JoinProbeFactory(List<Integer> probeOutputChannels, List<Integer> probeJoinChannels, OptionalInt probeHashChannel, boolean hasFilter)
        {
            this.probeOutputChannels = Ints.toArray(requireNonNull(probeOutputChannels, "probeOutputChannels is null"));
            this.probeJoinChannels = Ints.toArray(requireNonNull(probeJoinChannels, "probeJoinChannels is null"));
            this.probeHashChannel = requireNonNull(probeHashChannel, "probeHashChannel is null").orElse(-1);
            this.hasFilter = hasFilter;
        }

        public JoinProbe createJoinProbe(Page page, LookupSource lookupSource)
        {
            Page probePage = page.getLoadedPage(probeJoinChannels);
            return new JoinProbe(probeOutputChannels, page, probePage, lookupSource, probeHashChannel >= 0 ? page.getBlock(probeHashChannel).getLoadedBlock() : null, hasFilter);
        }
    }

    private final int[] probeOutputChannels;
    private final Page page;
    private final long[] joinPositionCache;
    private final boolean isRle;
    private int position = -1;

    private JoinProbe(int[] probeOutputChannels, Page page, Page probePage, LookupSource lookupSource, @Nullable Block probeHashBlock, boolean hasFilter)
    {
        this.probeOutputChannels = requireNonNull(probeOutputChannels, "probeOutputChannels is null");
        this.page = requireNonNull(page, "page is null");

        // if filter channels are not RLE encoded, then every probe
        // row might be unique and must be matched independently
        this.isRle = !hasFilter && hasOnlyRleBlocks(probePage);
        joinPositionCache = fillCache(lookupSource, page, probeHashBlock, probePage, isRle);
    }

    public int[] getOutputChannels()
    {
        return probeOutputChannels;
    }

    public boolean advanceNextPosition()
    {
        verify(++position <= page.getPositionCount(), "already finished");
        return !isFinished();
    }

    public void finish()
    {
        position = page.getPositionCount();
    }

    public boolean isFinished()
    {
        return position == page.getPositionCount();
    }

    public long getCurrentJoinPosition()
    {
        return joinPositionCache[position];
    }

    public int getPosition()
    {
        return position;
    }

    public boolean areProbeJoinChannelsRunLengthEncoded()
    {
        return isRle;
    }

    public Page getPage()
    {
        return page;
    }

    private static long[] fillCache(
            LookupSource lookupSource,
            Page page,
            Block probeHashBlock,
            Page probePage,
            boolean isRle)
    {
        int positionCount = page.getPositionCount();

        Block[] nullableBlocks = new Block[probePage.getChannelCount()];
        int nullableBlocksCount = 0;
        for (int channel = 0; channel < probePage.getChannelCount(); channel++) {
            Block probeBlock = probePage.getBlock(channel);
            if (probeBlock.mayHaveNull()) {
                nullableBlocks[nullableBlocksCount++] = probeBlock;
            }
        }

        if (isRle) {
            long[] joinPositionCache;
            // Null values cannot be joined, so if any column contains null, there is no match
            boolean anyAllNullsBlock = false;
            for (int i = 0; i < nullableBlocksCount; i++) {
                Block nullableBlock = nullableBlocks[i];
                if (nullableBlock.isNull(0)) {
                    anyAllNullsBlock = true;
                    break;
                }
            }
            if (anyAllNullsBlock) {
                joinPositionCache = new long[1];
                joinPositionCache[0] = -1;
            }
            else {
                joinPositionCache = new long[positionCount];
                // We can fall back to processing all positions in case there are multiple build rows matched for the first probe position
                Arrays.fill(joinPositionCache, lookupSource.getJoinPosition(0, probePage, page));
            }

            return joinPositionCache;
        }

        long[] joinPositionCache = new long[positionCount];
        if (nullableBlocksCount > 0) {
            Arrays.fill(joinPositionCache, -1);
            boolean[] isNull = new boolean[positionCount];
            int nonNullCount = getIsNull(nullableBlocks, nullableBlocksCount, positionCount, isNull);
            if (nonNullCount < positionCount) {
                // We only store positions that are not null
                int[] positions = new int[nonNullCount];
                nonNullCount = 0;
                for (int i = 0; i < positionCount; i++) {
                    if (!isNull[i]) {
                        positions[nonNullCount] = i;
                    }
                    // This way less code is in the if branch and CPU should be able to optimize branch prediction better
                    nonNullCount += isNull[i] ? 0 : 1;
                }
                if (probeHashBlock != null) {
                    long[] hashes = new long[positionCount];
                    for (int i = 0; i < positionCount; i++) {
                        hashes[i] = BIGINT.getLong(probeHashBlock, i);
                    }
                    lookupSource.getJoinPosition(positions, probePage, page, hashes, joinPositionCache);
                }
                else {
                    lookupSource.getJoinPosition(positions, probePage, page, joinPositionCache);
                }
                return joinPositionCache;
            } // else fall back to non-null path
        }
        int[] positions = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            positions[i] = i;
        }
        if (probeHashBlock != null) {
            long[] hashes = new long[positionCount];
            for (int i = 0; i < positionCount; i++) {
                hashes[i] = BIGINT.getLong(probeHashBlock, i);
            }
            lookupSource.getJoinPosition(positions, probePage, page, hashes, joinPositionCache);
        }
        else {
            lookupSource.getJoinPosition(positions, probePage, page, joinPositionCache);
        }

        return joinPositionCache;
    }

    private static int getIsNull(Block[] nullableBlocks, int nullableBlocksCount, int positionCount, boolean[] isNull)
    {
        for (int i = 0; i < nullableBlocksCount - 1; i++) {
            Block block = nullableBlocks[i];
            for (int position = 0; position < positionCount; position++) {
                isNull[position] |= block.isNull(position);
            }
        }
        // Last block will also calculate `nonNullCount`
        int nonNullCount = 0;
        Block lastBlock = nullableBlocks[nullableBlocksCount - 1];
        for (int position = 0; position < positionCount; position++) {
            isNull[position] |= lastBlock.isNull(position);
            nonNullCount += isNull[position] ? 0 : 1;
        }

        return nonNullCount;
    }

    private static boolean hasOnlyRleBlocks(Page probePage)
    {
        if (probePage.getChannelCount() == 0) {
            return false;
        }

        for (int i = 0; i < probePage.getChannelCount(); i++) {
            if (!(probePage.getBlock(i) instanceof RunLengthEncodedBlock)) {
                return false;
            }
        }
        return true;
    }
}
