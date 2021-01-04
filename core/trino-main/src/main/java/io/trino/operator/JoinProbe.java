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

import io.trino.spi.Page;
import io.trino.spi.block.Block;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.min;
import static java.util.Arrays.stream;

public class JoinProbe
{
    public static class JoinProbeFactory
    {
        private final int[] probeOutputChannels;
        private final List<Integer> probeJoinChannels;
        private final OptionalInt probeHashChannel;

        public JoinProbeFactory(int[] probeOutputChannels, List<Integer> probeJoinChannels, OptionalInt probeHashChannel)
        {
            this.probeOutputChannels = probeOutputChannels;
            this.probeJoinChannels = probeJoinChannels;
            this.probeHashChannel = probeHashChannel;
        }

        public JoinProbe createJoinProbe(Page page)
        {
            return new JoinProbe(probeOutputChannels, page, probeJoinChannels, probeHashChannel);
        }
    }

    /**
     * Cache size will be 2^JOIN_POSITIONS_CACHE_SIZE_EXP
     */
    private static final int JOIN_POSITIONS_CACHE_SIZE_EXP = 11;
    private static final int JOIN_POSITIONS_CACHE_SIZE = 1 << JOIN_POSITIONS_CACHE_SIZE_EXP;
    private static final int JOIN_POSITIONS_CACHE_MASK = JOIN_POSITIONS_CACHE_SIZE - 1;

    private final int[] probeOutputChannels;
    private final int positionCount;
    private final Block[] nullableProbeBlocks;
    private final Page page;
    private final Page probePage;
    private final Optional<Block> probeHashBlock;
    private final long[] joinPositionsCache;
    private boolean reloadCache = true;

    private int position = -1;

    private JoinProbe(int[] probeOutputChannels, Page page, List<Integer> probeJoinChannels, OptionalInt probeHashChannel)
    {
        this.probeOutputChannels = probeOutputChannels;
        this.positionCount = page.getPositionCount();
        Block[] probeBlocks = new Block[probeJoinChannels.size()];

        for (int i = 0; i < probeJoinChannels.size(); i++) {
            probeBlocks[i] = page.getBlock(probeJoinChannels.get(i));
        }
        nullableProbeBlocks = stream(probeBlocks).filter(Block::mayHaveNull).toArray(Block[]::new);
        this.page = page;
        this.probePage = new Page(page.getPositionCount(), probeBlocks);
        this.probeHashBlock = probeHashChannel.isPresent() ? Optional.of(page.getBlock(probeHashChannel.getAsInt())) : Optional.empty();
        joinPositionsCache = new long[JOIN_POSITIONS_CACHE_SIZE];
    }

    public int[] getOutputChannels()
    {
        return probeOutputChannels;
    }

    public boolean advanceNextPosition()
    {
        verify(position < positionCount, "already finished");
        position++;
        if ((position & JOIN_POSITIONS_CACHE_MASK) == 0) {
            reloadCache = true;
        }
        return !isFinished();
    }

    public boolean isFinished()
    {
        return position == positionCount;
    }

    public long getCurrentJoinPosition(LookupSource lookupSource)
    {
        if (lookupSource.supportsCaching()) {
            if (reloadCache) {
                fillJoinPositionCache(lookupSource);
                reloadCache = false;
            }

            return joinPositionsCache[position & JOIN_POSITIONS_CACHE_MASK];
        }
        return getJoinPosition(position, lookupSource);
    }

    private void fillJoinPositionCache(LookupSource lookupSource)
    {
        // Extracted to local variables for performance reasons
        int firstPosition = this.position & (~JOIN_POSITIONS_CACHE_MASK);
        int limit = min(JOIN_POSITIONS_CACHE_SIZE, positionCount - firstPosition);
        for (int i = 0; i < limit; ++i) {
            joinPositionsCache[i] = getJoinPosition(firstPosition + i, lookupSource);
        }
    }

    private long getJoinPosition(int position, LookupSource lookupSource)
    {
        if (rowContainsNull(position)) {
            return -1;
        }
        if (probeHashBlock.isPresent()) {
            long rawHash = BIGINT.getLong(probeHashBlock.get(), position);
            return lookupSource.getJoinPosition(position, probePage, page, rawHash);
        }
        return lookupSource.getJoinPosition(position, probePage, page);
    }

    public int getPosition()
    {
        return position;
    }

    public Page getPage()
    {
        return page;
    }

    private boolean rowContainsNull(int position)
    {
        for (Block probeBlock : nullableProbeBlocks) {
            if (probeBlock.isNull(position)) {
                return true;
            }
        }
        return false;
    }
}
