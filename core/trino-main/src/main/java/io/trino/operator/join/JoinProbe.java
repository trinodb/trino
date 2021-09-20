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
package io.trino.operator.join;

import com.google.common.primitives.Ints;
import io.trino.spi.Page;
import io.trino.spi.block.Block;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.min;

public class JoinProbe
{
    public static class JoinProbeFactory
    {
        private final int[] probeOutputChannels;
        private final int[] probeJoinChannels;
        private final int probeHashChannel; // only valid when >= 0
        private final boolean vectorizedJoinProbeEnabled;

        public JoinProbeFactory(int[] probeOutputChannels, List<Integer> probeJoinChannels, OptionalInt probeHashChannel, boolean vectorizedJoinProbeEnabled)
        {
            this.probeOutputChannels = probeOutputChannels;
            this.probeJoinChannels = Ints.toArray(probeJoinChannels);
            this.probeHashChannel = probeHashChannel.orElse(-1);
            this.vectorizedJoinProbeEnabled = vectorizedJoinProbeEnabled;
        }

        public JoinProbe createJoinProbe(Page page)
        {
            Page probePage = page.getLoadedPage(probeJoinChannels);
            return new JoinProbe(probeOutputChannels, page, probePage, probeHashChannel >= 0 ? page.getBlock(probeHashChannel).getLoadedBlock() : null, vectorizedJoinProbeEnabled);
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
    private final Page page;
    private final Page probePage;
    @Nullable
    private final Block probeHashBlock;
    private final boolean probeMayHaveNull;
    private final boolean vectorizedJoinProbeEnabled;
    private int position = -1;
    private long[] joinPositionsCache;
    private boolean reloadCache = true;

    private JoinProbe(int[] probeOutputChannels, Page page, Page probePage, @Nullable Block probeHashBlock, boolean vectorizedJoinProbeEnabled)
    {
        this.probeOutputChannels = probeOutputChannels;
        this.positionCount = page.getPositionCount();
        this.page = page;
        this.probePage = probePage;
        this.probeHashBlock = probeHashBlock;
        this.probeMayHaveNull = probeMayHaveNull(probePage);
        this.vectorizedJoinProbeEnabled = vectorizedJoinProbeEnabled && probePage.getPositionCount() >= 64;
        joinPositionsCache = new long[JOIN_POSITIONS_CACHE_SIZE];
    }

    public int[] getOutputChannels()
    {
        return probeOutputChannels;
    }

    public boolean advanceNextPosition()
    {
        verify(++position <= positionCount, "already finished");
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
        if (vectorizedJoinProbeEnabled && lookupSource.supportsCaching()) {
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
        // each probe position is processed sequentially, therefore new batch starts every JOIN_POSITIONS_CACHE_SIZE positions
        verify((position & JOIN_POSITIONS_CACHE_MASK) == 0);
        int limit = min(JOIN_POSITIONS_CACHE_SIZE, positionCount - position);
        int position = this.position;
        if (probeMayHaveNull) {
            int[] positions = nonNullPositions(position, limit);
            long[] result;
            if (probeHashBlock != null) {
                long[] rawHashes = new long[positions.length];
                for (int i = 0; i < positions.length; ++i) {
                    rawHashes[i] = BIGINT.getLong(probeHashBlock, positions[i]);
                }
                result = lookupSource.getJoinPositions(positions, probePage, page, rawHashes);
            }
            else {
                result = lookupSource.getJoinPositions(positions, probePage, page);
            }

            Arrays.fill(joinPositionsCache, -1);
            for (int i = 0; i < positions.length; i++) {
                joinPositionsCache[positions[i] - position] = result[i];
            }
        }
        else {
            if (probeHashBlock != null) {
                long[] rawHashes = new long[limit];
                for (int i = 0; i < limit; ++i) {
                    rawHashes[i] = BIGINT.getLong(probeHashBlock, position + i);
                }
                //joinPositionsCache = lookupSource.getJoinPositions(SelectedPositions.positionsRange(position, limit), probePage, page, rawHashes);
                joinPositionsCache = lookupSource.getJoinPositions(consecutivePositions(position, limit), probePage, page, rawHashes);
            }
            else {
                joinPositionsCache = lookupSource.getJoinPositions(consecutivePositions(position, limit), probePage, page);
            }
        }
    }

    private int[] nonNullPositions(int firstPosition, int limit)
    {
        // Loop split into two for performance reasons
        int nullPositions = 0;
        for (int i = 0; i < limit; ++i) {
            if (rowContainsNull(firstPosition + i)) {
                nullPositions++;
            }
        }
        int[] positions = new int[limit - nullPositions];
        int count = 0;
        for (int i = 0; i < limit; ++i) {
            if (!rowContainsNull(firstPosition + i)) {
                positions[count++] = firstPosition + i;
            }
        }
        return positions;
    }

    private int[] consecutivePositions(int firstPosition, int limit)
    {
        int[] result = new int[limit];
        for (int i = 0; i < limit; i++) {
            result[i] = firstPosition + i;
        }
        return result;
    }

    private long getJoinPosition(int position, LookupSource lookupSource)
    {
        if (probeMayHaveNull && rowContainsNull(position)) {
            return -1;
        }
        if (probeHashBlock != null) {
            long rawHash = BIGINT.getLong(probeHashBlock, position);
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
        for (int i = 0; i < probePage.getChannelCount(); i++) {
            if (probePage.getBlock(i).isNull(position)) {
                return true;
            }
        }
        return false;
    }

    private static boolean probeMayHaveNull(Page probePage)
    {
        for (int i = 0; i < probePage.getChannelCount(); i++) {
            if (probePage.getBlock(i).mayHaveNull()) {
                return true;
            }
        }
        return false;
    }
}
