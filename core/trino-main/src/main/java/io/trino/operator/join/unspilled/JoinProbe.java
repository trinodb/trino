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

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class JoinProbe
{
    private static final int RLE_MASK = 0;
    private static final int NON_RLE_MASK = -1;

    public static class JoinProbeFactory
    {
        private final int[] probeOutputChannels;
        private final int[] probeJoinChannels;
        private final int probeHashChannel; // only valid when >= 0
        private final boolean hasFilter;

        public JoinProbeFactory(int[] probeOutputChannels, List<Integer> probeJoinChannels, OptionalInt probeHashChannel, boolean hasFilter)
        {
            this.probeOutputChannels = probeOutputChannels;
            this.probeJoinChannels = Ints.toArray(probeJoinChannels);
            this.probeHashChannel = probeHashChannel.orElse(-1);
            this.hasFilter = hasFilter;
        }

        public JoinProbe createJoinProbe(Page page, LookupSource lookupSource)
        {
            Page probePage = page.getLoadedPage(probeJoinChannels);
            return new JoinProbe(probeOutputChannels, page, probePage, lookupSource, probeHashChannel >= 0 ? page.getBlock(probeHashChannel).getLoadedBlock() : null, hasFilter);
        }
    }

    private final int[] probeOutputChannels;
    private final int positionCount;
    private final Page page;
    private final Page probePage;
    @Nullable
    private final Block probeHashBlock;
    private final LookupSource lookupSource;
    private final long[] joinPositionCache;
    private final boolean isRle;
    /**
     * This value is 0xFFFFFFFF for non-rle and 0x00000000 for rle. This way if we access cache by:
     * `cache[position & rleMask]` we will always get the first position for RLE and the proper position for non-rle.
     * This way there is no branch (if) in the code
     */
    private final int rleMask;
    private int position = -1;

    private JoinProbe(int[] probeOutputChannels, Page page, Page probePage, LookupSource lookupSource, @Nullable Block probeHashBlock, boolean hasFilter)
    {
        this.probeOutputChannels = requireNonNull(probeOutputChannels, "probeOutputChannels is null");
        this.page = requireNonNull(page, "page is null");
        this.positionCount = page.getPositionCount();
        this.probePage = requireNonNull(probePage, "probePage is null");
        this.lookupSource = requireNonNull(lookupSource, "lookupSource is null");
        this.probeHashBlock = probeHashBlock;

        isRle = hasOnlyRleBlocks(probePage);
        rleMask = isRle ? RLE_MASK : NON_RLE_MASK;
        joinPositionCache = fillCache();
    }

    public int[] getOutputChannels()
    {
        return probeOutputChannels;
    }

    public boolean advanceNextPosition()
    {
        verify(++position <= positionCount, "already finished");
        return !isFinished();
    }

    public boolean isFinished()
    {
        return position == positionCount;
    }

    public long getCurrentJoinPosition()
    {
        return joinPositionCache[position & rleMask];
    }

    public int getPosition()
    {
        return position;
    }

    public Page getPage()
    {
        return page;
    }

    public boolean isRle()
    {
        return isRle;
    }

    private long[] fillCache()
    {
        if (isRle) {
            long[] joinPositionCache = new long[1];
            if (rowContainsNull(0)) {
                joinPositionCache[0] = -1;
            }
            else {
                joinPositionCache[0] = lookupSource.getJoinPosition(0, probePage, page);
            }

            return joinPositionCache;
        }

        long[] joinPositionCache = new long[positionCount];
        Arrays.fill(joinPositionCache, -1);
        if (probeMayHaveNull(probePage)) {
            int nonNullCount = 0;
            boolean[] isNull = new boolean[positionCount];
            for (int i = 0; i < positionCount; i++) {
                isNull[i] = rowContainsNull(i);
                nonNullCount += isNull[i] ? 0 : 1;
            }
            if (nonNullCount < positionCount) {
                // We only store positions that are not null
                int[] positions = new int[nonNullCount];
                nonNullCount = 0;
                for (int i = 0; i < positionCount; i++) {
                    if (!isNull[i]) {
                        positions[nonNullCount++] = i;
                    }
                }
                long[] packedPositionCache;
                if (probeHashBlock != null) {
                    long[] hashes = new long[nonNullCount];
                    for (int i = 0; i < nonNullCount; i++) {
                        hashes[i] = BIGINT.getLong(probeHashBlock, positions[i]);
                    }
                    packedPositionCache = lookupSource.getJoinPosition(positions, probePage, page, hashes);
                }
                else {
                    packedPositionCache = lookupSource.getJoinPosition(positions, probePage, page);
                }
                // Unpack
                nonNullCount = 0;
                for (int i = 0; i < positionCount; i++) {
                    if (!isNull[i]) {
                        joinPositionCache[i] = packedPositionCache[nonNullCount++];
                    }
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
            return lookupSource.getJoinPosition(positions, probePage, page, hashes);
        }
        else {
            return lookupSource.getJoinPosition(positions, probePage, page);
        }
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
