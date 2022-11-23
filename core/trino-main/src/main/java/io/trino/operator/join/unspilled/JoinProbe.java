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

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class JoinProbe
{
    public static class JoinProbeFactory
    {
        private final int[] probeOutputChannels;
        private final int[] probeJoinChannels;
        private final int probeHashChannel; // only valid when >= 0

        public JoinProbeFactory(List<Integer> probeOutputChannels, List<Integer> probeJoinChannels, OptionalInt probeHashChannel)
        {
            this.probeOutputChannels = Ints.toArray(requireNonNull(probeOutputChannels, "probeOutputChannels is null"));
            this.probeJoinChannels = Ints.toArray(requireNonNull(probeJoinChannels, "probeJoinChannels is null"));
            this.probeHashChannel = requireNonNull(probeHashChannel, "probeHashChannel is null").orElse(-1);
        }

        public JoinProbe createJoinProbe(Page page, LookupSource lookupSource)
        {
            Page probePage = page.getLoadedPage(probeJoinChannels);
            return new JoinProbe(probeOutputChannels, page, probePage, lookupSource, probeHashChannel >= 0 ? page.getBlock(probeHashChannel).getLoadedBlock() : null);
        }
    }

    private final int[] probeOutputChannels;
    private final Page page;
    private final long[] joinPositionCache;
    private int position = -1;

    private JoinProbe(int[] probeOutputChannels, Page page, Page probePage, LookupSource lookupSource, @Nullable Block probeHashBlock)
    {
        this.probeOutputChannels = requireNonNull(probeOutputChannels, "probeOutputChannels is null");
        this.page = requireNonNull(page, "page is null");

        joinPositionCache = fillCache(lookupSource, page, probeHashBlock, probePage);
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

    public Page getPage()
    {
        return page;
    }

    private static long[] fillCache(
            LookupSource lookupSource,
            Page page,
            Block probeHashBlock,
            Page probePage)
    {
        int positionCount = page.getPositionCount();
        List<Block> nullableBlocks = IntStream.range(0, probePage.getChannelCount())
                .mapToObj(i -> probePage.getBlock(i))
                .filter(Block::mayHaveNull)
                .collect(toImmutableList());

        long[] joinPositionCache = new long[positionCount];
        if (!nullableBlocks.isEmpty()) {
            Arrays.fill(joinPositionCache, -1);
            boolean[] isNull = new boolean[positionCount];
            int nonNullCount = getIsNull(nullableBlocks, positionCount, isNull);
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

    private static int getIsNull(List<Block> nullableBlocks, int positionCount, boolean[] isNull)
    {
        for (int i = 0; i < nullableBlocks.size() - 1; i++) {
            Block block = nullableBlocks.get(i);
            for (int position = 0; position < positionCount; position++) {
                isNull[position] |= block.isNull(position);
            }
        }
        // Last block will also calculate `nonNullCount`
        int nonNullCount = 0;
        Block lastBlock = nullableBlocks.get(nullableBlocks.size() - 1);
        for (int position = 0; position < positionCount; position++) {
            isNull[position] |= lastBlock.isNull(position);
            nonNullCount += isNull[position] ? 0 : 1;
        }

        return nonNullCount;
    }
}
