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

import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
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
    private final Page probePage;
    @Nullable
    private final Block probeHashBlock;
    private final boolean probeMayHaveNull;
    private final LookupSource lookupSource;
    private int position = -1;

    private JoinProbe(int[] probeOutputChannels, Page page, Page probePage, LookupSource lookupSource, @Nullable Block probeHashBlock)
    {
        this.probeOutputChannels = requireNonNull(probeOutputChannels, "probeOutputChannels is null");
        this.page = requireNonNull(page, "page is null");
        this.probePage = requireNonNull(probePage, "probePage is null");
        this.lookupSource = requireNonNull(lookupSource, "lookupSource is null");
        this.probeHashBlock = requireNonNull(probeHashBlock, "probeHashBlock is null");
        this.probeMayHaveNull = probeMayHaveNull(probePage);
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
        if (probeMayHaveNull && currentRowContainsNull()) {
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

    private boolean currentRowContainsNull()
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
