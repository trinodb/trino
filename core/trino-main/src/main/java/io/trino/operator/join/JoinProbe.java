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

import java.util.List;

import static com.google.common.base.Verify.verify;

public class JoinProbe
{
    public static class JoinProbeFactory
    {
        private final int[] probeOutputChannels;
        private final int[] probeJoinChannels;

        public JoinProbeFactory(int[] probeOutputChannels, List<Integer> probeJoinChannels)
        {
            this.probeOutputChannels = probeOutputChannels;
            this.probeJoinChannels = Ints.toArray(probeJoinChannels);
        }

        public JoinProbe createJoinProbe(Page page)
        {
            Page probePage = page.getColumns(probeJoinChannels);
            return new JoinProbe(probeOutputChannels, page, probePage);
        }
    }

    private final int[] probeOutputChannels;
    private final int positionCount;
    private final Page page;
    private final Page probePage;
    private final boolean probeMayHaveNull;
    private int position = -1;

    private JoinProbe(int[] probeOutputChannels, Page page, Page probePage)
    {
        this.probeOutputChannels = probeOutputChannels;
        this.positionCount = page.getPositionCount();
        this.page = page;
        this.probePage = probePage;
        this.probeMayHaveNull = probeMayHaveNull(probePage);
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

    public long getCurrentJoinPosition(LookupSource lookupSource)
    {
        if (probeMayHaveNull && currentRowContainsNull()) {
            return -1;
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
