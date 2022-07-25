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

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;

@NotThreadSafe
public interface LookupSource
        extends Closeable
{
    long getInMemorySizeInBytes();

    long getJoinPositionCount();

    long joinPositionWithinPartition(long joinPosition);

    long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash);

    default long[] getJoinPosition(int[] positions, Page hashChannelsPage, Page allChannelsPage, long[] rawHashes)
    {
        long[] result = new long[positions.length];
        for (int i = 0; i < positions.length; i++) {
            result[i] = getJoinPosition(positions[i], hashChannelsPage, allChannelsPage, rawHashes[i]);
        }

        return result;
    }

    long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage);

    default long[] getJoinPosition(int[] positions, Page hashChannelsPage, Page allChannelsPage)
    {
        long[] result = new long[positions.length];
        for (int i = 0; i < positions.length; i++) {
            result[i] = getJoinPosition(positions[i], hashChannelsPage, allChannelsPage);
        }

        return result;
    }

    long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage);

    void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset);

    boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage);

    boolean isEmpty();

    @Override
    void close();

    /**
     * @return true if there is a certainty that every position from the probe side is joined
     * with at most a single position on the build side.
     * This is true for queries where joins are carried out on the indexed/unique column.
     */
    default boolean isMappingUnique()
    {
        return false;
    }

    /**
     * @return true if `isJoinPositionEligible` always returns true, regardless of the input arguments
     */
    default boolean isJoinPositionAlwaysEligible()
    {
        return false;
    }
}
