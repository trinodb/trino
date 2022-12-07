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

public interface PagesHash
{
    int getPositionCount();

    long getInMemorySizeInBytes();

    int getAddressIndex(int position, Page hashChannelsPage);

    int getAddressIndex(int rightPosition, Page hashChannelsPage, long rawHash);

    default int[] getAddressIndex(int[] positions, Page hashChannelsPage)
    {
        int[] result = new int[positions.length];
        for (int i = 0; i < positions.length; i++) {
            result[i] = getAddressIndex(positions[i], hashChannelsPage);
        }
        return result;
    }

    default int[] getAddressIndex(int[] positions, Page hashChannelsPage, long[] rawHashes)
    {
        int[] result = new int[positions.length];
        for (int i = 0; i < positions.length; i++) {
            result[i] = getAddressIndex(positions[i], hashChannelsPage, rawHashes[positions[i]]);
        }
        return result;
    }

    void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset);

    static int getHashPosition(long rawHash, long mask)
    {
        // Avalanches the bits of a long integer by applying the finalisation step of MurmurHash3.
        //
        // This function implements the finalisation step of Austin Appleby's <a href="http://sites.google.com/site/murmurhash/">MurmurHash3</a>.
        // Its purpose is to avalanche the bits of the argument to within 0.25% bias. It is used, among other things, to scramble quickly (but deeply) the hash
        // values returned by {@link Object#hashCode()}.
        //

        rawHash ^= rawHash >>> 33;
        rawHash *= 0xff51afd7ed558ccdL;
        rawHash ^= rawHash >>> 33;
        rawHash *= 0xc4ceb9fe1a85ec53L;
        rawHash ^= rawHash >>> 33;

        return (int) (rawHash & mask);
    }
}
