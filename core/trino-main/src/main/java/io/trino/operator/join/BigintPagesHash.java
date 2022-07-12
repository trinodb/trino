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

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.operator.HashArraySizeSupplier;
import io.trino.operator.PagesHashStrategy;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;
import static io.trino.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// This implementation assumes:
// -There is only one join channel and it is of type bigint
// -arrays used in the hash are always a power of 2.
public final class BigintPagesHash
        implements PagesHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BigintPagesHash.class).instanceSize();
    private static final DataSize CACHE_SIZE = DataSize.of(128, KILOBYTE);
    private final LongArrayList addresses;
    private final List<Block> joinChannelBlocks;
    private final PagesHashStrategy pagesHashStrategy;

    private final int mask;
    private final int[] key;
    private final long[] values;
    private final long size;

    private final long hashCollisions;
    private final double expectedHashCollisions;

    public BigintPagesHash(
            LongArrayList addresses,
            PagesHashStrategy pagesHashStrategy,
            PositionLinks.FactoryBuilder positionLinks,
            HashArraySizeSupplier hashArraySizeSupplier,
            List<Page> pages,
            int joinChannel)
    {
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.pagesHashStrategy = requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");
        requireNonNull(pages, "pages is null");
        ImmutableList.Builder<Block> joinChannelBlocksBuilder = ImmutableList.builder();
        for (Page page : pages) {
            joinChannelBlocksBuilder.add(page.getBlock(joinChannel));
        }
        joinChannelBlocks = joinChannelBlocksBuilder.build();

        // reserve memory for the arrays
        int hashSize = hashArraySizeSupplier.getHashArraySize(addresses.size());

        mask = hashSize - 1;
        key = new int[hashSize];
        values = new long[hashSize];
        Arrays.fill(key, -1);

        // We will process addresses in batches, to save memory on array of hashes.
        int positionsInStep = Math.min(addresses.size() + 1, (int) CACHE_SIZE.toBytes() / Integer.SIZE);
        long hashCollisionsLocal = 0;

        for (int step = 0; step * positionsInStep <= addresses.size(); step++) {
            int stepBeginPosition = step * positionsInStep;
            int stepEndPosition = Math.min((step + 1) * positionsInStep, addresses.size());
            int stepSize = stepEndPosition - stepBeginPosition;

            // index pages
            for (int position = 0; position < stepSize; position++) {
                int realPosition = position + stepBeginPosition;
                if (isPositionNull(realPosition)) {
                    continue;
                }

                long address = addresses.getLong(realPosition);
                int blockIndex = decodeSliceIndex(address);
                int blockPosition = decodePosition(address);
                long value = joinChannelBlocks.get(blockIndex).getLong(blockPosition, 0);

                int pos = getHashPosition(value, mask);

                // look for an empty slot or a slot containing this key
                while (key[pos] != -1) {
                    int currentKey = key[pos];
                    long currentValue = values[pos];
                    if (value == currentValue) {
                        // found a slot for this key
                        // link the new key position to the current key position
                        realPosition = positionLinks.link(realPosition, currentKey);

                        // key[pos] updated outside of this loop
                        break;
                    }
                    // increment position and mask to handler wrap around
                    pos = (pos + 1) & mask;
                    hashCollisionsLocal++;
                }

                key[pos] = realPosition;
                values[pos] = value;
            }
        }

        size = sizeOf(addresses.elements()) + pagesHashStrategy.getSizeInBytes() +
                sizeOf(key) + sizeOf(values);
        hashCollisions = hashCollisionsLocal;
        expectedHashCollisions = estimateNumberOfHashCollisions(addresses.size(), hashSize);
    }

    @Override
    public int getPositionCount()
    {
        return addresses.size();
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return INSTANCE_SIZE + size;
    }

    @Override
    public long getHashCollisions()
    {
        return hashCollisions;
    }

    @Override
    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions;
    }

    @Override
    public int getAddressIndex(int position, Page hashChannelsPage, long rawHash)
    {
        return getAddressIndex(position, hashChannelsPage);
    }

    @Override
    public int getAddressIndex(int position, Page hashChannelsPage)
    {
        long value = hashChannelsPage.getBlock(0).getLong(position, 0);
        int pos = getHashPosition(value, mask);

        while (key[pos] != -1) {
            if (values[pos] == value) {
                return key[pos];
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }
        return -1;
    }

    @Override
    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long pageAddress = addresses.getLong(toIntExact(position));
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        pagesHashStrategy.appendTo(blockIndex, blockPosition, pageBuilder, outputChannelOffset);
    }

    private boolean isPositionNull(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return joinChannelBlocks.get(blockIndex).isNull(blockPosition);
    }

    private static int getHashPosition(long rawHash, long mask)
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
