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
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.airlift.slice.SizeOf.sizeOfLongArray;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;
import static io.trino.operator.join.PagesHash.getHashPosition;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * This implementation assumes:
 * -There is only one join channel and it is of type bigint
 * -arrays used in the hash are always a power of 2.
 */
public final class BigintPagesHash
        implements PagesHash
{
    private static final int INSTANCE_SIZE = instanceSize(BigintPagesHash.class);
    private static final DataSize CACHE_SIZE = DataSize.of(128, KILOBYTE);

    private final LongArrayList addresses;
    private final List<Block> joinChannelBlocks;
    private final PagesHashStrategy pagesHashStrategy;

    private final int mask;
    private final int[] keys;
    private final long[] values;
    private final long size;

    public static long getEstimatedRetainedSizeInBytes(
            int positionCount,
            HashArraySizeSupplier hashArraySizeSupplier,
            LongArrayList addresses,
            List<ObjectArrayList<Block>> channels,
            long blocksSizeInBytes)
    {
        return sizeOf(addresses.elements()) +
                (channels.size() > 0 ? sizeOf(channels.get(0).elements()) * channels.size() : 0) +
                blocksSizeInBytes +
                sizeOfIntArray(hashArraySizeSupplier.getHashArraySize(positionCount)) +
                sizeOfLongArray(positionCount);
    }

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
        keys = new int[hashSize];
        values = new long[addresses.size()];
        Arrays.fill(keys, -1);

        // We will process addresses in batches, to improve spatial and temporal memory locality
        int positionsInStep = Math.min(addresses.size() + 1, (int) CACHE_SIZE.toBytes() / Integer.SIZE);

        for (int step = 0; step * positionsInStep <= addresses.size(); step++) {
            int stepBeginPosition = step * positionsInStep;
            int stepEndPosition = Math.min((step + 1) * positionsInStep, addresses.size());
            int stepSize = stepEndPosition - stepBeginPosition;

            // index pages
            for (int batchIndex = 0; batchIndex < stepSize; batchIndex++) {
                int addressIndex = batchIndex + stepBeginPosition;
                if (isPositionNull(addressIndex)) {
                    continue;
                }

                long address = addresses.getLong(addressIndex);
                int blockIndex = decodeSliceIndex(address);
                int blockPosition = decodePosition(address);
                long value = joinChannelBlocks.get(blockIndex).getLong(blockPosition, 0);

                int pos = getHashPosition(value, mask);

                // look for an empty slot or a slot containing this key
                while (keys[pos] != -1) {
                    int currentKey = keys[pos];
                    if (value == values[currentKey]) {
                        // found a slot for this key
                        // link the new key position to the current key position
                        addressIndex = positionLinks.link(addressIndex, currentKey);

                        // key[pos] updated outside of this loop
                        break;
                    }
                    // increment position and mask to handler wrap around
                    pos = (pos + 1) & mask;
                }

                keys[pos] = addressIndex;
                values[addressIndex] = value;
            }
        }

        size = sizeOf(addresses.elements()) + pagesHashStrategy.getSizeInBytes() +
                sizeOf(keys) + sizeOf(values);
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
    public int getAddressIndex(int position, Page hashChannelsPage, long rawHash)
    {
        return getAddressIndex(position, hashChannelsPage);
    }

    @Override
    public int getAddressIndex(int position, Page hashChannelsPage)
    {
        long value = hashChannelsPage.getBlock(0).getLong(position, 0);
        int pos = getHashPosition(value, mask);

        while (keys[pos] != -1) {
            if (value == values[keys[pos]]) {
                return keys[pos];
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }
        return -1;
    }

    @Override
    public int[] getAddressIndex(int[] positions, Page hashChannelsPage, long[] rawHashes)
    {
        return getAddressIndex(positions, hashChannelsPage);
    }

    @Override
    public int[] getAddressIndex(int[] positions, Page hashChannelsPage)
    {
        checkArgument(hashChannelsPage.getChannelCount() == 1, "Multiple channel page passed to BigintPagesHash");

        int positionCount = positions.length;
        long[] incomingValues = new long[positionCount];
        int[] hashPositions = new int[positionCount];

        for (int i = 0; i < positionCount; i++) {
            incomingValues[i] = hashChannelsPage.getBlock(0).getLong(positions[i], 0);
            hashPositions[i] = getHashPosition(incomingValues[i], mask);
        }

        int[] found = new int[positionCount];
        int foundCount = 0;
        int[] result = new int[positionCount];
        Arrays.fill(result, -1);
        int[] foundKeys = new int[positionCount];

        // Search for positions in the hash array. This is the most CPU-consuming part as
        // it relies on random memory accesses
        for (int i = 0; i < positionCount; i++) {
            foundKeys[i] = keys[hashPositions[i]];
        }
        // Found positions are put into `found` array
        for (int i = 0; i < positionCount; i++) {
            if (foundKeys[i] != -1) {
                found[foundCount++] = i;
            }
        }

        // At this step we determine if the found keys were indeed the proper ones or it is a hash collision.
        // The result array is updated for the found ones, while the collisions land into `remaining` array.
        int[] remaining = found; // Rename for readability
        int remainingCount = 0;

        for (int i = 0; i < foundCount; i++) {
            int index = found[i];
            if (values[foundKeys[index]] == incomingValues[index]) {
                result[index] = foundKeys[index];
            }
            else {
                remaining[remainingCount++] = index;
            }
        }

        // At this point for any reasoable load factor of a hash array (< .75), there is no more than
        // 10 - 15% of positions left. We search for them in a sequential order and update the result array.
        for (int i = 0; i < remainingCount; i++) {
            int index = remaining[i];
            int position = (hashPositions[index] + 1) & mask; // hashPositions[index] position has already been checked

            while (keys[position] != -1) {
                if (values[keys[position]] == incomingValues[index]) {
                    result[index] = keys[position];
                    break;
                }
                // increment position and mask to handler wrap around
                position = (position + 1) & mask;
            }
        }

        return result;
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
}
