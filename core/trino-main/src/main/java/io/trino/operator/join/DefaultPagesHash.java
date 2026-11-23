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

import io.trino.operator.HashArraySizeSupplier;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.PagesHashStrategy;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.trino.operator.join.PagesHash.getHashPosition;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * The PagesHash object that handles all cases - single/multi channel joins
 * with any types.
 * This implementation assumes arrays used in the hash are always a power of 2
 */
public final class DefaultPagesHash
        implements PagesHash
{
    private static final int INSTANCE_SIZE = instanceSize(DefaultPagesHash.class);
    // When position count fits in 24 bits (up to 16,777,216 rows per partition), pack a 7-bit hash
    // tag (bits 24..30) in the upper byte of keys[] with bit 31 cleared so every occupied entry is
    // non-negative and distinct from the -1 empty sentinel. This avoids both allocating
    // positionToHashes[] and taking a second random memory read on hash collisions and probes.
    static final int MAX_PACKED_POSITIONS = 1 << 24;
    private static final int KEY_MASK = MAX_PACKED_POSITIONS - 1;
    private static final int TAG_MASK = 0x7F00_0000;

    private final int positionCount;
    private final PagesHashStrategy pagesHashStrategy;
    // Resolves a row number to its (block index, position) for output.
    private final BlockPositionIndex blockPositionIndex;

    private final int mask;
    private final int[] keys;
    private final long size;

    // Non-null only when positionCount > MAX_PACKED_POSITIONS (unpacked fallback)
    @Nullable
    private final byte[] positionToHashes;

    public DefaultPagesHash(
            LongArrayList addresses,
            PagesHashStrategy pagesHashStrategy,
            List<ObjectArrayList<Block>> channels,
            IntArrayList positionCounts,
            List<Integer> joinChannels,
            InterpretedHashGenerator hashGenerator,
            PositionLinks.FactoryBuilder positionLinks,
            HashArraySizeSupplier hashArraySizeSupplier)
    {
        this(addresses,
                pagesHashStrategy,
                channels,
                positionCounts,
                joinChannels,
                hashGenerator,
                positionLinks,
                hashArraySizeSupplier,
                addresses.size() <= MAX_PACKED_POSITIONS);
    }

    DefaultPagesHash(
            LongArrayList addresses,
            PagesHashStrategy pagesHashStrategy,
            List<ObjectArrayList<Block>> channels,
            IntArrayList positionCounts,
            List<Integer> joinChannels,
            InterpretedHashGenerator hashGenerator,
            PositionLinks.FactoryBuilder positionLinks,
            HashArraySizeSupplier hashArraySizeSupplier,
            boolean usePackedTags)
    {
        requireNonNull(addresses, "addresses is null");
        this.positionCount = addresses.size();
        this.pagesHashStrategy = requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");
        requireNonNull(positionCounts, "positionCounts is null");
        blockPositionIndex = new BlockPositionIndex(positionCounts);

        // reserve memory for the arrays
        int hashSize = hashArraySizeSupplier.getHashArraySize(positionCount);

        mask = hashSize - 1;
        keys = new int[hashSize];
        Arrays.fill(keys, -1);
        positionToHashes = (usePackedTags && positionCount <= MAX_PACKED_POSITIONS) ? null : new byte[positionCount];

        int pageCount = positionCounts.size();
        int maxPagePositions = 0;
        for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
            maxPagePositions = Math.max(maxPagePositions, positionCounts.getInt(pageIndex));
        }
        long[] pageHashes = new long[maxPagePositions];
        Block[] joinBlocks = new Block[joinChannels.size()];
        Block[] nullableBlocks = new Block[joinChannels.size()];

        int offset = 0;
        for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
            int pagePositions = positionCounts.getInt(pageIndex);
            int nullableCount = 0;
            for (int channelIndex = 0; channelIndex < joinChannels.size(); channelIndex++) {
                Block block = channels.get(joinChannels.get(channelIndex)).get(pageIndex);
                joinBlocks[channelIndex] = block;
                if (block.mayHaveNull()) {
                    nullableBlocks[nullableCount++] = block;
                }
            }

            Optional<int[]> nonNullPositions = NullablePositions.getNonNullPositions(
                    nullableBlocks, nullableCount, pagePositions);

            if (nonNullPositions.isEmpty()) {
                hashGenerator.hashBlocksBatched(joinBlocks, pageHashes, 0, pagePositions);
                indexRange(positionLinks, offset, pagePositions, pageHashes);
            }
            else {
                int[] positions = nonNullPositions.get();
                hashGenerator.hashNonNulls(joinBlocks, positions, pageHashes);
                indexPositions(positionLinks, offset, positions, pageHashes);
            }
            offset += pagePositions;
        }

        size = sizeOf(addresses.elements()) + pagesHashStrategy.getSizeInBytes() +
                sizeOf(keys) + (positionToHashes == null ? 0 : sizeOf(positionToHashes)) +
                blockPositionIndex.getRetainedSizeInBytes();
    }

    private static int encodeTag(@Nullable byte[] positionToHashes, long rawHash)
    {
        return positionToHashes == null ? (((int) rawHash) & 0x7F) << 24 : (byte) rawHash;
    }

    private static int recordTag(@Nullable byte[] positionToHashes, int position, long rawHash)
    {
        if (positionToHashes == null) {
            return (((int) rawHash) & 0x7F) << 24;
        }
        byte hashByte = (byte) rawHash;
        positionToHashes[position] = hashByte;
        return hashByte;
    }

    private void indexRange(PositionLinks.FactoryBuilder positionLinks, int offset, int length, long[] pageHashes)
    {
        byte[] positionToHashes = this.positionToHashes;
        int mask = this.mask;
        for (int index = 0; index < length; index++) {
            int position = offset + index;
            long hash = pageHashes[index];
            int tag = recordTag(positionToHashes, position, hash);
            int bucket = getHashPosition(hash, mask);
            insertValue(positionLinks, position, tag, bucket);
        }
    }

    private void indexPositions(PositionLinks.FactoryBuilder positionLinks, int offset, int[] positions, long[] pageHashes)
    {
        byte[] positionToHashes = this.positionToHashes;
        int mask = this.mask;
        for (int index : positions) {
            int position = offset + index;
            long hash = pageHashes[index];
            int tag = recordTag(positionToHashes, position, hash);
            int bucket = getHashPosition(hash, mask);
            insertValue(positionLinks, position, tag, bucket);
        }
    }

    private void insertValue(PositionLinks.FactoryBuilder positionLinks, int realPosition, int tag, int pos)
    {
        int[] keys = this.keys;
        byte[] positionToHashes = this.positionToHashes;
        int mask = this.mask;
        // look for an empty slot or a slot containing this key
        int entry;
        while ((entry = keys[pos]) != -1) {
            if ((positionToHashes == null ? (entry & TAG_MASK) : positionToHashes[entry]) == tag) {
                int currentKey = positionToHashes == null ? entry & KEY_MASK : entry;
                if (positionEqualsPositionIgnoreNulls(currentKey, realPosition)) {
                    // found a slot for this key
                    // link the new key position to the current key position
                    realPosition = positionLinks.link(realPosition, currentKey);

                    // keys[pos] updated outside of this loop
                    break;
                }
            }
            // increment position and mask to handle wrap around
            pos = (pos + 1) & mask;
        }

        keys[pos] = positionToHashes == null ? tag | realPosition : realPosition;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return INSTANCE_SIZE + size;
    }

    @Override
    public int getAddressIndex(int position, Page hashChannelsPage)
    {
        return getAddressIndex(position, hashChannelsPage, pagesHashStrategy.hashRow(position, hashChannelsPage));
    }

    @Override
    public int getAddressIndex(int rightPosition, Page hashChannelsPage, long rawHash)
    {
        int[] keys = this.keys;
        byte[] positionToHashes = this.positionToHashes;
        int mask = this.mask;
        int pos = getHashPosition(rawHash, mask);
        int tag = encodeTag(positionToHashes, rawHash);

        int entry;
        while ((entry = keys[pos]) != -1) {
            if ((positionToHashes == null ? (entry & TAG_MASK) : positionToHashes[entry]) == tag) {
                int key = positionToHashes == null ? entry & KEY_MASK : entry;
                if (positionEqualsCurrentRowIgnoreNulls(key, rightPosition, hashChannelsPage)) {
                    return key;
                }
            }
            // increment position and mask to handle wrap around
            pos = (pos + 1) & mask;
        }
        return -1;
    }

    @Override
    public int[] getAddressIndex(int[] positions, Page hashChannelsPage, long[] rawHashes)
    {
        int positionCount = positions.length;
        int[] hashPositions = calculateHashPositions(positions, rawHashes, positionCount);

        // Search for positions in the hash array directly into result[] to maximize
        // memory-level parallelism without allocating a separate foundKeys[] array.
        int[] result = new int[positionCount];
        findPositions(positionCount, hashPositions, result);

        byte[] positionToHashes = this.positionToHashes;
        int[] remaining = new int[positionCount];
        int remainingCount = 0;
        for (int i = 0; i < positionCount; i++) {
            int entry = result[i];
            if (entry != -1) {
                int rightPosition = positions[i];
                int tag = encodeTag(positionToHashes, rawHashes[rightPosition]);
                if ((positionToHashes == null ? (entry & TAG_MASK) : positionToHashes[entry]) == tag) {
                    int key = positionToHashes == null ? entry & KEY_MASK : entry;
                    if (positionEqualsCurrentRowIgnoreNulls(key, rightPosition, hashChannelsPage)) {
                        result[i] = key;
                        continue;
                    }
                }
                result[i] = -1;
                remaining[remainingCount++] = i;
            }
        }

        if (remainingCount > 0) {
            findRemainingPositions(positions, hashChannelsPage, rawHashes, hashPositions, result, remainingCount, remaining);
        }

        return result;
    }

    private void findRemainingPositions(int[] positions, Page hashChannelsPage, long[] rawHashes, int[] hashPositions, int[] result, int remainingCount, int[] remaining)
    {
        int[] keys = this.keys;
        byte[] positionToHashes = this.positionToHashes;
        int mask = this.mask;
        for (int i = 0; i < remainingCount; i++) {
            int index = remaining[i];
            int position = (hashPositions[index] + 1) & mask; // hashPositions[index] position has already been checked
            int rightPosition = positions[index];
            int tag = encodeTag(positionToHashes, rawHashes[rightPosition]);

            int entry;
            while ((entry = keys[position]) != -1) {
                if ((positionToHashes == null ? (entry & TAG_MASK) : positionToHashes[entry]) == tag) {
                    int key = positionToHashes == null ? entry & KEY_MASK : entry;
                    if (positionEqualsCurrentRowIgnoreNulls(key, rightPosition, hashChannelsPage)) {
                        result[index] = key;
                        break;
                    }
                }
                // increment position and mask to handle wrap around
                position = (position + 1) & mask;
            }
        }
    }

    private void findPositions(int positionCount, int[] hashPositions, int[] result)
    {
        for (int i = 0; i < positionCount; i++) {
            result[i] = keys[hashPositions[i]];
        }
    }

    private int[] calculateHashPositions(int[] positions, long[] rawHashes, int positionCount)
    {
        int[] hashPositions = new int[positionCount];

        for (int i = 0; i < positionCount; i++) {
            hashPositions[i] = getHashPosition(rawHashes[positions[i]], mask);
        }
        return hashPositions;
    }

    @Override
    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        int rowNumber = toIntExact(position);
        int blockIndex = blockPositionIndex.decodeBlockIndex(rowNumber);
        pagesHashStrategy.appendTo(blockIndex, blockPositionIndex.decodePosition(rowNumber, blockIndex), pageBuilder, outputChannelOffset);
    }

    private boolean positionEqualsCurrentRowIgnoreNulls(int leftPosition, int rightPosition, Page rightPage)
    {
        int blockIndex = blockPositionIndex.decodeBlockIndex(leftPosition);
        int blockPosition = blockPositionIndex.decodePosition(leftPosition, blockIndex);

        return pagesHashStrategy.positionEqualsRowIgnoreNulls(blockIndex, blockPosition, rightPosition, rightPage);
    }

    private boolean positionEqualsPositionIgnoreNulls(int leftPosition, int rightPosition)
    {
        int leftBlockIndex = blockPositionIndex.decodeBlockIndex(leftPosition);
        int leftBlockPosition = blockPositionIndex.decodePosition(leftPosition, leftBlockIndex);

        int rightBlockIndex = blockPositionIndex.decodeBlockIndex(rightPosition);
        int rightBlockPosition = blockPositionIndex.decodePosition(rightPosition, rightBlockIndex);

        return pagesHashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition);
    }

    public static long getEstimatedRetainedSizeInBytes(
            int positionCount,
            HashArraySizeSupplier hashArraySizeSupplier,
            LongArrayList addresses,
            List<ObjectArrayList<Block>> channels,
            long blocksSizeInBytes)
    {
        int blockCount = 0;
        if (!channels.isEmpty()) {
            blockCount = channels.getFirst().size();
        }
        return sizeOf(addresses.elements()) +
                (channels.size() > 0 ? sizeOf(channels.get(0).elements()) * channels.size() : 0) +
                blocksSizeInBytes +
                sizeOfIntArray(hashArraySizeSupplier.getHashArraySize(positionCount)) +
                (positionCount <= MAX_PACKED_POSITIONS ? 0 : sizeOfByteArray(positionCount)) +
                BlockPositionIndex.getEstimatedRetainedSizeInBytes(blockCount, positionCount);
    }
}
