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

package io.prestosql.spi.block;

import io.prestosql.spi.type.MapType;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.block.MapBlockBuilder.buildHashTable;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MapBlock
        extends AbstractMapBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapBlock.class).instanceSize();

    private final int startOffset;
    private final int positionCount;

    private final boolean[] mapIsNull;
    private final int[] offsets;
    private final Block keyBlock;
    private final Block valueBlock;
    private final int[] hashTables; // hash to location in map;

    private final long baseSizeInBytes;
    private volatile long keySizeInBytes = -1;
    private volatile long valueSizeInBytes = -1;
    private final long retainedSizeInBytes;

    /**
     * Create a map block directly from columnar nulls, keys, values, and offsets into the keys and values.
     * A null map must have no entries.
     */
    public static MapBlock fromKeyValueBlock(
            Optional<boolean[]> mapIsNull,
            int[] offsets,
            Block keyBlock,
            Block valueBlock,
            MapType mapType)
    {
        validateConstructorArguments(mapType, 0, offsets.length - 1, mapIsNull.orElse(null), offsets, keyBlock, valueBlock);

        int mapCount = offsets.length - 1;
        int elementCount = keyBlock.getPositionCount();
        int[] hashTables = new int[elementCount * HASH_MULTIPLIER];
        Arrays.fill(hashTables, -1);
        for (int i = 0; i < mapCount; i++) {
            int keyOffset = offsets[i];
            int keyCount = offsets[i + 1] - keyOffset;
            if (keyCount < 0) {
                throw new IllegalArgumentException(format("Offset is not monotonically ascending. offsets[%s]=%s, offsets[%s]=%s", i, offsets[i], i + 1, offsets[i + 1]));
            }
            if (mapIsNull.isPresent() && mapIsNull.get()[i] && keyCount != 0) {
                throw new IllegalArgumentException("A null map must have zero entries");
            }
            buildHashTable(keyBlock, keyOffset, keyCount, mapType, hashTables, keyOffset * HASH_MULTIPLIER, keyCount * HASH_MULTIPLIER);
        }

        return createMapBlockInternal(
                mapType,
                0,
                mapCount,
                mapIsNull,
                offsets,
                keyBlock,
                valueBlock,
                hashTables);
    }

    /**
     * Create a map block directly without per element validations.
     * <p>
     * Internal use by this package and io.prestosql.spi.Type only.
     */
    public static MapBlock createMapBlockInternal(
            MapType mapType,
            int startOffset,
            int positionCount,
            Optional<boolean[]> mapIsNull,
            int[] offsets,
            Block keyBlock,
            Block valueBlock,
            int[] hashTables)
    {
        validateConstructorArguments(mapType, startOffset, positionCount, mapIsNull.orElse(null), offsets, keyBlock, valueBlock);
        return new MapBlock(mapType, startOffset, positionCount, mapIsNull.orElse(null), offsets, keyBlock, valueBlock, hashTables);
    }

    private static void validateConstructorArguments(
            MapType mapType, int startOffset,
            int positionCount,
            @Nullable boolean[] mapIsNull,
            int[] offsets,
            Block keyBlock,
            Block valueBlock)
    {
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset is negative");
        }

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        if (mapIsNull != null && mapIsNull.length - startOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }

        requireNonNull(offsets, "offsets is null");
        if (offsets.length - startOffset < positionCount + 1) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }

        requireNonNull(keyBlock, "keyBlock is null");
        requireNonNull(valueBlock, "valueBlock is null");
        if (keyBlock.getPositionCount() != valueBlock.getPositionCount()) {
            throw new IllegalArgumentException(format("keyBlock and valueBlock has different size: %s %s", keyBlock.getPositionCount(), valueBlock.getPositionCount()));
        }

        requireNonNull(mapType, "mapType is null");
    }

    /**
     * Use createRowBlockInternal or fromKeyValueBlock instead of this method.  The caller of this method is assumed to have
     * validated the arguments with validateConstructorArguments.
     */
    private MapBlock(
            MapType mapType,
            int startOffset,
            int positionCount,
            @Nullable boolean[] mapIsNull,
            int[] offsets,
            Block keyBlock,
            Block valueBlock,
            int[] hashTables)
    {
        super(mapType);

        requireNonNull(hashTables, "hashTables is null");
        if (hashTables.length < keyBlock.getPositionCount() * HASH_MULTIPLIER) {
            throw new IllegalArgumentException(format("keyBlock/valueBlock size does not match hash table size: %s %s", keyBlock.getPositionCount(), hashTables.length));
        }

        this.startOffset = startOffset;
        this.positionCount = positionCount;
        this.mapIsNull = mapIsNull;
        this.offsets = offsets;
        this.keyBlock = keyBlock;
        this.valueBlock = valueBlock;
        this.hashTables = hashTables;

        int entryCount = offsets[startOffset + positionCount] - offsets[startOffset];
        this.baseSizeInBytes = Integer.BYTES * HASH_MULTIPLIER * (long) entryCount +
                (Integer.BYTES + Byte.BYTES) * (long) this.positionCount;

        this.retainedSizeInBytes = INSTANCE_SIZE + sizeOf(offsets) + sizeOf(mapIsNull) + sizeOf(hashTables);
    }

    @Override
    protected Block getRawKeyBlock()
    {
        return keyBlock;
    }

    @Override
    protected Block getRawValueBlock()
    {
        return valueBlock;
    }

    @Override
    protected int[] getHashTables()
    {
        return hashTables;
    }

    @Override
    protected int[] getOffsets()
    {
        return offsets;
    }

    @Override
    protected int getOffsetBase()
    {
        return startOffset;
    }

    @Override
    @Nullable
    protected boolean[] getMapIsNull()
    {
        return mapIsNull;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        if (keySizeInBytes < 0) {
            keySizeInBytes = calculateSize(keyBlock);
        }

        if (valueSizeInBytes < 0) {
            if (!valueBlock.isLoaded()) {
                return baseSizeInBytes + keySizeInBytes + valueBlock.getSizeInBytes();
            }
            valueSizeInBytes = calculateSize(valueBlock);
        }

        return baseSizeInBytes + keySizeInBytes + valueSizeInBytes;
    }

    private long calculateSize(Block block)
    {
        int entriesStart = offsets[startOffset];
        int entriesEnd = offsets[startOffset + positionCount];
        int entryCount = entriesEnd - entriesStart;
        return block.getRegionSizeInBytes(entriesStart, entryCount);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes + keyBlock.getRetainedSizeInBytes() + valueBlock.getRetainedSizeInBytes();
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(keyBlock, keyBlock.getRetainedSizeInBytes());
        consumer.accept(valueBlock, valueBlock.getRetainedSizeInBytes());
        consumer.accept(offsets, sizeOf(offsets));
        consumer.accept(mapIsNull, sizeOf(mapIsNull));
        consumer.accept(hashTables, sizeOf(hashTables));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("MapBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean isLoaded()
    {
        return keyBlock.isLoaded() && valueBlock.isLoaded();
    }

    @Override
    public Block getLoadedBlock()
    {
        if (keyBlock != keyBlock.getLoadedBlock()) {
            // keyBlock has to be loaded since MapBlock constructs hash table eagerly.
            throw new IllegalStateException();
        }

        Block loadedValueBlock = valueBlock.getLoadedBlock();
        if (loadedValueBlock == valueBlock) {
            return this;
        }
        return createMapBlockInternal(
                mapType,
                startOffset,
                positionCount,
                Optional.ofNullable(mapIsNull),
                offsets,
                keyBlock,
                loadedValueBlock,
                hashTables);
    }
}
