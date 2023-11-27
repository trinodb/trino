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

package io.trino.spi.block;

import io.trino.spi.type.MapType;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkReadablePosition;
import static io.trino.spi.block.BlockUtil.checkValidPositions;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.BlockUtil.compactOffsets;
import static io.trino.spi.block.BlockUtil.copyIsNullAndAppendNull;
import static io.trino.spi.block.BlockUtil.copyOffsetsAndAppendNull;
import static io.trino.spi.block.BlockUtil.countAndMarkSelectedPositionsFromOffsets;
import static io.trino.spi.block.BlockUtil.countSelectedPositionsFromOffsets;
import static io.trino.spi.block.MapHashTables.HASH_MULTIPLIER;
import static io.trino.spi.block.MapHashTables.HashBuildMode.DUPLICATE_NOT_CHECKED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class MapBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(MapBlock.class);

    private final MapType mapType;

    private final int startOffset;
    private final int positionCount;

    @Nullable
    private final boolean[] mapIsNull;
    private final int[] offsets;
    private final Block keyBlock;
    private final Block valueBlock;
    private final MapHashTables hashTables;

    private final long baseSizeInBytes;
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
        return fromKeyValueBlock(mapIsNull, offsets, offsets.length - 1, keyBlock, valueBlock, mapType);
    }

    public static MapBlock fromKeyValueBlock(
            Optional<boolean[]> mapIsNull,
            int[] offsets,
            int mapCount,
            Block keyBlock,
            Block valueBlock,
            MapType mapType)
    {
        validateConstructorArguments(mapType, 0, mapCount, mapIsNull.orElse(null), offsets, keyBlock, valueBlock);

        return createMapBlockInternal(
                mapType,
                0,
                mapCount,
                mapIsNull,
                offsets,
                keyBlock,
                valueBlock,
                new MapHashTables(mapType, DUPLICATE_NOT_CHECKED, mapCount, Optional.empty()));
    }

    /**
     * Create a map block directly without per element validations.
     * <p>
     * Internal use by this package and io.trino.spi.Type only.
     */
    public static MapBlock createMapBlockInternal(
            MapType mapType,
            int startOffset,
            int positionCount,
            Optional<boolean[]> mapIsNull,
            int[] offsets,
            Block keyBlock,
            Block valueBlock,
            MapHashTables hashTables)
    {
        validateConstructorArguments(mapType, startOffset, positionCount, mapIsNull.orElse(null), offsets, keyBlock, valueBlock);
        requireNonNull(hashTables, "hashTables is null");
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
            MapHashTables hashTables)
    {
        this.mapType = requireNonNull(mapType, "mapType is null");

        int[] rawHashTables = hashTables.tryGet().orElse(null);
        if (rawHashTables != null && rawHashTables.length < keyBlock.getPositionCount() * HASH_MULTIPLIER) {
            throw new IllegalArgumentException(format("keyBlock/valueBlock size does not match hash table size: %s %s", keyBlock.getPositionCount(), rawHashTables.length));
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
                (Integer.BYTES + Byte.BYTES) * (long) this.positionCount +
                calculateSize(keyBlock);

        this.retainedSizeInBytes = INSTANCE_SIZE + sizeOf(offsets) + sizeOf(mapIsNull);
    }

    Block getRawKeyBlock()
    {
        return keyBlock;
    }

    Block getRawValueBlock()
    {
        return valueBlock;
    }

    MapHashTables getHashTables()
    {
        return hashTables;
    }

    int[] getOffsets()
    {
        return offsets;
    }

    int getOffsetBase()
    {
        return startOffset;
    }

    @Override
    public boolean mayHaveNull()
    {
        return mapIsNull != null;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        if (valueSizeInBytes < 0) {
            if (!valueBlock.isLoaded()) {
                return baseSizeInBytes + valueBlock.getSizeInBytes();
            }
            valueSizeInBytes = calculateSize(valueBlock);
        }

        return baseSizeInBytes + valueSizeInBytes;
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
        return retainedSizeInBytes + keyBlock.getRetainedSizeInBytes() + valueBlock.getRetainedSizeInBytes() + hashTables.getRetainedSizeInBytes();
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(keyBlock, keyBlock.getRetainedSizeInBytes());
        consumer.accept(valueBlock, valueBlock.getRetainedSizeInBytes());
        consumer.accept(offsets, sizeOf(offsets));
        if (mapIsNull != null) {
            consumer.accept(mapIsNull, sizeOf(mapIsNull));
        }
        consumer.accept(hashTables, hashTables.getRetainedSizeInBytes());
        consumer.accept(this, INSTANCE_SIZE);
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
                getMapType(),
                startOffset,
                positionCount,
                Optional.ofNullable(mapIsNull),
                offsets,
                keyBlock,
                loadedValueBlock,
                hashTables);
    }

    void ensureHashTableLoaded()
    {
        hashTables.buildAllHashTablesIfNecessary(keyBlock, offsets, mapIsNull);
    }

    @Override
    public MapBlock copyWithAppendedNull()
    {
        boolean[] newMapIsNull = copyIsNullAndAppendNull(mapIsNull, startOffset, getPositionCount());
        int[] newOffsets = copyOffsetsAndAppendNull(offsets, startOffset, getPositionCount());

        return createMapBlockInternal(
                getMapType(),
                startOffset,
                getPositionCount() + 1,
                Optional.of(newMapIsNull),
                newOffsets,
                keyBlock,
                valueBlock,
                hashTables);
    }

    @Override
    public List<Block> getChildren()
    {
        return List.of(keyBlock, valueBlock);
    }

    MapType getMapType()
    {
        return mapType;
    }

    private int getOffset(int position)
    {
        return offsets[position + startOffset];
    }

    @Override
    public String getEncodingName()
    {
        return MapBlockEncoding.NAME;
    }

    @Override
    public MapBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int[] newOffsets = new int[length + 1];
        boolean[] newMapIsNull = new boolean[length];

        IntArrayList entriesPositions = new IntArrayList();
        int newPosition = 0;
        for (int i = offset; i < offset + length; ++i) {
            int position = positions[i];
            if (isNull(position)) {
                newMapIsNull[newPosition] = true;
                newOffsets[newPosition + 1] = newOffsets[newPosition];
            }
            else {
                int entriesStartOffset = getOffset(position);
                int entriesEndOffset = getOffset(position + 1);
                int entryCount = entriesEndOffset - entriesStartOffset;

                newOffsets[newPosition + 1] = newOffsets[newPosition] + entryCount;

                for (int elementIndex = entriesStartOffset; elementIndex < entriesEndOffset; elementIndex++) {
                    entriesPositions.add(elementIndex);
                }
            }
            newPosition++;
        }

        int[] rawHashTables = hashTables.tryGet().orElse(null);
        int[] newRawHashTables = null;
        int newHashTableEntries = newOffsets[newOffsets.length - 1] * HASH_MULTIPLIER;
        if (rawHashTables != null) {
            newRawHashTables = new int[newHashTableEntries];
            int newHashIndex = 0;
            for (int i = offset; i < offset + length; ++i) {
                int position = positions[i];
                int entriesStartOffset = getOffset(position);
                int entriesEndOffset = getOffset(position + 1);
                for (int hashIndex = entriesStartOffset * HASH_MULTIPLIER; hashIndex < entriesEndOffset * HASH_MULTIPLIER; hashIndex++) {
                    newRawHashTables[newHashIndex] = rawHashTables[hashIndex];
                    newHashIndex++;
                }
            }
        }

        Block newKeys = keyBlock.copyPositions(entriesPositions.elements(), 0, entriesPositions.size());
        Block newValues = valueBlock.copyPositions(entriesPositions.elements(), 0, entriesPositions.size());
        return createMapBlockInternal(
                mapType,
                0,
                length,
                Optional.of(newMapIsNull),
                newOffsets,
                newKeys,
                newValues,
                new MapHashTables(mapType, DUPLICATE_NOT_CHECKED, length, Optional.ofNullable(newRawHashTables)));
    }

    @Override
    public MapBlock getRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        return createMapBlockInternal(
                mapType,
                position + startOffset,
                length,
                Optional.ofNullable(mapIsNull),
                offsets,
                keyBlock,
                valueBlock,
                hashTables);
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int entriesStart = offsets[startOffset + position];
        int entriesEnd = offsets[startOffset + position + length];
        int entryCount = entriesEnd - entriesStart;

        return keyBlock.getRegionSizeInBytes(entriesStart, entryCount) +
                valueBlock.getRegionSizeInBytes(entriesStart, entryCount) +
                (Integer.BYTES + Byte.BYTES) * (long) length +
                Integer.BYTES * HASH_MULTIPLIER * (long) entryCount;
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return OptionalInt.empty(); // size per row is variable on the number of entries in each row
    }

    private OptionalInt keyAndValueFixedSizeInBytesPerRow()
    {
        OptionalInt keyFixedSizePerRow = keyBlock.fixedSizeInBytesPerPosition();
        if (keyFixedSizePerRow.isEmpty()) {
            return OptionalInt.empty();
        }
        OptionalInt valueFixedSizePerRow = valueBlock.fixedSizeInBytesPerPosition();
        if (valueFixedSizePerRow.isEmpty()) {
            return OptionalInt.empty();
        }

        return OptionalInt.of(keyFixedSizePerRow.getAsInt() + valueFixedSizePerRow.getAsInt());
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions, int selectedMapPositions)
    {
        int positionCount = getPositionCount();
        checkValidPositions(positions, positionCount);
        if (selectedMapPositions == 0) {
            return 0;
        }
        if (selectedMapPositions == positionCount) {
            return getSizeInBytes();
        }

        int[] offsets = this.offsets;
        int offsetBase = startOffset;
        OptionalInt fixedKeyAndValueSizePerRow = keyAndValueFixedSizeInBytesPerRow();

        int selectedEntryCount;
        long keyAndValuesSizeInBytes;
        if (fixedKeyAndValueSizePerRow.isPresent()) {
            // no new positions array need be created, we can just count the number of elements
            selectedEntryCount = countSelectedPositionsFromOffsets(positions, offsets, offsetBase);
            keyAndValuesSizeInBytes = fixedKeyAndValueSizePerRow.getAsInt() * (long) selectedEntryCount;
        }
        else {
            // We can use either the getRegionSizeInBytes or getPositionsSizeInBytes
            // from the underlying raw blocks to implement this function. We chose
            // getPositionsSizeInBytes with the assumption that constructing a
            // positions array is cheaper than calling getRegionSizeInBytes for each
            // used position.
            boolean[] entryPositions = new boolean[keyBlock.getPositionCount()];
            selectedEntryCount = countAndMarkSelectedPositionsFromOffsets(positions, offsets, offsetBase, entryPositions);
            keyAndValuesSizeInBytes = keyBlock.getPositionsSizeInBytes(entryPositions, selectedEntryCount) +
                    valueBlock.getPositionsSizeInBytes(entryPositions, selectedEntryCount);
        }

        return keyAndValuesSizeInBytes +
                (Integer.BYTES + Byte.BYTES) * (long) selectedMapPositions +
                Integer.BYTES * HASH_MULTIPLIER * (long) selectedEntryCount;
    }

    @Override
    public MapBlock copyRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + length);
        Block newKeys = keyBlock.copyRegion(startValueOffset, endValueOffset - startValueOffset);
        Block newValues = valueBlock.copyRegion(startValueOffset, endValueOffset - startValueOffset);

        int[] newOffsets = compactOffsets(offsets, position + startOffset, length);
        boolean[] mapIsNull = this.mapIsNull;
        boolean[] newMapIsNull;
        newMapIsNull = mapIsNull == null ? null : compactArray(mapIsNull, position + startOffset, length);
        int[] rawHashTables = hashTables.tryGet().orElse(null);
        int[] newRawHashTables = null;
        int expectedNewHashTableEntries = (endValueOffset - startValueOffset) * HASH_MULTIPLIER;
        if (rawHashTables != null) {
            newRawHashTables = compactArray(rawHashTables, startValueOffset * HASH_MULTIPLIER, expectedNewHashTableEntries);
        }

        if (newKeys == keyBlock && newValues == valueBlock && newOffsets == offsets && newMapIsNull == mapIsNull && newRawHashTables == rawHashTables) {
            return this;
        }
        return createMapBlockInternal(
                mapType,
                0,
                length,
                Optional.ofNullable(newMapIsNull),
                newOffsets,
                newKeys,
                newValues,
                new MapHashTables(mapType, DUPLICATE_NOT_CHECKED, length, Optional.ofNullable(newRawHashTables)));
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        if (clazz != SqlMap.class) {
            throw new IllegalArgumentException("clazz must be SqlMap.class");
        }
        return clazz.cast(getMap(position));
    }

    public SqlMap getMap(int position)
    {
        checkReadablePosition(this, position);
        int startEntryOffset = getOffset(position);
        int endEntryOffset = getOffset(position + 1);
        return new SqlMap(
                mapType,
                keyBlock,
                valueBlock,
                new SqlMap.HashTableSupplier(this),
                startEntryOffset,
                (endEntryOffset - startEntryOffset));
    }

    @Override
    public MapBlock getSingleValueBlock(int position)
    {
        checkReadablePosition(this, position);

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + 1);
        int valueLength = endValueOffset - startValueOffset;
        Block newKeys = keyBlock.copyRegion(startValueOffset, valueLength);
        Block newValues = valueBlock.copyRegion(startValueOffset, valueLength);
        int[] rawHashTables = hashTables.tryGet().orElse(null);
        int[] newRawHashTables = null;
        if (rawHashTables != null) {
            newRawHashTables = Arrays.copyOfRange(rawHashTables, startValueOffset * HASH_MULTIPLIER, endValueOffset * HASH_MULTIPLIER);
        }

        return createMapBlockInternal(
                mapType,
                0,
                1,
                Optional.of(new boolean[] {isNull(position)}),
                new int[] {0, valueLength},
                newKeys,
                newValues,
                new MapHashTables(mapType, DUPLICATE_NOT_CHECKED, 1, Optional.ofNullable(newRawHashTables)));
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        checkReadablePosition(this, position);

        if (isNull(position)) {
            return 0;
        }

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + 1);

        long size = 0;
        Block rawKeyBlock = keyBlock;
        Block rawValueBlock = valueBlock;
        for (int i = startValueOffset; i < endValueOffset; i++) {
            size += rawKeyBlock.getEstimatedDataSizeForStats(i);
            size += rawValueBlock.getEstimatedDataSizeForStats(i);
        }
        return size;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(this, position);
        boolean[] mapIsNull = this.mapIsNull;
        return mapIsNull != null && mapIsNull[position + startOffset];
    }

    @Override
    public MapBlock getUnderlyingValueBlock()
    {
        return this;
    }

    // only visible for testing
    public boolean isHashTablesPresent()
    {
        return hashTables.tryGet().isPresent();
    }
}
