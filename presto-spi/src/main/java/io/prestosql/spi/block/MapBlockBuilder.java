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
import static io.prestosql.spi.block.BlockUtil.calculateBlockResetSize;
import static io.prestosql.spi.block.BlockUtil.calculateNewArraySize;
import static io.prestosql.spi.block.MapBlock.createMapBlockInternal;
import static io.prestosql.spi.block.MapHashTables.HASH_MULTIPLIER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MapBlockBuilder
        extends AbstractMapBlock
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapBlockBuilder.class).instanceSize();

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;

    private int positionCount;
    private int[] offsets;
    private boolean[] mapIsNull;
    private final BlockBuilder keyBlockBuilder;
    private final BlockBuilder valueBlockBuilder;
    private final MapHashTables hashTables;

    private boolean currentEntryOpened;

    public MapBlockBuilder(MapType mapType, BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this(
                mapType,
                blockBuilderStatus,
                mapType.getKeyType().createBlockBuilder(blockBuilderStatus, expectedEntries),
                mapType.getValueType().createBlockBuilder(blockBuilderStatus, expectedEntries),
                new int[expectedEntries + 1],
                new boolean[expectedEntries]);
    }

    private MapBlockBuilder(
            MapType mapType,
            @Nullable BlockBuilderStatus blockBuilderStatus,
            BlockBuilder keyBlockBuilder,
            BlockBuilder valueBlockBuilder,
            int[] offsets,
            boolean[] mapIsNull)
    {
        super(mapType);

        this.blockBuilderStatus = blockBuilderStatus;

        this.positionCount = 0;
        this.offsets = requireNonNull(offsets, "offsets is null");
        this.mapIsNull = requireNonNull(mapIsNull, "mapIsNull is null");
        this.keyBlockBuilder = requireNonNull(keyBlockBuilder, "keyBlockBuilder is null");
        this.valueBlockBuilder = requireNonNull(valueBlockBuilder, "valueBlockBuilder is null");

        int[] hashTable = new int[mapIsNull.length * HASH_MULTIPLIER];
        Arrays.fill(hashTable, -1);
        this.hashTables = new MapHashTables(mapType, Optional.of(hashTable));
    }

    @Override
    protected Block getRawKeyBlock()
    {
        return keyBlockBuilder;
    }

    @Override
    protected Block getRawValueBlock()
    {
        return valueBlockBuilder;
    }

    @Override
    protected MapHashTables getHashTables()
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
        return 0;
    }

    @Override
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
        return keyBlockBuilder.getSizeInBytes() + valueBlockBuilder.getSizeInBytes() +
                (Integer.BYTES + Byte.BYTES) * (long) positionCount +
                Integer.BYTES * HASH_MULTIPLIER * (long) keyBlockBuilder.getPositionCount();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE
                + keyBlockBuilder.getRetainedSizeInBytes()
                + valueBlockBuilder.getRetainedSizeInBytes()
                + sizeOf(offsets)
                + sizeOf(mapIsNull)
                + hashTables.getRetainedSizeInBytes();
        if (blockBuilderStatus != null) {
            size += BlockBuilderStatus.INSTANCE_SIZE;
        }
        return size;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(keyBlockBuilder, keyBlockBuilder.getRetainedSizeInBytes());
        consumer.accept(valueBlockBuilder, valueBlockBuilder.getRetainedSizeInBytes());
        consumer.accept(offsets, sizeOf(offsets));
        consumer.accept(mapIsNull, sizeOf(mapIsNull));
        consumer.accept(hashTables, hashTables.getRetainedSizeInBytes());
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public SingleMapBlockWriter beginBlockEntry()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Expected current entry to be closed but was opened");
        }
        currentEntryOpened = true;
        return new SingleMapBlockWriter(keyBlockBuilder.getPositionCount() * 2, keyBlockBuilder, valueBlockBuilder);
    }

    @Override
    public BlockBuilder closeEntry()
    {
        if (!currentEntryOpened) {
            throw new IllegalStateException("Expected entry to be opened but was closed");
        }

        entryAdded(false);
        currentEntryOpened = false;

        ensureHashTableSize();
        int previousAggregatedEntryCount = offsets[positionCount - 1];
        int aggregatedEntryCount = offsets[positionCount];
        int entryCount = aggregatedEntryCount - previousAggregatedEntryCount;
        hashTables.buildHashTable(keyBlockBuilder, previousAggregatedEntryCount, entryCount);
        return this;
    }

    /**
     * This method will check duplicate keys and close entry.
     * <p>
     * When duplicate keys are discovered, the block is guaranteed to be in
     * a consistent state before {@link DuplicateMapKeyException} is thrown.
     * In other words, one can continue to use this BlockBuilder.
     */
    public void closeEntryStrict()
            throws DuplicateMapKeyException
    {
        if (!currentEntryOpened) {
            throw new IllegalStateException("Expected entry to be opened but was closed");
        }

        entryAdded(false);
        currentEntryOpened = false;

        ensureHashTableSize();
        int previousAggregatedEntryCount = offsets[positionCount - 1];
        int aggregatedEntryCount = offsets[positionCount];
        int entryCount = aggregatedEntryCount - previousAggregatedEntryCount;
        hashTables.buildHashTableStrict(keyBlockBuilder, previousAggregatedEntryCount, entryCount);
    }

    private void closeEntry(Optional<int[]> providedHashTable, int providedHashTableOffset)
    {
        if (!currentEntryOpened) {
            throw new IllegalStateException("Expected entry to be opened but was closed");
        }

        entryAdded(false);
        currentEntryOpened = false;

        ensureHashTableSize();
        int previousAggregatedEntryCount = offsets[positionCount - 1];
        int aggregatedEntryCount = offsets[positionCount];

        if (providedHashTable.isPresent()) {
            int hashTableOffset = previousAggregatedEntryCount * HASH_MULTIPLIER;
            int hashTableSize = (aggregatedEntryCount - previousAggregatedEntryCount) * HASH_MULTIPLIER;
            int[] rawProvidedHashTable = providedHashTable.get();
            int[] rawHashTables = hashTables.get();
            System.arraycopy(rawProvidedHashTable, providedHashTableOffset, rawHashTables, hashTableOffset, hashTableSize);
        }
        else {
            int entryCount = aggregatedEntryCount - previousAggregatedEntryCount;
            hashTables.buildHashTable(keyBlockBuilder, previousAggregatedEntryCount, entryCount);
        }
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        entryAdded(true);
        return this;
    }

    private void entryAdded(boolean isNull)
    {
        if (keyBlockBuilder.getPositionCount() != valueBlockBuilder.getPositionCount()) {
            throw new IllegalStateException(format("keyBlock and valueBlock has different size: %s %s", keyBlockBuilder.getPositionCount(), valueBlockBuilder.getPositionCount()));
        }
        if (mapIsNull.length <= positionCount) {
            int newSize = calculateNewArraySize(mapIsNull.length);
            mapIsNull = Arrays.copyOf(mapIsNull, newSize);
            offsets = Arrays.copyOf(offsets, newSize + 1);
        }
        offsets[positionCount + 1] = keyBlockBuilder.getPositionCount();
        mapIsNull[positionCount] = isNull;
        positionCount++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Integer.BYTES + Byte.BYTES);
            blockBuilderStatus.addBytes((offsets[positionCount] - offsets[positionCount - 1]) * HASH_MULTIPLIER * Integer.BYTES);
        }
    }

    private void ensureHashTableSize()
    {
        int[] rawHashTables = hashTables.get();
        if (rawHashTables.length < offsets[positionCount] * HASH_MULTIPLIER) {
            int newSize = calculateNewArraySize(offsets[positionCount] * HASH_MULTIPLIER);
            hashTables.growHashTables(newSize);
        }
    }

    @Override
    public Block build()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }

        int[] rawHashTables = hashTables.get();
        int hashTablesEntries = offsets[positionCount] * HASH_MULTIPLIER;
        return createMapBlockInternal(
                getMapType(),
                0,
                positionCount,
                Optional.of(mapIsNull),
                offsets,
                keyBlockBuilder.build(),
                valueBlockBuilder.build(),
                new MapHashTables(getMapType(), Optional.of(Arrays.copyOf(rawHashTables, hashTablesEntries))));
    }

    @Override
    public String toString()
    {
        return "MapBlockBuilder{" +
                "positionCount=" + getPositionCount() +
                '}';
    }

    @Override
    public BlockBuilder appendStructure(Block block)
    {
        if (!(block instanceof SingleMapBlock)) {
            throw new IllegalArgumentException("Expected SingleMapBlock");
        }
        if (currentEntryOpened) {
            throw new IllegalStateException("Expected current entry to be closed but was opened");
        }
        currentEntryOpened = true;

        SingleMapBlock singleMapBlock = (SingleMapBlock) block;
        int blockPositionCount = singleMapBlock.getPositionCount();
        if (blockPositionCount % 2 != 0) {
            throw new IllegalArgumentException(format("block position count is not even: %s", blockPositionCount));
        }
        for (int i = 0; i < blockPositionCount; i += 2) {
            if (singleMapBlock.isNull(i)) {
                throw new IllegalArgumentException("Map keys must not be null");
            }
            singleMapBlock.writePositionTo(i, keyBlockBuilder);
            if (singleMapBlock.isNull(i + 1)) {
                valueBlockBuilder.appendNull();
            }
            else {
                singleMapBlock.writePositionTo(i + 1, valueBlockBuilder);
            }
        }

        closeEntry(singleMapBlock.tryGetHashTable(), singleMapBlock.getOffset() / 2 * HASH_MULTIPLIER);
        return this;
    }

    @Override
    public BlockBuilder appendStructureInternal(Block block, int position)
    {
        if (!(block instanceof AbstractMapBlock)) {
            throw new IllegalArgumentException("Expected AbstractMapBlock");
        }
        if (currentEntryOpened) {
            throw new IllegalStateException("Expected current entry to be closed but was opened");
        }
        currentEntryOpened = true;

        AbstractMapBlock mapBlock = (AbstractMapBlock) block;
        int startValueOffset = mapBlock.getOffset(position);
        int endValueOffset = mapBlock.getOffset(position + 1);
        for (int i = startValueOffset; i < endValueOffset; i++) {
            if (mapBlock.getRawKeyBlock().isNull(i)) {
                throw new IllegalArgumentException("Map keys must not be null");
            }
            mapBlock.getRawKeyBlock().writePositionTo(i, keyBlockBuilder);
            if (mapBlock.getRawValueBlock().isNull(i)) {
                valueBlockBuilder.appendNull();
            }
            else {
                mapBlock.getRawValueBlock().writePositionTo(i, valueBlockBuilder);
            }
        }

        closeEntry(mapBlock.getHashTables().tryGet(), startValueOffset * HASH_MULTIPLIER);
        return this;
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        int newSize = calculateBlockResetSize(getPositionCount());
        return new MapBlockBuilder(
                getMapType(),
                blockBuilderStatus,
                keyBlockBuilder.newBlockBuilderLike(blockBuilderStatus),
                valueBlockBuilder.newBlockBuilderLike(blockBuilderStatus),
                new int[newSize + 1],
                new boolean[newSize]);
    }

    @Override
    protected void ensureHashTableLoaded() {}
}
