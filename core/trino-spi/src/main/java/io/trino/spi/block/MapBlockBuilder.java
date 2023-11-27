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

import io.trino.spi.block.MapHashTables.HashBuildMode;
import io.trino.spi.type.MapType;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.Optional;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.calculateNewArraySize;
import static io.trino.spi.block.MapBlock.createMapBlockInternal;
import static io.trino.spi.block.MapHashTables.HASH_MULTIPLIER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MapBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(MapBlockBuilder.class);

    private final MapType mapType;

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;

    private int positionCount;
    private int[] offsets;
    private boolean[] mapIsNull;
    private boolean hasNullValue;
    private final BlockBuilder keyBlockBuilder;
    private final BlockBuilder valueBlockBuilder;

    private boolean currentEntryOpened;
    private HashBuildMode hashBuildMode = HashBuildMode.DUPLICATE_NOT_CHECKED;

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
        this.mapType = requireNonNull(mapType, "mapType is null");

        this.blockBuilderStatus = blockBuilderStatus;

        this.positionCount = 0;
        this.offsets = requireNonNull(offsets, "offsets is null");
        this.mapIsNull = requireNonNull(mapIsNull, "mapIsNull is null");
        this.keyBlockBuilder = requireNonNull(keyBlockBuilder, "keyBlockBuilder is null");
        this.valueBlockBuilder = requireNonNull(valueBlockBuilder, "valueBlockBuilder is null");
    }

    public MapBlockBuilder strict()
    {
        this.hashBuildMode = HashBuildMode.STRICT_EQUALS;
        return this;
    }

    public MapBlockBuilder strictNotDistinctFrom()
    {
        this.hashBuildMode = HashBuildMode.STRICT_NOT_DISTINCT_FROM;
        return this;
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
                + sizeOf(mapIsNull);
        if (blockBuilderStatus != null) {
            size += BlockBuilderStatus.INSTANCE_SIZE;
        }
        return size;
    }

    public <E extends Throwable> void buildEntry(MapValueBuilder<E> builder)
            throws E
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Expected current entry to be closed but was opened");
        }

        currentEntryOpened = true;
        builder.build(keyBlockBuilder, valueBlockBuilder);
        entryAdded(false);
        currentEntryOpened = false;
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
        hasNullValue |= isNull;
        positionCount++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Integer.BYTES + Byte.BYTES);
            blockBuilderStatus.addBytes((offsets[positionCount] - offsets[positionCount - 1]) * HASH_MULTIPLIER * Integer.BYTES);
        }
    }

    @Override
    public Block build()
    {
        return buildValueBlock();
    }

    @Override
    public MapBlock buildValueBlock()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }

        Block keyBlock = keyBlockBuilder.build();
        Block valueBlock = valueBlockBuilder.build();
        MapHashTables hashTables = MapHashTables.create(hashBuildMode, mapType, positionCount, keyBlock, offsets, mapIsNull);

        return createMapBlockInternal(
                mapType,
                0,
                positionCount,
                hasNullValue ? Optional.of(mapIsNull) : Optional.empty(),
                offsets,
                keyBlock,
                valueBlock,
                hashTables);
    }

    @Override
    public String toString()
    {
        return "MapBlockBuilder{" +
                "positionCount=" + getPositionCount() +
                '}';
    }

    @Override
    public BlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus)
    {
        return new MapBlockBuilder(
                mapType,
                blockBuilderStatus,
                keyBlockBuilder.newBlockBuilderLike(blockBuilderStatus),
                valueBlockBuilder.newBlockBuilderLike(blockBuilderStatus),
                new int[expectedEntries + 1],
                new boolean[expectedEntries]);
    }
}
