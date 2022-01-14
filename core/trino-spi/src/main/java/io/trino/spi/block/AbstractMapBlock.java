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

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkValidPositions;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.BlockUtil.compactOffsets;
import static io.trino.spi.block.MapBlock.createMapBlockInternal;
import static io.trino.spi.block.MapHashTables.HASH_MULTIPLIER;
import static java.util.Objects.requireNonNull;

public abstract class AbstractMapBlock
        implements Block
{
    private final MapType mapType;

    public AbstractMapBlock(MapType mapType)
    {
        this.mapType = requireNonNull(mapType, "mapType is null");
    }

    @Override
    public final List<Block> getChildren()
    {
        return List.of(getRawKeyBlock(), getRawValueBlock());
    }

    protected MapType getMapType()
    {
        return mapType;
    }

    protected abstract Block getRawKeyBlock();

    protected abstract Block getRawValueBlock();

    protected abstract MapHashTables getHashTables();

    /**
     * offset is entry-based, not position-based. In other words,
     * if offset[1] is 6, it means the first map has 6 key-value pairs,
     * not 6 key/values (which would be 3 pairs).
     */
    protected abstract int[] getOffsets();

    /**
     * offset is entry-based, not position-based. (see getOffsets)
     */
    protected abstract int getOffsetBase();

    @Nullable
    protected abstract boolean[] getMapIsNull();

    protected abstract void ensureHashTableLoaded();

    int getOffset(int position)
    {
        return getOffsets()[position + getOffsetBase()];
    }

    @Override
    public String getEncodingName()
    {
        return MapBlockEncoding.NAME;
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
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

        int[] rawHashTables = getHashTables().tryGet().orElse(null);
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

        Block newKeys = getRawKeyBlock().copyPositions(entriesPositions.elements(), 0, entriesPositions.size());
        Block newValues = getRawValueBlock().copyPositions(entriesPositions.elements(), 0, entriesPositions.size());
        return createMapBlockInternal(
                mapType,
                0,
                length,
                Optional.of(newMapIsNull),
                newOffsets,
                newKeys,
                newValues,
                new MapHashTables(mapType, Optional.ofNullable(newRawHashTables)));
    }

    @Override
    public Block getRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        return createMapBlockInternal(
                mapType,
                position + getOffsetBase(),
                length,
                Optional.ofNullable(getMapIsNull()),
                getOffsets(),
                getRawKeyBlock(),
                getRawValueBlock(),
                getHashTables());
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int entriesStart = getOffsets()[getOffsetBase() + position];
        int entriesEnd = getOffsets()[getOffsetBase() + position + length];
        int entryCount = entriesEnd - entriesStart;

        return getRawKeyBlock().getRegionSizeInBytes(entriesStart, entryCount) +
                getRawValueBlock().getRegionSizeInBytes(entriesStart, entryCount) +
                (Integer.BYTES + Byte.BYTES) * (long) length +
                Integer.BYTES * HASH_MULTIPLIER * (long) entryCount;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        // We can use either the getRegionSizeInBytes or getPositionsSizeInBytes
        // from the underlying raw blocks to implement this function. We chose
        // getPositionsSizeInBytes with the assumption that constructing a
        // positions array is cheaper than calling getRegionSizeInBytes for each
        // used position.
        int positionCount = getPositionCount();
        checkValidPositions(positions, positionCount);
        boolean[] entryPositions = new boolean[getRawKeyBlock().getPositionCount()];
        int usedEntryCount = 0;
        int usedPositionCount = 0;
        for (int i = 0; i < positions.length; ++i) {
            if (positions[i]) {
                usedPositionCount++;
                int entriesStart = getOffsets()[getOffsetBase() + i];
                int entriesEnd = getOffsets()[getOffsetBase() + i + 1];
                for (int j = entriesStart; j < entriesEnd; j++) {
                    entryPositions[j] = true;
                }
                usedEntryCount += (entriesEnd - entriesStart);
            }
        }
        return getRawKeyBlock().getPositionsSizeInBytes(entryPositions) +
                getRawValueBlock().getPositionsSizeInBytes(entryPositions) +
                (Integer.BYTES + Byte.BYTES) * (long) usedPositionCount +
                Integer.BYTES * HASH_MULTIPLIER * (long) usedEntryCount;
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + length);
        Block newKeys = getRawKeyBlock().copyRegion(startValueOffset, endValueOffset - startValueOffset);
        Block newValues = getRawValueBlock().copyRegion(startValueOffset, endValueOffset - startValueOffset);

        int[] newOffsets = compactOffsets(getOffsets(), position + getOffsetBase(), length);
        boolean[] mapIsNull = getMapIsNull();
        boolean[] newMapIsNull = mapIsNull == null ? null : compactArray(mapIsNull, position + getOffsetBase(), length);
        int[] rawHashTables = getHashTables().tryGet().orElse(null);
        int[] newRawHashTables = null;
        int expectedNewHashTableEntries = (endValueOffset - startValueOffset) * HASH_MULTIPLIER;
        if (rawHashTables != null) {
            newRawHashTables = compactArray(rawHashTables, startValueOffset * HASH_MULTIPLIER, expectedNewHashTableEntries);
        }

        if (newKeys == getRawKeyBlock() && newValues == getRawValueBlock() && newOffsets == getOffsets() && newMapIsNull == mapIsNull && newRawHashTables == rawHashTables) {
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
                new MapHashTables(mapType, Optional.ofNullable(newRawHashTables)));
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        if (clazz != Block.class) {
            throw new IllegalArgumentException("clazz must be Block.class");
        }
        checkReadablePosition(position);

        int startEntryOffset = getOffset(position);
        int endEntryOffset = getOffset(position + 1);
        return clazz.cast(new SingleMapBlock(
                startEntryOffset * 2,
                (endEntryOffset - startEntryOffset) * 2,
                this));
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + 1);
        int valueLength = endValueOffset - startValueOffset;
        Block newKeys = getRawKeyBlock().copyRegion(startValueOffset, valueLength);
        Block newValues = getRawValueBlock().copyRegion(startValueOffset, valueLength);
        int[] rawHashTables = getHashTables().tryGet().orElse(null);
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
                new MapHashTables(mapType, Optional.ofNullable(newRawHashTables)));
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        checkReadablePosition(position);

        if (isNull(position)) {
            return 0;
        }

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + 1);

        long size = 0;
        Block rawKeyBlock = getRawKeyBlock();
        Block rawValueBlock = getRawValueBlock();
        for (int i = startValueOffset; i < endValueOffset; i++) {
            size += rawKeyBlock.getEstimatedDataSizeForStats(i);
            size += rawValueBlock.getEstimatedDataSizeForStats(i);
        }
        return size;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        boolean[] mapIsNull = getMapIsNull();
        return mapIsNull != null && mapIsNull[position + getOffsetBase()];
    }

    // only visible for testing
    public boolean isHashTablesPresent()
    {
        return getHashTables().tryGet().isPresent();
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
