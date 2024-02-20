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

import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class BufferedMapValueBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(BufferedMapValueBuilder.class);

    private final MapType mapType;
    private final HashBuildMode hashBuildMode;
    private BlockBuilder keyBlockBuilder;
    private BlockBuilder valueBlockBuilder;
    private int bufferSize;

    public static BufferedMapValueBuilder createBuffered(MapType mapType)
    {
        return new BufferedMapValueBuilder(mapType, HashBuildMode.DUPLICATE_NOT_CHECKED, 1024);
    }

    public static BufferedMapValueBuilder createBufferedStrict(MapType mapType)
    {
        return new BufferedMapValueBuilder(mapType, HashBuildMode.STRICT_EQUALS, 1024);
    }

    public static BufferedMapValueBuilder createBufferedDistinctStrict(MapType mapType)
    {
        return new BufferedMapValueBuilder(mapType, HashBuildMode.STRICT_NOT_DISTINCT_FROM, 1024);
    }

    BufferedMapValueBuilder(MapType mapType, HashBuildMode hashBuildMode, int bufferSize)
    {
        this.mapType = requireNonNull(mapType, "mapType is null");
        this.hashBuildMode = hashBuildMode;
        this.keyBlockBuilder = mapType.getKeyType().createBlockBuilder(null, bufferSize);
        this.valueBlockBuilder = mapType.getValueType().createBlockBuilder(null, bufferSize);
        this.bufferSize = bufferSize;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + keyBlockBuilder.getRetainedSizeInBytes() + valueBlockBuilder.getRetainedSizeInBytes();
    }

    public <E extends Throwable> SqlMap build(int entryCount, MapValueBuilder<E> builder)
            throws E
    {
        if (keyBlockBuilder.getPositionCount() != valueBlockBuilder.getPositionCount()) {
            // we could fix this by appending nulls to the shorter builder, but this is a sign the buffer is being used in a multithreaded environment which is not supported
            throw new IllegalStateException("Key and value builders were corrupted by a previous call to buildValue");
        }

        // grow or reset builders if necessary
        if (keyBlockBuilder.getPositionCount() + entryCount > bufferSize) {
            if (bufferSize < entryCount) {
                bufferSize = entryCount;
            }
            keyBlockBuilder = keyBlockBuilder.newBlockBuilderLike(bufferSize, null);
            valueBlockBuilder = valueBlockBuilder.newBlockBuilderLike(bufferSize, null);
        }

        int startSize = keyBlockBuilder.getPositionCount();

        // build the map
        try {
            builder.build(keyBlockBuilder, valueBlockBuilder);
        }
        catch (Exception e) {
            equalizeBlockBuilders();
            throw e;
        }

        // check that key and value builders have the same size
        if (equalizeBlockBuilders()) {
            throw new IllegalStateException("Expected key and value builders to have the same size");
        }
        int endSize = keyBlockBuilder.getPositionCount();

        // build the map block
        Block keyBlock = keyBlockBuilder.build().getRegion(startSize, endSize - startSize);
        Block valueBlock = valueBlockBuilder.build().getRegion(startSize, endSize - startSize);
        return new SqlMap(mapType, hashBuildMode, keyBlock, valueBlock);
    }

    private boolean equalizeBlockBuilders()
    {
        int keyBlockSize = keyBlockBuilder.getPositionCount();
        if (keyBlockSize == valueBlockBuilder.getPositionCount()) {
            return false;
        }

        // append nulls to even out the blocks
        int expectedSize = max(keyBlockSize, valueBlockBuilder.getPositionCount());
        while (keyBlockBuilder.getPositionCount() < expectedSize) {
            keyBlockBuilder.appendNull();
        }
        while (valueBlockBuilder.getPositionCount() < expectedSize) {
            valueBlockBuilder.appendNull();
        }
        return true;
    }
}
