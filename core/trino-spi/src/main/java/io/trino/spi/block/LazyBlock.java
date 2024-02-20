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

import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.function.Consumer;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

// This class is not considered thread-safe.
public final class LazyBlock
        implements Block
{
    private static final int INSTANCE_SIZE = instanceSize(LazyBlock.class) + instanceSize(LazyData.class);

    private final int positionCount;
    private final LazyData lazyData;

    public LazyBlock(int positionCount, LazyBlockLoader loader)
    {
        this.positionCount = positionCount;
        this.lazyData = new LazyData(positionCount, loader);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public ValueBlock getSingleValueBlock(int position)
    {
        return getBlock().getSingleValueBlock(position);
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        if (!isLoaded()) {
            return OptionalInt.empty();
        }
        return getBlock().fixedSizeInBytesPerPosition();
    }

    @Override
    public long getSizeInBytes()
    {
        if (!isLoaded()) {
            return 0;
        }
        return getBlock().getSizeInBytes();
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        if (!isLoaded()) {
            return 0;
        }
        return getBlock().getRegionSizeInBytes(position, length);
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions, int selectedPositionsCount)
    {
        if (!isLoaded()) {
            return 0;
        }
        return getBlock().getPositionsSizeInBytes(positions, selectedPositionsCount);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        if (!isLoaded()) {
            return INSTANCE_SIZE;
        }
        return INSTANCE_SIZE + getBlock().getRetainedSizeInBytes();
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return getBlock().getEstimatedDataSizeForStats(position);
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        getBlock().retainedBytesForEachPart(consumer);
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return LazyBlockEncoding.NAME;
    }

    @Override
    public Block copyWithAppendedNull()
    {
        throw new UnsupportedOperationException("LazyBlock does not support newBlockWithAppendedNull()");
    }

    @Override
    public Block getPositions(int[] positions, int offset, int length)
    {
        if (isLoaded()) {
            return getBlock().getPositions(positions, offset, length);
        }
        checkArrayRange(positions, offset, length);
        return new LazyBlock(length, new PositionLazyBlockLoader(lazyData, positions, offset, length));
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        return getBlock().copyPositions(positions, offset, length);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        if (isLoaded()) {
            return getBlock().getRegion(positionOffset, length);
        }
        checkValidRegion(getPositionCount(), positionOffset, length);
        return new LazyBlock(length, new RegionLazyBlockLoader(lazyData, positionOffset, length));
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        return getBlock().copyRegion(position, length);
    }

    @Override
    public boolean isNull(int position)
    {
        return getBlock().isNull(position);
    }

    @Override
    public boolean mayHaveNull()
    {
        return getBlock().mayHaveNull();
    }

    public Block getBlock()
    {
        return lazyData.getTopLevelBlock();
    }

    @Override
    public boolean isLoaded()
    {
        return lazyData.isFullyLoaded();
    }

    @Override
    public Block getLoadedBlock()
    {
        return lazyData.getFullyLoadedBlock();
    }

    @Override
    public ValueBlock getUnderlyingValueBlock()
    {
        return getBlock().getUnderlyingValueBlock();
    }

    @Override
    public int getUnderlyingValuePosition(int position)
    {
        return getBlock().getUnderlyingValuePosition(position);
    }

    public static void listenForLoads(Block block, Consumer<Block> listener)
    {
        requireNonNull(block, "block is null");
        requireNonNull(listener, "listener is null");

        LazyData.addListenersRecursive(block, singletonList(listener));
    }

    private static class RegionLazyBlockLoader
            implements LazyBlockLoader
    {
        private final LazyData delegate;
        private final int positionOffset;
        private final int length;

        public RegionLazyBlockLoader(LazyData delegate, int positionOffset, int length)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.positionOffset = positionOffset;
            this.length = length;
        }

        @Override
        public Block load()
        {
            return delegate.getTopLevelBlock().getRegion(positionOffset, length);
        }
    }

    private static class PositionLazyBlockLoader
            implements LazyBlockLoader
    {
        private final LazyData delegate;
        private final int[] positions;
        private final int offset;
        private final int length;

        public PositionLazyBlockLoader(LazyData delegate, int[] positions, int offset, int length)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.positions = requireNonNull(positions, "positions is null");
            this.offset = offset;
            this.length = length;
        }

        @Override
        public Block load()
        {
            return delegate.getTopLevelBlock().getPositions(positions, offset, length);
        }
    }

    private static class LazyData
    {
        private final int positionsCount;
        @Nullable
        private LazyBlockLoader loader;
        @Nullable
        private Block block;
        @Nullable
        private List<Consumer<Block>> listeners;

        public LazyData(int positionsCount, LazyBlockLoader loader)
        {
            this.positionsCount = positionsCount;
            this.loader = requireNonNull(loader, "loader is null");
        }

        public boolean isFullyLoaded()
        {
            return block != null && block.isLoaded();
        }

        public boolean isTopLevelBlockLoaded()
        {
            return block != null;
        }

        public Block getTopLevelBlock()
        {
            load(false);
            return block;
        }

        public Block getFullyLoadedBlock()
        {
            if (block != null) {
                return block.getLoadedBlock();
            }

            load(true);
            return block;
        }

        private void addListeners(List<Consumer<Block>> listeners)
        {
            if (isTopLevelBlockLoaded()) {
                throw new IllegalStateException("Top level block is already loaded");
            }
            if (this.listeners == null) {
                this.listeners = new ArrayList<>();
            }
            this.listeners.addAll(listeners);
        }

        private void load(boolean recursive)
        {
            if (loader == null) {
                return;
            }

            block = requireNonNull(loader.load(), "loader returned null");
            if (block.getPositionCount() != positionsCount) {
                throw new IllegalStateException(format("Loaded block positions count (%s) doesn't match lazy block positions count (%s)", block.getPositionCount(), positionsCount));
            }

            if (recursive) {
                block = block.getLoadedBlock();
            }
            else {
                // load and remove directly nested lazy blocks
                while (block instanceof LazyBlock) {
                    block = ((LazyBlock) block).getBlock();
                }
            }

            // clear reference to loader to free resources, since load was successful
            loader = null;

            // notify listeners
            List<Consumer<Block>> listeners = this.listeners;
            this.listeners = null;
            if (listeners != null) {
                listeners.forEach(listener -> listener.accept(block));

                // add listeners to unloaded child blocks
                if (!recursive) {
                    addListenersRecursive(block, listeners);
                }
            }
        }

        /**
         * If the block is unloaded, add the listeners; otherwise call this method on the nested blocks
         */
        private static void addListenersRecursive(Block block, List<Consumer<Block>> listeners)
        {
            if (block == null) {
                return;
            }

            switch (block) {
                case LazyBlock lazyBlock -> {
                    LazyData lazyData = lazyBlock.lazyData;
                    if (lazyData.isTopLevelBlockLoaded()) {
                        addListenersRecursive(lazyBlock.getBlock(), listeners);
                    }
                    else {
                        lazyData.addListeners(listeners);
                    }
                }
                case DictionaryBlock dictionaryBlock -> addListenersRecursive(dictionaryBlock.getDictionary(), listeners);
                case RunLengthEncodedBlock runLengthEncodedBlock -> addListenersRecursive(runLengthEncodedBlock.getValue(), listeners);
                case ValueBlock valueBlock -> {
                    switch (valueBlock) {
                        case ArrayBlock arrayBlock -> addListenersRecursive(arrayBlock.getRawElementBlock(), listeners);
                        case MapBlock mapBlock -> {
                            addListenersRecursive(mapBlock.getRawKeyBlock(), listeners);
                            addListenersRecursive(mapBlock.getRawValueBlock(), listeners);
                        }
                        case RowBlock rowBlock -> {
                            for (Block fieldBlock : rowBlock.getFieldBlocks()) {
                                addListenersRecursive(fieldBlock, listeners);
                            }
                        }
                        default -> {
                            // no other value blocks are container types
                        }
                    }
                }
            }
        }
    }
}
