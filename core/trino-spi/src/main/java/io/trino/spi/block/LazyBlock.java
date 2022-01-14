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

import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class LazyBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LazyBlock.class).instanceSize() + ClassLayout.parseClass(LazyData.class).instanceSize();

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
    public int getSliceLength(int position)
    {
        return getBlock().getSliceLength(position);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        return getBlock().getByte(position, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        return getBlock().getShort(position, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        return getBlock().getInt(position, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        return getBlock().getLong(position, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        return getBlock().getSlice(position, offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        return getBlock().getObject(position, clazz);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        return getBlock().bytesEqual(position, offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        return getBlock().bytesCompare(
                position,
                offset,
                length,
                otherSlice,
                otherOffset,
                otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        getBlock().writeBytesTo(position, offset, length, blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        return getBlock().equals(
                position,
                offset,
                otherBlock,
                otherPosition,
                otherOffset,
                length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        return getBlock().hash(position, offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        return getBlock().compareTo(
                leftPosition,
                leftOffset,
                leftLength,
                rightBlock,
                rightPosition,
                rightOffset,
                rightLength);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        return getBlock().getSingleValueBlock(position);
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
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        if (!isLoaded()) {
            return 0;
        }
        return getBlock().getPositionsSizeInBytes(positions);
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
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        getBlock().retainedBytesForEachPart(consumer);
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return LazyBlockEncoding.NAME;
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

    @Override
    public final List<Block> getChildren()
    {
        return singletonList(getBlock());
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
         * If block is unloaded, add the listeners; otherwise call this method on child blocks
         */
        @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
        private static void addListenersRecursive(Block block, List<Consumer<Block>> listeners)
        {
            if (block instanceof LazyBlock) {
                LazyData lazyData = ((LazyBlock) block).lazyData;
                if (!lazyData.isTopLevelBlockLoaded()) {
                    lazyData.addListeners(listeners);
                    return;
                }
            }

            for (Block child : block.getChildren()) {
                addListenersRecursive(child, listeners);
            }
        }
    }
}
