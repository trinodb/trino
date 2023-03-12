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
package io.trino.operator.aggregation.minmaxbyn;

import io.trino.array.ObjectBigArray;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.GroupedAccumulatorState;

import java.util.function.Function;
import java.util.function.LongFunction;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;

public final class MinMaxByNStateFactory
{
    private abstract static class AbstractMinMaxByNState
            implements MinMaxByNState
    {
        abstract TypedKeyValueHeap getTypedKeyValueHeap();
    }

    public abstract static class GroupedMinMaxByNState
            extends AbstractMinMaxByNState
            implements GroupedAccumulatorState
    {
        private static final int INSTANCE_SIZE = instanceSize(GroupedMinMaxByNState.class);

        private final LongFunction<TypedKeyValueHeap> heapFactory;
        private final Function<Block, TypedKeyValueHeap> deserializer;

        private final ObjectBigArray<TypedKeyValueHeap> heaps = new ObjectBigArray<>();
        private long groupId;
        private long size;

        public GroupedMinMaxByNState(LongFunction<TypedKeyValueHeap> heapFactory, Function<Block, TypedKeyValueHeap> deserializer)
        {
            this.heapFactory = heapFactory;
            this.deserializer = deserializer;
        }

        @Override
        public final void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public final void ensureCapacity(long size)
        {
            heaps.ensureCapacity(size);
        }

        @Override
        public final long getEstimatedSize()
        {
            return INSTANCE_SIZE + heaps.sizeOf() + size;
        }

        @Override
        public final void initialize(long n)
        {
            if (getTypedKeyValueHeap() == null) {
                TypedKeyValueHeap typedHeap = heapFactory.apply(n);
                setTypedKeyValueHeap(typedHeap);
                size += typedHeap.getEstimatedSize();
            }
        }

        @Override
        public final void add(Block keyBlock, Block valueBlock, int position)
        {
            TypedKeyValueHeap typedHeap = getTypedKeyValueHeap();

            size -= typedHeap.getEstimatedSize();
            typedHeap.add(keyBlock, valueBlock, position);
            size += typedHeap.getEstimatedSize();
        }

        @Override
        public final void merge(MinMaxByNState other)
        {
            TypedKeyValueHeap otherTypedHeap = ((AbstractMinMaxByNState) other).getTypedKeyValueHeap();
            if (otherTypedHeap == null) {
                return;
            }

            TypedKeyValueHeap typedHeap = getTypedKeyValueHeap();
            if (typedHeap == null) {
                setTypedKeyValueHeap(otherTypedHeap);
                size += otherTypedHeap.getEstimatedSize();
            }
            else {
                size -= typedHeap.getEstimatedSize();
                typedHeap.addAll(otherTypedHeap);
                size += typedHeap.getEstimatedSize();
            }
        }

        @Override
        public final void popAll(BlockBuilder out)
        {
            TypedKeyValueHeap typedHeap = getTypedKeyValueHeap();
            if (typedHeap == null || typedHeap.isEmpty()) {
                out.appendNull();
                return;
            }

            BlockBuilder arrayBlockBuilder = out.beginBlockEntry();

            size -= typedHeap.getEstimatedSize();
            typedHeap.popAllReverse(arrayBlockBuilder);
            size += typedHeap.getEstimatedSize();

            out.closeEntry();
        }

        @Override
        public final void serialize(BlockBuilder out)
        {
            TypedKeyValueHeap typedHeap = getTypedKeyValueHeap();
            if (typedHeap == null) {
                out.appendNull();
            }
            else {
                typedHeap.serialize(out);
            }
        }

        @Override
        public final void deserialize(Block rowBlock)
        {
            checkState(getTypedKeyValueHeap() == null, "State already initialized");

            TypedKeyValueHeap typedHeap = deserializer.apply(rowBlock);
            setTypedKeyValueHeap(typedHeap);
            size += typedHeap.getEstimatedSize();
        }

        @Override
        final TypedKeyValueHeap getTypedKeyValueHeap()
        {
            return heaps.get(groupId);
        }

        private void setTypedKeyValueHeap(TypedKeyValueHeap value)
        {
            heaps.set(groupId, value);
        }
    }

    public abstract static class SingleMinMaxByNState
            extends AbstractMinMaxByNState
    {
        private static final int INSTANCE_SIZE = instanceSize(SingleMinMaxByNState.class);

        private final LongFunction<TypedKeyValueHeap> heapFactory;
        private final Function<Block, TypedKeyValueHeap> deserializer;

        private TypedKeyValueHeap typedHeap;

        public SingleMinMaxByNState(LongFunction<TypedKeyValueHeap> heapFactory, Function<Block, TypedKeyValueHeap> deserializer)
        {
            this.heapFactory = heapFactory;
            this.deserializer = deserializer;
        }

        // for copying
        protected SingleMinMaxByNState(SingleMinMaxByNState state)
        {
            this.heapFactory = state.heapFactory;
            this.deserializer = state.deserializer;

            if (state.typedHeap != null) {
                this.typedHeap = state.typedHeap.copy();
            }
            else {
                this.typedHeap = null;
            }
        }

        @Override
        public abstract AccumulatorState copy();

        @Override
        public final long getEstimatedSize()
        {
            return INSTANCE_SIZE + (typedHeap == null ? 0 : typedHeap.getEstimatedSize());
        }

        @Override
        public final void initialize(long n)
        {
            if (typedHeap == null) {
                typedHeap = heapFactory.apply(n);
            }
        }

        @Override
        public final void add(Block keyBlock, Block valueBlock, int position)
        {
            typedHeap.add(keyBlock, valueBlock, position);
        }

        @Override
        public final void merge(MinMaxByNState other)
        {
            TypedKeyValueHeap otherTypedHeap = ((AbstractMinMaxByNState) other).getTypedKeyValueHeap();
            if (otherTypedHeap == null) {
                return;
            }
            if (typedHeap == null) {
                typedHeap = otherTypedHeap;
            }
            else {
                typedHeap.addAll(otherTypedHeap);
            }
        }

        @Override
        public final void popAll(BlockBuilder out)
        {
            if (typedHeap == null || typedHeap.isEmpty()) {
                out.appendNull();
                return;
            }

            BlockBuilder arrayBlockBuilder = out.beginBlockEntry();
            typedHeap.popAllReverse(arrayBlockBuilder);
            out.closeEntry();
        }

        @Override
        public final void serialize(BlockBuilder out)
        {
            if (typedHeap == null) {
                out.appendNull();
            }
            else {
                typedHeap.serialize(out);
            }
        }

        @Override
        public final void deserialize(Block rowBlock)
        {
            typedHeap = deserializer.apply(rowBlock);
        }

        @Override
        final TypedKeyValueHeap getTypedKeyValueHeap()
        {
            return typedHeap;
        }
    }
}
