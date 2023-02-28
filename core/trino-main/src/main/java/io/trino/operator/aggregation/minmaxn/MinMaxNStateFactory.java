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
package io.trino.operator.aggregation.minmaxn;

import io.trino.array.ObjectBigArray;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.GroupedAccumulatorState;

import java.util.function.Function;
import java.util.function.LongFunction;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public final class MinMaxNStateFactory
{
    private MinMaxNStateFactory() {}

    private abstract static class AbstractMinMaxNState
            implements MinMaxNState
    {
        abstract TypedHeap getTypedHeap();
    }

    public abstract static class GroupedMinMaxNState
            extends AbstractMinMaxNState
            implements GroupedAccumulatorState
    {
        private static final int INSTANCE_SIZE = instanceSize(GroupedMinMaxNState.class);

        private final LongFunction<TypedHeap> heapFactory;
        private final Function<Block, TypedHeap> deserializer;

        private final ObjectBigArray<TypedHeap> heaps = new ObjectBigArray<>();
        private long groupId;
        private long size;

        public GroupedMinMaxNState(LongFunction<TypedHeap> heapFactory, Function<Block, TypedHeap> deserializer)
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
            if (getTypedHeap() == null) {
                TypedHeap typedHeap = heapFactory.apply(n);
                setTypedHeap(typedHeap);
                size += typedHeap.getEstimatedSize();
            }
        }

        @Override
        public final void add(Block block, int position)
        {
            TypedHeap typedHeap = getTypedHeap();

            size -= typedHeap.getEstimatedSize();
            typedHeap.add(block, position);
            size += typedHeap.getEstimatedSize();
        }

        @Override
        public final void merge(MinMaxNState other)
        {
            TypedHeap otherTypedHeap = ((AbstractMinMaxNState) other).getTypedHeap();
            if (otherTypedHeap == null) {
                return;
            }

            TypedHeap typedHeap = getTypedHeap();
            if (typedHeap == null) {
                setTypedHeap(otherTypedHeap);
                size += otherTypedHeap.getEstimatedSize();
            }
            else {
                size -= typedHeap.getEstimatedSize();
                typedHeap.addAll(otherTypedHeap);
                size += typedHeap.getEstimatedSize();
            }
        }

        @Override
        public final void writeAll(BlockBuilder out)
        {
            TypedHeap typedHeap = getTypedHeap();
            if (typedHeap == null || typedHeap.isEmpty()) {
                out.appendNull();
                return;
            }

            BlockBuilder arrayBlockBuilder = out.beginBlockEntry();

            typedHeap.writeAll(arrayBlockBuilder);

            out.closeEntry();
        }

        @Override
        public final void serialize(BlockBuilder out)
        {
            TypedHeap typedHeap = getTypedHeap();
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
            checkState(getTypedHeap() == null, "State already initialized");

            TypedHeap typedHeap = deserializer.apply(rowBlock);
            setTypedHeap(typedHeap);
            size += typedHeap.getEstimatedSize();
        }

        @Override
        final TypedHeap getTypedHeap()
        {
            return heaps.get(groupId);
        }

        private void setTypedHeap(TypedHeap value)
        {
            heaps.set(groupId, value);
        }
    }

    public abstract static class SingleMinMaxNState
            extends AbstractMinMaxNState
    {
        private static final int INSTANCE_SIZE = instanceSize(SingleMinMaxNState.class);

        private final LongFunction<TypedHeap> heapFactory;
        private final Function<Block, TypedHeap> deserializer;

        private TypedHeap typedHeap;

        public SingleMinMaxNState(LongFunction<TypedHeap> heapFactory, Function<Block, TypedHeap> deserializer)
        {
            this.heapFactory = requireNonNull(heapFactory, "heapFactory is null");
            this.deserializer = requireNonNull(deserializer, "deserializer is null");
        }

        protected SingleMinMaxNState(SingleMinMaxNState state)
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
        public final void add(Block block, int position)
        {
            typedHeap.add(block, position);
        }

        @Override
        public final void merge(MinMaxNState other)
        {
            TypedHeap otherTypedHeap = ((AbstractMinMaxNState) other).getTypedHeap();
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
        public final void writeAll(BlockBuilder out)
        {
            if (typedHeap == null || typedHeap.isEmpty()) {
                out.appendNull();
                return;
            }

            BlockBuilder arrayBlockBuilder = out.beginBlockEntry();
            typedHeap.writeAll(arrayBlockBuilder);
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
        final TypedHeap getTypedHeap()
        {
            return typedHeap;
        }
    }
}
