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
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SingleRowBlock;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.GroupedAccumulatorState;
import io.trino.spi.type.ArrayType;

import java.util.function.LongFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class MinMaxNStateFactory
{
    private MinMaxNStateFactory() {}

    private abstract static class AbstractMinMaxNState
            implements MinMaxNState
    {
        abstract TypedHeap getTypedHeap();

        @Override
        public final void merge(MinMaxNState other)
        {
            SingleRowBlock serializedState = ((SingleMinMaxNState) other).removeTempSerializedState();

            int capacity = toIntExact(BIGINT.getLong(serializedState, 0));
            initialize(capacity);
            TypedHeap typedHeap = getTypedHeap();

            Block values = new ArrayType(typedHeap.getElementType()).getObject(serializedState, 1);
            typedHeap.addAll(values);
        }

        @Override
        public final void serialize(BlockBuilder out)
        {
            TypedHeap typedHeap = getTypedHeap();
            if (typedHeap == null) {
                out.appendNull();
            }
            else {
                ((RowBlockBuilder) out).buildEntry(fieldBuilders -> {
                    BIGINT.writeLong(fieldBuilders.get(0), typedHeap.getCapacity());

                    ((ArrayBlockBuilder) fieldBuilders.get(1)).buildEntry(typedHeap::writeAllUnsorted);
                });
            }
        }
    }

    public abstract static class GroupedMinMaxNState
            extends AbstractMinMaxNState
            implements GroupedAccumulatorState
    {
        private static final int INSTANCE_SIZE = instanceSize(GroupedMinMaxNState.class);

        private final LongFunction<TypedHeap> heapFactory;

        private final ObjectBigArray<TypedHeap> heaps = new ObjectBigArray<>();
        private long groupId;
        private long size;

        public GroupedMinMaxNState(LongFunction<TypedHeap> heapFactory)
        {
            this.heapFactory = heapFactory;
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
        public final void writeAllSorted(BlockBuilder out)
        {
            TypedHeap typedHeap = getTypedHeap();
            if (typedHeap == null || typedHeap.isEmpty()) {
                out.appendNull();
                return;
            }

            ((ArrayBlockBuilder) out).buildEntry(typedHeap::writeAllSorted);
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

        private TypedHeap typedHeap;
        private SingleRowBlock tempSerializedState;

        public SingleMinMaxNState(LongFunction<TypedHeap> heapFactory)
        {
            this.heapFactory = requireNonNull(heapFactory, "heapFactory is null");
        }

        protected SingleMinMaxNState(SingleMinMaxNState state)
        {
            // tempSerializedState should never be set during a copy operation it is only used during deserialization
            checkArgument(state.tempSerializedState == null);
            tempSerializedState = null;

            this.heapFactory = state.heapFactory;

            if (state.typedHeap != null) {
                this.typedHeap = new TypedHeap(state.typedHeap);
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
        public final void writeAllSorted(BlockBuilder out)
        {
            if (typedHeap == null || typedHeap.isEmpty()) {
                out.appendNull();
                return;
            }

            ((ArrayBlockBuilder) out).buildEntry(typedHeap::writeAllSorted);
        }

        @Override
        final TypedHeap getTypedHeap()
        {
            return typedHeap;
        }

        void setTempSerializedState(SingleRowBlock tempSerializedState)
        {
            this.tempSerializedState = tempSerializedState;
        }

        SingleRowBlock removeTempSerializedState()
        {
            SingleRowBlock block = tempSerializedState;
            checkState(block != null, "tempDeserializeBlock is null");
            tempSerializedState = null;
            return block;
        }
    }
}
