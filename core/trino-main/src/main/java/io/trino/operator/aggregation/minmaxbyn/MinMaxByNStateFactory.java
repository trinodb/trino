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
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.GroupedAccumulatorState;
import io.trino.spi.type.ArrayType;

import java.util.function.LongFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;

public final class MinMaxByNStateFactory
{
    private abstract static class AbstractMinMaxByNState
            implements MinMaxByNState
    {
        abstract TypedKeyValueHeap getTypedKeyValueHeap();

        @Override
        public final void merge(MinMaxByNState other)
        {
            SqlRow sqlRow = ((SingleMinMaxByNState) other).removeTempSerializedState();
            int rawIndex = sqlRow.getRawIndex();

            int capacity = toIntExact(BIGINT.getLong(sqlRow.getRawFieldBlock(0), rawIndex));
            initialize(capacity);
            TypedKeyValueHeap typedKeyValueHeap = getTypedKeyValueHeap();

            Block keys = new ArrayType(typedKeyValueHeap.getKeyType()).getObject(sqlRow.getRawFieldBlock(1), rawIndex);
            Block values = new ArrayType(typedKeyValueHeap.getValueType()).getObject(sqlRow.getRawFieldBlock(2), rawIndex);

            ValueBlock rawKeyValues = keys.getUnderlyingValueBlock();
            ValueBlock rawValueValues = values.getUnderlyingValueBlock();
            for (int i = 0; i < keys.getPositionCount(); i++) {
                typedKeyValueHeap.add(rawKeyValues, keys.getUnderlyingValuePosition(i), rawValueValues, values.getUnderlyingValuePosition(i));
            }
        }

        @Override
        public final void serialize(BlockBuilder out)
        {
            TypedKeyValueHeap typedHeap = getTypedKeyValueHeap();
            if (typedHeap == null) {
                out.appendNull();
            }
            else {
                ((RowBlockBuilder) out).buildEntry(fieldBuilders -> {
                    BIGINT.writeLong(fieldBuilders.get(0), typedHeap.getCapacity());

                    ArrayBlockBuilder keysColumn = (ArrayBlockBuilder) fieldBuilders.get(1);
                    ArrayBlockBuilder valuesColumn = (ArrayBlockBuilder) fieldBuilders.get(2);
                    keysColumn.buildEntry(keyBuilder -> valuesColumn.buildEntry(valueBuilder -> typedHeap.writeAllUnsorted(keyBuilder, valueBuilder)));
                });
            }
        }
    }

    public abstract static class GroupedMinMaxByNState
            extends AbstractMinMaxByNState
            implements GroupedAccumulatorState
    {
        private static final int INSTANCE_SIZE = instanceSize(GroupedMinMaxByNState.class);

        private final LongFunction<TypedKeyValueHeap> heapFactory;

        private final ObjectBigArray<TypedKeyValueHeap> heaps = new ObjectBigArray<>();
        private int groupId;
        private long size;

        public GroupedMinMaxByNState(LongFunction<TypedKeyValueHeap> heapFactory)
        {
            this.heapFactory = heapFactory;
        }

        @Override
        public final void setGroupId(int groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public final void ensureCapacity(int size)
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
                setTypedKeyValueHeapNew(typedHeap);
                size += typedHeap.getEstimatedSize();
            }
        }

        @Override
        public final void add(ValueBlock keyBlock, int keyPosition, ValueBlock valueBlock, int valuePosition)
        {
            TypedKeyValueHeap typedHeap = getTypedKeyValueHeap();

            size -= typedHeap.getEstimatedSize();
            typedHeap.add(keyBlock, keyPosition, valueBlock, valuePosition);
            size += typedHeap.getEstimatedSize();
        }

        @Override
        public final void popAll(BlockBuilder out)
        {
            TypedKeyValueHeap typedHeap = getTypedKeyValueHeap();
            if (typedHeap == null || typedHeap.isEmpty()) {
                out.appendNull();
                return;
            }

            size -= typedHeap.getEstimatedSize();
            ((ArrayBlockBuilder) out).buildEntry(typedHeap::writeValuesSorted);
            size += typedHeap.getEstimatedSize();
        }

        @Override
        final TypedKeyValueHeap getTypedKeyValueHeap()
        {
            return heaps.get(groupId);
        }

        private void setTypedKeyValueHeapNew(TypedKeyValueHeap value)
        {
            heaps.set(groupId, value);
        }
    }

    public abstract static class SingleMinMaxByNState
            extends AbstractMinMaxByNState
    {
        private static final int INSTANCE_SIZE = instanceSize(SingleMinMaxByNState.class);

        private final LongFunction<TypedKeyValueHeap> heapFactory;

        private TypedKeyValueHeap typedHeap;
        private SqlRow tempSerializedState;

        public SingleMinMaxByNState(LongFunction<TypedKeyValueHeap> heapFactory)
        {
            this.heapFactory = heapFactory;
        }

        // for copying
        protected SingleMinMaxByNState(SingleMinMaxByNState state)
        {
            // tempSerializedState should never be set during a copy operation it is only used during deserialization
            checkArgument(state.tempSerializedState == null);
            tempSerializedState = null;

            this.heapFactory = state.heapFactory;

            if (state.typedHeap != null) {
                this.typedHeap = new TypedKeyValueHeap(state.typedHeap);
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
        public final void add(ValueBlock keyBlock, int keyPosition, ValueBlock valueBlock, int valuePosition)
        {
            typedHeap.add(keyBlock, keyPosition, valueBlock, valuePosition);
        }

        @Override
        public final void popAll(BlockBuilder out)
        {
            if (typedHeap == null || typedHeap.isEmpty()) {
                out.appendNull();
                return;
            }

            ((ArrayBlockBuilder) out).buildEntry(typedHeap::writeValuesSorted);
        }

        @Override
        final TypedKeyValueHeap getTypedKeyValueHeap()
        {
            return typedHeap;
        }

        void setTempSerializedState(SqlRow tempSerializedState)
        {
            this.tempSerializedState = tempSerializedState;
        }

        SqlRow removeTempSerializedState()
        {
            SqlRow sqlRow = tempSerializedState;
            checkState(sqlRow != null, "tempDeserializeBlock is null");
            tempSerializedState = null;
            return sqlRow;
        }
    }
}
