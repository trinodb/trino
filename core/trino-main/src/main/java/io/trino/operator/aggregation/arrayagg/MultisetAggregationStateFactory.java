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
package io.trino.operator.aggregation.arrayagg;

import io.trino.array.ObjectBigArray;
import io.trino.operator.aggregation.state.AbstractGroupedAccumulatorState;
import io.trino.spi.block.Block;
import io.trino.spi.function.AccumulatorStateFactory;

public class MultisetAggregationStateFactory
        implements AccumulatorStateFactory<MultisetAggregationState>
{
    @Override
    public MultisetAggregationState createSingleState()
    {
        return new SingleState();
    }

    @Override
    public MultisetAggregationState createGroupedState()
    {
        return new GroupedState();
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements MultisetAggregationState
    {
        private final ObjectBigArray<Block> multisets = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(int size)
        {
            multisets.ensureCapacity(size);
        }

        @Override
        public Block get()
        {
            return multisets.get(getGroupId());
        }

        @Override
        public void set(Block value)
        {
            Block previous = multisets.getAndSet(getGroupId(), value);
            if (value != null) {
                size += value.getRetainedSizeInBytes();
            }
            if (previous != null) {
                size -= previous.getRetainedSizeInBytes();
            }
        }

        @Override
        public long getEstimatedSize()
        {
            return size + multisets.sizeOf();
        }
    }

    public static class SingleState
            implements MultisetAggregationState
    {
        private Block multiset;

        @Override
        public Block get()
        {
            return multiset;
        }

        @Override
        public void set(Block value)
        {
            multiset = value;
        }

        @Override
        public long getEstimatedSize()
        {
            return multiset == null ? 0 : multiset.getRetainedSizeInBytes();
        }
    }
}
