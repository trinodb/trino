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
package io.trino.operator.aggregation.state;

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.type.TrinoNumber;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class NumberStateFactory
        implements AccumulatorStateFactory<NumberState>
{
    @Override
    public NumberState createSingleState()
    {
        return new SingleNumberState();
    }

    @Override
    public NumberState createGroupedState()
    {
        return new GroupedNumberState();
    }

    public static class SingleNumberState
            implements NumberState
    {
        private static final int INSTANCE_SIZE = instanceSize(SingleNumberState.class);
        private TrinoNumber value;

        @Override
        public TrinoNumber getValue()
        {
            return value;
        }

        @Override
        public void setValue(TrinoNumber value)
        {
            this.value = value;
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (value != null) {
                estimatedSize += value.getRetainedSizeInBytes();
            }
            return estimatedSize;
        }
    }

    public static class GroupedNumberState
            extends AbstractGroupedAccumulatorState
            implements NumberState
    {
        private static final int INSTANCE_SIZE = instanceSize(GroupedNumberState.class);
        private final ObjectBigArray<TrinoNumber> values = new ObjectBigArray<>();
        private long valuesSizeInBytes;

        @Override
        public void ensureCapacity(int size)
        {
            values.ensureCapacity(size);
        }

        @Override
        public TrinoNumber getValue()
        {
            return values.get(getGroupId());
        }

        @Override
        public void setValue(TrinoNumber value)
        {
            requireNonNull(value, "value is null");
            TrinoNumber previous = values.get(getGroupId());
            if (previous != null) {
                valuesSizeInBytes -= previous.getRetainedSizeInBytes();
            }
            values.set(getGroupId(), value);
            valuesSizeInBytes += value.getRetainedSizeInBytes();
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + values.sizeOf() + valuesSizeInBytes;
        }
    }
}
