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

import io.trino.array.LongBigArray;
import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.type.TrinoNumber;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class LongAndNumberStateFactory
        implements AccumulatorStateFactory<LongAndNumberState>
{
    @Override
    public LongAndNumberState createSingleState()
    {
        return new SingleLongAndNumberState();
    }

    @Override
    public LongAndNumberState createGroupedState()
    {
        return new GroupedLongAndNumberState();
    }

    public static class SingleLongAndNumberState
            implements LongAndNumberState
    {
        private static final int INSTANCE_SIZE = instanceSize(SingleLongAndNumberState.class);
        private long longValue;
        private TrinoNumber number;

        @Override
        public long getLong()
        {
            return longValue;
        }

        @Override
        public void setLong(long value)
        {
            this.longValue = value;
        }

        @Override
        public TrinoNumber getNumber()
        {
            return number;
        }

        @Override
        public void setNumber(TrinoNumber number)
        {
            this.number = number;
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (number != null) {
                estimatedSize += number.getRetainedSizeInBytes();
            }
            return estimatedSize;
        }
    }

    public static class GroupedLongAndNumberState
            extends AbstractGroupedAccumulatorState
            implements LongAndNumberState
    {
        private static final int INSTANCE_SIZE = instanceSize(GroupedLongAndNumberState.class);
        private final LongBigArray longs = new LongBigArray();
        private final ObjectBigArray<TrinoNumber> numbers = new ObjectBigArray<>();
        private long numbersSizeInBytes;

        @Override
        public void ensureCapacity(int size)
        {
            longs.ensureCapacity(size);
            numbers.ensureCapacity(size);
        }

        @Override
        public long getLong()
        {
            return longs.get(getGroupId());
        }

        @Override
        public void setLong(long value)
        {
            longs.set(getGroupId(), value);
        }

        @Override
        public TrinoNumber getNumber()
        {
            return numbers.get(getGroupId());
        }

        @Override
        public void setNumber(TrinoNumber value)
        {
            requireNonNull(value, "value is null");
            TrinoNumber previous = numbers.get(getGroupId());
            if (previous != null) {
                numbersSizeInBytes -= previous.getRetainedSizeInBytes();
            }
            numbers.set(getGroupId(), value);
            numbersSizeInBytes += value.getRetainedSizeInBytes();
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + longs.sizeOf() + numbers.sizeOf() + numbersSizeInBytes;
        }
    }
}
