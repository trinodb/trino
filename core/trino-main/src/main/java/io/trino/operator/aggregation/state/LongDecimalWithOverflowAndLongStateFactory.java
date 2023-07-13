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
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;

import javax.annotation.Nullable;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.System.arraycopy;

public class LongDecimalWithOverflowAndLongStateFactory
        implements AccumulatorStateFactory<LongDecimalWithOverflowAndLongState>
{
    @Override
    public LongDecimalWithOverflowAndLongState createSingleState()
    {
        return new SingleLongDecimalWithOverflowAndLongState();
    }

    @Override
    public LongDecimalWithOverflowAndLongState createGroupedState()
    {
        return new GroupedLongDecimalWithOverflowAndLongState();
    }

    private static final class GroupedLongDecimalWithOverflowAndLongState
            extends AbstractGroupedAccumulatorState
            implements LongDecimalWithOverflowAndLongState
    {
        private static final int INSTANCE_SIZE = instanceSize(GroupedLongDecimalWithOverflowAndLongState.class);
        private final LongBigArray longs = new LongBigArray();
        /**
         * Stores 128-bit decimals as pairs of longs
         */
        private final LongBigArray unscaledDecimals = new LongBigArray();
        @Nullable
        private LongBigArray overflows; // lazily initialized on the first overflow

        @Override
        public void ensureCapacity(long size)
        {
            longs.ensureCapacity(size);
            unscaledDecimals.ensureCapacity(size * 2);
            if (overflows != null) {
                overflows.ensureCapacity(size);
            }
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
        public void addLong(long value)
        {
            longs.add(getGroupId(), value);
        }

        @Override
        public long[] getDecimalArray()
        {
            return unscaledDecimals.getSegment(getGroupId() * 2);
        }

        @Override
        public int getDecimalArrayOffset()
        {
            return unscaledDecimals.getOffset(getGroupId() * 2);
        }

        @Override
        public long getOverflow()
        {
            if (overflows == null) {
                return 0;
            }
            return overflows.get(getGroupId());
        }

        @Override
        public void setOverflow(long overflow)
        {
            // setOverflow(0) must overwrite any existing overflow value
            if (overflow == 0 && overflows == null) {
                return;
            }
            long groupId = getGroupId();
            if (overflows == null) {
                overflows = new LongBigArray();
                overflows.ensureCapacity(longs.getCapacity());
            }
            overflows.set(groupId, overflow);
        }

        @Override
        public void addOverflow(long overflow)
        {
            if (overflow != 0) {
                long groupId = getGroupId();
                if (overflows == null) {
                    overflows = new LongBigArray();
                    overflows.ensureCapacity(longs.getCapacity());
                }
                overflows.add(groupId, overflow);
            }
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + longs.sizeOf() + unscaledDecimals.sizeOf() + (overflows == null ? 0 : overflows.sizeOf());
        }
    }

    private static final class SingleLongDecimalWithOverflowAndLongState
            implements LongDecimalWithOverflowAndLongState
    {
        private static final int INSTANCE_SIZE = instanceSize(SingleLongDecimalWithOverflowAndLongState.class);
        private static final int SIZE = (int) sizeOf(new long[2]) + SIZE_OF_LONG + SIZE_OF_LONG;

        private final long[] unscaledDecimal = new long[2];
        private long longValue;
        private long overflow;

        public SingleLongDecimalWithOverflowAndLongState() {}

        // for copying
        private SingleLongDecimalWithOverflowAndLongState(long[] unscaledDecimal, long longValue, long overflow)
        {
            arraycopy(unscaledDecimal, 0, this.unscaledDecimal, 0, 2);
            this.longValue = longValue;
            this.overflow = overflow;
        }

        @Override
        public long getLong()
        {
            return longValue;
        }

        @Override
        public void setLong(long longValue)
        {
            this.longValue = longValue;
        }

        @Override
        public void addLong(long value)
        {
            longValue += value;
        }

        @Override
        public long[] getDecimalArray()
        {
            return unscaledDecimal;
        }

        @Override
        public int getDecimalArrayOffset()
        {
            return 0;
        }

        @Override
        public long getOverflow()
        {
            return overflow;
        }

        @Override
        public void setOverflow(long overflow)
        {
            this.overflow = overflow;
        }

        @Override
        public void addOverflow(long overflow)
        {
            this.overflow += overflow;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + SIZE;
        }

        @Override
        public AccumulatorState copy()
        {
            return new SingleLongDecimalWithOverflowAndLongState(unscaledDecimal, longValue, overflow);
        }
    }
}
