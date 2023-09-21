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

import io.trino.array.BooleanBigArray;
import io.trino.array.LongBigArray;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import jakarta.annotation.Nullable;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.System.arraycopy;

public class LongDecimalWithOverflowStateFactory
        implements AccumulatorStateFactory<LongDecimalWithOverflowState>
{
    @Override
    public LongDecimalWithOverflowState createSingleState()
    {
        return new SingleLongDecimalWithOverflowState();
    }

    @Override
    public LongDecimalWithOverflowState createGroupedState()
    {
        return new GroupedLongDecimalWithOverflowState();
    }

    public static class GroupedLongDecimalWithOverflowState
            extends AbstractGroupedAccumulatorState
            implements LongDecimalWithOverflowState
    {
        private static final int INSTANCE_SIZE = instanceSize(GroupedLongDecimalWithOverflowState.class);
        private final BooleanBigArray isNotNull = new BooleanBigArray();
        /**
         * Stores 128-bit decimals as pairs of longs
         */
        private final LongBigArray unscaledDecimals = new LongBigArray();
        @Nullable
        private LongBigArray overflows; // lazily initialized on the first overflow

        @Override
        public void ensureCapacity(long size)
        {
            isNotNull.ensureCapacity(size);
            unscaledDecimals.ensureCapacity(size * 2);
            if (overflows != null) {
                overflows.ensureCapacity(size);
            }
        }

        @Override
        public boolean isNotNull()
        {
            return isNotNull.get(getGroupId());
        }

        @Override
        public void setNotNull()
        {
            isNotNull.set(getGroupId(), true);
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
                overflows.ensureCapacity(isNotNull.getCapacity());
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
                    overflows.ensureCapacity(isNotNull.getCapacity());
                }
                overflows.add(groupId, overflow);
            }
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + isNotNull.sizeOf() + unscaledDecimals.sizeOf() + (overflows == null ? 0 : overflows.sizeOf());
        }
    }

    public static class SingleLongDecimalWithOverflowState
            implements LongDecimalWithOverflowState
    {
        private static final int INSTANCE_SIZE = instanceSize(SingleLongDecimalWithOverflowState.class);
        private static final int SIZE = (int) sizeOf(new long[2]) + SIZE_OF_BYTE + SIZE_OF_LONG;

        private final long[] unscaledDecimal = new long[2];
        private boolean isNotNull;
        private long overflow;

        public SingleLongDecimalWithOverflowState() {}

        // for copying
        private SingleLongDecimalWithOverflowState(long[] unscaledDecimal, boolean isNotNull, long overflow)
        {
            arraycopy(unscaledDecimal, 0, this.unscaledDecimal, 0, 2);
            this.isNotNull = isNotNull;
            this.overflow = overflow;
        }

        @Override
        public boolean isNotNull()
        {
            return isNotNull;
        }

        @Override
        public void setNotNull()
        {
            isNotNull = true;
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
            return new SingleLongDecimalWithOverflowState(unscaledDecimal, isNotNull, overflow);
        }
    }
}
