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
package io.trino.execution.buffer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.round;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SpoolingOutputStats
{
    private final int partitionCount;
    private final AtomicLong rowCount = new AtomicLong();
    private volatile AtomicLongArray partitionDataSizes;
    private volatile Snapshot finalSnapshot;

    public SpoolingOutputStats(int partitionCount)
    {
        checkArgument(partitionCount > 0, "partitionCount must be greater than zero");
        this.partitionCount = partitionCount;
        partitionDataSizes = new AtomicLongArray(partitionCount);
    }

    public void updatePartitionDataSize(int partition, long dataSizeInBytes)
    {
        checkPositionIndex(partition, partitionCount);
        checkArgument(dataSizeInBytes >= 0, "dataSizeInBytes must be greater than or equal to zero");
        AtomicLongArray partitionDataSizes = this.partitionDataSizes;
        if (partitionDataSizes != null) {
            partitionDataSizes.addAndGet(partition, dataSizeInBytes);
        }
    }

    public void updateRowCount(long rowCount)
    {
        this.rowCount.addAndGet(rowCount);
    }

    public void finish()
    {
        AtomicLongArray partitionDataSizes = this.partitionDataSizes;
        this.partitionDataSizes = null;
        if (partitionDataSizes == null) {
            // already processed
            return;
        }
        finalSnapshot = createSnapshot(partitionDataSizes, rowCount);
    }

    public Optional<Snapshot> getFinalSnapshot()
    {
        return Optional.ofNullable(finalSnapshot);
    }

    private static Snapshot createSnapshot(AtomicLongArray partitionDataSizes, AtomicLong rowCount)
    {
        int size = partitionDataSizes.length();
        Slice values = Slices.allocate(Short.BYTES * size);
        for (int i = 0; i < size; i++) {
            values.setShort(Short.BYTES * i, truncate(partitionDataSizes.get(i)));
        }
        return new Snapshot(values, rowCount.get());
    }

    private static short truncate(long value)
    {
        int floatBits = floatToRawIntBits((float) value);
        // always positive, drop the sign
        floatBits <<= 1;
        // truncate to two bytes
        floatBits >>>= 16;
        return (short) floatBits;
    }

    private static long expand(short value)
    {
        int floatBits = value & 0xFFFF;
        // recover mantissa
        floatBits <<= 16;
        // recover the sign
        floatBits >>>= 1;
        return round((double) intBitsToFloat(floatBits));
    }

    public static class Snapshot
    {
        private final Slice partitionDataSizes;
        private final long rowCount;

        @JsonCreator
        public Snapshot(@JsonProperty("partitionDataSizes") Slice partitionDataSizes, @JsonProperty("rowCount") long rowCount)
        {
            this.partitionDataSizes = requireNonNull(partitionDataSizes, "partitionDataSizes is null");
            this.rowCount = rowCount;
        }

        // visible for Jackson
        @JsonProperty
        public Slice getPartitionDataSizes()
        {
            return partitionDataSizes;
        }

        @JsonProperty
        public long getRowCount()
        {
            return rowCount;
        }

        public long getPartitionSizeInBytes(int partition)
        {
            int partitionCount = partitionDataSizes.length() / Short.BYTES;
            checkArgument(partition < partitionCount, "partition must be less than %s", partitionCount);
            return expand(partitionDataSizes.getShort(partition * Short.BYTES));
        }
    }
}
