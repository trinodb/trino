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
package io.trino.parquet.writer.valuewriter;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import static java.lang.Math.max;
import static org.apache.parquet.column.Encoding.DELTA_LENGTH_BYTE_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLengthByteArrayValuesWriter
{
    private static final int INITIAL_SIZE = 64;
    private static final int PAGE_SIZE = 8 * 1024 * 1024;
    private static final int MAXIMUM_SLAB_SIZE = 1024 * 1024;
    // the delta-encoded lengths of the test pages are far smaller
    private static final int SLAB_ALLOCATION_THRESHOLD = 64 * 1024;

    @Test
    public void testBytesAreLengthsFollowedByValues()
            throws IOException
    {
        // 6 MB of values spans several slabs, and the last value alone is larger than a slab
        List<Slice> values = ImmutableList.<Slice>builder()
                .addAll(randomValues(3_000, 2_000))
                .addAll(randomValues(1, 3 * 1024 * 1024))
                .build();

        RecordingAllocator allocator = new RecordingAllocator();
        DeltaLengthByteArrayValuesWriter writer = new DeltaLengthByteArrayValuesWriter(INITIAL_SIZE, PAGE_SIZE, allocator);
        for (int i = 0; i < values.size(); i++) {
            Slice value = values.get(i);
            if (i % 2 == 0) {
                writer.writeBytes(value);
            }
            else {
                byte[] padded = new byte[value.length() + 8];
                value.getBytes(0, padded, 3, value.length());
                writer.writeBytes(Slices.wrappedBuffer(padded), 3, value.length());
            }
        }

        assertThat(writer.getEncoding()).isEqualTo(DELTA_LENGTH_BYTE_ARRAY);
        assertThat(writer.getBytes().toByteArray()).isEqualTo(expectedBytes(values));
        assertThat(allocator.largestAllocation).isLessThanOrEqualTo(MAXIMUM_SLAB_SIZE);
    }

    @Test
    public void testSlabsAreReusedAfterLargePage()
            throws IOException
    {
        RecordingAllocator allocator = new RecordingAllocator();
        DeltaLengthByteArrayValuesWriter writer = new DeltaLengthByteArrayValuesWriter(INITIAL_SIZE, PAGE_SIZE, allocator);
        randomValues(4_200, 2_000).forEach(writer::writeBytes);
        writer.getBytes();
        writer.reset();
        int slabAllocations = allocator.slabAllocations;

        List<Slice> secondPage = randomValues(1_900, 2_000);
        secondPage.forEach(writer::writeBytes);
        assertThat(writer.getBytes().toByteArray()).isEqualTo(expectedBytes(secondPage));
        assertThat(allocator.largestAllocation).isLessThanOrEqualTo(MAXIMUM_SLAB_SIZE);
        assertThat(allocator.slabAllocations).isEqualTo(slabAllocations);
        writer.close();
    }

    @Test
    public void testResetStartsNewPage()
            throws IOException
    {
        DeltaLengthByteArrayValuesWriter writer = new DeltaLengthByteArrayValuesWriter(INITIAL_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
        List<Slice> firstPage = randomValues(2_000, 2_000);
        firstPage.forEach(writer::writeBytes);
        assertThat(writer.getBytes().toByteArray()).isEqualTo(expectedBytes(firstPage));
        writer.reset();

        assertThat(writer.getBufferedSize()).isEqualTo(0);
        List<Slice> secondPage = randomValues(1_900, 2_000);
        secondPage.forEach(writer::writeBytes);
        assertThat(writer.getBytes().toByteArray()).isEqualTo(expectedBytes(secondPage));
        writer.close();
    }

    @Test
    public void testSizeAccounting()
    {
        DeltaLengthByteArrayValuesWriter writer = new DeltaLengthByteArrayValuesWriter(INITIAL_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
        ValuesWriter lengthWriter = newLengthWriter();
        assertThat(writer.getBufferedSize()).isEqualTo(lengthWriter.getBufferedSize());
        assertThat(writer.getAllocatedSize()).isEqualTo(lengthWriter.getAllocatedSize());

        long valueBytes = 0;
        for (Slice value : randomValues(1_000, 1_500)) {
            writer.writeBytes(value);
            lengthWriter.writeInteger(value.length());
            valueBytes += value.length();
        }
        assertThat(writer.getBufferedSize()).isEqualTo(lengthWriter.getBufferedSize() + valueBytes);
        assertThat(writer.getAllocatedSize()).isGreaterThanOrEqualTo(lengthWriter.getAllocatedSize() + valueBytes);
        // slabs of at most 1 MB double with the bytes written, so the slack is bounded by the bytes written
        assertThat(writer.getAllocatedSize()).isLessThan(lengthWriter.getAllocatedSize() + 2 * valueBytes);
        assertThat(writer.memUsageString("prefix")).contains("DeltaLengthByteArrayValuesWriter");
    }

    private static byte[] expectedBytes(List<Slice> values)
            throws IOException
    {
        ValuesWriter lengthWriter = newLengthWriter();
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        for (Slice value : values) {
            lengthWriter.writeInteger(value.length());
        }
        lengthWriter.getBytes().writeAllTo(expected);
        for (Slice value : values) {
            expected.write(value.getBytes());
        }
        return expected.toByteArray();
    }

    private static ValuesWriter newLengthWriter()
    {
        return new DeltaBinaryPackingValuesWriterForInteger(
                DeltaBinaryPackingValuesWriter.DEFAULT_NUM_BLOCK_VALUES,
                DeltaBinaryPackingValuesWriter.DEFAULT_NUM_MINIBLOCKS,
                INITIAL_SIZE,
                PAGE_SIZE,
                new HeapByteBufferAllocator());
    }

    private static List<Slice> randomValues(int count, int length)
    {
        Random random = new Random(count * 31L + length);
        ImmutableList.Builder<Slice> values = ImmutableList.builderWithExpectedSize(count);
        for (int i = 0; i < count; i++) {
            byte[] value = new byte[length];
            random.nextBytes(value);
            values.add(Slices.wrappedBuffer(value));
        }
        return values.build();
    }

    private static class RecordingAllocator
            implements ByteBufferAllocator
    {
        private final ByteBufferAllocator delegate = new HeapByteBufferAllocator();
        private int largestAllocation;
        private int slabAllocations;

        @Override
        public ByteBuffer allocate(int size)
        {
            largestAllocation = max(largestAllocation, size);
            if (size >= SLAB_ALLOCATION_THRESHOLD) {
                slabAllocations++;
            }
            return delegate.allocate(size);
        }

        @Override
        public void release(ByteBuffer buffer)
        {
            delegate.release(buffer);
        }

        @Override
        public boolean isDirect()
        {
            return delegate.isDirect();
        }
    }
}
