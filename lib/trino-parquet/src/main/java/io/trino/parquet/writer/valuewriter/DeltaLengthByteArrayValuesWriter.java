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
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;

/**
 * Writes lengths of byte arrays with delta encoding, followed by the concatenated byte arrays.
 * The byte arrays are written into slabs of bounded size that are kept across pages, avoiding a per-value Binary
 * on the write path and a contiguous buffer the size of the page.
 */
public final class DeltaLengthByteArrayValuesWriter
        extends ValuesWriter
{
    private static final int MAXIMUM_SLAB_SIZE = 1024 * 1024;

    private final org.apache.parquet.column.values.ValuesWriter lengthWriter;
    private final ByteBufferAllocator allocator;
    private final int initialSlabSize;
    private final List<ByteBuffer> slabs = new ArrayList<>();
    private int currentSlab;
    private long allocatedSlabBytes;
    private long size;

    public DeltaLengthByteArrayValuesWriter(int initialSize, int pageSize, ByteBufferAllocator allocator)
    {
        this.lengthWriter = new DeltaBinaryPackingValuesWriterForInteger(
                DeltaBinaryPackingValuesWriter.DEFAULT_NUM_BLOCK_VALUES,
                DeltaBinaryPackingValuesWriter.DEFAULT_NUM_MINIBLOCKS,
                initialSize,
                pageSize,
                allocator);
        this.allocator = allocator;
        this.initialSlabSize = max(1, min(initialSize, MAXIMUM_SLAB_SIZE));
    }

    @Override
    public long getBufferedSize()
    {
        return lengthWriter.getBufferedSize() + size;
    }

    @Override
    public BytesInput getBytes()
    {
        ImmutableList.Builder<ByteBuffer> values = ImmutableList.builder();
        for (ByteBuffer slab : slabs) {
            if (slab.position() > 0) {
                values.add(slab.duplicate().flip());
            }
        }
        return BytesInput.concat(lengthWriter.getBytes(), BytesInput.from(values.build()));
    }

    @Override
    public Encoding getEncoding()
    {
        return Encoding.DELTA_LENGTH_BYTE_ARRAY;
    }

    @Override
    public void reset()
    {
        lengthWriter.reset();
        slabs.forEach(ByteBuffer::clear);
        currentSlab = 0;
        size = 0;
    }

    @Override
    public long getAllocatedSize()
    {
        return lengthWriter.getAllocatedSize() + allocatedSlabBytes;
    }

    @Override
    public void writeBytes(Slice value)
    {
        lengthWriter.writeInteger(value.length());
        writeValue(value, 0, value.length());
    }

    @Override
    public void writeBytes(Slice base, int offset, int length)
    {
        lengthWriter.writeInteger(length);
        writeValue(base, offset, length);
    }

    private void writeValue(Slice base, int offset, int length)
    {
        byte[] bytes = base.byteArray();
        int position = base.byteArrayOffset() + offset;
        int remaining = length;
        while (remaining > 0) {
            if (currentSlab == slabs.size()) {
                int slabSize = slabs.isEmpty() ? initialSlabSize : min(slabs.getLast().capacity() * 2, MAXIMUM_SLAB_SIZE);
                slabs.add(allocator.allocate(slabSize));
                allocatedSlabBytes += slabSize;
            }
            ByteBuffer slab = slabs.get(currentSlab);
            int batch = min(remaining, slab.remaining());
            slab.put(bytes, position, batch);
            position += batch;
            remaining -= batch;
            if (!slab.hasRemaining()) {
                currentSlab++;
            }
        }
        size += length;
    }

    @Override
    public String memUsageString(String prefix)
    {
        return lengthWriter.memUsageString(format("%s DeltaLengthByteArrayValuesWriter %d bytes", prefix, allocatedSlabBytes));
    }

    @Override
    public void close()
    {
        lengthWriter.close();
        slabs.forEach(allocator::release);
        slabs.clear();
        allocatedSlabBytes = 0;
        currentSlab = 0;
        size = 0;
    }
}
