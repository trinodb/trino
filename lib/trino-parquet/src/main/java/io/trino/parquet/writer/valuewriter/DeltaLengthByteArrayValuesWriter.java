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

import io.airlift.slice.Slice;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;

import static java.lang.Math.min;
import static java.lang.String.format;

/**
 * Writes lengths of byte arrays with delta encoding, followed by the concatenated byte arrays.
 * The byte arrays are written into slabs of bounded size, avoiding a per-value Binary on the write path
 * and a contiguous buffer the size of the page.
 */
public final class DeltaLengthByteArrayValuesWriter
        extends ValuesWriter
{
    private static final int MAXIMUM_SLAB_SIZE = 1024 * 1024;
    // CapacityByteArrayOutputStream stops doubling its slabs at a fifth of its capacity hint
    private static final int SLAB_CAPACITY_HINT = 5 * MAXIMUM_SLAB_SIZE;

    private final org.apache.parquet.column.values.ValuesWriter lengthWriter;
    private final int initialSize;
    private final ByteBufferAllocator allocator;
    private CapacityByteArrayOutputStream valuesOutput;

    public DeltaLengthByteArrayValuesWriter(int initialSize, int pageSize, ByteBufferAllocator allocator)
    {
        this.lengthWriter = new DeltaBinaryPackingValuesWriterForInteger(
                DeltaBinaryPackingValuesWriter.DEFAULT_NUM_BLOCK_VALUES,
                DeltaBinaryPackingValuesWriter.DEFAULT_NUM_MINIBLOCKS,
                initialSize,
                pageSize,
                allocator);
        this.initialSize = min(initialSize, MAXIMUM_SLAB_SIZE);
        this.allocator = allocator;
        this.valuesOutput = newValuesOutput();
    }

    private CapacityByteArrayOutputStream newValuesOutput()
    {
        return new CapacityByteArrayOutputStream(initialSize, SLAB_CAPACITY_HINT, allocator);
    }

    @Override
    public long getBufferedSize()
    {
        return lengthWriter.getBufferedSize() + valuesOutput.size();
    }

    @Override
    public BytesInput getBytes()
    {
        return BytesInput.concat(lengthWriter.getBytes(), BytesInput.from(valuesOutput));
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
        // CapacityByteArrayOutputStream.reset() sizes its first slab from the released page, which can exceed the maximum slab size
        valuesOutput.close();
        valuesOutput = newValuesOutput();
    }

    @Override
    public long getAllocatedSize()
    {
        return lengthWriter.getAllocatedSize() + valuesOutput.getCapacity();
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

    // a value larger than the current slab would otherwise be given a slab of its own size
    private void writeValue(Slice base, int offset, int length)
    {
        int written = 0;
        while (written < length) {
            int batch = min(length - written, MAXIMUM_SLAB_SIZE);
            valuesOutput.write(base.byteArray(), base.byteArrayOffset() + offset + written, batch);
            written += batch;
        }
    }

    @Override
    public String memUsageString(String prefix)
    {
        return lengthWriter.memUsageString(format("%s DeltaLengthByteArrayValuesWriter %d bytes", prefix, valuesOutput.getCapacity()));
    }

    @Override
    public void close()
    {
        lengthWriter.close();
        valuesOutput.close();
    }
}
