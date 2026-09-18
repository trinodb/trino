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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;

import java.io.IOException;
import java.io.UncheckedIOException;

import static java.lang.String.format;

/**
 * Writes lengths of byte arrays with delta encoding, followed by the concatenated byte arrays.
 * The byte arrays are written into a Slice, avoiding a per-value Binary on the write path.
 */
public final class DeltaLengthByteArrayValuesWriter
        extends ValuesWriter
{
    private final org.apache.parquet.column.values.ValuesWriter lengthWriter;
    private final SliceOutput sliceOutput;

    public DeltaLengthByteArrayValuesWriter(int initialSize, int pageSize, ByteBufferAllocator allocator)
    {
        this.lengthWriter = new DeltaBinaryPackingValuesWriterForInteger(
                DeltaBinaryPackingValuesWriter.DEFAULT_NUM_BLOCK_VALUES,
                DeltaBinaryPackingValuesWriter.DEFAULT_NUM_MINIBLOCKS,
                initialSize,
                pageSize,
                allocator);
        this.sliceOutput = new DynamicSliceOutput(initialSize);
    }

    @Override
    public long getBufferedSize()
    {
        return lengthWriter.getBufferedSize() + sliceOutput.size();
    }

    @Override
    public BytesInput getBytes()
    {
        return BytesInput.concat(lengthWriter.getBytes(), BytesInput.from(sliceOutput.slice().toByteBuffer()));
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
        sliceOutput.reset();
    }

    @Override
    public long getAllocatedSize()
    {
        return lengthWriter.getAllocatedSize() + sliceOutput.getRetainedSize();
    }

    @Override
    public void writeBytes(Slice value)
    {
        lengthWriter.writeInteger(value.length());
        sliceOutput.writeBytes(value);
    }

    @Override
    public void writeBytes(Slice base, int offset, int length)
    {
        lengthWriter.writeInteger(length);
        sliceOutput.writeBytes(base, offset, length);
    }

    @Override
    public String memUsageString(String prefix)
    {
        return lengthWriter.memUsageString(format("%s DeltaLengthByteArrayValuesWriter %d bytes", prefix, sliceOutput.getRetainedSize()));
    }

    @Override
    public void close()
    {
        lengthWriter.close();
        try {
            sliceOutput.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
