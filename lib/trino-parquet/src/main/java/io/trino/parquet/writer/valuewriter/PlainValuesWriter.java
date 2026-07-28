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
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;

import java.io.IOException;
import java.io.UncheckedIOException;

import static java.lang.String.format;

/**
 * PLAIN encoder which writes into a Slice, avoiding a per-value Binary on the write path.
 */
public final class PlainValuesWriter
        extends ValuesWriter
{
    private final SliceOutput sliceOutput;

    public PlainValuesWriter(int initialSize)
    {
        this.sliceOutput = new DynamicSliceOutput(initialSize);
    }

    @Override
    public long getBufferedSize()
    {
        return sliceOutput.size();
    }

    @Override
    public BytesInput getBytes()
    {
        return BytesInput.from(sliceOutput.slice().toByteBuffer());
    }

    @Override
    public Encoding getEncoding()
    {
        return Encoding.PLAIN;
    }

    @Override
    public void reset()
    {
        sliceOutput.reset();
    }

    @Override
    public long getAllocatedSize()
    {
        return sliceOutput.getRetainedSize();
    }

    @Override
    public void writeByte(int value)
    {
        sliceOutput.writeByte(value);
    }

    @Override
    public void writeBoolean(boolean value)
    {
        sliceOutput.writeBoolean(value);
    }

    @Override
    public void writeBytes(Slice value)
    {
        sliceOutput.writeInt(value.length());
        sliceOutput.writeBytes(value);
    }

    @Override
    public void writeInteger(int value)
    {
        sliceOutput.writeInt(value);
    }

    @Override
    public void writeLong(long value)
    {
        sliceOutput.writeLong(value);
    }

    @Override
    public void writeDouble(double value)
    {
        sliceOutput.writeDouble(value);
    }

    @Override
    public void writeFloat(float value)
    {
        sliceOutput.writeFloat(value);
    }

    @Override
    public String memUsageString(String prefix)
    {
        return format("%s PlainValuesWriter %d bytes", prefix, sliceOutput.getRetainedSize());
    }

    @Override
    public void close()
    {
        try {
            sliceOutput.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
