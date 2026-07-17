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
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.io.api.Binary;

import static java.util.Objects.requireNonNull;

/**
 * Adapts a parquet-mr {@link org.apache.parquet.column.values.ValuesWriter} to the Trino
 * {@link ValuesWriter}. Binary values arriving as a Slice are materialized to a Binary only here,
 * where the underlying encoder requires it.
 */
public class ParquetValuesWriterAdapter
        extends ValuesWriter
{
    private final org.apache.parquet.column.values.ValuesWriter delegate;

    public ParquetValuesWriterAdapter(org.apache.parquet.column.values.ValuesWriter delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    public org.apache.parquet.column.values.ValuesWriter getDelegate()
    {
        return delegate;
    }

    @Override
    public long getBufferedSize()
    {
        return delegate.getBufferedSize();
    }

    @Override
    public BytesInput getBytes()
    {
        return delegate.getBytes();
    }

    @Override
    public Encoding getEncoding()
    {
        return delegate.getEncoding();
    }

    @Override
    public void reset()
    {
        delegate.reset();
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public DictionaryPage toDictPageAndClose()
    {
        return delegate.toDictPageAndClose();
    }

    @Override
    public void resetDictionary()
    {
        delegate.resetDictionary();
    }

    @Override
    public long getAllocatedSize()
    {
        return delegate.getAllocatedSize();
    }

    @Override
    public void writeByte(int value)
    {
        delegate.writeByte(value);
    }

    @Override
    public void writeBoolean(boolean value)
    {
        delegate.writeBoolean(value);
    }

    @Override
    public void writeBytes(Slice value)
    {
        delegate.writeBytes(Binary.fromReusedByteArray(value.byteArray(), value.byteArrayOffset(), value.length()));
    }

    @Override
    public void writeBytes(Binary value)
    {
        delegate.writeBytes(value);
    }

    @Override
    public void writeInteger(int value)
    {
        delegate.writeInteger(value);
    }

    @Override
    public void writeLong(long value)
    {
        delegate.writeLong(value);
    }

    @Override
    public void writeDouble(double value)
    {
        delegate.writeDouble(value);
    }

    @Override
    public void writeFloat(float value)
    {
        delegate.writeFloat(value);
    }

    @Override
    public String memUsageString(String prefix)
    {
        return delegate.memUsageString(prefix);
    }
}
