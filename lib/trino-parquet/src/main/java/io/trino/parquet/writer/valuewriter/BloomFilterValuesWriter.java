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

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.io.api.Binary;

import java.util.Optional;

import static java.lang.Math.toIntExact;

public class BloomFilterValuesWriter
        extends ValuesWriter
{
    private final ValuesWriter inner;
    private final BloomFilter filter;

    public static ValuesWriter createBloomFilterValuesWriter(ValuesWriter inner, Optional<BloomFilter> filter)
    {
        if (filter.isPresent()) {
            return new BloomFilterValuesWriter(inner, filter.orElseThrow());
        }
        return inner;
    }

    private BloomFilterValuesWriter(ValuesWriter inner, BloomFilter filter)
    {
        this.inner = inner;
        this.filter = filter;
    }

    @Override
    public long getBufferedSize()
    {
        return inner.getBufferedSize();
    }

    @Override
    public BytesInput getBytes()
    {
        return inner.getBytes();
    }

    @Override
    public Encoding getEncoding()
    {
        return inner.getEncoding();
    }

    @Override
    public void reset()
    {
        inner.reset();
    }

    @Override
    public void close()
    {
        inner.close();
    }

    @Override
    public DictionaryPage toDictPageAndClose()
    {
        return inner.toDictPageAndClose();
    }

    @Override
    public void resetDictionary()
    {
        inner.resetDictionary();
    }

    @Override
    public long getAllocatedSize()
    {
        return inner.getAllocatedSize();
    }

    @Override
    public void writeByte(int value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBoolean(boolean v)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBytes(Binary v)
    {
        inner.writeBytes(v);
        filter.insertHash(filter.hash(v));
    }

    @Override
    public void writeInteger(int v)
    {
        inner.writeInteger(v);
        filter.insertHash(filter.hash(toIntExact(((Number) v).longValue())));
    }

    @Override
    public void writeLong(long v)
    {
        inner.writeLong(v);
        filter.insertHash(filter.hash(((Number) v).longValue()));
    }

    @Override
    public void writeDouble(double v)
    {
        inner.writeDouble(v);
        filter.insertHash(filter.hash(v));
    }

    @Override
    public void writeFloat(float v)
    {
        inner.writeFloat(v);
        filter.insertHash(filter.hash(v));
    }

    @Override
    public String memUsageString(String s)
    {
        return inner.memUsageString(s);
    }
}
