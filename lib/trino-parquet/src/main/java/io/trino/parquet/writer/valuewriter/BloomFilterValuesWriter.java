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

import com.google.common.annotations.VisibleForTesting;
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
    private final ValuesWriter writer;
    private final BloomFilter bloomFilter;

    public static ValuesWriter createBloomFilterValuesWriter(ValuesWriter writer, Optional<BloomFilter> bloomFilter)
    {
        if (bloomFilter.isPresent()) {
            return new BloomFilterValuesWriter(writer, bloomFilter.orElseThrow());
        }
        return writer;
    }

    private BloomFilterValuesWriter(ValuesWriter writer, BloomFilter bloomFilter)
    {
        this.writer = writer;
        this.bloomFilter = bloomFilter;
    }

    @VisibleForTesting
    public ValuesWriter getWriter()
    {
        return writer;
    }

    @Override
    public long getBufferedSize()
    {
        return writer.getBufferedSize() + bloomFilter.getBitsetSize();
    }

    @Override
    public BytesInput getBytes()
    {
        return writer.getBytes();
    }

    @Override
    public Encoding getEncoding()
    {
        return writer.getEncoding();
    }

    @Override
    public void reset()
    {
        writer.reset();
    }

    @Override
    public void close()
    {
        writer.close();
    }

    @Override
    public DictionaryPage toDictPageAndClose()
    {
        return writer.toDictPageAndClose();
    }

    @Override
    public void resetDictionary()
    {
        writer.resetDictionary();
    }

    @Override
    public long getAllocatedSize()
    {
        return writer.getAllocatedSize();
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
        writer.writeBytes(v);
        bloomFilter.insertHash(bloomFilter.hash(v));
    }

    @Override
    public void writeInteger(int v)
    {
        writer.writeInteger(v);
        bloomFilter.insertHash(bloomFilter.hash(toIntExact(((Number) v).longValue())));
    }

    @Override
    public void writeLong(long v)
    {
        writer.writeLong(v);
        bloomFilter.insertHash(bloomFilter.hash(((Number) v).longValue()));
    }

    @Override
    public void writeDouble(double v)
    {
        writer.writeDouble(v);
        bloomFilter.insertHash(bloomFilter.hash(v));
    }

    @Override
    public void writeFloat(float v)
    {
        writer.writeFloat(v);
        bloomFilter.insertHash(bloomFilter.hash(v));
    }

    @Override
    public String memUsageString(String s)
    {
        return writer.memUsageString(s);
    }
}
