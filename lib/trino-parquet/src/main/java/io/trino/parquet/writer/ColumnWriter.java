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
package io.trino.parquet.writer;

import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.format.ColumnMetaData;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public interface ColumnWriter
{
    void writeBlock(ColumnChunk columnChunk)
            throws IOException;

    void close();

    List<BufferData> getBuffer()
            throws IOException;

    long getBufferedBytes();

    long getRetainedBytes();

    class BufferData
    {
        private final ColumnMetaData metaData;
        private final List<ParquetDataOutput> data;
        private final OptionalInt dictionaryPageSize;
        private final Optional<BloomFilter> bloomFilter;

        public BufferData(List<ParquetDataOutput> data, OptionalInt dictionaryPageSize, Optional<BloomFilter> bloomFilter, ColumnMetaData metaData)
        {
            this.data = requireNonNull(data, "data is null");
            this.dictionaryPageSize = requireNonNull(dictionaryPageSize, "dictionaryPageSize is null");
            this.bloomFilter = requireNonNull(bloomFilter, "bloomFilter is null");
            this.metaData = requireNonNull(metaData, "metaData is null");
        }

        public ColumnMetaData getMetaData()
        {
            return metaData;
        }

        public List<ParquetDataOutput> getData()
        {
            return data;
        }

        public OptionalInt getDictionaryPageSize()
        {
            return dictionaryPageSize;
        }

        public Optional<BloomFilter> getBloomFilter()
        {
            return bloomFilter;
        }
    }
}
