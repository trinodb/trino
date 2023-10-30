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
package io.trino.plugin.hive.parquet;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.parquet.ChunkReader;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.reader.ChunkedInputStream;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class MemoryParquetDataSource
        implements ParquetDataSource
{
    private final ParquetDataSourceId id;
    private final long readTimeNanos;
    private final long readBytes;
    private final LocalMemoryContext memoryUsage;
    @Nullable
    private Slice data;

    public MemoryParquetDataSource(TrinoInputFile inputFile, AggregatedMemoryContext memoryContext, FileFormatDataSourceStats stats)
            throws IOException
    {
        try (TrinoInput input = inputFile.newInput()) {
            long readStart = System.nanoTime();
            this.data = input.readTail(toIntExact(inputFile.length()));
            this.readTimeNanos = System.nanoTime() - readStart;
            stats.readDataBytesPerSecond(data.length(), readTimeNanos);
        }
        this.memoryUsage = memoryContext.newLocalMemoryContext(MemoryParquetDataSource.class.getSimpleName());
        this.memoryUsage.setBytes(data.length());
        this.readBytes = data.length();
        this.id = new ParquetDataSourceId(inputFile.location().toString());
    }

    @Override
    public ParquetDataSourceId getId()
    {
        return id;
    }

    @Override
    public long getReadBytes()
    {
        return readBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getEstimatedSize()
    {
        return readBytes;
    }

    @Override
    public Slice readTail(int length)
    {
        int readSize = min(data.length(), length);
        return readFully(data.length() - readSize, readSize);
    }

    @Override
    public final Slice readFully(long position, int length)
    {
        return data.slice(toIntExact(position), length);
    }

    @Override
    public <K> Map<K, ChunkedInputStream> planRead(ListMultimap<K, DiskRange> diskRanges, AggregatedMemoryContext memoryContext)
    {
        requireNonNull(diskRanges, "diskRanges is null");

        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<K, ChunkedInputStream> builder = ImmutableMap.builder();
        for (Map.Entry<K, Collection<DiskRange>> entry : diskRanges.asMap().entrySet()) {
            List<ChunkReader> chunkReaders = entry.getValue().stream()
                    .map(diskRange -> new ChunkReader()
                    {
                        @Override
                        public long getDiskOffset()
                        {
                            return diskRange.getOffset();
                        }

                        @Override
                        public Slice read()
                        {
                            return data.slice(toIntExact(diskRange.getOffset()), toIntExact(diskRange.getLength()));
                        }

                        @Override
                        public void free() {}
                    })
                    .collect(toImmutableList());
            builder.put(entry.getKey(), new ChunkedInputStream(chunkReaders));
        }
        return builder.buildOrThrow();
    }

    @Override
    public void close()
            throws IOException
    {
        data = null;
        memoryUsage.close();
    }

    @Override
    public final String toString()
    {
        return id.toString();
    }
}
