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
package io.prestosql.plugin.hive.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.prestosql.parquet.ChunkReader;
import io.prestosql.parquet.DiskRange;
import io.prestosql.parquet.ParquetDataSource;
import io.prestosql.parquet.ParquetDataSourceId;
import io.prestosql.parquet.ParquetReaderOptions;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.util.FSDataInputStreamTail;
import io.prestosql.spi.PrestoException;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

public class HdfsParquetDataSource
        implements ParquetDataSource
{
    private final ParquetDataSourceId id;
    private final long estimatedSize;
    private final FSDataInputStream inputStream;
    private long readTimeNanos;
    private long readBytes;
    private final FileFormatDataSourceStats stats;
    private final ParquetReaderOptions options;

    public HdfsParquetDataSource(
            ParquetDataSourceId id,
            long estimatedSize,
            FSDataInputStream inputStream,
            FileFormatDataSourceStats stats,
            ParquetReaderOptions options)
    {
        this.id = requireNonNull(id, "id is null");
        this.estimatedSize = estimatedSize;
        this.inputStream = inputStream;
        this.stats = stats;
        this.options = requireNonNull(options, "options is null");
    }

    @Override
    public ParquetDataSourceId getId()
    {
        return id;
    }

    @Override
    public final long getReadBytes()
    {
        return readBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public final long getEstimatedSize()
    {
        return estimatedSize;
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
    }

    @Override
    public Slice readTail(int length)
    {
        long start = System.nanoTime();
        Slice tailSlice;
        try {
            //  Handle potentially imprecise file lengths by reading the footer
            FSDataInputStreamTail fileTail = FSDataInputStreamTail.readTail(getId().toString(), getEstimatedSize(), inputStream, length);
            tailSlice = fileTail.getTailSlice();
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, format("Error reading tail from %s with length %s", id, length), e);
        }
        long currentReadTimeNanos = System.nanoTime() - start;

        readTimeNanos += currentReadTimeNanos;
        readBytes += tailSlice.length();
        return tailSlice;
    }

    @Override
    public final Slice readFully(long position, int length)
    {
        byte[] buffer = new byte[length];
        readFully(position, buffer, 0, length);
        return Slices.wrappedBuffer(buffer);
    }

    private void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        readBytes += bufferLength;

        long start = System.nanoTime();
        try {
            inputStream.readFully(position, buffer, bufferOffset, bufferLength);
        }
        catch (PrestoException e) {
            // just in case there is a Presto wrapper or hook
            throw e;
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, format("Error reading from %s at position %s", id, position), e);
        }
        long currentReadTimeNanos = System.nanoTime() - start;

        readTimeNanos += currentReadTimeNanos;
        stats.readDataBytesPerSecond(bufferLength, currentReadTimeNanos);
    }

    @Override
    public final <K> Map<K, ChunkReader> planRead(Map<K, DiskRange> diskRanges)
    {
        requireNonNull(diskRanges, "diskRanges is null");

        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        //
        // Note: this code does not use the stream APIs to avoid any extra object allocation
        //

        // split disk ranges into "big" and "small"
        ImmutableMap.Builder<K, DiskRange> smallRangesBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<K, DiskRange> largeRangesBuilder = ImmutableMap.builder();
        for (Map.Entry<K, DiskRange> entry : diskRanges.entrySet()) {
            if (entry.getValue().getLength() <= options.getMaxBufferSize().toBytes()) {
                smallRangesBuilder.put(entry);
            }
            else {
                largeRangesBuilder.put(entry);
            }
        }
        Map<K, DiskRange> smallRanges = smallRangesBuilder.build();
        Map<K, DiskRange> largeRanges = largeRangesBuilder.build();

        // read ranges
        ImmutableMap.Builder<K, ChunkReader> slices = ImmutableMap.builder();
        slices.putAll(readSmallDiskRanges(smallRanges));
        slices.putAll(readLargeDiskRanges(largeRanges));

        return slices.build();
    }

    private <K> Map<K, ChunkReader> readSmallDiskRanges(Map<K, DiskRange> diskRanges)
    {
        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        Iterable<DiskRange> mergedRanges = mergeAdjacentDiskRanges(diskRanges.values(), options.getMaxMergeDistance(), options.getMaxBufferSize());

        ImmutableMap.Builder<K, ChunkReader> slices = ImmutableMap.builder();
        for (DiskRange mergedRange : mergedRanges) {
            ReferenceCountedReader mergedRangeLoader = new ReferenceCountedReader(mergedRange);

            for (Map.Entry<K, DiskRange> diskRangeEntry : diskRanges.entrySet()) {
                DiskRange diskRange = diskRangeEntry.getValue();
                if (mergedRange.contains(diskRange)) {
                    mergedRangeLoader.addReference();

                    slices.put(diskRangeEntry.getKey(), new ChunkReader()
                    {
                        @Override
                        public Slice read()
                        {
                            int offset = toIntExact(diskRange.getOffset() - mergedRange.getOffset());
                            return mergedRangeLoader.read().slice(offset, diskRange.getLength());
                        }

                        @Override
                        public void free()
                        {
                            mergedRangeLoader.free();
                        }
                    });
                }
            }

            mergedRangeLoader.free();
        }

        Map<K, ChunkReader> sliceStreams = slices.build();
        verify(sliceStreams.keySet().equals(diskRanges.keySet()));
        return sliceStreams;
    }

    private <K> Map<K, ChunkReader> readLargeDiskRanges(Map<K, DiskRange> diskRanges)
    {
        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<K, ChunkReader> slices = ImmutableMap.builder();
        for (Map.Entry<K, DiskRange> entry : diskRanges.entrySet()) {
            slices.put(entry.getKey(), new ReferenceCountedReader(entry.getValue()));
        }
        return slices.build();
    }

    private static List<DiskRange> mergeAdjacentDiskRanges(Collection<DiskRange> diskRanges, DataSize maxMergeDistance, DataSize maxReadSize)
    {
        // sort ranges by start offset
        List<DiskRange> ranges = new ArrayList<>(diskRanges);
        ranges.sort(comparingLong(DiskRange::getOffset));

        long maxReadSizeBytes = maxReadSize.toBytes();
        long maxMergeDistanceBytes = maxMergeDistance.toBytes();

        // merge overlapping ranges
        ImmutableList.Builder<DiskRange> result = ImmutableList.builder();
        DiskRange last = ranges.get(0);
        for (int i = 1; i < ranges.size(); i++) {
            DiskRange current = ranges.get(i);
            DiskRange merged = null;
            boolean blockTooLong = false;
            try {
                merged = last.span(current);
            }
            catch (ArithmeticException e) {
                blockTooLong = true;
            }
            if (!blockTooLong && merged.getLength() <= maxReadSizeBytes && last.getEnd() + maxMergeDistanceBytes >= current.getOffset()) {
                last = merged;
            }
            else {
                result.add(last);
                last = current;
            }
        }
        result.add(last);

        return result.build();
    }

    private class ReferenceCountedReader
            implements ChunkReader
    {
        private final DiskRange range;
        private Slice data;
        private int referenceCount = 1;

        public ReferenceCountedReader(DiskRange range)
        {
            this.range = range;
        }

        public void addReference()
        {
            checkState(referenceCount > 0, "Chunk reader is already closed");
            referenceCount++;
        }

        @Override
        public Slice read()
        {
            checkState(referenceCount > 0, "Chunk reader is already closed");

            if (data == null) {
                byte[] buffer = new byte[range.getLength()];
                readFully(range.getOffset(), buffer, 0, buffer.length);
                data = Slices.wrappedBuffer(buffer);
            }

            return data;
        }

        @Override
        public void free()
        {
            checkState(referenceCount > 0, "Reference count is already 0");

            referenceCount--;
            if (referenceCount == 0) {
                data = null;
            }
        }
    }
}
