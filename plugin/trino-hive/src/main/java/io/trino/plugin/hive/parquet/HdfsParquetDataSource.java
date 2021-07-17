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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.parquet.ChunkReader;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.util.FSDataInputStreamTail;
import io.trino.spi.TrinoException;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
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
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, format("Error reading tail from %s with length %s", id, length), e);
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
        catch (TrinoException e) {
            // just in case there is a Trino wrapper or hook
            throw e;
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, format("Error reading from %s at position %s", id, position), e);
        }
        long currentReadTimeNanos = System.nanoTime() - start;

        readTimeNanos += currentReadTimeNanos;
        stats.readDataBytesPerSecond(bufferLength, currentReadTimeNanos);
    }

    @Override
    public final <K> ListMultimap<K, ChunkReader> planRead(ListMultimap<K, DiskRange> diskRanges)
    {
        requireNonNull(diskRanges, "diskRanges is null");

        if (diskRanges.isEmpty()) {
            return ImmutableListMultimap.of();
        }

        //
        // Note: this code does not use the stream APIs to avoid any extra object allocation
        //

        // split disk ranges into "big" and "small"
        ImmutableListMultimap.Builder<K, DiskRange> smallRangesBuilder = ImmutableListMultimap.builder();
        ImmutableListMultimap.Builder<K, DiskRange> largeRangesBuilder = ImmutableListMultimap.builder();
        for (Map.Entry<K, DiskRange> entry : diskRanges.entries()) {
            if (entry.getValue().getLength() <= options.getMaxBufferSize().toBytes()) {
                smallRangesBuilder.put(entry);
            }
            else {
                largeRangesBuilder.put(entry);
            }
        }
        ListMultimap<K, DiskRange> smallRanges = smallRangesBuilder.build();
        ListMultimap<K, DiskRange> largeRanges = largeRangesBuilder.build();

        // read ranges
        ImmutableListMultimap.Builder<K, ChunkReader> slices = ImmutableListMultimap.builder();
        slices.putAll(readSmallDiskRanges(smallRanges));
        slices.putAll(readLargeDiskRanges(largeRanges));

        return slices.build();
    }

    private <K> ListMultimap<K, ChunkReader> readSmallDiskRanges(ListMultimap<K, DiskRange> diskRanges)
    {
        if (diskRanges.isEmpty()) {
            return ImmutableListMultimap.of();
        }

        Iterable<DiskRange> mergedRanges = mergeAdjacentDiskRanges(diskRanges.values(), options.getMaxMergeDistance(), options.getMaxBufferSize());

        ImmutableListMultimap.Builder<K, ChunkReader> slices = ImmutableListMultimap.builder();
        for (DiskRange mergedRange : mergedRanges) {
            ReferenceCountedReader mergedRangeLoader = new ReferenceCountedReader(mergedRange);

            for (Map.Entry<K, DiskRange> diskRangeEntry : diskRanges.entries()) {
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

        ListMultimap<K, ChunkReader> sliceStreams = slices.build();
        verify(sliceStreams.keySet().equals(diskRanges.keySet()));
        return sliceStreams;
    }

    private <K> ListMultimap<K, ChunkReader> readLargeDiskRanges(ListMultimap<K, DiskRange> diskRanges)
    {
        if (diskRanges.isEmpty()) {
            return ImmutableListMultimap.of();
        }

        ImmutableListMultimap.Builder<K, ChunkReader> slices = ImmutableListMultimap.builder();
        for (Map.Entry<K, DiskRange> entry : diskRanges.entries()) {
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
