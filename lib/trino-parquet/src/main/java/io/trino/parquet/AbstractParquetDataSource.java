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
package io.trino.parquet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.parquet.reader.ChunkedInputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.Math.toIntExact;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

public abstract class AbstractParquetDataSource
        implements ParquetDataSource
{
    private final ParquetDataSourceId id;
    private final long estimatedSize;
    private final ParquetReaderOptions options;
    private long readTimeNanos;
    private long readBytes;

    protected AbstractParquetDataSource(ParquetDataSourceId id, long estimatedSize, ParquetReaderOptions options)
    {
        this.id = requireNonNull(id, "id is null");
        this.estimatedSize = estimatedSize;
        this.options = requireNonNull(options, "options is null");
    }

    protected Slice readTailInternal(int length)
            throws IOException
    {
        return readFully(estimatedSize - length, length);
    }

    protected abstract void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException;

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
    public final long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public final long getEstimatedSize()
    {
        return estimatedSize;
    }

    @Override
    public Slice readTail(int length)
            throws IOException
    {
        long start = System.nanoTime();

        Slice tailSlice = readTailInternal(length);

        readTimeNanos += System.nanoTime() - start;
        readBytes += tailSlice.length();

        return tailSlice;
    }

    @Override
    public final Slice readFully(long position, int length)
            throws IOException
    {
        byte[] buffer = new byte[length];
        readFully(position, buffer, 0, length);
        return Slices.wrappedBuffer(buffer);
    }

    private void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        long start = System.nanoTime();

        readInternal(position, buffer, bufferOffset, bufferLength);

        readTimeNanos += System.nanoTime() - start;
        readBytes += bufferLength;
    }

    @Override
    public final <K> Map<K, ChunkedInputStream> planRead(ListMultimap<K, DiskRange> diskRanges, AggregatedMemoryContext memoryContext)
    {
        requireNonNull(diskRanges, "diskRanges is null");

        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        return planChunksRead(diskRanges, memoryContext).asMap()
                .entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> new ChunkedInputStream(entry.getValue())));
    }

    @VisibleForTesting
    public <K> ListMultimap<K, ChunkReader> planChunksRead(ListMultimap<K, DiskRange> diskRanges, AggregatedMemoryContext memoryContext)
    {
        checkArgument(!diskRanges.isEmpty(), "diskRanges is empty");

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
                largeRangesBuilder.putAll(entry.getKey(), splitLargeRange(entry.getValue()));
            }
        }
        ListMultimap<K, DiskRange> smallRanges = smallRangesBuilder.build();
        ListMultimap<K, DiskRange> largeRanges = largeRangesBuilder.build();

        // read ranges
        ImmutableListMultimap.Builder<K, ChunkReader> slices = ImmutableListMultimap.builder();
        slices.putAll(readSmallDiskRanges(smallRanges, memoryContext));
        slices.putAll(readLargeDiskRanges(largeRanges, memoryContext));
        // Re-order ChunkReaders by their DiskRange offsets as ParquetColumnChunkIterator expects
        // the input slices to be in the order that they're present in the file
        slices.orderValuesBy(comparingLong(ChunkReader::getDiskOffset));

        return slices.build();
    }

    private List<DiskRange> splitLargeRange(DiskRange range)
    {
        int maxBufferSizeBytes = toIntExact(options.getMaxBufferSize().toBytes());
        checkArgument(maxBufferSizeBytes > 0, "maxBufferSize must by larger than zero but is %s bytes", maxBufferSizeBytes);
        ImmutableList.Builder<DiskRange> ranges = ImmutableList.builder();
        long endOffset = range.getOffset() + range.getLength();
        long offset = range.getOffset();
        while (offset + maxBufferSizeBytes < endOffset) {
            ranges.add(new DiskRange(offset, maxBufferSizeBytes));
            offset += maxBufferSizeBytes;
        }

        long lengthLeft = endOffset - offset;
        if (lengthLeft > 0) {
            ranges.add(new DiskRange(offset, toIntExact(lengthLeft)));
        }

        return ranges.build();
    }

    private <K> ListMultimap<K, ChunkReader> readSmallDiskRanges(ListMultimap<K, DiskRange> diskRanges, AggregatedMemoryContext memoryContext)
    {
        if (diskRanges.isEmpty()) {
            return ImmutableListMultimap.of();
        }

        Iterable<DiskRange> mergedRanges = mergeAdjacentDiskRanges(diskRanges.values(), options.getMaxMergeDistance(), options.getMaxBufferSize());

        ImmutableListMultimap.Builder<K, ChunkReader> slices = ImmutableListMultimap.builder();
        for (DiskRange mergedRange : mergedRanges) {
            ReferenceCountedReader mergedRangeLoader = new ReferenceCountedReader(mergedRange, memoryContext);

            for (Map.Entry<K, DiskRange> diskRangeEntry : diskRanges.entries()) {
                DiskRange diskRange = diskRangeEntry.getValue();
                if (mergedRange.contains(diskRange)) {
                    mergedRangeLoader.addReference();

                    slices.put(diskRangeEntry.getKey(), new ChunkReader()
                    {
                        @Override
                        public long getDiskOffset()
                        {
                            return diskRange.getOffset();
                        }

                        @Override
                        public Slice read()
                                throws IOException
                        {
                            int offset = toIntExact(diskRange.getOffset() - mergedRange.getOffset());
                            return mergedRangeLoader.read().slice(offset, toIntExact(diskRange.getLength()));
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

    private <K> ListMultimap<K, ChunkReader> readLargeDiskRanges(ListMultimap<K, DiskRange> diskRanges, AggregatedMemoryContext memoryContext)
    {
        if (diskRanges.isEmpty()) {
            return ImmutableListMultimap.of();
        }

        ImmutableListMultimap.Builder<K, ChunkReader> slices = ImmutableListMultimap.builder();
        for (Map.Entry<K, DiskRange> entry : diskRanges.entries()) {
            slices.put(entry.getKey(), new ReferenceCountedReader(entry.getValue(), memoryContext));
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
        // See jdk.internal.util.ArraysSupport.SOFT_MAX_ARRAY_LENGTH for an explanation
        private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
        private final DiskRange range;
        private final LocalMemoryContext readerMemoryUsage;
        private Slice data;
        private int referenceCount = 1;

        public ReferenceCountedReader(DiskRange range, AggregatedMemoryContext memoryContext)
        {
            this.range = range;
            checkArgument(range.getLength() <= MAX_ARRAY_SIZE, "Cannot read range bigger than %s but got %s", MAX_ARRAY_SIZE, range);
            this.readerMemoryUsage = memoryContext.newLocalMemoryContext(ReferenceCountedReader.class.getSimpleName());
        }

        public void addReference()
        {
            checkState(referenceCount > 0, "Chunk reader is already closed");
            referenceCount++;
        }

        @Override
        public long getDiskOffset()
        {
            return range.getOffset();
        }

        @Override
        public Slice read()
                throws IOException
        {
            checkState(referenceCount > 0, "Chunk reader is already closed");

            if (data == null) {
                byte[] buffer = new byte[toIntExact(range.getLength())];
                readerMemoryUsage.setBytes(buffer.length);
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
                readerMemoryUsage.setBytes(0);
            }
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("range", range)
                    .add("referenceCount", referenceCount)
                    .toString();
        }
    }
}
