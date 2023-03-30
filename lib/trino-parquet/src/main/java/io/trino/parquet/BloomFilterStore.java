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

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter.UPPER_BOUND_BYTES;

public class BloomFilterStore
{
    // since bloomfilter header is relatively small(18 bytes when testing) we can read in a larger buffer(BlockSplitBloomFilter.HEADER_SIZE*4 in this case)
    // and get actual bytes used when deserializing header in order to calculate the correct offset for bloomfilter data.
    private static final int MAX_HEADER_LENGTH = BlockSplitBloomFilter.HEADER_SIZE * 4;

    private final ParquetDataSource dataSource;
    private final Map<ColumnPath, Long> bloomFilterOffsets;

    public BloomFilterStore(ParquetDataSource dataSource, BlockMetaData block, Set<ColumnPath> columnsFiltered)
    {
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        requireNonNull(block, "block is null");
        requireNonNull(columnsFiltered, "columnsFiltered is null");

        ImmutableMap.Builder<ColumnPath, Long> bloomFilterOffsetBuilder = ImmutableMap.builder();
        for (ColumnChunkMetaData column : block.getColumns()) {
            ColumnPath path = column.getPath();
            if (hasBloomFilter(column) && columnsFiltered.contains(path)) {
                bloomFilterOffsetBuilder.put(path, column.getBloomFilterOffset());
            }
        }
        this.bloomFilterOffsets = bloomFilterOffsetBuilder.buildOrThrow();
    }

    public Optional<BloomFilter> getBloomFilter(ColumnPath columnPath)
    {
        BloomFilterHeader bloomFilterHeader;
        long bloomFilterDataOffset;
        try {
            Long columnBloomFilterOffset = bloomFilterOffsets.get(columnPath);
            if (columnBloomFilterOffset == null) {
                return Optional.empty();
            }
            BasicSliceInput headerSliceInput = dataSource.readFully(columnBloomFilterOffset, MAX_HEADER_LENGTH).getInput();
            bloomFilterHeader = Util.readBloomFilterHeader(headerSliceInput);
            bloomFilterDataOffset = columnBloomFilterOffset + headerSliceInput.position();
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Failed to read Bloom filter header", exception);
        }

        if (!bloomFilterSupported(columnPath, bloomFilterHeader)) {
            return Optional.empty();
        }

        try {
            Slice bloomFilterData = dataSource.readFully(bloomFilterDataOffset, bloomFilterHeader.getNumBytes());
            verify(bloomFilterData.length() > 0, "Read empty bloom filter %s", bloomFilterHeader);
            return Optional.of(new BlockSplitBloomFilter(bloomFilterData.getBytes()));
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Failed to read Bloom filter data", exception);
        }
    }

    public static boolean hasBloomFilter(ColumnChunkMetaData columnMetaData)
    {
        return columnMetaData.getBloomFilterOffset() > 0;
    }

    private static boolean bloomFilterSupported(ColumnPath columnPath, BloomFilterHeader bloomFilterHeader)
    {
        int numBytes = bloomFilterHeader.getNumBytes();
        if (numBytes <= 0 || numBytes > UPPER_BOUND_BYTES) {
            throw new ParquetDecodingException(format("Column: %s has bloom filter number of bytes value of %d, which is out of bound of lower limit: %d and upper limit: %d", columnPath, numBytes, 0, UPPER_BOUND_BYTES));
        }
        return bloomFilterHeader.getHash().isSetXXHASH() && bloomFilterHeader.getAlgorithm().isSetBLOCK() && bloomFilterHeader.getCompression().isSetUNCOMPRESSED();
    }
}
