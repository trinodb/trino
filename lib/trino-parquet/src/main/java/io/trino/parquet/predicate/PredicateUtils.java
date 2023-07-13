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
package io.trino.parquet.predicate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.trino.parquet.BloomFilterStore;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetEncoding;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.parquet.ParquetCompressionUtils.decompress;
import static io.trino.parquet.ParquetReaderUtils.isOnlyDictionaryEncodingPages;
import static io.trino.parquet.ParquetTypeUtils.getParquetEncoding;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class PredicateUtils
{
    // Maximum size of dictionary that we will read for row-group pruning.
    // Reading larger dictionaries is typically not beneficial. Before checking
    // the dictionary, the row-group, page indexes and bloomfilters have already been checked
    // and when the dictionary does not eliminate a row-group, the work done to
    // decode the dictionary and match it with predicates is wasted.
    private static final int MAX_DICTIONARY_SIZE = 8096;

    private PredicateUtils() {}

    public static boolean isStatisticsOverflow(Type type, long min, long max)
    {
        if (type == TINYINT) {
            return min < Byte.MIN_VALUE || max > Byte.MAX_VALUE;
        }
        if (type == SMALLINT) {
            return min < Short.MIN_VALUE || max > Short.MAX_VALUE;
        }
        if (type == INTEGER || type == DATE) {
            return min < Integer.MIN_VALUE || max > Integer.MAX_VALUE;
        }
        if (type == BIGINT) {
            return false;
        }
        if (type instanceof DecimalType decimalType) {
            if (!decimalType.isShort()) {
                // Smallest long decimal type with 0 scale has broader range than representable in long, as used in ParquetLongStatistics
                return false;
            }
            return BigDecimal.valueOf(min, decimalType.getScale()).compareTo(minimalValue(decimalType)) < 0 ||
                    BigDecimal.valueOf(max, decimalType.getScale()).compareTo(maximalValue(decimalType)) > 0;
        }

        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    private static BigDecimal minimalValue(DecimalType decimalType)
    {
        return new BigDecimal(format("-%s.%s", "9".repeat(decimalType.getPrecision() - decimalType.getScale()), "9".repeat(decimalType.getScale())));
    }

    private static BigDecimal maximalValue(DecimalType decimalType)
    {
        return new BigDecimal(format("+%s.%s", "9".repeat(decimalType.getPrecision() - decimalType.getScale()), "9".repeat(decimalType.getScale())));
    }

    public static TupleDomainParquetPredicate buildPredicate(
            MessageType requestedSchema,
            TupleDomain<ColumnDescriptor> parquetTupleDomain,
            Map<List<String>, ColumnDescriptor> descriptorsByPath,
            DateTimeZone timeZone)
    {
        ImmutableList.Builder<ColumnDescriptor> columnReferences = ImmutableList.builder();
        for (String[] paths : requestedSchema.getPaths()) {
            ColumnDescriptor descriptor = descriptorsByPath.get(Arrays.asList(paths));
            if (descriptor != null) {
                columnReferences.add(descriptor);
            }
        }
        return new TupleDomainParquetPredicate(parquetTupleDomain, columnReferences.build(), timeZone);
    }

    public static boolean predicateMatches(
            TupleDomainParquetPredicate parquetPredicate,
            BlockMetaData block,
            ParquetDataSource dataSource,
            Map<List<String>, ColumnDescriptor> descriptorsByPath,
            TupleDomain<ColumnDescriptor> parquetTupleDomain,
            Optional<ColumnIndexStore> columnIndexStore,
            Optional<BloomFilterStore> bloomFilterStore,
            DateTimeZone timeZone,
            int domainCompactionThreshold)
            throws IOException
    {
        if (block.getRowCount() == 0) {
            return false;
        }
        Map<ColumnDescriptor, Statistics<?>> columnStatistics = getStatistics(block, descriptorsByPath);
        Map<ColumnDescriptor, Long> columnValueCounts = getColumnValueCounts(block, descriptorsByPath);
        Optional<List<ColumnDescriptor>> candidateColumns = parquetPredicate.getIndexLookupCandidates(columnValueCounts, columnStatistics, dataSource.getId());
        if (candidateColumns.isEmpty()) {
            return false;
        }
        if (candidateColumns.get().isEmpty()) {
            return true;
        }
        // Perform column index, bloom filter checks and dictionary lookups only for the subset of columns where it can be useful.
        // This prevents unnecessary filesystem reads and decoding work when the predicate on a column comes from
        // file-level min/max stats or more generally when the predicate selects a range equal to or wider than row-group min/max.
        TupleDomainParquetPredicate indexPredicate = new TupleDomainParquetPredicate(parquetTupleDomain, candidateColumns.get(), timeZone);

        // Page stats is finer grained but relatively more expensive, so we do the filtering after above block filtering.
        if (columnIndexStore.isPresent() && !indexPredicate.matches(columnValueCounts, columnIndexStore.get(), dataSource.getId())) {
            return false;
        }

        if (bloomFilterStore.isPresent() && !indexPredicate.matches(bloomFilterStore.get(), domainCompactionThreshold)) {
            return false;
        }

        return dictionaryPredicatesMatch(
                indexPredicate,
                block,
                dataSource,
                descriptorsByPath,
                ImmutableSet.copyOf(candidateColumns.get()),
                columnIndexStore);
    }

    private static Map<ColumnDescriptor, Statistics<?>> getStatistics(BlockMetaData blockMetadata, Map<List<String>, ColumnDescriptor> descriptorsByPath)
    {
        ImmutableMap.Builder<ColumnDescriptor, Statistics<?>> statistics = ImmutableMap.builder();
        for (ColumnChunkMetaData columnMetaData : blockMetadata.getColumns()) {
            Statistics<?> columnStatistics = columnMetaData.getStatistics();
            if (columnStatistics != null) {
                ColumnDescriptor descriptor = descriptorsByPath.get(Arrays.asList(columnMetaData.getPath().toArray()));
                if (descriptor != null) {
                    statistics.put(descriptor, columnStatistics);
                }
            }
        }
        return statistics.buildOrThrow();
    }

    private static Map<ColumnDescriptor, Long> getColumnValueCounts(BlockMetaData blockMetadata, Map<List<String>, ColumnDescriptor> descriptorsByPath)
    {
        ImmutableMap.Builder<ColumnDescriptor, Long> columnValueCounts = ImmutableMap.builder();
        for (ColumnChunkMetaData columnMetaData : blockMetadata.getColumns()) {
            ColumnDescriptor descriptor = descriptorsByPath.get(Arrays.asList(columnMetaData.getPath().toArray()));
            if (descriptor != null) {
                columnValueCounts.put(descriptor, columnMetaData.getValueCount());
            }
        }
        return columnValueCounts.buildOrThrow();
    }

    private static boolean dictionaryPredicatesMatch(
            TupleDomainParquetPredicate parquetPredicate,
            BlockMetaData blockMetadata,
            ParquetDataSource dataSource,
            Map<List<String>, ColumnDescriptor> descriptorsByPath,
            Set<ColumnDescriptor> candidateColumns,
            Optional<ColumnIndexStore> columnIndexStore)
            throws IOException
    {
        for (ColumnChunkMetaData columnMetaData : blockMetadata.getColumns()) {
            ColumnDescriptor descriptor = descriptorsByPath.get(Arrays.asList(columnMetaData.getPath().toArray()));
            if (descriptor == null || !candidateColumns.contains(descriptor)) {
                continue;
            }
            if (isOnlyDictionaryEncodingPages(columnMetaData)) {
                Statistics<?> columnStatistics = columnMetaData.getStatistics();
                boolean nullAllowed = columnStatistics == null || columnStatistics.getNumNulls() != 0;
                //  Early abort, predicate already filters block so no more dictionaries need be read
                if (!parquetPredicate.matches(new DictionaryDescriptor(
                        descriptor,
                        nullAllowed,
                        readDictionaryPage(dataSource, columnMetaData, columnIndexStore)))) {
                    return false;
                }
            }
        }
        return true;
    }

    private static Optional<DictionaryPage> readDictionaryPage(
            ParquetDataSource dataSource,
            ColumnChunkMetaData columnMetaData,
            Optional<ColumnIndexStore> columnIndexStore)
            throws IOException
    {
        int dictionaryPageSize;
        if (columnMetaData.getDictionaryPageOffset() == 0 || columnMetaData.getFirstDataPageOffset() <= columnMetaData.getDictionaryPageOffset()) {
            /*
             * See org.apache.parquet.hadoop.Offsets for reference.
             * The offsets might not contain the proper values in the below cases:
             * - The dictionaryPageOffset might not be set; in this case 0 is returned
             *   (0 cannot be a valid offset because of the MAGIC bytes)
             * - The firstDataPageOffset might point to the dictionary page
             *
             * Such parquet files may have been produced by parquet-mr writers before PARQUET-1977.
             * We find the dictionary page size from OffsetIndex if that exists,
             * otherwise fallback to reading the full column chunk.
             */
            dictionaryPageSize = columnIndexStore.flatMap(index -> getDictionaryPageSize(index, columnMetaData))
                    .orElseGet(() -> toIntExact(columnMetaData.getTotalSize()));
        }
        else {
            dictionaryPageSize = toIntExact(columnMetaData.getFirstDataPageOffset() - columnMetaData.getDictionaryPageOffset());
        }
        // Get the dictionary page header and the dictionary in single read
        Slice buffer = dataSource.readFully(columnMetaData.getStartingPos(), dictionaryPageSize);
        return readPageHeaderWithData(buffer.getInput()).map(data -> decodeDictionaryPage(data, columnMetaData));
    }

    private static Optional<Integer> getDictionaryPageSize(ColumnIndexStore columnIndexStore, ColumnChunkMetaData columnMetaData)
    {
        OffsetIndex offsetIndex = columnIndexStore.getOffsetIndex(columnMetaData.getPath());
        if (offsetIndex == null) {
            return Optional.empty();
        }
        long rowGroupOffset = columnMetaData.getStartingPos();
        long firstPageOffset = offsetIndex.getOffset(0);
        if (rowGroupOffset < firstPageOffset) {
            return Optional.of(toIntExact(firstPageOffset - rowGroupOffset));
        }
        return Optional.empty();
    }

    private static Optional<PageHeaderWithData> readPageHeaderWithData(SliceInput inputStream)
    {
        PageHeader pageHeader;
        try {
            pageHeader = Util.readPageHeader(inputStream);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (pageHeader.type != PageType.DICTIONARY_PAGE) {
            return Optional.empty();
        }
        DictionaryPageHeader dictionaryHeader = pageHeader.getDictionary_page_header();
        if (dictionaryHeader.getNum_values() > MAX_DICTIONARY_SIZE) {
            return Optional.empty();
        }
        return Optional.of(new PageHeaderWithData(
                pageHeader,
                inputStream.readSlice(pageHeader.getCompressed_page_size())));
    }

    private static DictionaryPage decodeDictionaryPage(PageHeaderWithData pageHeaderWithData, ColumnChunkMetaData chunkMetaData)
    {
        PageHeader pageHeader = pageHeaderWithData.pageHeader();
        DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
        ParquetEncoding encoding = getParquetEncoding(Encoding.valueOf(dicHeader.getEncoding().name()));
        int dictionarySize = dicHeader.getNum_values();

        Slice compressedData = pageHeaderWithData.compressedData();
        try {
            return new DictionaryPage(decompress(chunkMetaData.getCodec().getParquetCompressionCodec(), compressedData, pageHeader.getUncompressed_page_size()), dictionarySize, encoding);
        }
        catch (IOException e) {
            throw new ParquetDecodingException("Could not decode the dictionary for " + chunkMetaData.getPath(), e);
        }
    }

    private record PageHeaderWithData(PageHeader pageHeader, Slice compressedData)
    {
        private PageHeaderWithData(PageHeader pageHeader, Slice compressedData)
        {
            this.pageHeader = requireNonNull(pageHeader, "pageHeader is null");
            this.compressedData = requireNonNull(compressedData, "compressedData is null");
        }
    }
}
