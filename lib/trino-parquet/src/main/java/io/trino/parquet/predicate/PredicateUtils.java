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
import io.trino.parquet.BloomFilterStore;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.metadata.PrunedBlockMetadata;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.parquet.BloomFilterStore.getBloomFilterStore;
import static io.trino.parquet.metadata.PrunedBlockMetadata.createPrunedColumnsMetadata;
import static io.trino.parquet.reader.TrinoColumnIndexStore.getColumnIndexStore;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;

public final class PredicateUtils
{
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

    private static PredicateMatchResult predicateMatches(
            TupleDomainParquetPredicate parquetPredicate,
            PrunedBlockMetadata columnsMetadata,
            ParquetDataSource dataSource,
            Map<List<String>, ColumnDescriptor> descriptorsByPath,
            TupleDomain<ColumnDescriptor> parquetTupleDomain,
            Optional<ColumnIndexStore> columnIndexStore,
            Optional<BloomFilterStore> bloomFilterStore,
            DateTimeZone timeZone,
            int domainCompactionThreshold)
            throws IOException
    {
        if (columnsMetadata.getRowCount() == 0) {
            return PredicateMatchResult.FALSE_WITH_DICT_MATCHING_NOT_NEEDED;
        }
        Map<ColumnDescriptor, Statistics<?>> columnStatistics = getStatistics(columnsMetadata, descriptorsByPath);
        Map<ColumnDescriptor, Long> columnValueCounts = getColumnValueCounts(columnsMetadata, descriptorsByPath);
        Optional<List<ColumnDescriptor>> candidateColumns = parquetPredicate.getIndexLookupCandidates(columnValueCounts, columnStatistics, dataSource.getId());
        if (candidateColumns.isEmpty()) {
            return PredicateMatchResult.FALSE_WITH_DICT_MATCHING_NOT_NEEDED;
        }
        if (candidateColumns.get().isEmpty()) {
            return PredicateMatchResult.TRUE_WITH_DICT_MATCHING_NOT_NEEDED;
        }
        // Perform column index, bloom filter checks and dictionary lookups only for the subset of columns where it can be useful.
        // This prevents unnecessary filesystem reads and decoding work when the predicate on a column comes from
        // file-level min/max stats or more generally when the predicate selects a range equal to or wider than row-group min/max.
        TupleDomainParquetPredicate indexPredicate = new TupleDomainParquetPredicate(parquetTupleDomain, candidateColumns.get(), timeZone);

        // Page stats is finer grained but relatively more expensive, so we do the filtering after above block filtering.
        if (columnIndexStore.isPresent() && !indexPredicate.matches(columnValueCounts, columnIndexStore.get(), dataSource.getId())) {
            return PredicateMatchResult.FALSE_WITH_DICT_MATCHING_NOT_NEEDED;
        }

        if (bloomFilterStore.isPresent() && !indexPredicate.matches(bloomFilterStore.get(), domainCompactionThreshold)) {
            return PredicateMatchResult.FALSE_WITH_DICT_MATCHING_NOT_NEEDED;
        }

        return new PredicateMatchResult(true, indexPredicate, ImmutableSet.copyOf(candidateColumns.get()));
    }

    public static List<RowGroupInfo> getFilteredRowGroups(
            long splitStart,
            long splitLength,
            ParquetDataSource dataSource,
            ParquetMetadata parquetMetadata,
            List<TupleDomain<ColumnDescriptor>> parquetTupleDomains,
            List<TupleDomainParquetPredicate> parquetPredicates,
            Map<List<String>, ColumnDescriptor> descriptorsByPath,
            DateTimeZone timeZone,
            int domainCompactionThreshold,
            ParquetReaderOptions options)
            throws IOException
    {
        ImmutableList.Builder<RowGroupInfo> rowGroupInfoBuilder = ImmutableList.builder();
        for (BlockMetadata block : parquetMetadata.getBlocks(splitStart, splitLength)) {
            for (int i = 0; i < parquetTupleDomains.size(); i++) {
                TupleDomain<ColumnDescriptor> parquetTupleDomain = parquetTupleDomains.get(i);
                TupleDomainParquetPredicate parquetPredicate = parquetPredicates.get(i);
                Optional<ColumnIndexStore> columnIndex = getColumnIndexStore(dataSource, block, descriptorsByPath, parquetTupleDomain, options, parquetMetadata.getDecryptionContext());
                Optional<BloomFilterStore> bloomFilterStore = getBloomFilterStore(dataSource, block, parquetTupleDomain, options, parquetMetadata.getDecryptionContext());
                PrunedBlockMetadata columnsMetadata = createPrunedColumnsMetadata(block, dataSource.getId(), descriptorsByPath);
                PredicateMatchResult predicateMatchResult = predicateMatches(
                        parquetPredicate,
                        columnsMetadata,
                        dataSource,
                        descriptorsByPath,
                        parquetTupleDomain,
                        columnIndex,
                        bloomFilterStore,
                        timeZone,
                        domainCompactionThreshold);
                if (predicateMatchResult.matchesWithoutDictionary()) {
                    rowGroupInfoBuilder.add(new RowGroupInfo(columnsMetadata, block.fileRowCountOffset(), columnIndex, Optional.ofNullable(predicateMatchResult.indexPredicate()), Optional.ofNullable(predicateMatchResult.candidateColumnsForDictionaryMatching())));
                    break;
                }
            }
        }
        return rowGroupInfoBuilder.build();
    }

    private static Map<ColumnDescriptor, Statistics<?>> getStatistics(PrunedBlockMetadata columnsMetadata, Map<List<String>, ColumnDescriptor> descriptorsByPath)
            throws ParquetCorruptionException
    {
        ImmutableMap.Builder<ColumnDescriptor, Statistics<?>> statistics = ImmutableMap.builderWithExpectedSize(descriptorsByPath.size());
        for (ColumnDescriptor descriptor : descriptorsByPath.values()) {
            ColumnChunkMetadata columnMetaData = columnsMetadata.getColumnChunkMetaData(descriptor);
            Statistics<?> columnStatistics = columnMetaData.getStatistics();
            if (columnStatistics != null) {
                statistics.put(descriptor, columnStatistics);
            }
        }
        return statistics.buildOrThrow();
    }

    private static Map<ColumnDescriptor, Long> getColumnValueCounts(PrunedBlockMetadata columnsMetadata, Map<List<String>, ColumnDescriptor> descriptorsByPath)
            throws ParquetCorruptionException
    {
        ImmutableMap.Builder<ColumnDescriptor, Long> columnValueCounts = ImmutableMap.builderWithExpectedSize(descriptorsByPath.size());
        for (ColumnDescriptor descriptor : descriptorsByPath.values()) {
            ColumnChunkMetadata columnMetaData = columnsMetadata.getColumnChunkMetaData(descriptor);
            columnValueCounts.put(descriptor, columnMetaData.getValueCount());
        }
        return columnValueCounts.buildOrThrow();
    }

    private record PredicateMatchResult(boolean matchesWithoutDictionary, TupleDomainParquetPredicate indexPredicate, Set<ColumnDescriptor> candidateColumnsForDictionaryMatching)
    {
        private static final PredicateMatchResult FALSE_WITH_DICT_MATCHING_NOT_NEEDED = new PredicateMatchResult(false, null, null);
        private static final PredicateMatchResult TRUE_WITH_DICT_MATCHING_NOT_NEEDED = new PredicateMatchResult(true, null, null);
    }
}
