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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.RichColumnDescriptor;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
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

import static com.google.common.base.Verify.verify;
import static io.trino.parquet.ParquetCompressionUtils.decompress;
import static io.trino.parquet.ParquetTypeUtils.getParquetEncoding;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.column.Encoding.RLE;

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
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
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

    public static Predicate buildPredicate(
            MessageType requestedSchema,
            TupleDomain<ColumnDescriptor> parquetTupleDomain,
            Map<List<String>, RichColumnDescriptor> descriptorsByPath,
            DateTimeZone timeZone)
    {
        ImmutableList.Builder<RichColumnDescriptor> columnReferences = ImmutableList.builder();
        for (String[] paths : requestedSchema.getPaths()) {
            RichColumnDescriptor descriptor = descriptorsByPath.get(Arrays.asList(paths));
            if (descriptor != null) {
                columnReferences.add(descriptor);
            }
        }
        return new TupleDomainParquetPredicate(parquetTupleDomain, columnReferences.build(), timeZone);
    }

    public static boolean predicateMatches(Predicate parquetPredicate, BlockMetaData block, ParquetDataSource dataSource, Map<List<String>, RichColumnDescriptor> descriptorsByPath, TupleDomain<ColumnDescriptor> parquetTupleDomain)
            throws ParquetCorruptionException
    {
        return predicateMatches(parquetPredicate, block, dataSource, descriptorsByPath, parquetTupleDomain, Optional.empty());
    }

    public static boolean predicateMatches(Predicate parquetPredicate, BlockMetaData block, ParquetDataSource dataSource, Map<List<String>, RichColumnDescriptor> descriptorsByPath, TupleDomain<ColumnDescriptor> parquetTupleDomain, Optional<ColumnIndexStore> columnIndexStore)
            throws ParquetCorruptionException
    {
        Map<ColumnDescriptor, Statistics<?>> columnStatistics = getStatistics(block, descriptorsByPath);
        if (!parquetPredicate.matches(block.getRowCount(), columnStatistics, dataSource.getId())) {
            return false;
        }

        // Page stats is finer grained but relatively more expensive, so we do the filtering after above block filtering.
        if (columnIndexStore.isPresent() && !parquetPredicate.matches(block.getRowCount(), columnIndexStore.get(), dataSource.getId())) {
            return false;
        }

        return dictionaryPredicatesMatch(parquetPredicate, block, dataSource, descriptorsByPath, parquetTupleDomain);
    }

    private static Map<ColumnDescriptor, Statistics<?>> getStatistics(BlockMetaData blockMetadata, Map<List<String>, RichColumnDescriptor> descriptorsByPath)
    {
        ImmutableMap.Builder<ColumnDescriptor, Statistics<?>> statistics = ImmutableMap.builder();
        for (ColumnChunkMetaData columnMetaData : blockMetadata.getColumns()) {
            Statistics<?> columnStatistics = columnMetaData.getStatistics();
            if (columnStatistics != null) {
                RichColumnDescriptor descriptor = descriptorsByPath.get(Arrays.asList(columnMetaData.getPath().toArray()));
                if (descriptor != null) {
                    statistics.put(descriptor, columnStatistics);
                }
            }
        }
        return statistics.build();
    }

    private static boolean dictionaryPredicatesMatch(Predicate parquetPredicate, BlockMetaData blockMetadata, ParquetDataSource dataSource, Map<List<String>, RichColumnDescriptor> descriptorsByPath, TupleDomain<ColumnDescriptor> parquetTupleDomain)
    {
        for (ColumnChunkMetaData columnMetaData : blockMetadata.getColumns()) {
            RichColumnDescriptor descriptor = descriptorsByPath.get(Arrays.asList(columnMetaData.getPath().toArray()));
            if (descriptor != null) {
                if (isOnlyDictionaryEncodingPages(columnMetaData) && isColumnPredicate(descriptor, parquetTupleDomain)) {
                    Slice buffer = dataSource.readFully(columnMetaData.getStartingPos(), toIntExact(columnMetaData.getTotalSize()));
                    //  Early abort, predicate already filters block so no more dictionaries need be read
                    if (!parquetPredicate.matches(new DictionaryDescriptor(descriptor, readDictionaryPage(buffer, columnMetaData.getCodec())))) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private static Optional<DictionaryPage> readDictionaryPage(Slice data, CompressionCodecName codecName)
    {
        try {
            SliceInput inputStream = data.getInput();
            PageHeader pageHeader = Util.readPageHeader(inputStream);

            if (pageHeader.type != PageType.DICTIONARY_PAGE) {
                return Optional.empty();
            }

            Slice compressedData = inputStream.readSlice(pageHeader.getCompressed_page_size());
            DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
            ParquetEncoding encoding = getParquetEncoding(Encoding.valueOf(dicHeader.getEncoding().name()));
            int dictionarySize = dicHeader.getNum_values();

            return Optional.of(new DictionaryPage(decompress(codecName, compressedData, pageHeader.getUncompressed_page_size()), dictionarySize, encoding));
        }
        catch (IOException ignored) {
            return Optional.empty();
        }
    }

    private static boolean isColumnPredicate(ColumnDescriptor columnDescriptor, TupleDomain<ColumnDescriptor> parquetTupleDomain)
    {
        verify(parquetTupleDomain.getDomains().isPresent(), "parquetTupleDomain is empty");
        return parquetTupleDomain.getDomains().get().containsKey(columnDescriptor);
    }

    @VisibleForTesting
    @SuppressWarnings("deprecation")
    static boolean isOnlyDictionaryEncodingPages(ColumnChunkMetaData columnMetaData)
    {
        // Files written with newer versions of Parquet libraries (e.g. parquet-mr 1.9.0) will have EncodingStats available
        // Otherwise, fallback to v1 logic
        EncodingStats stats = columnMetaData.getEncodingStats();
        if (stats != null) {
            return stats.hasDictionaryPages() && !stats.hasNonDictionaryEncodedPages();
        }

        Set<Encoding> encodings = columnMetaData.getEncodings();
        if (encodings.contains(PLAIN_DICTIONARY)) {
            // PLAIN_DICTIONARY was present, which means at least one page was
            // dictionary-encoded and 1.0 encodings are used
            // The only other allowed encodings are RLE and BIT_PACKED which are used for repetition or definition levels
            return Sets.difference(encodings, ImmutableSet.of(PLAIN_DICTIONARY, RLE, BIT_PACKED)).isEmpty();
        }

        return false;
    }
}
