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
package io.trino.parquet.reader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SliceInput;
import io.airlift.units.DataSize;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Count;
import io.trino.spi.metrics.Metric;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.BloomFilterStore.hasBloomFilter;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.generateInputPagesWithBlockData;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.parquet.reader.ParquetReader.PARQUET_READER_DICTIONARY_FILTERED_ROWGROUPS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetReaderDictionaryFiltering
{
    @Test
    public void testFilteringWithSingleRowGroup()
            throws Exception
    {
        List<String> columnNames = ImmutableList.of("int_col", "bigint_col", "varchar_col");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT, VARCHAR);
        int totalPositions = 1000;
        int pageCount = 100;
        // generate multiples of 5 as distinct values in blocks
        int[] distinctValues = IntStream.range(1, 21).map(i -> i * 5).toArray();
        List<Page> inputPages = generatePages(types, distinctValues, totalPositions, pageCount);

        try (ParquetDataSource dataSource = writeParquetAndAssertDictionary(types, columnNames, inputPages, DataSize.of(128, MEGABYTE), 1, distinctValues.length)) {
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());

            int totalRows = 100000;
            int totalRowGroups = 1;
            // TupleDomain.all() --> All rows read, no rowgroups filtered out
            TupleDomain<String> predicateDomain = TupleDomain.all();
            assertDictionaryFiltered(dataSource, parquetMetadata, types, columnNames, predicateDomain, totalRows, 0);

            // int_col = 6, not present in dictionary --> No rows read, all rowgroups filtered out
            predicateDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            "int_col", Domain.multipleValues(INTEGER, ImmutableList.of(6L))));
            assertDictionaryFiltered(dataSource, parquetMetadata, types, columnNames, predicateDomain, 0, totalRowGroups);

            // 1 < int_col < 5, overlapping range with dictionary values --> All rows read, no rowgroups filtered out
            predicateDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            "int_col", Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 1L), Range.lessThan(INTEGER, 5L)), false)));
            assertDictionaryFiltered(dataSource, parquetMetadata, types, columnNames, predicateDomain, totalRows, 0);

            // varchar_col="varchar-11", not present in dictionary --> No rows read, all rowgroups filtered out
            predicateDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            "varchar_col", Domain.multipleValues(VARCHAR, ImmutableList.of(utf8Slice("varchar-11")))));
            assertDictionaryFiltered(dataSource, parquetMetadata, types, columnNames, predicateDomain, 0, totalRowGroups);

            // varchar_col="varchar-5", present in dictionary --> All rows read, no rowgroups filtered out
            predicateDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            "varchar_col", Domain.multipleValues(VARCHAR, ImmutableList.of(utf8Slice("varchar-5")))));
            assertDictionaryFiltered(dataSource, parquetMetadata, types, columnNames, predicateDomain, totalRows, 0);

            // varchar_col in ["varchar-5",varchar-11], overlapping with dictionary values --> All rows read, no rowgroups filtered out
            predicateDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            "varchar_col", Domain.multipleValues(VARCHAR, ImmutableList.of(utf8Slice("varchar-5"), utf8Slice("varchar-11")))));
            assertDictionaryFiltered(dataSource, parquetMetadata, types, columnNames, predicateDomain, totalRows, 0);
        }
    }

    @Test
    public void testFilteringWithMultipleRowGroups()
            throws Exception
    {
        List<String> columnNames = ImmutableList.of("int_col", "bigint_col", "varchar_col");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT, VARCHAR);
        int totalPositions = 40000;
        int pageCount = 20;
        int numDistinctValuesInPage = 20;

        // each column have certain distinct(across rowgroups) values, that are then repeated for all the positions
        // the combination of current values of totalPositions, pageCount, numDistinctValuesInPage, and MaxBlockSize
        // give 20 rowgroups
        // int_col [1000, 1005, 1010...1000, 1005, 1010...] - rowgroup1
        // int_col [1200, 1205, 1210...1200, 1205, 1210...] - rowgroup2
        // ...so on

        List<Page> inputPages = new ArrayList<>();
        for (int i = 0; i < pageCount; i++) {
            int start = (i + 10) * numDistinctValuesInPage;
            int end = start + numDistinctValuesInPage;
            int[] distinctValues = IntStream.range(start, end).map(n -> n * 5).toArray();
            inputPages.addAll(generatePages(types, distinctValues, totalPositions, 1));
        }

        try (ParquetDataSource dataSource = writeParquetAndAssertDictionary(types, columnNames, inputPages, DataSize.of(1024, KILOBYTE), pageCount, numDistinctValuesInPage)) {
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());

            // int_col in [1001, 1101] --> 0 rows read. 2 rowgroups are passed to ParquetReader after min/max filtering, both of them are then filtered by dictionary filter
            TupleDomain<String> predicateDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of("int_col", Domain.multipleValues(INTEGER, ImmutableList.of(1001L, 1101L), false)));
            assertDictionaryFiltered(dataSource, parquetMetadata, types, columnNames, predicateDomain, 0, 2);

            // int_col in [1005] --> 40000 (1 rowgroup) rows read. Others filtered by min/max, not passed to ParquetReader
            predicateDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of("int_col", Domain.multipleValues(INTEGER, ImmutableList.of(1005L), false)));
            assertDictionaryFiltered(dataSource, parquetMetadata, types, columnNames, predicateDomain, 40000, 0);

            // int_col in [1005, 1101, 1201] --> 40000 (1 rowgroup having int_col=1005) rows read. 2 rowgroups filtered by dictionary filter
            predicateDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of("int_col", Domain.multipleValues(INTEGER, ImmutableList.of(1005L, 1101L, 1201L), false)));
            assertDictionaryFiltered(dataSource, parquetMetadata, types, columnNames, predicateDomain, 40000, 2);

            // varchar_col in [varchar-2005, varchar-2101, varchar-2201, varchar-2301] --> 40000 (1 rowgroup having varchar_col=varchar-2005) rows read. 3 rowgroups filtered by dictionary filter
            predicateDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            "varchar_col", Domain.multipleValues(VARCHAR,
                                    ImmutableList.of(utf8Slice("varchar-2005"), utf8Slice("varchar-2101"), utf8Slice("varchar-2201"), utf8Slice("varchar-2301")))));
            assertDictionaryFiltered(dataSource, parquetMetadata, types, columnNames, predicateDomain, 40000, 3);

            // 40000 (1 rowgroup having int_col=1005 and varchar_col=varchar-2005) rows read. 1 rowgroup filtered by dictionary filter (varchar-2101)
            predicateDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            "int_col", Domain.multipleValues(INTEGER, ImmutableList.of(1005L, 2100L), false),
                            "varchar_col", Domain.multipleValues(VARCHAR, ImmutableList.of(utf8Slice("varchar-1005"), utf8Slice("varchar-2101")))));
            assertDictionaryFiltered(dataSource, parquetMetadata, types, columnNames, predicateDomain, 40000, 1);
        }
    }

    private static ParquetDataSource writeParquetAndAssertDictionary(List<Type> types, List<String> columnNames, List<Page> inputPages,
                                                                     DataSize rowGroupMaxSize, int expectedRowGroupCount, int expectedDictionaryValuesCount)
            throws IOException
    {
        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxBlockSize(rowGroupMaxSize)
                                .build(),
                        types,
                        columnNames,
                        inputPages),
                ParquetReaderOptions.defaultOptions());

        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks().size()).isEqualTo(expectedRowGroupCount);
        for (BlockMetadata blockMetaData : parquetMetadata.getBlocks()) {
            assertThat(blockMetaData.columns()).hasSize(columnNames.size());
            for (ColumnChunkMetadata chunkMetadata : blockMetaData.columns()) {
                assertThat(chunkMetadata.getDictionaryPageOffset()).isGreaterThan(0);
                int dictionaryPageSize = toIntExact(chunkMetadata.getFirstDataPageOffset() - chunkMetadata.getDictionaryPageOffset());
                assertThat(dictionaryPageSize).isGreaterThan(0);
                assertThat(chunkMetadata.getEncodingStats().hasDictionaryPages()).isTrue();
                assertThat(chunkMetadata.getEncodingStats().hasDictionaryEncodedPages()).isTrue();
                assertThat(chunkMetadata.getEncodingStats().hasNonDictionaryEncodedPages()).isFalse();
                assertThat(hasBloomFilter(chunkMetadata)).isFalse();

                // verify dictionary page
                SliceInput inputStream = dataSource.readFully(chunkMetadata.getStartingPos(), dictionaryPageSize).getInput();
                PageHeader pageHeader = Util.readPageHeader(inputStream);
                assertThat(pageHeader.getType()).isEqualTo(PageType.DICTIONARY_PAGE);
                assertThat(pageHeader.getDictionary_page_header().getNum_values()).isEqualTo(expectedDictionaryValuesCount);
            }
        }
        return dataSource;
    }

    private static void assertDictionaryFiltered(ParquetDataSource dataSource, ParquetMetadata parquetMetadata, List<Type> types, List<String> columnNames, TupleDomain<String> predicate, int expectedRowsRead, long expectedDictionaryFilteredRowGroups)
            throws IOException
    {
        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, ParquetReaderOptions.defaultOptions(), newSimpleAggregatedMemoryContext(), types, columnNames, predicate)) {
            SourcePage page = reader.nextPage();
            int rowsRead = 0;
            while (page != null) {
                rowsRead += page.getPositionCount();
                page = reader.nextPage();
            }
            assertThat(rowsRead).isEqualTo(expectedRowsRead);
            Map<String, Metric<?>> metrics = reader.getMetrics().getMetrics();
            assertThat(metrics).containsKey(PARQUET_READER_DICTIONARY_FILTERED_ROWGROUPS);
            assertThat(((Count<?>) metrics.get(PARQUET_READER_DICTIONARY_FILTERED_ROWGROUPS)).getTotal())
                    .isEqualTo(expectedDictionaryFilteredRowGroups);
        }
    }

    private List<Page> generatePages(List<Type> types, int[] distinctValues, int totalPositions, int pageCount)
    {
        List<? extends List<?>> blockData = types.stream().map(type -> generateBlockValues(type, distinctValues, totalPositions)).toList();
        return generateInputPagesWithBlockData(types, blockData, pageCount);
    }

    private List<?> generateBlockValues(Type type, int[] distinctValues, int totalPositions)
    {
        IntStream intValues = IntStream.range(0, totalPositions).map(i -> distinctValues[i % distinctValues.length]);
        if (type == INTEGER || type == BIGINT) {
            return intValues.asLongStream().boxed().toList();
        }
        else if (type == VARCHAR) {
            return intValues.boxed().map(i -> "varchar-" + i).toList();
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
}
