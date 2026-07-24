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
import com.google.common.collect.ListMultimap;
import com.google.common.io.Resources;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Count;
import io.trino.spi.metrics.Metric;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.generateInputPages;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.parquet.reader.ParquetReader.COLUMN_INDEX_ROWS_FILTERED;
import static io.trino.parquet.reader.ParquetReader.isRowSelectionBeneficial;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestParquetReader
{
    @Test
    public void testSelectedPositionsPushdownHeuristic()
    {
        assertThat(isRowSelectionBeneficial(8_192, 0, 0, INT32, false, false, 4, 1)).isTrue();

        // Required fixed-width values use a 5% selectivity limit.
        assertThat(isRowSelectionBeneficial(8_192, 410, 16, INT32, false, false, 4, 1)).isTrue();
        assertThat(isRowSelectionBeneficial(8_192, 411, 16, INT32, false, false, 4, 1)).isFalse();

        // Every value family uses the same locality requirement.
        assertThat(isRowSelectionBeneficial(8_192, 409, 22, INT32, false, false, 4, 1)).isTrue();
        assertThat(isRowSelectionBeneficial(8_192, 409, 23, INT32, false, false, 4, 1)).isFalse();

        // Boolean row-level skipping is deferred until its representation changes.
        assertThat(isRowSelectionBeneficial(8_192, 409, 16, BOOLEAN, false, false, 1, 1)).isFalse();

        // Nullable readers fall back except for expensive fixed-length byte arrays.
        assertThat(isRowSelectionBeneficial(8_192, 409, 16, INT32, false, true, 4, 1)).isFalse();
        assertThat(isRowSelectionBeneficial(8_192, 2_048, 16, FIXED_LEN_BYTE_ARRAY, false, true, 16, 1)).isTrue();
        assertThat(isRowSelectionBeneficial(8_192, 2_049, 16, FIXED_LEN_BYTE_ARRAY, false, true, 16, 1)).isFalse();

        // Dictionary indexes share a 10% limit across physical value types.
        assertThat(isRowSelectionBeneficial(8_192, 819, 16, INT32, true, false, 1, 1)).isTrue();
        assertThat(isRowSelectionBeneficial(8_192, 820, 16, INT32, true, false, 1, 1)).isFalse();
        assertThat(isRowSelectionBeneficial(8_192, 819, 16, BINARY, true, false, 1, 1)).isTrue();

        // Plain binary values use a 25% limit independent of value width.
        assertThat(isRowSelectionBeneficial(8_192, 2_048, 16, BINARY, false, false, 256, 50)).isTrue();
        assertThat(isRowSelectionBeneficial(8_192, 2_049, 16, BINARY, false, false, 256, 50)).isFalse();

        // Very wide values use row-level decoding only for small projections.
        assertThat(isRowSelectionBeneficial(8_192, 2_048, 16, BINARY, false, false, 1_024, 4)).isTrue();
        assertThat(isRowSelectionBeneficial(8_192, 2_048, 16, BINARY, false, false, 1_024, 5)).isFalse();
    }

    @Test
    public void testColumnReaderMemoryUsage()
            throws IOException
    {
        // Write a file with 100 rows per row-group
        List<String> columnNames = ImmutableList.of("columnA", "columnB");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);

        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxBlockSize(DataSize.ofBytes(1000))
                                .build(),
                        types,
                        columnNames,
                        generateInputPages(types, 100, 5)),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks().size()).isGreaterThan(1);
        // Verify file has only non-dictionary encodings as dictionary memory usage is already tested in TestFlatColumnReader#testMemoryUsage
        parquetMetadata.getBlocks().forEach(block -> {
            block.columns()
                    .forEach(columnChunkMetaData -> assertThat(columnChunkMetaData.getEncodingStats().hasDictionaryEncodedPages()).isFalse());
            assertThat(block.rowCount()).isEqualTo(100);
        });

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        ParquetReader reader = createParquetReader(dataSource, parquetMetadata, memoryContext, types, columnNames);

        SourcePage page = reader.nextPage();
        assertThat(memoryContext.getBytes()).isEqualTo(0);
        page.getBlock(0);
        // Memory usage due to reading data and decoding parquet page of 1st block
        long initialMemoryUsage = memoryContext.getBytes();
        assertThat(initialMemoryUsage).isGreaterThan(0);

        // Memory usage due to decoding parquet page of 2nd block
        page.getBlock(1);
        long currentMemoryUsage = memoryContext.getBytes();
        assertThat(currentMemoryUsage).isGreaterThan(initialMemoryUsage);

        // Memory usage does not change until next row group (1 page per row-group)
        long rowGroupRowCount = parquetMetadata.getBlocks().get(0).rowCount();
        int rowsRead = page.getPositionCount();
        while (rowsRead < rowGroupRowCount) {
            rowsRead += reader.nextPage().getPositionCount();
            if (rowsRead <= rowGroupRowCount) {
                assertThat(memoryContext.getBytes()).isEqualTo(currentMemoryUsage);
            }
        }

        // New row-group should release memory from old column readers, while using some memory for data read
        reader.nextPage();
        assertThat(memoryContext.getBytes()).isBetween(1L, currentMemoryUsage - 1);

        reader.close();
        assertThat(memoryContext.getBytes()).isEqualTo(0);
    }

    @Test
    public void testEmptyRowRangesWithColumnIndex()
            throws URISyntaxException, IOException
    {
        List<String> columnNames = ImmutableList.of("l_shipdate", "l_commitdate");
        List<Type> types = ImmutableList.of(DATE, DATE);

        ParquetDataSource dataSource = new FileParquetDataSource(
                new File(Resources.getResource("lineitem_sorted_by_shipdate/data.parquet").toURI()),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks()).hasSize(2);
        // The predicate and the file are prepared so that page indexes will result in non-overlapping row ranges and eliminate the entire first row group
        // while the second row group still has to be read
        TupleDomain<String> predicate = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        "l_shipdate", Domain.multipleValues(DATE, ImmutableList.of(LocalDate.of(1993, 1, 1).toEpochDay(), LocalDate.of(1997, 1, 1).toEpochDay())),
                        "l_commitdate", Domain.create(ValueSet.ofRanges(Range.greaterThan(DATE, LocalDate.of(1995, 1, 1).toEpochDay())), false)));

        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, ParquetReaderOptions.defaultOptions(), newSimpleAggregatedMemoryContext(), types, columnNames, predicate)) {
            SourcePage page = reader.nextPage();
            int rowsRead = 0;
            while (page != null) {
                rowsRead += page.getPositionCount();
                page = reader.nextPage();
            }
            assertThat(rowsRead).isEqualTo(2387);
            Map<String, Metric<?>> metrics = reader.getMetrics().getMetrics();
            assertThat(metrics).containsKey(COLUMN_INDEX_ROWS_FILTERED);
            // Column index should filter at least the first row group
            assertThat(((Count<?>) metrics.get(COLUMN_INDEX_ROWS_FILTERED)).getTotal())
                    .isGreaterThanOrEqualTo(parquetMetadata.getBlocks().get(0).rowCount());
        }
    }

    @Test
    public void testSelectPositionsOnLoadedBlocks()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("columna", "columnb");
        List<Type> types = ImmutableList.of(BIGINT, BIGINT);

        // columnA has row number values, columnB has row number * 10
        int rowCount = 100;
        BlockBuilder columnA = BIGINT.createFixedSizeBlockBuilder(rowCount);
        BlockBuilder columnB = BIGINT.createFixedSizeBlockBuilder(rowCount);
        for (int i = 0; i < rowCount; i++) {
            BIGINT.writeLong(columnA, i);
            BIGINT.writeLong(columnB, i * 10L);
        }
        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder().build(),
                        types,
                        columnNames,
                        ImmutableList.of(new Page(rowCount, columnA.build(), columnB.build()))),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, newSimpleAggregatedMemoryContext(), types, columnNames)) {
            // batch sizes grow from 1, skip to a page with at least 8 positions
            long firstRow = 0;
            SourcePage page = reader.nextPage();
            while (page.getPositionCount() < 8) {
                firstRow += page.getPositionCount();
                page = reader.nextPage();
            }

            page.selectPositions(new int[] {1, 3, 5, 7}, 0, 4);
            assertThat(blockValues(page.getBlock(0))).containsExactly(firstRow + 1, firstRow + 3, firstRow + 5, firstRow + 7);
            assertThat(page.getSizeInBytes()).isEqualTo(page.getBlock(0).getSizeInBytes());

            // select again with positions relative to the previous selection
            page.selectPositions(new int[] {1, 2}, 0, 2);
            assertThat(page.getPositionCount()).isEqualTo(2);
            // columnA was loaded before the second selection, columnB is loaded after it
            assertThat(blockValues(page.getBlock(0))).containsExactly(firstRow + 3, firstRow + 5);
            assertThat(page.getSizeInBytes()).isEqualTo(page.getBlock(0).getSizeInBytes());
            assertThat(blockValues(page.getBlock(1))).containsExactly((firstRow + 3) * 10, (firstRow + 5) * 10);

            SourcePage nextPage = reader.nextPage();
            assertThat(blockValues(nextPage.getBlock(1)))
                    .startsWith((firstRow + 8) * 10, (firstRow + 9) * 10, (firstRow + 10) * 10);
        }
    }

    @Test
    public void testRepeatedSelectionsWithInterleavedColumnLoads()
            throws IOException
    {
        int rowCount = 65_536;
        List<String> columnNames = ImmutableList.of("filter_a", "filter_b", "projected");
        List<Type> types = ImmutableList.of(BIGINT, BIGINT, BIGINT);
        BlockBuilder filterA = BIGINT.createFixedSizeBlockBuilder(rowCount);
        BlockBuilder filterB = BIGINT.createFixedSizeBlockBuilder(rowCount);
        BlockBuilder projected = BIGINT.createFixedSizeBlockBuilder(rowCount);
        for (int position = 0; position < rowCount; position++) {
            BIGINT.writeLong(filterA, position);
            BIGINT.writeLong(filterB, position * 10L);
            BIGINT.writeLong(projected, position * 100L);
        }

        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxPageValueCount(1_024)
                                .build(),
                        types,
                        columnNames,
                        ImmutableList.of(new Page(rowCount, filterA.build(), filterB.build(), projected.build()))),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        try (ParquetReader reader = createParquetReader(
                dataSource,
                parquetMetadata,
                ParquetReaderOptions.defaultOptions(),
                newSimpleAggregatedMemoryContext(),
                types,
                columnNames,
                TupleDomain.all(),
                true)) {
            for (SourcePage page = reader.nextPage(); page != null; page = reader.nextPage()) {
                Block firstFilterBlock = page.getBlock(0);
                int[] firstSelection = IntStream.range(0, page.getPositionCount())
                        .filter(position -> BIGINT.getLong(firstFilterBlock, position) % 512 < 8)
                        .toArray();
                if (firstSelection.length == 0) {
                    continue;
                }
                page.selectPositions(firstSelection, 0, firstSelection.length);

                Block secondFilterBlock = page.getBlock(1);
                int[] secondSelection = IntStream.range(0, page.getPositionCount())
                        .filter(position -> (BIGINT.getLong(secondFilterBlock, position) / 10) % 2 == 0)
                        .toArray();
                if (secondSelection.length == 0) {
                    continue;
                }
                assertThat(page.trySelectPositions(secondSelection, 0, secondSelection.length)).isTrue();

                Block projectedBlock = page.getBlock(2);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    assertThat(BIGINT.getLong(projectedBlock, position))
                            .isEqualTo(BIGINT.getLong(page.getBlock(0), position) * 100);
                }
            }
        }
    }

    @Test
    public void testSelectNonAscendingPositions()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("column");
        List<Type> types = ImmutableList.of(BIGINT);
        int rowCount = 100;
        BlockBuilder values = BIGINT.createFixedSizeBlockBuilder(rowCount);
        for (int i = 0; i < rowCount; i++) {
            BIGINT.writeLong(values, i);
        }

        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder().build(),
                        types,
                        columnNames,
                        ImmutableList.of(new Page(values.build()))),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, newSimpleAggregatedMemoryContext(), types, columnNames)) {
            long firstRow = 0;
            SourcePage page = reader.nextPage();
            while (page.getPositionCount() < 4) {
                firstRow += page.getPositionCount();
                page = reader.nextPage();
            }

            int[] positions = new int[13];
            positions[10] = 3;
            positions[11] = 1;
            positions[12] = 3;
            page.selectPositions(positions, 10, 3);
            assertThat(page.getPositionCount()).isEqualTo(3);
            assertThat(blockValues(page.getBlock(0))).containsExactly(firstRow + 3, firstRow + 1, firstRow + 3);
            assertThat(reader.getSelectedPositionsPushdownCount()).isZero();
            assertThat(reader.getSelectedPositionsFallbackCount()).isOne();
        }
    }

    @Test
    public void testSingleSelectedRunAtPageEdgeFallsBack()
            throws IOException
    {
        int rowCount = 4_096;
        List<String> columnNames = ImmutableList.of("column");
        List<Type> types = ImmutableList.of(BIGINT);
        BlockBuilder values = BIGINT.createFixedSizeBlockBuilder(rowCount);
        for (int position = 0; position < rowCount; position++) {
            BIGINT.writeLong(values, position);
        }

        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxPageValueCount(rowCount)
                                .build(),
                        types,
                        columnNames,
                        ImmutableList.of(new Page(values.build()))),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, newSimpleAggregatedMemoryContext(), types, columnNames)) {
            long firstRow = 0;
            SourcePage page = reader.nextPage();
            while (page.getPositionCount() < 1_024) {
                firstRow += page.getPositionCount();
                page = reader.nextPage();
            }

            int[] edgeRun = IntStream.range(0, 40).toArray();
            page.selectPositions(edgeRun, 0, edgeRun.length);
            long edgeFirstRow = firstRow;
            assertThat(blockValues(page.getBlock(0))).containsExactlyElementsOf(
                    IntStream.range(0, 40).mapToLong(position -> edgeFirstRow + position).boxed().toList());
            assertThat(reader.getSelectedPositionsPushdownCount()).isZero();
            assertThat(reader.getSelectedPositionsFallbackCount()).isOne();

            firstRow += 1_024;
            page = reader.nextPage();
            int[] interiorRun = IntStream.range(400, 480).toArray();
            page.selectPositions(interiorRun, 0, interiorRun.length);
            long interiorFirstRow = firstRow;
            assertThat(blockValues(page.getBlock(0))).containsExactlyElementsOf(
                    IntStream.range(400, 480).mapToLong(position -> interiorFirstRow + position).boxed().toList());
            assertThat(reader.getSelectedPositionsPushdownCount()).isOne();
            assertThat(reader.getSelectedPositionsFallbackCount()).isOne();
        }
    }

    @Test
    public void testSelectPositionOutsidePage()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("column");
        List<Type> types = ImmutableList.of(BIGINT);
        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder().build(),
                        types,
                        columnNames,
                        generateInputPages(types, 100, 1)),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, newSimpleAggregatedMemoryContext(), types, columnNames)) {
            SourcePage page = reader.nextPage();
            assertThatThrownBy(() -> page.selectPositions(new int[] {page.getPositionCount()}, 0, 1))
                    .isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    @Test
    public void testSelectedPositionsAreCopied()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("column");
        List<Type> types = ImmutableList.of(BIGINT);
        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder().build(),
                        types,
                        columnNames,
                        generateInputPages(types, 100, 1)),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        try (ParquetReader reader = createParquetReader(
                dataSource,
                parquetMetadata,
                ParquetReaderOptions.defaultOptions(),
                newSimpleAggregatedMemoryContext(),
                types,
                columnNames,
                TupleDomain.all(),
                true)) {
            long firstRow = 0;
            SourcePage page = reader.nextPage();
            while (page.getPositionCount() < 4) {
                firstRow += page.getPositionCount();
                page = reader.nextPage();
            }

            // No unloaded column can benefit, so the source must remain unchanged.
            int[] offeredPositions = {1, 3};
            int originalPositionCount = page.getPositionCount();
            page.getBlock(0);
            assertThat(page.trySelectPositions(offeredPositions, 0, offeredPositions.length)).isFalse();
            assertThat(page.getPositionCount()).isEqualTo(originalPositionCount);

            // Use an unloaded page for the existing ownership check below.
            firstRow += originalPositionCount;
            page = reader.nextPage();
            while (page.getPositionCount() < 4) {
                firstRow += page.getPositionCount();
                page = reader.nextPage();
            }

            int mandatoryPagePositionCount = page.getPositionCount();
            int[] positions = {1, 3};
            page.selectPositions(positions, 0, positions.length);
            Arrays.fill(positions, 0);

            assertThat(blockValues(page.getBlock(0))).containsExactly(firstRow + 1, firstRow + 3);

            // An accepted optional selection must also retain its own positions.
            firstRow += mandatoryPagePositionCount;
            page = reader.nextPage();
            while (page.getPositionCount() < 4) {
                firstRow += page.getPositionCount();
                page = reader.nextPage();
            }

            int[] offeredSelection = {-1, 1, 3, -1};
            assertThat(page.trySelectPositions(offeredSelection, 1, 2)).isTrue();
            Arrays.fill(offeredSelection, 0);

            assertThat(blockValues(page.getBlock(0))).containsExactly(firstRow + 1, firstRow + 3);
        }
    }

    @Test
    public void testSelectedPositionsUpdateAdaptiveBatchSizeBasedOnInputRows()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("payload");
        List<Type> types = ImmutableList.of(VARCHAR);
        int rowCount = 3_000;
        BlockBuilder values = VARCHAR.createBlockBuilder(null, rowCount);
        for (int position = 0; position < rowCount; position++) {
            VARCHAR.writeSlice(values, utf8Slice("%08d".formatted(position) + "x".repeat(248)));
        }

        ParquetReaderOptions readerOptions = ParquetReaderOptions.builder()
                .withMaxReadBlockSize(DataSize.ofBytes(200_000))
                .withMaxReadBlockRowCount(rowCount)
                .build();
        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxPageValueCount(rowCount)
                                .build(),
                        types,
                        columnNames,
                        ImmutableList.of(new Page(values.build()))),
                readerOptions);
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, readerOptions, newSimpleAggregatedMemoryContext(), types, columnNames, TupleDomain.all())) {
            for (int expectedBatchSize : new int[] {1, 2, 4, 8, 16}) {
                SourcePage page = reader.nextPage();
                assertThat(page.getPositionCount()).isEqualTo(expectedBatchSize);
                assertThat(page.getBlock(0).getPositionCount()).isEqualTo(expectedBatchSize);
            }
            for (int expectedBatchSize : new int[] {32, 64, 128}) {
                SourcePage page = reader.nextPage();
                assertThat(page.getPositionCount()).isEqualTo(expectedBatchSize);
                page.selectPositions(new int[] {0}, 0, 1);
                assertThat(page.getBlock(0).getPositionCount()).isEqualTo(1);
            }

            assertThat(reader.nextPage().getPositionCount()).isEqualTo(256);
        }
    }

    @Test
    public void testSelectPositionsWithColumnIndexRowRanges()
            throws Exception
    {
        File parquetFile = new File(Resources.getResource("lineitem_sorted_by_shipdate/data.parquet").toURI());
        List<String> columnNames = ImmutableList.of("l_shipdate", "l_commitdate");
        List<Type> types = ImmutableList.of(DATE, DATE);
        TupleDomain<String> predicate = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        "l_shipdate", Domain.multipleValues(DATE, ImmutableList.of(LocalDate.of(1993, 1, 1).toEpochDay(), LocalDate.of(1997, 1, 1).toEpochDay())),
                        "l_commitdate", Domain.create(ValueSet.ofRanges(Range.greaterThan(DATE, LocalDate.of(1995, 1, 1).toEpochDay())), false)));

        try (ParquetReader expectedReader = createFilteredParquetReader(parquetFile, types, columnNames, predicate);
                ParquetReader selectedReader = createFilteredParquetReader(parquetFile, types, columnNames, predicate)) {
            SourcePage expectedPage = expectedReader.nextPage();
            SourcePage selectedPage = selectedReader.nextPage();
            int selectedRowCount = 0;
            while (expectedPage != null) {
                assertThat(selectedPage).isNotNull();
                assertThat(selectedPage.getPositionCount()).isEqualTo(expectedPage.getPositionCount());

                int[] positions = IntStream.range(0, expectedPage.getPositionCount())
                        .filter(position -> position % 5 <= 1)
                        .toArray();
                Block expectedShipDate = expectedPage.getBlock(0).copyPositions(positions, 0, positions.length);
                Block expectedCommitDate = expectedPage.getBlock(1).copyPositions(positions, 0, positions.length);
                selectedPage.selectPositions(positions, 0, positions.length);
                assertThat(dateValues(selectedPage.getBlock(0))).isEqualTo(dateValues(expectedShipDate));
                assertThat(dateValues(selectedPage.getBlock(1))).isEqualTo(dateValues(expectedCommitDate));
                selectedRowCount += positions.length;

                expectedPage = expectedReader.nextPage();
                selectedPage = selectedReader.nextPage();
            }
            assertThat(selectedPage).isNull();
            assertThat(selectedRowCount).isPositive();
        }
    }

    private static ParquetReader createFilteredParquetReader(File parquetFile, List<Type> types, List<String> columnNames, TupleDomain<String> predicate)
            throws IOException
    {
        ParquetDataSource dataSource = new FileParquetDataSource(parquetFile, ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        return createParquetReader(
                dataSource,
                parquetMetadata,
                ParquetReaderOptions.defaultOptions(),
                newSimpleAggregatedMemoryContext(),
                types,
                columnNames,
                predicate);
    }

    private static List<Long> blockValues(Block block)
    {
        ImmutableList.Builder<Long> values = ImmutableList.builder();
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.add(BIGINT.getLong(block, position));
        }
        return values.build();
    }

    private static List<Long> dateValues(Block block)
    {
        ImmutableList.Builder<Long> values = ImmutableList.builder();
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.add(DATE.getLong(block, position));
        }
        return values.build();
    }

    @Test
    public void testBackwardsCompatibleRepeatedStringField()
            throws Exception
    {
        File parquetFile = new File(Resources.getResource("parquet_repeated_primitives/string/old-repeated-string.parquet").toURI());
        List<List<String>> expectedValues = ImmutableList.of(Arrays.asList("hello", "world"), Arrays.asList("good", "bye"), Arrays.asList("one", "two", "three"));
        testReadingOldParquetFiles(parquetFile, ImmutableList.of("myString"), new ArrayType(VARCHAR), expectedValues);
    }

    @Test
    public void testBackwardsCompatibleRepeatedIntegerField()
            throws Exception
    {
        File parquetFile = new File(Resources.getResource("parquet_repeated_primitives/int/old-repeated-int.parquet").toURI());
        List<List<Integer>> expectedValues = ImmutableList.of(Arrays.asList(1, 2, 3));
        testReadingOldParquetFiles(parquetFile, ImmutableList.of("repeatedInt"), new ArrayType(INTEGER), expectedValues);
    }

    @Test
    public void testBackwardsCompatibleRepeatedPrimitiveFieldDefinedAsPrimitive()
    {
        assertThatThrownBy(() -> {
            File parquetFile = new File(Resources.getResource("parquet_repeated_primitives/int/old-repeated-int.parquet").toURI());
            List<List<Integer>> expectedValues = ImmutableList.of(Arrays.asList(1, 2, 3));
            testReadingOldParquetFiles(parquetFile, ImmutableList.of("repeatedInt"), INTEGER, expectedValues);
        }).hasMessage("Unsupported Trino column type (integer) for Parquet column ([repeatedint] repeated int32 repeatedint)")
                .isInstanceOf(TrinoException.class);
    }

    @Test
    void testReadMetadataWithSplitOffset()
            throws IOException
    {
        // Write a file with 100 rows per row-group
        List<String> columnNames = ImmutableList.of("columna", "columnb");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);

        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxBlockSize(DataSize.ofBytes(1000))
                                .build(),
                        types,
                        columnNames,
                        generateInputPages(types, 100, 5)),
                ParquetReaderOptions.defaultOptions());

        // Read both columns, 1 row group
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        List<BlockMetadata> columnBlocks = parquetMetadata.getBlocks(0, 800);
        assertThat(columnBlocks.size()).isEqualTo(1);
        assertThat(columnBlocks.getFirst().columns().size()).isEqualTo(2);
        assertThat(columnBlocks.getFirst().rowCount()).isEqualTo(100);

        // Read both columns, half row groups
        parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        columnBlocks = parquetMetadata.getBlocks(0, 2500);
        assertThat(columnBlocks.stream().allMatch(block -> block.columns().size() == 2)).isTrue();
        assertThat(columnBlocks.stream().mapToLong(BlockMetadata::rowCount).sum()).isEqualTo(300);

        // Read both columns, all row groups
        parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        columnBlocks = parquetMetadata.getBlocks();
        assertThat(columnBlocks.stream().allMatch(block -> block.columns().size() == 2)).isTrue();
        assertThat(columnBlocks.stream().mapToLong(BlockMetadata::rowCount).sum()).isEqualTo(500);
    }

    @Test
    void testMaxFooterReadSize()
            throws IOException
    {
        // Write a file with 10 rows per row-group
        List<String> columnNames = ImmutableList.of("columna", "columnb");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);

        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxBlockSize(DataSize.ofBytes(10))
                                .build(),
                        types,
                        columnNames,
                        generateInputPages(types, 10, 50)),
                ParquetReaderOptions.defaultOptions());

        ParquetReaderOptions readerOptions = ParquetReaderOptions.builder()
                .withMaxFooterReadSize(DataSize.ofBytes(1000))
                .build();

        assertThatThrownBy(() -> MetadataReader.readFooter(dataSource, readerOptions, Optional.empty(), Optional.empty()))
                .hasMessageMatching(".* Parquet footer size .* exceeds maximum allowed size .*");
    }

    @Test
    void testFooterReadSize()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("columna", "columnb");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);

        RecordingParquetDataSource dataSource = new RecordingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxBlockSize(DataSize.ofBytes(10))
                                .build(),
                        types,
                        columnNames,
                        generateInputPages(types, 10, 50)));

        ParquetReaderOptions readerOptions = ParquetReaderOptions.builder()
                .withFooterReadSize(DataSize.ofBytes(128))
                .build();

        MetadataReader.readFooter(dataSource, readerOptions, Optional.empty(), Optional.empty());

        assertThat(dataSource.getTailReadLengths()).startsWith(128);
    }

    @Test
    void testParseFooterFromSuppliedFooterBytes()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("columna", "columnb");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);
        Slice parquetFile = writeParquetFile(
                ParquetWriterOptions.builder()
                        .setMaxBlockSize(DataSize.ofBytes(10))
                        .build(),
                types,
                columnNames,
                generateInputPages(types, 10, 50));

        RecordingParquetDataSource source = new RecordingParquetDataSource(parquetFile);
        Slice footerBytes = MetadataReader.readFooterBytes(source, ParquetReaderOptions.defaultOptions());

        ParquetMetadata metadata = MetadataReader.parseFooter(new ParquetDataSourceId("test"), parquetFile.length(), footerBytes, Optional.empty(), Optional.empty());

        assertThat(metadata.getBlocks().stream().mapToLong(BlockMetadata::rowCount).sum()).isEqualTo(500);
    }

    private void testReadingOldParquetFiles(File file, List<String> columnNames, Type columnType, List<?> expectedValues)
            throws IOException
    {
        ParquetDataSource dataSource = new FileParquetDataSource(
                file,
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, newSimpleAggregatedMemoryContext(), ImmutableList.of(columnType), columnNames)) {
            SourcePage page = reader.nextPage();
            Iterator<?> expected = expectedValues.iterator();
            while (page != null) {
                Block block = page.getBlock(0);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    assertThat(columnType.getObjectValue(block, i)).isEqualTo(expected.next());
                }
                page = reader.nextPage();
            }
            assertThat(expected.hasNext())
                    .describedAs("Read fewer values than expected")
                    .isFalse();
        }
    }

    private static class RecordingParquetDataSource
            implements ParquetDataSource
    {
        private final ParquetDataSourceId id = new ParquetDataSourceId("recording");
        private final Slice input;
        private final List<Integer> tailReadLengths = new ArrayList<>();
        private long readBytes;

        public RecordingParquetDataSource(Slice input)
        {
            this.input = input;
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
            return 0;
        }

        @Override
        public long getEstimatedSize()
        {
            return input.length();
        }

        @Override
        public Slice readTail(int length)
        {
            tailReadLengths.add(length);
            int readSize = toIntExact(min(input.length(), length));
            readBytes += readSize;
            return input.slice(input.length() - readSize, readSize);
        }

        @Override
        public Slice readFully(long position, int length)
        {
            readBytes += length;
            return input.slice(toIntExact(position), length);
        }

        @Override
        public <K> Map<K, ChunkedInputStream> planRead(ListMultimap<K, DiskRange> diskRanges, AggregatedMemoryContext memoryContext)
        {
            throw new UnsupportedOperationException();
        }

        public List<Integer> getTailReadLengths()
        {
            return tailReadLengths;
        }
    }
}
