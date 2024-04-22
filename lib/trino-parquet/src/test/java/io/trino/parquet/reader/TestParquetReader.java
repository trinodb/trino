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
import com.google.common.io.Resources;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.metrics.Count;
import io.trino.spi.metrics.Metric;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.generateInputPages;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.parquet.reader.ParquetReader.COLUMN_INDEX_ROWS_FILTERED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestParquetReader
{
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
                new ParquetReaderOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks().size()).isGreaterThan(1);
        // Verify file has only non-dictionary encodings as dictionary memory usage is already tested in TestFlatColumnReader#testMemoryUsage
        parquetMetadata.getBlocks().forEach(block -> {
            block.getColumns()
                    .forEach(columnChunkMetaData -> assertThat(columnChunkMetaData.getEncodingStats().hasDictionaryEncodedPages()).isFalse());
            assertThat(block.getRowCount()).isEqualTo(100);
        });

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        ParquetReader reader = createParquetReader(dataSource, parquetMetadata, memoryContext, types, columnNames);

        Page page = reader.nextPage();
        assertThat(page.getBlock(0)).isInstanceOf(LazyBlock.class);
        assertThat(memoryContext.getBytes()).isEqualTo(0);
        page.getBlock(0).getLoadedBlock();
        // Memory usage due to reading data and decoding parquet page of 1st block
        long initialMemoryUsage = memoryContext.getBytes();
        assertThat(initialMemoryUsage).isGreaterThan(0);

        // Memory usage due to decoding parquet page of 2nd block
        page.getBlock(1).getLoadedBlock();
        long currentMemoryUsage = memoryContext.getBytes();
        assertThat(currentMemoryUsage).isGreaterThan(initialMemoryUsage);

        // Memory usage does not change until next row group (1 page per row-group)
        long rowGroupRowCount = parquetMetadata.getBlocks().get(0).getRowCount();
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
                new ParquetReaderOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks().size()).isEqualTo(2);
        // The predicate and the file are prepared so that page indexes will result in non-overlapping row ranges and eliminate the entire first row group
        // while the second row group still has to be read
        TupleDomain<String> predicate = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        "l_shipdate", Domain.multipleValues(DATE, ImmutableList.of(LocalDate.of(1993, 1, 1).toEpochDay(), LocalDate.of(1997, 1, 1).toEpochDay())),
                        "l_commitdate", Domain.create(ValueSet.ofRanges(Range.greaterThan(DATE, LocalDate.of(1995, 1, 1).toEpochDay())), false)));

        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, new ParquetReaderOptions(), newSimpleAggregatedMemoryContext(), types, columnNames, predicate)) {
            Page page = reader.nextPage();
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
                    .isGreaterThanOrEqualTo(parquetMetadata.getBlocks().get(0).getRowCount());
        }
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

    private void testReadingOldParquetFiles(File file, List<String> columnNames, Type columnType, List<?> expectedValues)
            throws IOException
    {
        ParquetDataSource dataSource = new FileParquetDataSource(
                file,
                new ParquetReaderOptions());
        ConnectorSession session = TestingConnectorSession.builder().build();
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, newSimpleAggregatedMemoryContext(), ImmutableList.of(columnType), columnNames)) {
            Page page = reader.nextPage();
            Iterator<?> expected = expectedValues.iterator();
            while (page != null) {
                Block block = page.getBlock(0);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    assertThat(columnType.getObjectValue(session, block, i)).isEqualTo(expected.next());
                }
                page = reader.nextPage();
            }
            assertThat(expected.hasNext())
                    .describedAs("Read fewer values than expected")
                    .isFalse();
        }
    }
}
