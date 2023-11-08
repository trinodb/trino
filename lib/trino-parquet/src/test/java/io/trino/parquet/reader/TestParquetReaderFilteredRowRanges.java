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
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.generateInputPagesSequential;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetReaderFilteredRowRanges
{
    @Test
    public void testFilteredRowRanges()
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
                        generateInputPagesSequential(types, 100, 5)),
                new ParquetReaderOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks().size()).isEqualTo(5);

        // No filter
        ParquetReader reader = createParquetReader(
                dataSource,
                parquetMetadata,
                newSimpleAggregatedMemoryContext(),
                types,
                columnNames);
        Optional<ConnectorPageSource.RowRanges> rowRanges = reader.getNextFilteredRowRanges();
        verifyRowRanges(500, rowRanges.orElseThrow(), 0, 500);
        verifyReturnedRowCount(reader, 500);
        // Another getNextFilteredRowRanges call gives an empty result
        rowRanges = reader.getNextFilteredRowRanges();
        verifyRowRanges(0, rowRanges.orElseThrow());

        // No filter, split covers 3rd and 4th row-groups in file
        reader = createParquetReader(
                dataSource,
                parquetMetadata,
                newSimpleAggregatedMemoryContext(),
                types,
                columnNames,
                TupleDomain.all(),
                2000,
                1500);
        rowRanges = reader.getNextFilteredRowRanges();
        verifyRowRanges(200, rowRanges.orElseThrow(), 0, 200);
        verifyReturnedRowCount(reader, 200);

        // non-selective filter
        reader = createParquetReader(
                dataSource,
                parquetMetadata,
                newSimpleAggregatedMemoryContext(),
                types,
                columnNames,
                TupleDomain.withColumnDomains(ImmutableMap.of("columnA", Domain.multipleValues(INTEGER, ImmutableList.of(50L, 150L, 250L, 350L, 450L)))),
                0,
                dataSource.getEstimatedSize());
        rowRanges = reader.getNextFilteredRowRanges();
        verifyRowRanges(500, rowRanges.orElseThrow(), 0, 500);
        verifyReturnedRowCount(reader, 500);

        // selective filter, predicate selects 2nd, 4th and 5th row groups
        reader = createParquetReader(
                dataSource,
                parquetMetadata,
                newSimpleAggregatedMemoryContext(),
                types,
                columnNames,
                TupleDomain.withColumnDomains(ImmutableMap.of("columnA", Domain.multipleValues(INTEGER, ImmutableList.of(150L, 350L, 450L)))),
                0,
                dataSource.getEstimatedSize());
        rowRanges = reader.getNextFilteredRowRanges();
        verifyRowRanges(300, rowRanges.orElseThrow(), 100, 200, 300, 500);
        verifyReturnedRowCount(reader, 300);

        // all rows filtered
        reader = createParquetReader(
                dataSource,
                parquetMetadata,
                newSimpleAggregatedMemoryContext(),
                types,
                columnNames,
                TupleDomain.withColumnDomains(ImmutableMap.of("columnA", Domain.singleValue(INTEGER, 1000L))),
                0,
                dataSource.getEstimatedSize());
        rowRanges = reader.getNextFilteredRowRanges();
        verifyRowRanges(0, rowRanges.orElseThrow());
        verifyReturnedRowCount(reader, 0);

        // selective filter, split covers 3rd and 4th row-groups in file
        reader = createParquetReader(
                dataSource,
                parquetMetadata,
                newSimpleAggregatedMemoryContext(),
                types,
                columnNames,
                // predicate selects 2nd, 4th and 5th row groups
                TupleDomain.withColumnDomains(ImmutableMap.of("columnA", Domain.multipleValues(INTEGER, ImmutableList.of(150L, 350L, 450L)))),
                2000,
                1500);
        rowRanges = reader.getNextFilteredRowRanges();
        verifyRowRanges(100, rowRanges.orElseThrow(), 100, 200);
        verifyReturnedRowCount(reader, 100);
    }

    @Test
    public void testFilteredRowRangesWithColumnIndex()
            throws URISyntaxException, IOException
    {
        File parquetFile = new File(Resources.getResource("lineitem_with_column_index_sorted_by_suppkey.parquet").toURI());
        try (ParquetDataSource dataSource = new FileParquetDataSource(parquetFile, new ParquetReaderOptions())) {
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            assertThat(parquetMetadata.getBlocks().size()).isEqualTo(1);

            List<String> columnNames = ImmutableList.of("suppkey", "shipmode", "comment");
            List<Type> types = ImmutableList.of(BIGINT, VARCHAR, VARCHAR);

            // No filter
            ParquetReader reader = createParquetReader(
                    dataSource,
                    parquetMetadata,
                    newSimpleAggregatedMemoryContext(),
                    types,
                    columnNames);
            Optional<ConnectorPageSource.RowRanges> rowRanges = reader.getNextFilteredRowRanges();
            verifyRowRanges(60175, rowRanges.orElseThrow(), 0, 60175);
            verifyReturnedRowCount(reader, 60175);

            // non-selective filter
            reader = createParquetReader(
                    dataSource,
                    parquetMetadata,
                    newSimpleAggregatedMemoryContext(),
                    types,
                    columnNames,
                    TupleDomain.withColumnDomains(ImmutableMap.of("shipmode", Domain.multipleValues(VARCHAR, ImmutableList.of("AIR", "TRUCK")))),
                    0,
                    dataSource.getEstimatedSize());
            rowRanges = reader.getNextFilteredRowRanges();
            verifyRowRanges(60175, rowRanges.orElseThrow(), 0, 60175);
            verifyReturnedRowCount(reader, 60175);

            // selective filter
            reader = createParquetReader(
                    dataSource,
                    parquetMetadata,
                    newSimpleAggregatedMemoryContext(),
                    types,
                    columnNames,
                    TupleDomain.withColumnDomains(ImmutableMap.of("suppkey", Domain.multipleValues(BIGINT, ImmutableList.of(25L, 35L, 50L, 80L)))),
                    0,
                    dataSource.getEstimatedSize());
            rowRanges = reader.getNextFilteredRowRanges();
            verifyRowRanges(
                    18811,
                    rowRanges.orElseThrow(),
                    11323, 15041,
                    18819, 22524,
                    26255, 33792,
                    45135, 48986);
            verifyReturnedRowCount(reader, 18811);
        }
    }

    private static void verifyReturnedRowCount(ParquetReader reader, long expectedRowCount)
            throws IOException
    {
        long rowCount = 0;
        Page page = reader.nextPage();
        while (page != null) {
            rowCount += page.getPositionCount();
            page = reader.nextPage();
        }
        assertThat(rowCount).isEqualTo(expectedRowCount);
    }

    private static void verifyRowRanges(long expectedRowCount, ConnectorPageSource.RowRanges rowRanges, long... expectedValues)
    {
        checkArgument(expectedValues.length % 2 == 0);
        assertThat(rowRanges.getRowCount()).isEqualTo(expectedRowCount);
        assertThat(rowRanges.getRangesCount()).isEqualTo(expectedValues.length / 2);
        assertThat(rowRanges.isNoMoreRowRanges()).isTrue();
        for (int rangeIndex = 0; rangeIndex < rowRanges.getRangesCount(); rangeIndex++) {
            assertThat(rowRanges.getLowerInclusive(rangeIndex)).isEqualTo(expectedValues[2 * rangeIndex]);
            assertThat(rowRanges.getUpperExclusive(rangeIndex)).isEqualTo(expectedValues[(2 * rangeIndex) + 1]);
        }
    }
}
