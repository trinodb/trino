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
import com.google.common.collect.ImmutableSet;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetTypeWidenedPredicate
{
    @Test
    public void testFloatFileQueriedAsDouble()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("c");
        BlockBuilder realValues = REAL.createFixedSizeBlockBuilder(2);
        REAL.writeLong(realValues, floatToRawIntBits(1.5f));
        REAL.writeLong(realValues, floatToRawIntBits(2.5f));

        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder().build(),
                        ImmutableList.of(REAL),
                        columnNames,
                        ImmutableList.of(new Page(2, realValues.build()))),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());

        assertThat(readMatchingRows(dataSource, parquetMetadata, DOUBLE, columnNames, Domain.singleValue(DOUBLE, 1.5d)))
                .containsExactly(1.5d);
        assertThat(readMatchingRows(dataSource, parquetMetadata, DOUBLE, columnNames, Domain.singleValue(DOUBLE, 9.9d)))
                .isEmpty();
    }

    @Test
    public void testInt32FileQueriedAsBigint()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("c");
        BlockBuilder intValues = INTEGER.createFixedSizeBlockBuilder(2);
        INTEGER.writeLong(intValues, 123);
        INTEGER.writeLong(intValues, 456);

        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder().build(),
                        ImmutableList.of(INTEGER),
                        columnNames,
                        ImmutableList.of(new Page(2, intValues.build()))),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());

        assertThat(readMatchingLongs(dataSource, parquetMetadata, BIGINT, columnNames, Domain.singleValue(BIGINT, 123L)))
                .containsExactly(123L);
        assertThat(readMatchingLongs(dataSource, parquetMetadata, BIGINT, columnNames, Domain.singleValue(BIGINT, 999L)))
                .isEmpty();
    }

    @Test
    public void testFloatBloomFilterQueriedAsDouble()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("c");
        BlockBuilder first = REAL.createFixedSizeBlockBuilder(1);
        REAL.writeLong(first, floatToRawIntBits(1.5f));
        BlockBuilder second = REAL.createFixedSizeBlockBuilder(1);
        REAL.writeLong(second, floatToRawIntBits(2.5f));

        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxRowGroupRowCount(1)
                                .setBloomFilterColumns(ImmutableSet.copyOf(columnNames))
                                .build(),
                        ImmutableList.of(REAL),
                        columnNames,
                        ImmutableList.of(new Page(1, first.build()), new Page(1, second.build()))),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks()).hasSize(2);

        ParquetReaderOptions bloomOptions = ParquetReaderOptions.builder().withBloomFilter(true).build();
        assertThat(readMatchingRows(dataSource, parquetMetadata, bloomOptions, DOUBLE, columnNames, Domain.singleValue(DOUBLE, 1.5d)))
                .containsExactly(1.5d);
        assertThat(readMatchingRows(dataSource, parquetMetadata, bloomOptions, DOUBLE, columnNames, Domain.singleValue(DOUBLE, 9.9d)))
                .isEmpty();
    }

    private static List<Double> readMatchingRows(
            ParquetDataSource dataSource,
            ParquetMetadata parquetMetadata,
            Type readType,
            List<String> columnNames,
            Domain predicate)
            throws IOException
    {
        return readMatchingRows(dataSource, parquetMetadata, ParquetReaderOptions.defaultOptions(), readType, columnNames, predicate);
    }

    private static List<Double> readMatchingRows(
            ParquetDataSource dataSource,
            ParquetMetadata parquetMetadata,
            ParquetReaderOptions options,
            Type readType,
            List<String> columnNames,
            Domain predicate)
            throws IOException
    {
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of("c", predicate));
        List<Double> values = new ArrayList<>();
        try (ParquetReader reader = createParquetReader(
                dataSource,
                parquetMetadata,
                options,
                newSimpleAggregatedMemoryContext(),
                ImmutableList.of(readType),
                columnNames,
                tupleDomain)) {
            SourcePage page = reader.nextPage();
            while (page != null) {
                for (int i = 0; i < page.getPositionCount(); i++) {
                    values.add(DOUBLE.getDouble(page.getBlock(0), i));
                }
                page = reader.nextPage();
            }
        }
        return values;
    }

    private static List<Long> readMatchingLongs(
            ParquetDataSource dataSource,
            ParquetMetadata parquetMetadata,
            Type readType,
            List<String> columnNames,
            Domain predicate)
            throws IOException
    {
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of("c", predicate));
        List<Long> values = new ArrayList<>();
        try (ParquetReader reader = createParquetReader(
                dataSource,
                parquetMetadata,
                ParquetReaderOptions.defaultOptions(),
                newSimpleAggregatedMemoryContext(),
                ImmutableList.of(readType),
                columnNames,
                tupleDomain)) {
            SourcePage page = reader.nextPage();
            while (page != null) {
                for (int i = 0; i < page.getPositionCount(); i++) {
                    values.add(BIGINT.getLong(page.getBlock(0), i));
                }
                page = reader.nextPage();
            }
        }
        return values;
    }
}
