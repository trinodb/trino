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
package io.trino.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.filesystem.local.LocalOutputFile;
import io.trino.orc.OrcWriteValidation.OrcWriteValidationMode;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.OrcType;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.fullyProjectedLayout;
import static io.trino.orc.OrcTester.READER_OPTIONS;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOrcReaderFilteredRowRanges
{
    @Test
    public void tesFilteredRowRanges()
            throws IOException
    {
        TempFile tempFile = new TempFile();
        // Write a file with 100 rows per stripe and 1 row-group per stripe
        List<String> columnNames = ImmutableList.of("test1", "test2");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);

        OrcWriter writer = new OrcWriter(
                OutputStreamOrcDataSink.create(new LocalOutputFile(tempFile.getFile())),
                columnNames,
                types,
                OrcType.createRootOrcType(columnNames, types),
                NONE,
                new OrcWriterOptions()
                        .withStripeMinSize(DataSize.of(0, MEGABYTE))
                        .withStripeMaxRowCount(100)
                        .withRowGroupMaxRowCount(100),
                ImmutableMap.of(),
                false,
                OrcWriteValidationMode.BOTH,
                new OrcWriterStats());

        List<Page> inputPages = generateInputPagesSequential(types, 100, 5);
        for (Page inputPage : inputPages) {
            writer.write(inputPage);
        }
        writer.close();

        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS);
        OrcReader orcReader = OrcReader.createOrcReader(orcDataSource, READER_OPTIONS)
                .orElseThrow(() -> new RuntimeException("File is empty"));
        assertThat(orcReader.getFooter().getStripes().size()).isEqualTo(5);

        // no filter, split covers entire file
        OrcRecordReader reader = orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                types,
                OrcPredicate.TRUE,
                DateTimeZone.UTC,
                newSimpleAggregatedMemoryContext(),
                100,
                RuntimeException::new);
        ConnectorPageSource.RowRanges rowRanges = reader.getNextFilteredRowRanges().orElseThrow();
        assertThat(rowRanges.isNoMoreRowRanges()).isFalse();
        verifyRowRanges(0, rowRanges);

        for (int i = 0; i < 5; i++) {
            Page page = reader.nextPage();
            assertThat(page.getPositionCount()).isEqualTo(100);
            rowRanges = reader.getNextFilteredRowRanges().orElseThrow();
            assertThat(rowRanges.isNoMoreRowRanges()).isEqualTo(i == 4);
            verifyRowRanges(100, rowRanges, i * 100L, (i + 1) * 100L);
        }

        // no filter, split covers 3rd and 4th stripes in file
        reader = orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                types,
                Collections.nCopies(types.size(), fullyProjectedLayout()),
                OrcPredicate.TRUE,
                150,
                200,
                DateTimeZone.UTC,
                newSimpleAggregatedMemoryContext(),
                100,
                RuntimeException::new,
                NameBasedFieldMapper::create);
        rowRanges = reader.getNextFilteredRowRanges().orElseThrow();
        assertThat(rowRanges.isNoMoreRowRanges()).isFalse();
        verifyRowRanges(0, rowRanges);

        for (int i = 0; i < 2; i++) {
            Page page = reader.nextPage();
            assertThat(page.getPositionCount()).isEqualTo(100);
            rowRanges = reader.getNextFilteredRowRanges().orElseThrow();
            assertThat(rowRanges.isNoMoreRowRanges()).isEqualTo(i == 1);
            verifyRowRanges(100, rowRanges, i * 100L, (i + 1) * 100L);
        }

        // selective filter, split covers entire file
        reader = orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                types,
                TupleDomainOrcPredicate.builder()
                        // predicate selects 2nd and 4th stripes
                        .addColumn(new OrcColumnId(1), Domain.multipleValues(INTEGER, ImmutableList.of(150L, 350L)))
                        .build(),
                DateTimeZone.UTC,
                newSimpleAggregatedMemoryContext(),
                100,
                RuntimeException::new);
        rowRanges = reader.getNextFilteredRowRanges().orElseThrow();
        assertThat(rowRanges.isNoMoreRowRanges()).isFalse();
        verifyRowRanges(0, rowRanges);

        for (long stripe : ImmutableList.of(1, 3)) {
            Page page = reader.nextPage();
            assertThat(page.getPositionCount()).isEqualTo(100);
            rowRanges = reader.getNextFilteredRowRanges().orElseThrow();
            assertThat(rowRanges.isNoMoreRowRanges()).isEqualTo(stripe == 3);
            verifyRowRanges(100, rowRanges, stripe * 100, (stripe + 1) * 100);
        }

        // selective filter, split covers 3rd and 4th stripes in file
        reader = orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                types,
                Collections.nCopies(types.size(), fullyProjectedLayout()),
                TupleDomainOrcPredicate.builder()
                        // predicate selects 2nd and 4th stripes
                        .addColumn(new OrcColumnId(1), Domain.multipleValues(INTEGER, ImmutableList.of(150L, 350L)))
                        .build(),
                150,
                200,
                DateTimeZone.UTC,
                newSimpleAggregatedMemoryContext(),
                100,
                RuntimeException::new,
                NameBasedFieldMapper::create);
        rowRanges = reader.getNextFilteredRowRanges().orElseThrow();
        assertThat(rowRanges.isNoMoreRowRanges()).isFalse();
        verifyRowRanges(0, rowRanges);

        Page page = reader.nextPage();
        assertThat(page.getPositionCount()).isEqualTo(100);
        rowRanges = reader.getNextFilteredRowRanges().orElseThrow();
        assertThat(rowRanges.isNoMoreRowRanges()).isTrue();
        verifyRowRanges(100, rowRanges, 100, 200);
    }

    @Test
    public void tesFilteredRowRangesWithMultipleRowGroupsPerStripe()
            throws IOException
    {
        TempFile tempFile = new TempFile();
        // Write a file with 200 rows per stripe and 10 row-groups per stripe
        List<String> columnNames = ImmutableList.of("test1", "test2");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);

        OrcWriter writer = new OrcWriter(
                OutputStreamOrcDataSink.create(new LocalOutputFile(tempFile.getFile())),
                columnNames,
                types,
                OrcType.createRootOrcType(columnNames, types),
                NONE,
                new OrcWriterOptions()
                        .withStripeMinSize(DataSize.of(0, MEGABYTE))
                        .withStripeMaxRowCount(200)
                        .withRowGroupMaxRowCount(20),
                ImmutableMap.of(),
                false,
                OrcWriteValidationMode.BOTH,
                new OrcWriterStats());

        List<Page> inputPages = generateInputPagesSequential(types, 200, 3);
        for (Page inputPage : inputPages) {
            writer.write(inputPage);
        }
        writer.close();

        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS);
        OrcReader orcReader = OrcReader.createOrcReader(orcDataSource, READER_OPTIONS)
                .orElseThrow(() -> new RuntimeException("File is empty"));
        assertThat(orcReader.getFooter().getStripes().size()).isEqualTo(3);

        // selective filter, split covers entire file
        OrcRecordReader reader = orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                types,
                TupleDomainOrcPredicate.builder()
                        // predicate selects a couple of row-groups in 1st and 3rd stripes
                        .addColumn(new OrcColumnId(1), Domain.multipleValues(INTEGER, ImmutableList.of(50L, 150L, 450L, 550L)))
                        .build(),
                DateTimeZone.UTC,
                newSimpleAggregatedMemoryContext(),
                20,
                RuntimeException::new);
        ConnectorPageSource.RowRanges rowRanges = reader.getNextFilteredRowRanges().orElseThrow();
        assertThat(rowRanges.isNoMoreRowRanges()).isFalse();
        verifyRowRanges(0, rowRanges);

        for (long stripe : ImmutableList.of(0, 2)) {
            Page page = reader.nextPage();
            assertThat(page.getPositionCount()).isEqualTo(20);
            rowRanges = reader.getNextFilteredRowRanges().orElseThrow();
            assertThat(rowRanges.isNoMoreRowRanges()).isEqualTo(stripe == 2);
            verifyRowRanges(
                    40,
                    rowRanges,
                    (stripe * 200) + 40, (stripe * 200) + 60,
                    (stripe * 200) + 140, (stripe * 200) + 160);
            page = reader.nextPage();
            assertThat(page.getPositionCount()).isEqualTo(20);
            rowRanges = reader.getNextFilteredRowRanges().orElseThrow();
            assertThat(rowRanges.isNoMoreRowRanges()).isEqualTo(stripe == 2);
            verifyRowRanges(0, rowRanges);
        }

        // All rows filtered
        reader = orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                types,
                TupleDomainOrcPredicate.builder()
                        .addColumn(new OrcColumnId(1), Domain.multipleValues(INTEGER, ImmutableList.of(1000L)))
                        .build(),
                DateTimeZone.UTC,
                newSimpleAggregatedMemoryContext(),
                20,
                RuntimeException::new);
        rowRanges = reader.getNextFilteredRowRanges().orElseThrow();
        assertThat(rowRanges.isNoMoreRowRanges()).isTrue();
        verifyRowRanges(0, rowRanges);
    }

    public static List<Page> generateInputPagesSequential(List<Type> types, int positionsPerPage, int pageCount)
    {
        ImmutableList.Builder<Page> pagesBuilder = ImmutableList.builder();
        int baseIndex = 0;
        for (int i = 0; i < pageCount; i++) {
            int finalBaseIndex = baseIndex;
            List<Block> blocks = types.stream()
                    .map(type -> generateBlock(type, finalBaseIndex, positionsPerPage))
                    .collect(toImmutableList());
            baseIndex += positionsPerPage;
            pagesBuilder.add(new Page(blocks.toArray(Block[]::new)));
        }
        return pagesBuilder.build();
    }

    private static Block generateBlock(Type type, int baseIndex, int positions)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, positions);
        for (int i = 0; i < positions; i++) {
            writeNativeValue(type, blockBuilder, (long) baseIndex + i);
        }
        return blockBuilder.build();
    }

    private static void verifyRowRanges(long expectedRowCount, ConnectorPageSource.RowRanges rowRanges, long... expectedValues)
    {
        checkArgument(expectedValues.length % 2 == 0);
        assertThat(rowRanges.getRangesCount()).isEqualTo(expectedValues.length / 2);
        assertThat(rowRanges.getRowCount()).isEqualTo(expectedRowCount);
        for (int rangeIndex = 0; rangeIndex < rowRanges.getRangesCount(); rangeIndex++) {
            assertThat(rowRanges.getLowerInclusive(rangeIndex)).isEqualTo(expectedValues[2 * rangeIndex]);
            assertThat(rowRanges.getUpperExclusive(rangeIndex)).isEqualTo(expectedValues[(2 * rangeIndex) + 1]);
        }
    }
}
