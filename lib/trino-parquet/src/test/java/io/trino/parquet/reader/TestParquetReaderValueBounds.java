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
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetReaderValueBounds
{
    private static final int ROW_COUNT = 5_000;
    private static final int ROW_GROUP_ROWS = 1_000;

    private static final int ASCENDING = 0;
    private static final int DESCENDING = 1;
    private static final int DAY = 2;
    private static final int NULLABLE = 3;
    private static final int ONLY_NULL = 4;
    private static final int FLOATING = 5;
    private static final int TEXT = 6;

    private static final List<String> COLUMN_NAMES = ImmutableList.of("ascending", "descending", "day", "nullable", "only_null", "floating", "text");
    private static final List<Type> COLUMN_TYPES = ImmutableList.of(BIGINT, INTEGER, DATE, BIGINT, BIGINT, DOUBLE, VARCHAR);

    @Test
    public void testValueBounds()
            throws IOException
    {
        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxRowGroupRowCount(ROW_GROUP_ROWS)
                                .build(),
                        COLUMN_TYPES,
                        COLUMN_NAMES,
                        createPages()),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        List<BlockMetadata> rowGroups = parquetMetadata.getBlocks();
        assertThat(rowGroups).hasSizeGreaterThan(1);

        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, COLUMN_TYPES, COLUMN_NAMES)) {
            int rowGroup = 0;
            long rowGroupStart = 0;
            long row = 0;
            Domain previousBounds = null;
            for (SourcePage page = reader.nextPage(); page != null; page = reader.nextPage()) {
                if (row == rowGroupStart + rowGroups.get(rowGroup).rowCount()) {
                    rowGroupStart = row;
                    rowGroup++;
                    previousBounds = null;
                }
                long first = rowGroupStart;
                long last = rowGroupStart + rowGroups.get(rowGroup).rowCount() - 1;
                // a page never crosses a row group
                assertThat(row + page.getPositionCount() - 1).isLessThanOrEqualTo(last);

                assertThat(page.getValueBounds(ASCENDING)).contains(notNull(Range.range(BIGINT, first, true, last, true)));
                assertThat(page.getValueBounds(DESCENDING)).contains(notNull(Range.range(INTEGER, -last, true, -first, true)));
                assertThat(page.getValueBounds(DAY)).contains(notNull(Range.range(DATE, first / 10, true, last / 10, true)));
                assertThat(page.getValueBounds(ONLY_NULL)).contains(Domain.onlyNull(BIGINT));

                // statistics order floating point values differently than the engine and truncate long text
                assertThat(page.getValueBounds(FLOATING)).isEmpty();
                assertThat(page.getValueBounds(TEXT)).isEmpty();

                Domain nullableBounds = page.getValueBounds(NULLABLE).orElseThrow();
                assertThat(nullableBounds.isNullAllowed()).isTrue();
                assertThat(nullableBounds.isAll()).isFalse();
                Block nullable = page.getBlock(NULLABLE);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    assertThat(nullableBounds.includesNullableValue(nullable.isNull(position) ? null : BIGINT.getLong(nullable, position))).isTrue();
                }

                // pages of one row group share the instance, as consumers cache their decision by identity
                Domain bounds = page.getValueBounds(ASCENDING).orElseThrow();
                assertThat(page.getValueBounds(ASCENDING).orElseThrow()).isSameAs(bounds);
                if (previousBounds != null) {
                    assertThat(bounds).isSameAs(previousBounds);
                }
                previousBounds = bounds;

                row += page.getPositionCount();
            }
            assertThat(row).isEqualTo(ROW_COUNT);
            assertThat(rowGroup).isEqualTo(rowGroups.size() - 1);
        }
    }

    private static Domain notNull(Range range)
    {
        return Domain.create(ValueSet.ofRanges(range), false);
    }

    private static List<Page> createPages()
    {
        PageBuilder pageBuilder = new PageBuilder(COLUMN_TYPES);
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        for (int row = 0; row < ROW_COUNT; row++) {
            pageBuilder.declarePosition();
            BIGINT.writeLong(pageBuilder.getBlockBuilder(ASCENDING), row);
            INTEGER.writeLong(pageBuilder.getBlockBuilder(DESCENDING), -row);
            DATE.writeLong(pageBuilder.getBlockBuilder(DAY), row / 10);
            if (row % 7 == 0) {
                pageBuilder.getBlockBuilder(NULLABLE).appendNull();
            }
            else {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(NULLABLE), row * 3L);
            }
            pageBuilder.getBlockBuilder(ONLY_NULL).appendNull();
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(FLOATING), row / 2.0);
            VARCHAR.writeSlice(pageBuilder.getBlockBuilder(TEXT), utf8Slice("value " + row));
            if (pageBuilder.getPositionCount() == 500) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }
        return pages.build();
    }
}
