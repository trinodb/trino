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
package io.trino.plugin.hive.rcfile;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.hive.formats.encodings.binary.BinaryColumnEncodingFactory;
import io.trino.hive.formats.rcfile.RcFileReader;
import io.trino.hive.formats.rcfile.RcFileWriter;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestRcFilePageSource
{
    private static final int ROW_COUNT = 100;

    @Test
    void testSelectPositionsOnLoadedBlocks(@TempDir Path tempDir)
            throws Exception
    {
        try (RcFilePageSource pageSource = createPageSource(tempDir.resolve("test.rc").toFile())) {
            SourcePage page = pageSource.getNextSourcePage();
            assertThat(page.getPositionCount()).isEqualTo(ROW_COUNT);

            page.selectPositions(new int[] {1, 3, 5, 7}, 0, 4);
            assertThat(blockValues(page.getBlock(0))).containsExactly(1L, 3L, 5L, 7L);

            // select again with positions relative to the previous selection
            page.selectPositions(new int[] {1, 2}, 0, 2);
            assertThat(page.getPositionCount()).isEqualTo(2);
            // columnA was loaded before the second selection, columnB is loaded after it
            assertThat(blockValues(page.getBlock(0))).containsExactly(3L, 5L);
            assertThat(blockValues(page.getBlock(1))).containsExactly(30L, 50L);
        }
    }

    @Test
    void testSelectPositionsValidatesSelectedPositions(@TempDir Path tempDir)
            throws Exception
    {
        try (RcFilePageSource pageSource = createPageSource(tempDir.resolve("test.rc").toFile())) {
            SourcePage page = pageSource.getNextSourcePage();
            assertThatThrownBy(() -> page.selectPositions(new int[] {ROW_COUNT}, 0, 1))
                    .isInstanceOf(IndexOutOfBoundsException.class);

            // the offset into the positions array is independent of the page size
            int[] positions = new int[ROW_COUNT * 2];
            positions[150] = 10;
            positions[151] = 20;
            page.selectPositions(positions, 150, 2);
            assertThat(blockValues(page.getBlock(0))).containsExactly(10L, 20L);
            assertThat(blockValues(page.getBlock(1))).containsExactly(100L, 200L);
        }
    }

    @Test
    void testSelectRange(@TempDir Path tempDir)
            throws Exception
    {
        assertSelection(tempDir, page -> page.selectPositions(0, 4), 0, 1, 2, 3);
        assertSelection(tempDir, page -> page.selectPositions(ROW_COUNT - 3, 3), ROW_COUNT - 3, ROW_COUNT - 2, ROW_COUNT - 1);
        assertSelection(tempDir, page -> page.selectPositions(0, 0));
        assertSelection(
                tempDir,
                page -> {
                    page.selectPositions(2, 5);
                    page.selectPositions(new int[] {0, 3}, 0, 2);
                },
                2,
                5);
        assertSelection(
                tempDir,
                page -> {
                    page.selectPositions(new int[] {1, 3, 5, 7}, 0, 4);
                    page.selectPositions(1, 2);
                },
                3,
                5);
        assertSelection(
                tempDir,
                page -> {
                    page.selectPositions(1, 6);
                    page.selectPositions(2, 3);
                },
                3,
                4,
                5);
        assertSelection(
                tempDir,
                page -> assertThatThrownBy(() -> page.selectPositions(ROW_COUNT - 1, 2))
                        .isInstanceOf(IndexOutOfBoundsException.class),
                IntStream.range(0, ROW_COUNT).toArray());
    }

    // columnA is loaded before the selection
    private static void assertSelection(Path tempDir, Consumer<SourcePage> selection, int... expectedPositions)
            throws IOException
    {
        try (RcFilePageSource pageSource = createPageSource(tempDir.resolve("test.rc").toFile())) {
            SourcePage page = pageSource.getNextSourcePage();
            page.getBlock(0);
            selection.accept(page);

            List<Long> expectedRows = Arrays.stream(expectedPositions)
                    .mapToObj(position -> (long) position)
                    .collect(toImmutableList());
            assertThat(page.getPositionCount()).isEqualTo(expectedPositions.length);
            assertThat(blockValues(page.getBlock(0))).isEqualTo(expectedRows);
            assertThat(blockValues(page.getBlock(1))).isEqualTo(expectedRows.stream()
                    .map(row -> row * 10)
                    .collect(toImmutableList()));
        }
    }

    // columnA has row number values, columnB has row number * 10
    private static RcFilePageSource createPageSource(File file)
            throws IOException
    {
        BlockBuilder columnA = BIGINT.createFixedSizeBlockBuilder(ROW_COUNT);
        BlockBuilder columnB = BIGINT.createFixedSizeBlockBuilder(ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            BIGINT.writeLong(columnA, i);
            BIGINT.writeLong(columnB, i * 10L);
        }
        BinaryColumnEncodingFactory encoding = new BinaryColumnEncodingFactory(DateTimeZone.UTC);
        try (FileOutputStream outputStream = new FileOutputStream(file)) {
            RcFileWriter writer = new RcFileWriter(
                    outputStream,
                    ImmutableList.of(BIGINT, BIGINT),
                    encoding,
                    Optional.empty(),
                    ImmutableMap.of(),
                    true);
            writer.write(new Page(ROW_COUNT, columnA.build(), columnB.build()));
            writer.close();
        }

        RcFileReader reader = new RcFileReader(
                new LocalInputFile(file),
                encoding,
                ImmutableMap.of(0, BIGINT, 1, BIGINT),
                0,
                file.length());
        List<HiveColumnHandle> columns = ImmutableList.of(
                HiveColumnHandle.createBaseColumn("columna", 0, HIVE_LONG, BIGINT, REGULAR, Optional.empty()),
                HiveColumnHandle.createBaseColumn("columnb", 1, HIVE_LONG, BIGINT, REGULAR, Optional.empty()));
        return new RcFilePageSource(reader, columns);
    }

    private static List<Long> blockValues(Block block)
    {
        ImmutableList.Builder<Long> values = ImmutableList.builder();
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.add(BIGINT.getLong(block, position));
        }
        return values.build();
    }
}
