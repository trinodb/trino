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

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

final class TestRcFilePageSource
{
    @Test
    void testSelectPositionsOnLoadedBlocks()
            throws Exception
    {
        File file = File.createTempFile("test-select-positions", ".rc");
        try {
            // columnA has row number values, columnB has row number * 10
            int rowCount = 100;
            BlockBuilder columnA = BIGINT.createFixedSizeBlockBuilder(rowCount);
            BlockBuilder columnB = BIGINT.createFixedSizeBlockBuilder(rowCount);
            for (int i = 0; i < rowCount; i++) {
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
                writer.write(new Page(rowCount, columnA.build(), columnB.build()));
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
            try (RcFilePageSource pageSource = new RcFilePageSource(reader, columns)) {
                SourcePage page = pageSource.getNextSourcePage();
                assertThat(page.getPositionCount()).isEqualTo(rowCount);

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
        finally {
            Files.deleteIfExists(file.toPath());
        }
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
