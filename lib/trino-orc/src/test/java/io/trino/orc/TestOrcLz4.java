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
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import org.joda.time.DateTimeZone;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.io.Resources.getResource;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.orc.OrcTester.Format.ORC_12;
import static io.trino.orc.OrcTester.writeOrcColumnsHiveFile;
import static io.trino.orc.metadata.CompressionKind.LZ4;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.nio.file.Files.readAllBytes;
import static org.testng.Assert.assertEquals;

public class TestOrcLz4
{
    private static final long POSITION_COUNT = 10_000;

    @Test(dataProvider = "testOrcDataProvider")
    public void testReadLz4(byte[] data)
            throws Exception
    {
        OrcReader orcReader = OrcReader.createOrcReader(new MemoryOrcDataSource(new OrcDataSourceId("memory"), Slices.wrappedBuffer(data)), new OrcReaderOptions())
                .orElseThrow(() -> new RuntimeException("File is empty"));

        assertEquals(orcReader.getCompressionKind(), LZ4);
        assertEquals(orcReader.getFooter().getNumberOfRows(), POSITION_COUNT);

        try (OrcRecordReader reader = orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                ImmutableList.of(BIGINT, INTEGER, BIGINT),
                OrcPredicate.TRUE,
                DateTimeZone.UTC,
                newSimpleAggregatedMemoryContext(),
                INITIAL_BATCH_SIZE,
                RuntimeException::new)) {
            int rows = 0;
            while (true) {
                Page page = reader.nextPage();
                if (page == null) {
                    break;
                }
                page = page.getLoadedPage();
                rows += page.getPositionCount();

                Block xBlock = page.getBlock(0);
                Block yBlock = page.getBlock(1);
                Block zBlock = page.getBlock(2);

                for (int position = 0; position < page.getPositionCount(); position++) {
                    BIGINT.getLong(xBlock, position);
                    INTEGER.getInt(yBlock, position);
                    BIGINT.getLong(zBlock, position);
                }
            }

            assertEquals(rows, reader.getFileRowCount());
        }
    }

    @DataProvider
    public static Object[][] testOrcDataProvider()
            throws Exception
    {
        return new Object[][] {
                {readOrcTestData("apache-lz4.orc")},
                {generateOrcTestData()}
        };
    }

    private static byte[] readOrcTestData(String resourceFile)
            throws Exception
    {
        return readAllBytes(Path.of(getResource(resourceFile).toURI()));
    }

    private static byte[] generateOrcTestData()
            throws Exception
    {
        try (TempFile dataFile = new TempFile()) {
            Random random = new Random();
            writeOrcColumnsHiveFile(
                    dataFile.getFile(),
                    ORC_12,
                    LZ4,
                    ImmutableList.of("x", "y", "z"),
                    ImmutableList.of(BIGINT, INTEGER, BIGINT),
                    Stream.generate(() -> (Function<Integer, Object>) (fieldIndex) -> switch (fieldIndex) {
                        case 0, 2 -> random.nextLong();
                        case 1 -> random.nextInt();
                        default -> new IllegalArgumentException();
                    }).limit(POSITION_COUNT).iterator());

            return readAllBytes(dataFile.getFile().toPath());
        }
    }
}
