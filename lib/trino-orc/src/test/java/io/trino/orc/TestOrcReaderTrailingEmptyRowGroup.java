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
import io.trino.filesystem.local.LocalOutputFile;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcType;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.MAX_BATCH_SIZE;
import static io.trino.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static io.trino.orc.OrcTester.READER_OPTIONS;
import static io.trino.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for reading a compressed stream whose last row groups carry no payload.
 * <p>
 * {@link OrcOutputBuffer} writes values of at least 32 KiB straight to the compressed output and leaves its
 * buffer empty. When such a value is the last non-null value of a column in a stripe, the checkpoints of the
 * remaining row groups point exactly at the end of the DATA stream. That position is valid ORC, but the reader
 * used to reject it with "Seek past end of stream".
 */
public class TestOrcReaderTrailingEmptyRowGroup
{
    private static final int ROW_GROUP_MAX_ROW_COUNT = 10;
    // must exceed the direct flush threshold of OrcOutputBuffer (32 KiB)
    private static final int LARGE_VALUE_LENGTH = 64 * 1024;

    @Test
    public void testLargeValueFollowedByNullRowGroups()
            throws Exception
    {
        // row group 0: small values and one large value at its end
        // row groups 1..3: nulls only
        int rowCount = ROW_GROUP_MAX_ROW_COUNT * 4;
        String[] values = new String[rowCount];
        for (int i = 0; i < ROW_GROUP_MAX_ROW_COUNT - 1; i++) {
            values[i] = "value-" + i;
        }
        values[ROW_GROUP_MAX_ROW_COUNT - 1] = "x".repeat(LARGE_VALUE_LENGTH);

        assertRoundTrip(values, CompressionKind.ZSTD);
        assertRoundTrip(values, CompressionKind.SNAPPY);
        assertRoundTrip(values, CompressionKind.NONE);
    }

    @Test
    public void testLargeValueInMiddleRowGroupFollowedByNullRowGroups()
            throws Exception
    {
        // row group 0: nulls, row group 1: one large value, row groups 2..3: nulls only
        int rowCount = ROW_GROUP_MAX_ROW_COUNT * 4;
        String[] values = new String[rowCount];
        values[ROW_GROUP_MAX_ROW_COUNT + 3] = "y".repeat(LARGE_VALUE_LENGTH);

        assertRoundTrip(values, CompressionKind.ZSTD);
    }

    @Test
    public void testLargeValueFollowedByEmptyStringRowGroups()
            throws Exception
    {
        // empty strings are not nulls, but they add nothing to the DATA stream either,
        // so the trailing row groups have no payload just like with nulls
        int rowCount = ROW_GROUP_MAX_ROW_COUNT * 4;
        String[] values = new String[rowCount];
        Arrays.fill(values, "");
        values[ROW_GROUP_MAX_ROW_COUNT - 1] = "w".repeat(LARGE_VALUE_LENGTH);

        assertRoundTrip(values, CompressionKind.ZSTD);
    }

    @Test
    public void testLargeValueFollowedByValuesInLaterRowGroup()
            throws Exception
    {
        // the stream continues after the direct write, so no checkpoint points at the stream end;
        // this guards the fix against breaking the regular path
        int rowCount = ROW_GROUP_MAX_ROW_COUNT * 4;
        String[] values = new String[rowCount];
        values[ROW_GROUP_MAX_ROW_COUNT - 1] = "z".repeat(LARGE_VALUE_LENGTH);
        values[rowCount - 1] = "tail";

        assertRoundTrip(values, CompressionKind.ZSTD);
    }

    private static void assertRoundTrip(String[] values, CompressionKind compression)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            writeFile(tempFile.getFile(), compression, values);

            OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS);
            OrcReader orcReader = OrcReader.createOrcReader(orcDataSource, READER_OPTIONS)
                    .orElseThrow(() -> new RuntimeException("File is empty"));
            assertThat(orcReader.getFooter().getRowsInRowGroup().orElse(0)).isEqualTo(ROW_GROUP_MAX_ROW_COUNT);

            try (OrcRecordReader reader = orcReader.createRecordReader(
                    orcReader.getRootColumn().getNestedColumns(),
                    ImmutableList.of(VARCHAR),
                    false,
                    OrcPredicate.TRUE,
                    HIVE_STORAGE_TIME_ZONE,
                    newSimpleAggregatedMemoryContext(),
                    MAX_BATCH_SIZE,
                    RuntimeException::new)) {
                int row = 0;
                for (SourcePage sourcePage = reader.nextPage(); sourcePage != null; sourcePage = reader.nextPage()) {
                    Page page = sourcePage.getPage();
                    Block block = page.getBlock(0);
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        if (values[row] == null) {
                            assertThat(block.isNull(position)).as("row %s", row).isTrue();
                        }
                        else {
                            assertThat(block.isNull(position)).as("row %s", row).isFalse();
                            assertThat(VARCHAR.getSlice(block, position).toStringUtf8()).as("row %s", row).isEqualTo(values[row]);
                        }
                        row++;
                    }
                }
                assertThat(row).isEqualTo(values.length);
            }
        }
    }

    private static void writeFile(File file, CompressionKind compression, String[] values)
            throws Exception
    {
        List<String> columnNames = ImmutableList.of("test");
        List<Type> types = ImmutableList.of(VARCHAR);

        OrcWriter writer = new OrcWriter(
                OutputStreamOrcDataSink.create(new LocalOutputFile(file)),
                columnNames,
                types,
                OrcType.createRootOrcType(columnNames, types),
                compression,
                new OrcWriterOptions()
                        .withRowGroupMaxRowCount(ROW_GROUP_MAX_ROW_COUNT),
                ImmutableMap.of(),
                true,
                BOTH,
                new OrcWriterStats());

        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, values.length);
        for (String value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                VARCHAR.writeSlice(blockBuilder, utf8Slice(value));
            }
        }
        writer.write(new Page(blockBuilder.build()));
        writer.close();
        writer.validate(new FileOrcDataSource(file, READER_OPTIONS));
    }
}
