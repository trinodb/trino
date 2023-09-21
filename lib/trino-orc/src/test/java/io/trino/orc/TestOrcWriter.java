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
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.filesystem.local.LocalOutputFile;
import io.trino.orc.OrcWriteValidation.OrcWriteValidationMode;
import io.trino.orc.metadata.Footer;
import io.trino.orc.metadata.OrcMetadataReader;
import io.trino.orc.metadata.OrcType;
import io.trino.orc.metadata.Stream;
import io.trino.orc.metadata.StripeFooter;
import io.trino.orc.metadata.StripeInformation;
import io.trino.orc.stream.OrcChunkLoader;
import io.trino.orc.stream.OrcInputStream;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcTester.READER_OPTIONS;
import static io.trino.orc.StripeReader.isIndexStream;
import static io.trino.orc.TestingOrcPredicate.ORC_ROW_GROUP_SIZE;
import static io.trino.orc.TestingOrcPredicate.ORC_STRIPE_SIZE;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertFalse;

public class TestOrcWriter
{
    @Test
    public void testWriteOutputStreamsInOrder()
            throws IOException
    {
        for (OrcWriteValidationMode validationMode : OrcWriteValidationMode.values()) {
            TempFile tempFile = new TempFile();

            List<String> columnNames = ImmutableList.of("test1", "test2", "test3", "test4", "test5");
            List<Type> types = ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR);

            OrcWriter writer = new OrcWriter(
                    OutputStreamOrcDataSink.create(new LocalOutputFile(tempFile.getFile())),
                    ImmutableList.of("test1", "test2", "test3", "test4", "test5"),
                    types,
                    OrcType.createRootOrcType(columnNames, types),
                    NONE,
                    new OrcWriterOptions()
                            .withStripeMinSize(DataSize.of(0, MEGABYTE))
                            .withStripeMaxSize(DataSize.of(32, MEGABYTE))
                            .withStripeMaxRowCount(ORC_STRIPE_SIZE)
                            .withRowGroupMaxRowCount(ORC_ROW_GROUP_SIZE)
                            .withDictionaryMaxMemory(DataSize.of(32, MEGABYTE))
                            .withBloomFilterColumns(ImmutableSet.copyOf(columnNames)),
                    ImmutableMap.of(),
                    true,
                    validationMode,
                    new OrcWriterStats());

            // write down some data with unsorted streams
            String[] data = new String[] {"a", "bbbbb", "ccc", "dd", "eeee"};
            Block[] blocks = new Block[data.length];
            int entries = 65536;
            VariableWidthBlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, entries);
            for (int i = 0; i < data.length; i++) {
                byte[] bytes = data[i].getBytes(UTF_8);
                for (int j = 0; j < entries; j++) {
                    // force to write different data
                    bytes[0] = (byte) ((bytes[0] + 1) % 128);
                    blockBuilder.writeEntry(Slices.wrappedBuffer(bytes, 0, bytes.length));
                }
                blocks[i] = blockBuilder.build();
                blockBuilder = (VariableWidthBlockBuilder) blockBuilder.newBlockBuilderLike(null);
            }

            writer.write(new Page(blocks));
            writer.close();

            // read the footer and verify the streams are ordered by size
            OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS);
            Footer footer = OrcReader.createOrcReader(orcDataSource, READER_OPTIONS)
                    .orElseThrow(() -> new RuntimeException("File is empty"))
                    .getFooter();

            // OrcReader closes the original data source because it buffers the full file, so we need to reopen
            orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS);

            for (StripeInformation stripe : footer.getStripes()) {
                // read the footer
                Slice tailBuffer = orcDataSource.readFully(stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength(), toIntExact(stripe.getFooterLength()));
                try (InputStream inputStream = new OrcInputStream(OrcChunkLoader.create(orcDataSource.getId(), tailBuffer, Optional.empty(), newSimpleAggregatedMemoryContext()))) {
                    StripeFooter stripeFooter = new OrcMetadataReader(new OrcReaderOptions()).readStripeFooter(footer.getTypes(), inputStream, ZoneId.of("UTC"));

                    int size = 0;
                    boolean dataStreamStarted = false;
                    for (Stream stream : stripeFooter.getStreams()) {
                        if (isIndexStream(stream)) {
                            assertFalse(dataStreamStarted);
                            continue;
                        }
                        dataStreamStarted = true;
                        // verify sizes in order
                        assertGreaterThanOrEqual(stream.getLength(), size);
                        size = stream.getLength();
                    }
                }
            }
        }
    }
}
