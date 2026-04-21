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
import io.trino.orc.metadata.CompressionKind;
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
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcTester.READER_OPTIONS;
import static io.trino.orc.StripeReader.isIndexStream;
import static io.trino.orc.TestingOrcPredicate.ORC_ROW_GROUP_SIZE;
import static io.trino.orc.TestingOrcPredicate.ORC_STRIPE_SIZE;
import static io.trino.orc.metadata.CalendarKind.PROLEPTIC_GREGORIAN;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.DateTimeTestingUtils.sqlDateOf;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOrcWriter
{
    @Test
    public void testWriteOutputStreamsInOrder()
            throws IOException
    {
        testWriteOutput(ImmutableList.of("test1", "test2", "test3", "test4", "test5"),
                new String[] {"a", "bbbbb", "ccc", "dd", "eeee"});
    }

    @Test
    public void testWriteHugeChunk()
            throws IOException
    {
        int columnCount = 500;
        ImmutableList.Builder<String> columnNameBuilder = ImmutableList.builder();
        String[] data = new String[columnCount];

        for (int i = 0; i < columnCount; i++) {
            columnNameBuilder.add(String.valueOf(i));
            data[i] = "LONG_STRING";
        }
        testWriteOutput(columnNameBuilder.build(), data);
    }

    @Test
    public void testCalendarEntryInFooter()
    {
        List<String> strings = ImmutableList.of("aaa1", "qwerty", "asdf", "zxcvb", "1234");
        assertFooterHasProlepticGregorianCalendar(VARCHAR, strings);

        List<SqlDate> dates = ImmutableList.of("2020-01-01", "2021-02-02", "2022-03-03", "2023-04-04", "2024-05-05").stream()
                .map(text -> sqlDateOf(LocalDate.parse(text)))
                .collect(toImmutableList());
        assertFooterHasProlepticGregorianCalendar(DATE, dates);

        List<SqlTimestamp> timestamps = ImmutableList.of("2023-04-11T05:16:12.123", "2021-04-11T05:16:12.123", "1999-04-11T05:16:12.123").stream()
                .map(text -> sqlTimestampOf(TIMESTAMP_MILLIS.getPrecision(), LocalDateTime.parse(text)))
                .collect(toImmutableList());
        assertFooterHasProlepticGregorianCalendar(TIMESTAMP_MILLIS, timestamps);
    }

    private static void assertFooterHasProlepticGregorianCalendar(Type type, List<?> values)
    {
        try (TempFile tempFile = new TempFile()) {
            OrcTester.writeOrcColumnTrino(tempFile.getFile(), CompressionKind.NONE, type, values.iterator(), new OrcWriterStats());

            OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS);

            assertThat(OrcReader.createOrcReader(orcDataSource, READER_OPTIONS).orElseThrow(() -> new RuntimeException("File is empty")).getFooter().getCalendar())
                    .isEqualTo(PROLEPTIC_GREGORIAN);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void testWriteOutput(List<String> columnNames, String[] data)
            throws IOException
    {
        for (OrcWriteValidationMode validationMode : OrcWriteValidationMode.values()) {
            TempFile tempFile = new TempFile();

            ImmutableList.Builder<Type> builder = ImmutableList.builder();
            for (int i = 0; i < columnNames.size(); i++) {
                builder.add(VARCHAR);
            }
            List<Type> types = builder.build();

            OrcWriter writer = new OrcWriter(
                    OutputStreamOrcDataSink.create(new LocalOutputFile(tempFile.getFile())),
                    columnNames,
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
                            assertThat(dataStreamStarted).isFalse();
                            continue;
                        }
                        dataStreamStarted = true;
                        // verify sizes in order
                        assertThat(stream.getLength()).isGreaterThanOrEqualTo(size);
                        size = stream.getLength();
                    }
                }
            }
        }
    }
}
