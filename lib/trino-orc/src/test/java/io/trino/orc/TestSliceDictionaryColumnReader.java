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

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.metadata.Footer;
import io.trino.orc.metadata.OrcMetadataReader;
import io.trino.orc.metadata.StripeInformation;
import io.trino.orc.reader.SliceDictionaryColumnReader;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcTester.writeOrcColumnTrino;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.orc.metadata.PostScript.HiveWriterVersion.ORIGINAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.file.Files.readAllBytes;
import static java.time.ZoneOffset.UTC;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestSliceDictionaryColumnReader
{
    public static final int ROWS = 100_000;
    private static final int DICTIONARY = 22;
    private static final int MAX_STRING = 19;

    @Test
    public void testDictionaryReaderUpdatesRetainedSize()
            throws Exception
    {
        // create orc file
        List<String> values = createValues();
        File temporaryDirectory = Files.createTempDirectory(null).toFile();
        File orcFile = new File(temporaryDirectory, randomUUID().toString());
        writeOrcColumnTrino(orcFile, NONE, VARCHAR, values.iterator(), new OrcWriterStats());

        // prepare for read
        OrcDataSource dataSource = new MemoryOrcDataSource(new OrcDataSourceId(orcFile.getPath()), Slices.wrappedBuffer(readAllBytes(orcFile.toPath())));
        OrcReader orcReader = OrcReader.createOrcReader(dataSource, new OrcReaderOptions())
                .orElseThrow(() -> new RuntimeException("File is empty"));
        Footer footer = orcReader.getFooter();
        List<OrcColumn> columns = orcReader.getRootColumn().getNestedColumns();
        assertTrue(columns.size() == 1);
        StripeReader stripeReader = new StripeReader(
                dataSource,
                UTC,
                Optional.empty(),
                footer.getTypes(),
                ImmutableSet.copyOf(columns),
                footer.getRowsInRowGroup(),
                OrcPredicate.TRUE,
                ORIGINAL,
                new OrcMetadataReader(new OrcReaderOptions()),
                Optional.empty());
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        SliceDictionaryColumnReader columnReader = new SliceDictionaryColumnReader(columns.get(0), memoryContext.newLocalMemoryContext(TestSliceDictionaryColumnReader.class.getSimpleName()), -1, false);

        List<StripeInformation> stripeInformations = footer.getStripes();
        for (StripeInformation stripeInformation : stripeInformations) {
            Stripe stripe = stripeReader.readStripe(stripeInformation, newSimpleAggregatedMemoryContext());
            List<RowGroup> rowGroups = stripe.getRowGroups();
            columnReader.startStripe(stripe.getFileTimeZone(), stripe.getDictionaryStreamSources(), stripe.getColumnEncodings());

            for (RowGroup rowGroup : rowGroups) {
                columnReader.startRowGroup(rowGroup.getStreamSources());
                columnReader.prepareNextRead(1000);
                columnReader.readBlock();
                // memory usage check
                assertEquals(memoryContext.getBytes(), columnReader.getRetainedSizeInBytes());
            }
        }

        columnReader.close();
        assertTrue(memoryContext.getBytes() == 0);
    }

    private List<String> createValues()
    {
        Random random = new Random();
        List<String> dictionary = createDictionary(random);

        List<String> values = new ArrayList<>();
        for (int i = 0; i < ROWS; ++i) {
            if (random.nextBoolean()) {
                values.add(dictionary.get(random.nextInt(dictionary.size())));
            }
            else {
                values.add(null);
            }
        }
        return values;
    }

    private List<String> createDictionary(Random random)
    {
        List<String> dictionary = new ArrayList<>();
        for (int dictionaryIndex = 0; dictionaryIndex < DICTIONARY; dictionaryIndex++) {
            dictionary.add(randomAsciiString(random));
        }
        return dictionary;
    }

    private String randomAsciiString(Random random)
    {
        char[] value = new char[random.nextInt(MAX_STRING)];
        for (int i = 0; i < value.length; i++) {
            value[i] = (char) random.nextInt(Byte.MAX_VALUE);
        }
        return new String(value);
    }
}
