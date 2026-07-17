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
import io.trino.orc.metadata.OrcColumnId;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.CharType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.MAX_BATCH_SIZE;
import static io.trino.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static io.trino.orc.OrcTester.READER_OPTIONS;
import static io.trino.orc.OrcTester.writeOrcColumnTrino;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOrcCharPredicatePruning
{
    private static final CharType CHAR2 = CharType.createCharType(2);

    @Test
    public void testCharStripeNotPrunedForControlCharValue()
            throws Exception
    {
        List<String> values = ImmutableList.of("z", "z\t", "za");

        try (TempFile tempFile = new TempFile()) {
            writeOrcColumnTrino(tempFile.getFile(), NONE, CHAR2, values.iterator(), new OrcWriterStats());

            Domain predicate = Domain.singleValue(CHAR2, utf8Slice("z\t"));
            List<String> read = readWithPredicate(tempFile, predicate);
            assertThat(read).contains("z\t");
        }
    }

    private static List<String> readWithPredicate(TempFile tempFile, Domain domain)
            throws IOException
    {
        TupleDomainOrcPredicate predicate = TupleDomainOrcPredicate.builder()
                .addColumn(new OrcColumnId(1), domain)
                .build();

        List<String> result = new ArrayList<>();
        try (OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS)) {
            OrcReader orcReader = OrcReader.createOrcReader(orcDataSource, READER_OPTIONS)
                    .orElseThrow(() -> new RuntimeException("File is empty"));
            try (OrcRecordReader recordReader = orcReader.createRecordReader(
                    orcReader.getRootColumn().getNestedColumns(),
                    ImmutableList.of(CHAR2),
                    false,
                    predicate,
                    HIVE_STORAGE_TIME_ZONE,
                    newSimpleAggregatedMemoryContext(),
                    MAX_BATCH_SIZE,
                    RuntimeException::new)) {
                while (true) {
                    SourcePage page = recordReader.nextPage();
                    if (page == null) {
                        break;
                    }
                    Block block = page.getBlock(0);
                    for (int i = 0; i < block.getPositionCount(); i++) {
                        result.add(CHAR2.getSlice(block, i).toStringUtf8());
                    }
                }
            }
        }
        return result;
    }
}
