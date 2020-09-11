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
package io.prestosql.orc;

import com.google.common.collect.ImmutableList;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.RowType;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.OptionalInt;

import static com.google.common.io.Resources.getResource;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.orc.OrcReader.createOrcReader;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static org.testng.Assert.assertEquals;

public class TestOrcWithoutRowGroupInfo
{
    @Test
    public void testReadOrcFileWithoutRowGroupInfo()
            throws Exception
    {
        testAndVerifyResults(OrcPredicate.TRUE);
    }

    @Test
    public void testReadOrcFileWithoutRowGroupInfoWithPredicate()
            throws Exception
    {
        testAndVerifyResults(TupleDomainOrcPredicate.builder().addColumn(new OrcColumnId(7),
                Domain.singleValue(BIGINT, 2L))
                .build());
    }

    private void testAndVerifyResults(OrcPredicate orcPredicate)
            throws IOException
    {
        // this file was written by minor compaction in hive
        File file = new File(getResource("orcFileWithoutRowGroupInfo.orc").getPath());

        OrcReader orcReader = createOrcReader(new FileOrcDataSource(file, new OrcReaderOptions()), new OrcReaderOptions()).orElseThrow();

        assertEquals(orcReader.getFooter().getNumberOfRows(), 2);
        assertEquals(orcReader.getFooter().getRowsInRowGroup(), OptionalInt.empty());

        RowType rowType = RowType.from(ImmutableList.of(RowType.field("a", BIGINT)));
        OrcRecordReader reader = orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                ImmutableList.of(INTEGER, BIGINT, INTEGER, BIGINT, BIGINT, rowType),
                orcPredicate,
                DateTimeZone.UTC,
                newSimpleAggregatedMemoryContext(),
                INITIAL_BATCH_SIZE,
                RuntimeException::new);

        int rows = 0;
        while (true) {
            Page page = reader.nextPage();
            if (page == null) {
                break;
            }
            page = page.getLoadedPage();
            rows += page.getPositionCount();

            Block rowBlock = page.getBlock(5);

            for (int position = 0; position < page.getPositionCount(); position++) {
                BIGINT.getLong(
                        rowType.getObject(rowBlock, 0),
                        0);
            }
        }

        assertEquals(rows, reader.getFileRowCount());
    }
}
