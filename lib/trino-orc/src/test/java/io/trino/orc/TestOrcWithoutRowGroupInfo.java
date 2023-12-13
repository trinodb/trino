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
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlRow;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.RowType;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.OptionalInt;

import static com.google.common.io.Resources.getResource;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.orc.OrcReader.createOrcReader;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

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
            throws Exception
    {
        // this file was written by minor compaction in hive
        File file = new File(getResource("orcFileWithoutRowGroupInfo.orc").toURI());

        OrcReader orcReader = createOrcReader(new FileOrcDataSource(file, new OrcReaderOptions()), new OrcReaderOptions()).orElseThrow();

        assertThat(orcReader.getFooter().getNumberOfRows()).isEqualTo(2);
        assertThat(orcReader.getFooter().getRowsInRowGroup()).isEqualTo(OptionalInt.empty());

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
                SqlRow sqlRow = rowType.getObject(rowBlock, 0);
                BIGINT.getLong(sqlRow.getRawFieldBlock(0), sqlRow.getRawIndex());
            }
        }

        assertThat(rows).isEqualTo(reader.getFileRowCount());
    }
}
