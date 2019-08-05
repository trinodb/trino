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
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.MAX_BATCH_SIZE;
import static io.prestosql.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestOrcRecordReaderWithAcidFile
{
    /*
     * Reads a Acid file with single data column and having single row in dataset
     *
     * Content of read file:
     *  {"operation":0,"originalTransaction":2,"bucket":536870912,"rowId":0,"currentTransaction":2,"row":{"col":2}}
     *
     *  This file is generated using hive3.1 by writing values(2) into a transactional table of schema <col: integer>
     */
    @Test
    public void testSimpleFileRead()
            throws IOException
    {
        OrcPredicate predicate = createLongPredicate(2L);
        OrcRecordReader recordReader = createOrcRecordReader("fullacid_basefile_singleCol_singleRow.orc", predicate);

        List<Object> data = readData(recordReader, INTEGER);

        assertTrue(data.size() == 1, "Unexpected amount of rows read: " + data);
        assertTrue((Integer) data.get(0) == 2, "Unexpected data read: " + data);
    }

    /*
     * Test pruning of full file based on file stats
     *
     * This test reads Acid file nationFile25kRowsSortedOnNationKey/bucket_00000
     * It has replicated version of 25 rows of nations replicated into 25000 rows
     * It has row group size = 1000 and has content in sorted order of nationkey
     * Hence first row group has all nationkey=0 rows, next has nationkey=1 and so on
     *
     * This file has 5 stripes and 25 rowgroups
     */
    @Test
    public void testFullFileSkipped()
            throws IOException
    {
        OrcPredicate predicate = createLongPredicate(100L);

        OrcRecordReader recordReader = createOrcRecordReader("nationFile25kRowsSortedOnNationKey/bucket_00000", predicate);
        List<Object> data = readData(recordReader, INTEGER);

        assertTrue(data.size() == 0);
        assertTrue(recordReader.isFileSkipped());
    }

    /*
     * Tests stripe stats and row groups stats based pruning works fine
     *
     * This test reads Acid file nationFile25kRowsSortedOnNationKey/bucket_00000
     * as described in previous test
     */
    @Test
    public void testSomeStripesAndRowGroupRead()
            throws IOException
    {
        OrcPredicate predicate = createLongPredicate(0L);
        OrcRecordReader recordReader = createOrcRecordReader("nationFile25kRowsSortedOnNationKey/bucket_00000", predicate);

        List<Object> data = readData(recordReader, INTEGER);

        assertTrue(data.size() != 0);
        assertFalse(recordReader.isFileSkipped());
        assertTrue(recordReader.stripesRead() == 1);  // 1 out of 5 stripes should be read
        assertTrue(recordReader.getRowGroupsRead() == 1); // 1 out of 25 rowgroups should be read
    }

    private OrcRecordReader createOrcRecordReader(String filename, OrcPredicate predicate)
            throws IOException
    {
        File targetFile = new File((Thread.currentThread().getContextClassLoader().getResource(filename).getPath()));
        OrcDataSource orcDataSource = new FileOrcDataSource(targetFile, new OrcReaderOptions());
        OrcReader orcReader = new OrcReader(orcDataSource, new OrcReaderOptions());
        Type type = INTEGER;

        // First data column in Acid file would be at index=5 because first 5 indexes would be of meta columns
        return orcReader.createAcidRecordReader(ImmutableMap.of(5, type), predicate, HIVE_STORAGE_TIME_ZONE, newSimpleAggregatedMemoryContext(), MAX_BATCH_SIZE);
    }

    private OrcPredicate createLongPredicate(long value)
    {
        Domain testingColumnHandleDomain = Domain.singleValue(INTEGER, value);
        TupleDomain.ColumnDomain<String> column0 = new TupleDomain.ColumnDomain<>("n_nationkey", testingColumnHandleDomain);
        TupleDomain<String> effectivePredicate = TupleDomain.fromColumnDomains(Optional.of(ImmutableList.of(column0)));
        List<TupleDomainOrcPredicate.ColumnReference<String>> columnReferences = ImmutableList.<TupleDomainOrcPredicate.ColumnReference<String>>builder()
                .add(new TupleDomainOrcPredicate.ColumnReference<>("n_nationkey", 5, INTEGER))
                .build();
        return new TupleDomainOrcPredicate<>(effectivePredicate, columnReferences, false);
    }

    private List<Object> readData(OrcRecordReader recordReader, Type type)
            throws IOException
    {
        List<Object> data = new ArrayList<>();
        int batchSize = recordReader.nextBatch();
        while (batchSize > 0) {
            Block block = recordReader.readBlock(5);
            for (int position = 0; position < block.getPositionCount(); position++) {
                data.add(type.getObjectValue(SESSION, block, position));
            }
            batchSize = recordReader.nextBatch();
        }
        return data;
    }
}
