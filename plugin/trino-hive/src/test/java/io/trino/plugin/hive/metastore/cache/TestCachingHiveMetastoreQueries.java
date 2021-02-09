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
package io.trino.plugin.hive.metastore.cache;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.google.common.base.Verify.verify;
import static io.trino.tpch.TpchTable.NATION;
import static java.util.Collections.nCopies;

public class TestCachingHiveMetastoreQueries
        extends AbstractTestQueryFramework
{
    private static final int nodeCount = 3;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        verify(nodeCount > 1, "this test requires a multinode query runner");
        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.of("hive.metastore-cache-ttl", "60m"))
                // only so that tpch schema is created (TODO https://github.com/trinodb/trino/issues/6861)
                .setInitialTables(ImmutableList.of(NATION))
                .setNodeCount(nodeCount)
                // Exclude coordinator from workers to make testPartitionAppend deterministically reproduce the original problem
                .setCoordinatorProperties(ImmutableMap.of("node-scheduler.include-coordinator", "false"))
                .build();
    }

    @Test
    public void testPartitionAppend()
    {
        getQueryRunner().execute("CREATE TABLE test_part_append " +
                "(name varchar, partkey varchar) " +
                "WITH (partitioned_by = ARRAY['partkey'])");

        String row = "('some name', 'part1')";

        // if metastore caching was enabled on workers than any worker which tries to INSERT into same partition twice
        // will fail because it would've cached the absence of the partition
        for (int i = 0; i < nodeCount + 1; i++) {
            getQueryRunner().execute("INSERT INTO test_part_append VALUES " + row);
        }

        String expected = Joiner.on(",").join(nCopies(nodeCount + 1, row));
        assertQuery("SELECT * FROM test_part_append", "VALUES " + expected);
    }
}
