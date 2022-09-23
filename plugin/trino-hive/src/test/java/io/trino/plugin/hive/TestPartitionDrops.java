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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.tpch.TpchTable.NATION;

public class TestPartitionDrops
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.of("hive.max-partition-drops-per-query", "4"))
                .setInitialTables(ImmutableList.of(NATION))
                .build();
    }

    @Test
    public void testPartitionDropsLimit()
    {
        assertUpdate("CREATE TABLE test_partitioned_table (a BIGINT, b BIGINT) WITH (partitioned_by = ARRAY['b'])");
        assertUpdate("INSERT INTO test_partitioned_table VALUES (1,1), (2,2), (3,3), (4,4), (5,5)", 5);

        assertQueryFails(
                "DELETE FROM test_partitioned_table WHERE b <= 6",
                "Failed to drop partitions. The number of partitions to be dropped is greater than the maximum allowed partitions \\(4\\).");
        assertQuery("SELECT * FROM test_partitioned_table", "VALUES (1,1), (2,2), (3,3), (4,4), (5,5)");

        assertUpdate("DELETE FROM test_partitioned_table WHERE b <= 4");
        assertQuery("SELECT * FROM test_partitioned_table", "VALUES (5,5)");

        assertUpdate("DROP TABLE test_partitioned_table");
    }
}
