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
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.tpch.TpchTable.NATION;

public class TestAnalyzeForQuery
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                // create nation so tpch schema got created
                .setInitialTables(ImmutableList.of(NATION))
                .setNodeCount(1)
                .build();
    }

    @BeforeClass
    public void setUp()
    {
        assertUpdate("CREATE TABLE nation_partitioned(nationkey BIGINT, name VARCHAR, comment VARCHAR, regionkey BIGINT) WITH (partitioned_by = ARRAY['regionkey'])");
        assertUpdate("INSERT INTO nation_partitioned SELECT nationkey, name, comment, regionkey FROM tpch.tiny.nation", 25);
        assertUpdate("CREATE TABLE region AS SELECT * FROM tpch.tiny.region", 5);
    }

    @Test
    public void testAnalyzeForQuery()
    {
        // non-partitioned table
        assertUpdate("ANALYZE FOR (SELECT * FROM region)", 5);

        // partitioned table
        assertUpdate("ANALYZE FOR (SELECT * FROM nation_partitioned)", 25);
        assertUpdate("ANALYZE FOR (SELECT * FROM nation_partitioned WHERE regionkey IS NOT NULL)", 25);
        assertUpdate("ANALYZE FOR (SELECT * FROM nation_partitioned WHERE regionkey IS NULL)", 0);
        assertUpdate("ANALYZE FOR (SELECT * FROM nation_partitioned WHERE regionkey = 1)", 5);
        assertUpdate("ANALYZE FOR (SELECT * FROM nation_partitioned WHERE regionkey IN (1, 3))", 10);
        assertUpdate("ANALYZE FOR (SELECT * FROM nation_partitioned WHERE regionkey BETWEEN 1 AND 1 + 2)", 15);
        assertUpdate("ANALYZE FOR (SELECT * FROM nation_partitioned WHERE regionkey > 3)", 5);
        assertUpdate("ANALYZE FOR (SELECT * FROM nation_partitioned WHERE regionkey < 1)", 5);
        assertUpdate("ANALYZE FOR (SELECT * FROM nation_partitioned WHERE regionkey > 0 and regionkey < 4)", 15);
        assertUpdate("ANALYZE FOR (SELECT * FROM nation_partitioned WHERE regionkey > 10 or regionkey < 0)", 0);
        assertUpdate("ANALYZE FOR (SELECT *, * FROM nation_partitioned WHERE regionkey > 10 or regionkey < 0)", 0);
        assertUpdate("ANALYZE FOR (SELECT *, *, regionkey FROM nation_partitioned WHERE regionkey > 10 or regionkey < 0)", 0);
        assertUpdate("ANALYZE FOR (SELECT *, regionkey FROM nation_partitioned)", 25);
        assertUpdate("ANALYZE FOR (SELECT *, * FROM nation_partitioned)", 25);
        assertUpdate("ANALYZE FOR (SELECT regionkey FROM nation_partitioned WHERE regionkey BETWEEN 1 AND 1 + 2)", 15);
        assertUpdate("ANALYZE FOR (SELECT regionkey, nationkey FROM nation_partitioned WHERE regionkey BETWEEN 1 AND 1 + 2)", 15);

        // merge same tables and union with different partitions
        assertUpdate("ANALYZE FOR (SELECT * FROM nation_partitioned WHERE regionkey = 1 UNION ALL SELECT * FROM nation_partitioned WHERE regionkey = 3)", 10);

        // prepared statement
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "ANALYZE FOR (SELECT * FROM nation_partitioned WHERE regionkey = ?)")
                .build();
        assertUpdate(session, "EXECUTE my_query USING 1", 5);
    }
}
