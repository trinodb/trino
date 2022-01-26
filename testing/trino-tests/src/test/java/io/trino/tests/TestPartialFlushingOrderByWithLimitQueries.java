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
package io.trino.tests;

import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestPartialFlushingOrderByWithLimitQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(SystemSessionProperties.TASK_CONCURRENCY, "2")
                .setSystemProperty(SystemSessionProperties.MAX_PARTIAL_TOP_N_MEMORY, "1B")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .setNodeCount(2)
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    @Test
    public void testOrderByLimit()
    {
        assertQueryOrdered("SELECT custkey, orderstatus FROM orders ORDER BY orderkey DESC LIMIT 10");
        assertQueryOrdered("SELECT custkey, orderstatus FROM orders ORDER BY orderkey + 1 DESC LIMIT 10");
        assertQuery("SELECT custkey, totalprice FROM orders ORDER BY orderkey LIMIT 0");
    }

    @Test
    public void testOrderByLimitAll()
    {
        assertQuery("SELECT custkey, totalprice FROM orders ORDER BY orderkey LIMIT ALL", "SELECT custkey, totalprice FROM orders ORDER BY orderkey");
    }

    @Test
    public void testOrderByWithSimilarExpressions()
    {
        assertQuery(
                "WITH t AS (SELECT 1 x, 2 y) SELECT x, y FROM t ORDER BY x, y",
                "SELECT 1, 2");
        assertQuery(
                "WITH t AS (SELECT 1 x, 2 y) SELECT x, y FROM t ORDER BY x, y LIMIT 1",
                "SELECT 1, 2");
        assertQuery(
                "WITH t AS (SELECT 1 x, 1 y) SELECT x, y FROM t ORDER BY x, y LIMIT 1",
                "SELECT 1, 1");
        assertQuery(
                "WITH t AS (SELECT orderkey x, orderkey y FROM orders) SELECT x, y FROM t ORDER BY x, y LIMIT 1",
                "SELECT 1, 1");
        assertQuery(
                "WITH t AS (SELECT orderkey x, orderkey y FROM orders) SELECT x, y FROM t ORDER BY x, y DESC LIMIT 1",
                "SELECT 1, 1");
        assertQuery(
                "WITH t AS (SELECT orderkey x, totalprice y, orderkey z FROM orders) SELECT x, y, z FROM t ORDER BY x, y, z LIMIT 1",
                "SELECT 1, 172799.49, 1");
    }
}
