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
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestJoinIsNotDistinct
        extends AbstractTestQueryFramework
{
    private final Session memorySession = testSessionBuilder()
            .setCatalog("memory")
            .setSchema("default")
            .build();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = TpchQueryRunner.builder().build();

        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory");

        return queryRunner;
    }

    @Test
    public void testJoinWithIsNotDistinctFromOnNulls()
    {
        String tableName1 = "test_tab_" + randomNameSuffix();
        getQueryRunner().execute(memorySession, format("CREATE TABLE %s (k1 INT, k2 INT)", tableName1));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (1, NULL)", tableName1));

        String tableName2 = "test_tab_" + randomNameSuffix();
        getQueryRunner().execute(memorySession, format("CREATE TABLE %s (k1 INT, k2 INT)", tableName2));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (1, NULL)", tableName2));

        assertQuery(
                memorySession,
                format("SELECT *" +
                        " FROM %s t" +
                        "  INNER JOIN %s AS s" +
                        "    ON s.k1 IS NOT DISTINCT FROM t.k1" +
                        "    AND s.k2 IS NOT DISTINCT FROM t.k2", tableName1, tableName2),
                "VALUES (1, NULL, 1, NULL)");
    }

    @Test
    public void testJoinWithIsNotDistinctFromOnNulls2()
    {
        String tableName1 = "test_tab_" + randomNameSuffix();
        getQueryRunner().execute(memorySession, format("CREATE TABLE %s (k1 INT, k2 INT)", tableName1));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (1, NULL)", tableName1));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (NULL, 2)", tableName1));

        String tableName2 = "test_tab_" + randomNameSuffix();
        getQueryRunner().execute(memorySession, format("CREATE TABLE %s (k1 INT, k2 INT)", tableName2));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (1, NULL)", tableName2));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (3, NULL)", tableName2));

        assertQuery(
                memorySession,
                format("SELECT *" +
                        " FROM %s t" +
                        "  INNER JOIN %s AS s" +
                        "    ON s.k1 IS NOT DISTINCT FROM t.k1" +
                        "    AND s.k2 IS NOT DISTINCT FROM t.k2", tableName1, tableName2),
                "VALUES (1, NULL, 1, NULL)");
    }

    @Test
    public void testJoinWithIsNotDistinctFromOnNulls3()
    {
        String tableName1 = "test_tab_" + randomNameSuffix();
        getQueryRunner().execute(memorySession, format("CREATE TABLE %s (k1 INT, k2 INT)", tableName1));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (1, NULL)", tableName1));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (NULL, 2)", tableName1));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (NULL, NULL)", tableName1));

        String tableName2 = "test_tab_" + randomNameSuffix();
        getQueryRunner().execute(memorySession, format("CREATE TABLE %s (k1 INT, k2 INT)", tableName2));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (1, NULL)", tableName2));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (3, NULL)", tableName2));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (NULL, NULL)", tableName2));

        assertQuery(
                memorySession,
                format("SELECT *" +
                        " FROM %s t" +
                        "  INNER JOIN %s AS s" +
                        "    ON s.k1 IS NOT DISTINCT FROM t.k1" +
                        "    AND s.k2 IS NOT DISTINCT FROM t.k2", tableName1, tableName2),
                "VALUES (1, NULL, 1, NULL), (NULL, NULL, NULL, NULL)");
    }

    @Test
    public void testJoinWithIsNotDistinctFromOnNulls4()
    {
        String tableName1 = "test_tab_" + randomNameSuffix();
        getQueryRunner().execute(memorySession, format("CREATE TABLE %s (k1 INT, k2 INT)", tableName1));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (1, NULL)", tableName1));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (NULL, 2)", tableName1));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (NULL, NULL)", tableName1));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (2, 2)", tableName1));

        String tableName2 = "test_tab_" + randomNameSuffix();
        getQueryRunner().execute(memorySession, format("CREATE TABLE %s (k1 INT, k2 INT)", tableName2));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (1, NULL)", tableName2));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (3, NULL)", tableName2));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (NULL, NULL)", tableName2));
        getQueryRunner().execute(memorySession, format("INSERT INTO %s VALUES (2, 2)", tableName2));

        assertQuery(
                memorySession,
                format("SELECT *" +
                        " FROM %s t" +
                        "  INNER JOIN %s AS s" +
                        "    ON s.k1 IS NOT DISTINCT FROM t.k1" +
                        "    AND s.k2 IS NOT DISTINCT FROM t.k2", tableName1, tableName2),
                "VALUES (1, NULL, 1, NULL), (NULL, NULL, NULL, NULL), (2, 2, 2, 2)");
    }

    @Test
    public void testJoinWithIsNotDistinctFromOnNullsInMemory()
    {
        assertQuery(
                memorySession,
                "SELECT *" +
                        " FROM (SELECT 1 AS k1, NULL AS k2) t" +
                        "  INNER JOIN (SELECT 1 AS k1, NULL AS k2) AS s" +
                        "    ON s.k1 IS NOT DISTINCT FROM t.k1" +
                        "    AND s.k2 IS NOT DISTINCT FROM t.k2",
                "VALUES (1, NULL, 1, NULL)");
    }
}
