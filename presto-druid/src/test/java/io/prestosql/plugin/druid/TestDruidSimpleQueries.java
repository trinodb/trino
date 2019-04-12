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
package io.prestosql.plugin.druid;

import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test
public class TestDruidSimpleQueries
        extends AbstractTestQueryFramework
{
    private final TestingDruidServer druidServer;

    public TestDruidSimpleQueries()
    {
        this(new TestingDruidServer());
    }

    protected TestDruidSimpleQueries(TestingDruidServer testingDruidServer)
    {
        super(() -> DruidQueryRunner.createDruidQueryRunner(testingDruidServer));
        this.druidServer = testingDruidServer;
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
    }

    @Test
    public void testNonEqalityJoinWithScalarRequiringSessionParameter()
    {
        assertQuery("SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND from_unixtime(b) > current_timestamp",
                "VALUES (1, 1, NULL, NULL), (1, 2, NULL, NULL)");
    }

    @Test
    public void testNonEqualityJoinWithTryInFilter()
    {
        assertQuery("SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) " +
                        "             ON a=c AND TRY(1 / (b-a) != 1000)",
                "VALUES (1, 1, NULL, NULL), (1, 2, 1, 1), (1, 2, 1, 2)");

        // use of scalar requiring session parameter within try
        assertQuery("SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) " +
                        "             ON a=c AND TRY(1 / (b-a) != 1000 OR from_unixtime(b) > current_timestamp)",
                "VALUES (1, 1, NULL, NULL), (1, 2, 1, 1), (1, 2, 1, 2)");
    }

    @Test
    public void testNonEqualityFullJoin()
    {
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > d",
                "VALUES (1, 2, 1, 1), (NULL, NULL, 1, 2), (1, 1, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b < d",
                "VALUES (1, 1, 1, 2), (NULL, NULL, 1, 1), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 2",
                "VALUES (NULL, NULL, 1, 1), (NULL, NULL, 1, 2), (1, 1, NULL, NULL), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 2",
                "VALUES (NULL, NULL, 1, 1), (NULL, NULL, 1, 2), (1, 1, NULL, NULL), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) FULL OUTER JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (2, 11), (2, 10), (1, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) FULL OUTER JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (NULL, 10), (1, 11), (2, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) FULL OUTER JOIN (VALUES 10, 11) t2(b) ON a > b",
                "VALUES (NULL, 10), (NULL, 11), (1, NULL), (2, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) FULL OUTER JOIN (VALUES 10, 11) t2(b) ON a < b",
                "VALUES (1, 10), (1, 11), (2, 10), (2, 11)");
    }

    @Test
    public void testFullJoinWithCoalesce()
    {
        assertQuery(
                "SELECT coalesce(t.a, u.a, if(t.b is null, 100, t.b)), count(*) " +
                        "FROM (VALUES (1, 10), (2, 20), (3, 30), (null, 40), (100, 50)) t(a, b) " +
                        "FULL OUTER JOIN (VALUES 1, 4, null) u(a) ON t.a = u.a " +
                        "GROUP BY 1",
                "VALUES (1, 1), (2, 1), (3, 1), (4, 1), (40, 1), (100, 2)");
    }

    @Test
    public void testLateralJoin()
    {
        assertQuery(
                "SELECT y FROM (VALUES array[2, 3]) a(x) CROSS JOIN LATERAL(SELECT x[1]) b(y)",
                "SELECT 2");
        assertQuery(
                "SELECT * FROM (VALUES 2) a(x) CROSS JOIN LATERAL(SELECT x + 1)",
                "SELECT 2, 3");
        assertQuery(
                "SELECT * FROM (VALUES 2) a(x) CROSS JOIN LATERAL(SELECT x)",
                "SELECT 2, 2");
        assertQuery(
                "SELECT * FROM (VALUES 2) a(x) CROSS JOIN LATERAL(SELECT x, x + 1)",
                "SELECT 2, 2, 3");

        assertQueryFails(
                "SELECT * FROM (VALUES array[2, 2]) a(x) LEFT OUTER JOIN LATERAL(VALUES x) ON true",
                "line .*: LATERAL on other than the right side of CROSS JOIN is not supported");
        assertQueryFails(
                "SELECT * FROM (VALUES array[2, 2]) a(x) RIGHT OUTER JOIN LATERAL(VALUES x) ON true",
                "line .*: LATERAL on other than the right side of CROSS JOIN is not supported");
        assertQueryFails(
                "SELECT * FROM (VALUES array[2, 2]) a(x) FULL OUTER JOIN LATERAL(VALUES x) ON true",
                "line .*: LATERAL on other than the right side of CROSS JOIN is not supported");
    }

    @Test
    public void testPruningCountAggregationOverScalar()
    {
        assertQuery("SELECT count(*) FROM (VALUES 2) t(a) GROUP BY a", "VALUES 1");
        assertQuery("SELECT a, count(*) FROM (VALUES 2) t(a) GROUP BY a", "VALUES (2, 1)");
        assertQuery("SELECT count(*) FROM (VALUES 2) t(a) GROUP BY a+1", "VALUES 1");
    }

    @Test
    public void testShowSchemas()
    {
        assertQuery("SHOW SCHEMAS FROM druid", "VALUES 'druid', 'information_schema', 'sys'");
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        druidServer.close();
    }
}
