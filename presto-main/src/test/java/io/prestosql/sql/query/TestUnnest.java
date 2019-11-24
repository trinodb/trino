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
package io.prestosql.sql.query;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestUnnest
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testUnnestArrayRows()
    {
        assertions.assertQuery(
                "SELECT * FROM UNNEST(ARRAY[ROW(1, 1.1), ROW(3, 3.3)], ARRAY[ROW('a', true), ROW('b', false)])",
                "VALUES (1, 1.1, 'a', true), (3, 3.3, 'b', false)");
        assertions.assertQuery(
                "SELECT x, y FROM (VALUES (ARRAY[ROW(1.0, 2), ROW(3, 4.123)])) AS t(a) CROSS JOIN UNNEST(a) t(x, y)",
                "VALUES (1.0, 2), (3, 4.123)");
        assertions.assertQuery(
                "SELECT x, y, z FROM (VALUES (ARRAY[ROW(1, 2), ROW(3, 4)])) t(a) CROSS JOIN (VALUES (1), (2)) s(z) CROSS JOIN UNNEST(a) t(x, y)",
                "VALUES (1, 2, 1), (1, 2, 2), (3, 4, 1), (3, 4, 2)");
    }

    @Test
    public void testUnnestPreserveColumnName()
    {
        assertions.assertQuery(
                "SELECT x FROM UNNEST(CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] as ARRAY(ROW(x int, y varchar))))",
                "VALUES (1), (2)");

        assertions.assertFails(
                "SELECT x FROM" +
                        "(VALUES (3)) AS t(x)" +
                        "CROSS JOIN UNNEST(CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] as ARRAY(ROW(x int, y varchar))))",
                ".*Column 'x' is ambiguous.*");

        assertions.assertQuery(
                "SELECT t.x FROM" +
                        "(VALUES (3)) AS t(x)" +
                        "CROSS JOIN UNNEST(CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] as ARRAY(ROW(x int, y varchar))))",
                "VALUES (3), (3)");

        assertions.assertQuery(
                "SELECT u.x FROM" +
                        "(VALUES (3)) AS t(x)" +
                        "CROSS JOIN UNNEST(CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] as ARRAY(ROW(x int, y varchar)))) u",
                "VALUES (1), (2)");
    }

    @Test
    public void testUnnestMultiExpr()
    {
        assertions.assertFails(
                "SELECT x " +
                        "FROM UNNEST(" +
                        "   CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] as ARRAY(ROW(x int, y varchar)))," +
                        "   CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] as ARRAY(ROW(x int, y varchar))))",
                ".*Column 'x' is ambiguous.*");

        assertions.assertQuery(
                "SELECT t3 " +
                        "FROM UNNEST(" +
                        "   CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] as ARRAY(ROW(x int, y varchar)))," +
                        "   CAST(ARRAY[ROW(3, 'c'), ROW(4, 'd')] as ARRAY(ROW(x int, y varchar)))) t(t1,t2,t3,t4)",
                "VALUES (3), (4)");

        assertions.assertQuery(
                "SELECT x " +
                        "FROM UNNEST(" +
                        "   CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] as ARRAY(ROW(a int, b varchar)))," +
                        "   CAST(ARRAY[ROW(3, 'c'), ROW(4, 'd')] as ARRAY(ROW(x int, y varchar))))",
                "VALUES (3), (4)");
    }

    @Test
    public void testLeftJoinUnnest()
    {
        assertions.assertQuery(
                "SELECT * FROM (VALUES ARRAY[1, null]) a(x) LEFT OUTER JOIN UNNEST(x) ON true",
                "VALUES (ARRAY[1, null], 1), (ARRAY[1, null], null)");
        assertions.assertQuery(
                "SELECT * FROM (VALUES ARRAY[1, null]) a(x) LEFT OUTER JOIN UNNEST(x) WITH ORDINALITY ON true",
                "VALUES (ARRAY[1, null], 1, BIGINT '1'), (ARRAY[1, null], null, BIGINT '2')");
        assertions.assertQuery(
                "SELECT * FROM (VALUES ARRAY[]) a(x) LEFT OUTER JOIN UNNEST(x) ON true",
                "VALUES (ARRAY[], null)");
        assertions.assertQuery(
                "SELECT * FROM (VALUES ARRAY[]) a(x) LEFT OUTER JOIN UNNEST(x) WITH ORDINALITY ON true",
                "VALUES (ARRAY[], null, CAST(NULL AS bigint))");
        assertions.assertFails(
                "SELECT * FROM (VALUES ARRAY[1, null]) a(x) LEFT OUTER JOIN UNNEST(x) b(y) ON b.y = 1",
                "line .*: UNNEST in conditional JOIN is not supported");
        assertions.assertQuery(
                "SELECT * FROM (VALUES 'a', 'b') LEFT JOIN UNNEST(ARRAY[]) ON TRUE",
                "VALUES ('a', null), ('b', null)");
        assertions.assertQuery(
                "SELECT id, e FROM (VALUES (1, ARRAY[3,4]), (2, NULL), (3, ARRAY[4]), (4, NULL), (5, ARRAY[]), (6, ARRAY[7,8])) x(id, a) LEFT JOIN UNNEST(a) AS y(e) ON true",
                "VALUES (1,3), (1,4), (2,NULL), (3,4), (4,NULL), (5,NULL), (6,7), (6,8)");
        // misalignment
        assertions.assertQuery(
                "SELECT * FROM (VALUES 1) LEFT OUTER JOIN UNNEST (MAP(ARRAY[1, 2], ARRAY['a', 'b']), ARRAY[ROW(3, 'c', true)]) WITH ORDINALITY ON TRUE",
                "VALUES (1, 1, 'a', 3, 'c', true, BIGINT '1'), (1, 2, 'b', null, null, null, BIGINT '2')");
        assertions.assertQuery(
                "SELECT * FROM (VALUES 1) LEFT OUTER JOIN UNNEST (MAP(ARRAY[1], ARRAY['a']), ARRAY[true, false], ARRAY[]) WITH ORDINALITY ON TRUE",
                "VALUES (1, 1, 'a', true, null, BIGINT '1'), (1, null, null, false, null, BIGINT '2')");
    }

    @Test
    public void testRightJoinUnnest()
    {
        assertions.assertQuery(
                "SELECT * FROM (VALUES ARRAY[1, null]) a(x) RIGHT OUTER JOIN UNNEST(x) ON true",
                "VALUES (ARRAY[1, null], 1), (ARRAY[1, null], null)");
        assertions.assertQueryReturnsEmptyResult(
                "SELECT * FROM (VALUES ARRAY[]) a(x) RIGHT OUTER JOIN UNNEST(x) ON true");
        assertions.assertQuery(
                "SELECT * FROM (VALUES ARRAY[1, null]) a(x) RIGHT OUTER JOIN UNNEST(x) WITH ORDINALITY ON true",
                "VALUES (ARRAY[1, null], 1, BIGINT '1'), (ARRAY[1, null], null, BIGINT '2')");
        assertions.assertFails(
                "SELECT * FROM (VALUES ARRAY[1, null]) a(x) RIGHT OUTER JOIN UNNEST(x) b(y) ON b.y = 1",
                "line .*: UNNEST in conditional JOIN is not supported");
    }

    @Test
    public void testFullJoinUnnest()
    {
        assertions.assertQuery(
                "SELECT * FROM (VALUES ARRAY[1, null]) a(x) FULL OUTER JOIN UNNEST(x) ON true",
                "VALUES (ARRAY[1, null], 1), (ARRAY[1, null], null)");
        assertions.assertQuery(
                "SELECT * FROM (VALUES ARRAY[1, null]) a(x) FULL OUTER JOIN UNNEST(x) WITH ORDINALITY ON true",
                "VALUES (ARRAY[1, null], 1, BIGINT '1'), (ARRAY[1, null], null, BIGINT '2')");
        assertions.assertQuery(
                "SELECT * FROM (VALUES ARRAY[]) a(x) FULL OUTER JOIN UNNEST(x) ON true",
                "VALUES (ARRAY[], null)");
        assertions.assertQuery(
                "SELECT * FROM (VALUES ARRAY[]) a(x) FULL OUTER JOIN UNNEST(x) WITH ORDINALITY ON true",
                "VALUES (ARRAY[], null, CAST(NULL AS bigint))");
        assertions.assertFails(
                "SELECT * FROM (VALUES ARRAY[1, null]) a(x) FULL OUTER JOIN UNNEST(x) b(y) ON b.y = 1",
                "line .*: UNNEST in conditional JOIN is not supported");
    }

    @Test
    public void testInnerJoinUnnest()
    {
        assertions.assertQuery(
                "SELECT * FROM (VALUES ARRAY[1, null]) a(x) INNER JOIN UNNEST(x) ON true",
                "VALUES (ARRAY[1, null], 1), (ARRAY[1, null], null)");
        assertions.assertQuery(
                "SELECT * FROM (VALUES ARRAY[1, null]) a(x) INNER JOIN UNNEST(x) WITH ORDINALITY ON true",
                "VALUES (ARRAY[1, null], 1, BIGINT '1'), (ARRAY[1, null], null, BIGINT '2')");
        assertions.assertQueryReturnsEmptyResult(
                "SELECT * FROM (VALUES ARRAY[]) a(x) INNER JOIN UNNEST(x) ON true");
        assertions.assertQueryReturnsEmptyResult(
                "SELECT * FROM (VALUES ARRAY[]) a(x) INNER JOIN UNNEST(x) WITH ORDINALITY ON true");
        assertions.assertFails(
                "SELECT * FROM (VALUES ARRAY[1, null]) a(x) INNER JOIN UNNEST(x) b(y) ON b.y = 1",
                "line .*: UNNEST in conditional JOIN is not supported");
    }
}
