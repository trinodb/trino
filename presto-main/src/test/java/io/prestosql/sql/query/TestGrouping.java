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

public class TestGrouping
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
    public void testImplicitCoercions()
    {
        // GROUPING + implicit coercions (see https://github.com/prestodb/presto/issues/8738)
        assertions.assertQuery(
                "SELECT GROUPING(k), SUM(v) + 1e0 FROM (VALUES (1, 1)) AS t(k,v) GROUP BY k",
                "VALUES (0, 2e0)");

        assertions.assertQuery(
                "SELECT\n" +
                        "    1e0 * count(*), " +
                        "    grouping(x) " +
                        "FROM (VALUES 1) t(x) " +
                        "GROUP BY GROUPING SETS ((x), ()) ",
                "VALUES (1e0, 1), (1e0, 0)");
    }

    @Test
    public void testFilter()
    {
        assertions.assertQuery(
                "SELECT a, b, grouping(a, b) " +
                        "FROM (VALUES ('x0', 'y0'), ('x1', 'y1') ) AS t (a, b) " +
                        "GROUP BY CUBE (a, b)" +
                        "HAVING grouping(a, b) = 0",
                "VALUES ('x0', 'y0', 0), ('x1', 'y1', 0)");
    }

    @Test
    public void testCastDifferentCase()
    {
        assertions.assertQuery(
                "SELECT CAST(x AS bigint) " +
                        "FROM (VALUES 42) t(x) " +
                        "GROUP BY CAST(x AS BIGINT)", // CAST type in a different case
                "VALUES BIGINT '42'");

        assertions.assertQuery(
                "SELECT CAST(row(x) AS row(\"A\" bigint)) " +
                        "FROM (VALUES 42) t(x) " +
                        "GROUP BY CAST(row(x) AS row(\"A\" bigint))", // same expression including ROW with a delimited field name
                "SELECT CAST(row(BIGINT '42') AS row(\"A\" bigint))");

        assertions.assertQuery(
                "SELECT CAST(row(x) AS row(abc bigint)) " +
                        "FROM (VALUES 42) t(x) " +
                        "GROUP BY CAST(row(x) AS row(ABC bigint))", // ROW field name in a different case, not delimited
                "SELECT CAST(row(BIGINT '42') AS row(\"A\" bigint))");

        assertions.assertQuery(
                "SELECT CAST(row(x) AS row(\"A\" bigint)) " +
                        "FROM (VALUES 42) t(x) " +
                        "GROUP BY CAST(row(x) AS row(\"A\" BigINT))", // ROW field type in a different case
                "SELECT CAST(row(BIGINT '42') AS row(\"A\" bigint))");

        assertions.assertFails(
                "SELECT CAST(row(x) AS row(\"a\" bigint)) " +
                        "FROM (VALUES 42) t(x) " +
                        "GROUP BY CAST(row(x) AS row(\"A\" bigint))", // ROW field name in a different case, delimited
                "\\Qline 1:8: 'CAST(ROW (x) AS ROW(\"a\" bigint))' must be an aggregate expression or appear in GROUP BY clause");
    }
}
