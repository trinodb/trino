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
package io.trino.sql.query;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestGroupBy
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testCastDifferentCase()
    {
        // CAST type in a different case
        assertThat(assertions.query(
                "SELECT CAST(x AS bigint) " +
                        "FROM (VALUES 42) t(x) " +
                        "GROUP BY CAST(x AS BIGINT)"))
                .matches("VALUES BIGINT '42'");

        // same expression including ROW with a delimited field name
        assertThat(assertions.query(
                "SELECT CAST(row(x) AS row(\"A\" bigint)) " +
                        "FROM (VALUES 42) t(x) " +
                        "GROUP BY CAST(row(x) AS row(\"A\" bigint))"))
                .matches("SELECT CAST(row(BIGINT '42') AS row(\"A\" bigint))");

        // ROW field name in a different case, not delimited
        assertThat(assertions.query(
                "SELECT CAST(row(x) AS row(abc bigint)) " +
                        "FROM (VALUES 42) t(x) " +
                        "GROUP BY CAST(row(x) AS row(ABC bigint))"))
                .matches("SELECT CAST(row(BIGINT '42') AS row(abc bigint))");

        // ROW field type in a different case
        assertThat(assertions.query(
                "SELECT CAST(row(x) AS row(\"A\" bigint)) " +
                        "FROM (VALUES 42) t(x) " +
                        "GROUP BY CAST(row(x) AS row(\"A\" BigINT))"))
                .matches("SELECT CAST(row(BIGINT '42') AS row(\"A\" bigint))");

        // ROW field name in a different case, delimited
        assertThat(assertions.query(
                "SELECT CAST(row(x) AS row(\"a\" bigint)) " +
                        "FROM (VALUES 42) t(x) " +
                        "GROUP BY CAST(row(x) AS row(\"A\" bigint))"))
                .failure().hasMessage("line 1:8: 'CAST(ROW (x) AS ROW(\"a\" bigint))' must be an aggregate expression or appear in GROUP BY clause");
    }

    @Test
    public void testDuplicateComplexExpressions()
    {
        assertThat(assertions.query(
                "SELECT a + 1, a + 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY 1, 2"))
                .matches("VALUES (2, 2)");

        assertThat(assertions.query(
                "SELECT 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY a + 1, a + 1"))
                .matches("VALUES 1");

        assertThat(assertions.query(
                "SELECT 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY t.a + 1, a + 1"))
                .matches("VALUES 1");

        assertThat(assertions.query(
                "SELECT 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY A + 1, a + 1"))
                .matches("VALUES 1");

        assertThat(assertions.query(
                "SELECT 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY t.A + 1, a + 1"))
                .matches("VALUES 1");

        assertThat(assertions.query(
                "SELECT a + 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY t.A + 1, 1"))
                .matches("VALUES 2");
    }

    @Test
    public void testReferenceWithMixedStyle()
    {
        assertThat(assertions.query(
                "SELECT a + 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY A + 1"))
                .matches("VALUES 2");

        assertThat(assertions.query(
                "SELECT a + 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY t.a + 1"))
                .matches("VALUES 2");

        assertThat(assertions.query(
                "SELECT a + 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY t.A + 1"))
                .matches("VALUES 2");

        assertThat(assertions.query(
                "SELECT t.a + 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY a + 1"))
                .matches("VALUES 2");

        assertThat(assertions.query(
                "SELECT t.a + 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY A + 1"))
                .matches("VALUES 2");

        assertThat(assertions.query(
                "SELECT t.a + 1 " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY t.A + 1"))
                .matches("VALUES 2");
    }

    @Test
    public void testGroupByRepeatedOrdinals()
    {
        assertThat(assertions.query(
                "SELECT null GROUP BY 1, 1"))
                .matches("VALUES null");
    }

    @Test
    void testGroupByAuto()
    {
        assertThat(assertions.query(
                """
                SELECT *
                FROM (VALUES 1) t(a)
                GROUP BY AUTO
                """))
                .matches("VALUES 1");

        assertThat(assertions.query(
                """
                SELECT *
                FROM (VALUES 1, 2) t(a)
                GROUP BY AUTO
                """))
                .matches("VALUES 1, 2");

        assertThat(assertions.query(
                """
                SELECT sum(a)
                FROM (VALUES (1), (2)) t(a)
                GROUP BY AUTO
                """))
                .matches("VALUES BIGINT '3'");

        assertThat(assertions.query(
                """
                SELECT a, sum(b)
                FROM (VALUES (1, 10), (1, 20)) t(a, b)
                GROUP BY AUTO
                """))
                .matches("VALUES (1, BIGINT '30')");

        assertThat(assertions.query(
                """
                SELECT a AS new_a, sum(b) AS sum_b
                FROM (VALUES (1, 10), (1, 20)) t(a, b)
                GROUP BY AUTO
                """))
                .matches("VALUES (1, BIGINT '30')");

        assertThat(assertions.query(
                """
                SELECT a + 1, sum(b)
                FROM (VALUES (1, 10), (1, 20)) t(a, b)
                GROUP BY AUTO
                """))
                .matches("VALUES (2, BIGINT '30')");

        assertThat(assertions.query(
                """
                SELECT abs(a), sum(b)
                FROM (VALUES (-1, 10), (-1, 20)) t(a, b)
                GROUP BY AUTO
                """))
                .matches("VALUES (1, BIGINT '30')");

        assertThat(assertions.query(
                """
                SELECT sum(b), a
                FROM (VALUES (1, 10), (1, 20)) t(a, b)
                GROUP BY AUTO
                """))
                .matches("VALUES (BIGINT '30', 1)");

        assertThat(assertions.query(
                """
                SELECT sum(a)
                FROM (VALUES (1, 10), (1, 20)) t(a, b)
                GROUP BY AUTO
                """))
                .matches("VALUES (BIGINT '2')");

        // ALL AUTO
        assertThat(assertions.query(
                """
                SELECT sum(a)
                FROM (VALUES (1, 10), (1, 20)) t(a, b)
                GROUP BY ALL AUTO
                """))
                .matches("VALUES (BIGINT '2')");

        // DISTINCT AUTO
        assertThat(assertions.query(
                """
                SELECT sum(a)
                FROM (VALUES (1, 10), (1, 20)) t(a, b)
                GROUP BY DISTINCT AUTO
                """))
                .matches("VALUES (BIGINT '2')");

        // ROLLUP
        assertThat(assertions.query(
                """
                SELECT a, sum(b)
                FROM (VALUES (1, 10), (1, 20)) t(a, b)
                GROUP BY AUTO, ROLLUP(b)
                """))
                .matches("VALUES (1, BIGINT '10'), (1, BIGINT '20'), (1, BIGINT '30')");

        assertThat(assertions.query(
                """
                SELECT a, sum(b)
                FROM (VALUES (1, 10), (1, 20)) t(a, b)
                GROUP BY ALL AUTO, ROLLUP(b)
                """))
                .matches("VALUES (1, BIGINT '10'), (1, BIGINT '20'), (1, BIGINT '30')");

        assertThat(assertions.query(
                """
                SELECT a, sum(b)
                FROM (VALUES (1, 10), (1, 20)) t(a, b)
                GROUP BY DISTINCT AUTO, ROLLUP(a)
                """))
                .matches("VALUES (1, BIGINT '30')");

        assertThat(assertions.query(
                """
                SELECT a, b, c, sum(b)
                FROM (VALUES (1, 1, 1, 1), (1, 1, 1, 2), (2, 2, 2, 3)) t(a, b, c, d)
                GROUP BY AUTO, ROLLUP(a)
                """))
                .matches(
                        """
                        SELECT a, b, c, sum(b)
                        FROM (VALUES (1, 1, 1, 1), (1, 1, 1, 2), (2, 2, 2, 3)) t(a, b, c, d)
                        GROUP BY (a, b, c), ROLLUP(a)
                        """);

        // CUBE
        assertThat(assertions.query(
                """
                SELECT a, sum(b)
                FROM (VALUES (1, 10), (1, 20)) t(a, b)
                GROUP BY AUTO, CUBE(b)
                """))
                .matches("VALUES (1, BIGINT '10'), (1, BIGINT '20'), (1, BIGINT '30')");

        assertThat(assertions.query(
                """
                SELECT a, sum(b)
                FROM (VALUES (1, 10), (1, 20)) t(a, b)
                GROUP BY ALL AUTO, CUBE(b)
                """))
                .matches("VALUES (1, BIGINT '10'), (1, BIGINT '20'), (1, BIGINT '30')");

        assertThat(assertions.query(
                """
                SELECT a, sum(b)
                FROM (VALUES (1, 10), (1, 20)) t(a, b)
                GROUP BY DISTINCT AUTO, CUBE(a)
                """))
                .matches("VALUES (1, BIGINT '30')");

        assertThat(assertions.query(
                """
                SELECT a, b, c, sum(b)
                FROM (VALUES (1, 1, 1, 1), (1, 1, 1, 2), (2, 2, 2, 3)) t(a, b, c, d)
                GROUP BY AUTO, CUBE(a)
                """))
                .matches(
                        """
                        SELECT a, b, c, sum(b)
                        FROM (VALUES (1, 1, 1, 1), (1, 1, 1, 2), (2, 2, 2, 3)) t(a, b, c, d)
                        GROUP BY (a, b, c), CUBE(a)
                        """);

        // GROUPING SETS
        assertThat(assertions.query(
                """
                SELECT a, sum(b)
                FROM (VALUES (1, 10), (1, 20)) t(a, b)
                GROUP BY AUTO, GROUPING SETS((b))
                """))
                .matches("VALUES (1, BIGINT '10'), (1, BIGINT '20')");

        assertThat(assertions.query(
                """
                SELECT a, sum(b)
                FROM (VALUES (1, 10), (1, 20)) t(a, b)
                GROUP BY ALL AUTO, GROUPING SETS((b))
                """))
                .matches("VALUES (1, BIGINT '10'), (1, BIGINT '20')");

        assertThat(assertions.query(
                """
                SELECT a, sum(b)
                FROM (VALUES (1, 10), (1, 20)) t(a, b)
                GROUP BY DISTINCT AUTO, GROUPING SETS((a))
                """))
                .matches("VALUES (1, BIGINT '30')");

        assertThat(assertions.query(
                """
                SELECT a, b, c, sum(b)
                FROM (VALUES (1, 1, 1, 1), (1, 1, 1, 2), (2, 2, 2, 3)) t(a, b, c, d)
                GROUP BY AUTO, GROUPING SETS((a))
                """))
                .matches(
                        """
                        SELECT a, b, c, sum(b)
                        FROM (VALUES (1, 1, 1, 1), (1, 1, 1, 2), (2, 2, 2, 3)) t(a, b, c, d)
                        GROUP BY (a, b, c), GROUPING SETS((a))
                        """);
    }
}
