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

import io.trino.Session;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.SystemSessionProperties.MAX_RECURSION_DEPTH;
import static io.trino.SystemSessionProperties.getMaxRecursionDepth;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestRecursiveCte
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testSimpleRecursion()
    {
        assertThat(assertions.query("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT n + 2 FROM t WHERE n < 6" +
                "          )" +
                "          SELECT * from t"))
                .matches("VALUES (1), (3), (5), (7)");

        assertThat(assertions.query("WITH RECURSIVE t(n, m) AS (" +
                "          SELECT * FROM (VALUES(1, 2), (4, 100))" +
                "          UNION ALL" +
                "          SELECT n + 1, m - 1 FROM t WHERE n < 5" +
                "          )" +
                "          SELECT * from t"))
                .matches("VALUES (1, 2), (4, 100), (2, 1), (5, 99), (3, 0), (4, -1), (5, -2)");

        assertThat(assertions.query("WITH RECURSIVE t(n, m, o) AS (" +
                "          SELECT * FROM (VALUES(1, 2, ROW(3, 4)), (5, 6, ROW(7, 8)))" +
                "          UNION ALL" +
                "          SELECT t.o.*, ROW(10, 10) FROM t WHERE m < 3" +
                "          )" +
                "          SELECT * from t"))
                .matches("VALUES (1, 2, ROW(3, 4)), (5, 6, ROW(7, 8)), (3, 4, ROW(10, 10))");
    }

    @Test
    public void testUnionDistinct()
    {
        assertThat(assertions.query("WITH RECURSIVE t(n) AS (" +
                "          SELECT * FROM (VALUES(1), (1), (10))" +
                "          UNION" +
                "          SELECT n + 2 FROM t WHERE n < 4" +
                "          )" +
                "          SELECT * from t"))
                .matches("VALUES (1), (10), (3), (5)");

        assertThat(assertions.query("WITH RECURSIVE t(n, m) AS (" +
                "          SELECT * FROM (VALUES(1, 2), (2, 3))" +
                "          UNION" +
                "          SELECT n + 1, m + 1 FROM t WHERE n < 3" +
                "          )" +
                "          SELECT * from t"))
                .matches("VALUES (1, 2), (2, 3), (3, 4)");
    }

    @Test
    public void testNestedWith()
    {
        // recursive reference visible in subquery containing WITH
        assertThat(assertions.query("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT * FROM (WITH t2(m) AS (SELECT 4) SELECT m FROM t2 UNION SELECT n + 1 FROM t) t(n) WHERE n < 4" +
                "          )" +
                "          SELECT * from t"))
                .matches("VALUES (1), (2), (3)");

        // recursive reference shadowed by WITH in subquery. The query is effectively not recursive
        assertThat(assertions.query("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT * FROM (WITH t(n) AS (SELECT 5) SELECT n + 1 FROM t)" +
                "          )" +
                "          SELECT * from t"))
                .matches("VALUES (1), (6)");

        // multiple nesting
        assertThat(assertions.query("WITH t(n) AS (" +
                "          WITH t2(m) AS (" +
                "               WITH RECURSIVE t3(p) AS (" +
                "                   SELECT 1" +
                "                   UNION ALL" +
                "                   SELECT * FROM (WITH t4(q) AS (SELECT 4) SELECT p + 1 FROM t3 WHERE p < 3)" +
                "                   )" +
                "               SELECT * from t3" +
                "               )" +
                "           SELECT * FROM t2" +
                "           )" +
                "       SELECT * FROM t"))
                .matches("VALUES (1), (2), (3)");
    }

    @Test
    public void testMultipleWithListEntries()
    {
        // second and third WITH-queries are recursive
        assertThat(assertions.query("WITH RECURSIVE a(x) AS (SELECT 1)," +
                "          b(y) AS (" +
                "               SELECT x FROM a" +
                "               UNION ALL" +
                "               SELECT y + 1 FROM b WHERE y < 2" +
                "               )," +
                "          c(z) AS (" +
                "               SELECT y FROM b" +
                "               UNION ALL" +
                "               SELECT z * 4 FROM c WHERE z < 4" +
                "               )" +
                "          SELECT * FROM a, b, c"))
                .matches("VALUES " +
                        "(1, 1, 1), " +
                        "(1, 1, 2), " +
                        "(1, 1, 4), " +
                        "(1, 1, 8), " +
                        "(1, 2, 1), " +
                        "(1, 2, 2), " +
                        "(1, 2, 4), " +
                        "(1, 2, 8)");
    }

    @Test
    public void testVarchar()
    {
        assertThat(assertions.query("WITH RECURSIVE t(n) AS (" +
                "          SELECT CAST(n AS varchar) FROM (VALUES('a'), ('b')) AS T(n)" +
                "          UNION ALL" +
                "          SELECT n || 'x' FROM t WHERE n < 'axx'" +
                "          )" +
                "          SELECT * from t"))
                .matches("VALUES (varchar 'a'), (varchar 'b'), (varchar 'ax'), (varchar 'axx')");
    }

    @Test
    public void testTypeCoercion()
    {
        // integer result of step relation coerced to bigint
        assertThat(assertions.query("WITH RECURSIVE t(n) AS (" +
                "          SELECT BIGINT '1'" +
                "          UNION ALL" +
                "          SELECT CAST(n + 1 AS integer) FROM t WHERE n < 3" +
                "          )" +
                "          SELECT * from t"))
                .matches("VALUES (BIGINT '1'), (BIGINT '2'), (BIGINT '3')");

        // result of step relation coerced from decimal(10,0) to decimal(20,10)
        assertThat(assertions.query("WITH RECURSIVE t(n) AS (" +
                "          SELECT CAST(1 AS decimal(20,10))" +
                "          UNION ALL" +
                "          SELECT CAST(n + 1 AS decimal(10,0)) FROM t WHERE n < 2" +
                "          )" +
                "          SELECT * from t"))
                .matches("VALUES (CAST(1 AS decimal(20,10))), (CAST(2 AS decimal(20,10)))");

        // result of step relation coerced from varchar(5) to varchar
        assertThat(assertions.query("WITH RECURSIVE t(n) AS (" +
                "          SELECT VARCHAR 'ABCDE'" +
                "          UNION ALL" +
                "          SELECT CAST(substr(n, 2) AS varchar(5)) FROM t WHERE n < 'E'" +
                "          )" +
                "          SELECT * from t"))
                .matches("VALUES (VARCHAR 'ABCDE'), (VARCHAR 'BCDE'), (VARCHAR 'CDE'), (VARCHAR 'DE'), (VARCHAR 'E')");

        //multiple coercions
        assertThat(assertions.query("WITH RECURSIVE t(n, m) AS (" +
                "          SELECT BIGINT '1', INTEGER '2'" +
                "          UNION ALL" +
                "          SELECT CAST(n + 1 AS tinyint), CAST(m + 2 AS smallint) FROM t WHERE n < 3" +
                "          )" +
                "          SELECT * from t"))
                .matches("VALUES " +
                        "(BIGINT '1', INTEGER '2'), " +
                        "(BIGINT '2', INTEGER '4'), " +
                        "(BIGINT '3', INTEGER '6')");
    }

    @Test
    public void testJoin()
    {
        assertThat(assertions.query("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT b + 1 FROM ((SELECT 5) JOIN t ON true) t(a, b)  WHERE b < 3" +
                "          )" +
                "          SELECT * from t"))
                .matches("VALUES (1), (2), (3)");

        assertThat(assertions.query("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT n + 2 FROM (SELECT 10) u RIGHT JOIN t ON true WHERE n < 6" +
                "          )" +
                "          SELECT * FROM t"))
                .matches("VALUES (1), (3), (5), (7)");

        assertThat(assertions.query("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT n + 2 FROM t LEFT JOIN (SELECT 10) u ON true WHERE n < 6" +
                "          )" +
                "          SELECT * FROM t"))
                .matches("VALUES (1), (3), (5), (7)");
    }

    @Test
    public void testCorrelatedSubquery()
    {
        assertThat(assertions.query("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT b + 1 FROM ((SELECT 5) t1(m) JOIN LATERAL (SELECT * FROM t WHERE n * 3 < m) ON true) t(a, b)  WHERE b < 3" +
                "          )" +
                "          SELECT * from t"))
                .matches("VALUES (1), (2)");

        assertThat(assertions.query("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT * FROM (SELECT * FROM (VALUES 5) t1(m) WHERE 1 IN (SELECT * FROM t WHERE n * 3 < m))" +
                "          )" +
                "          SELECT * from t"))
                .matches("VALUES (1), (5)");
    }

    @Test
    public void testSetOperation()
    {
        assertThat(assertions.query("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          (SELECT n + 2 FROM ((TABLE t) INTERSECT DISTINCT (SELECT 1)) u(n))" +
                "          )" +
                "          SELECT * FROM t"))
                .matches("VALUES (1), (3)");

        assertThat(assertions.query("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          (SELECT n + 2 FROM ((TABLE t) EXCEPT DISTINCT (SELECT 10)) u(n) WHERE n < 3)" +
                "          )" +
                "          SELECT * FROM t"))
                .matches("VALUES (1), (3)");
    }

    @Test
    public void testRecursionDepthLimitExceeded()
    {
        assertThatThrownBy(() -> assertions.query("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT * FROM t" +
                "          )" +
                "          SELECT * FROM t"))
                .hasMessage("Recursion depth limit exceeded (%s). Use 'max_recursion_depth' session property to modify the limit.", getMaxRecursionDepth(assertions.getDefaultSession()));
    }

    @Test
    public void testDuplicateOutputsInAnchorAndStepRelation()
    {
        // This example tests recursive query with symbol ambiguity on different stages of recursion planning:
        // - the base relation outputs the same symbol (`a`) twice, once as `a`, and once as `root_a`
        // - the step relation in the first recursion step also outputs the same symbol twice:
        // `T.orig_a` is the same as `CTE.root_a`, and the planner uses the same output symbol for both.
        // In each case, such ambiguity is resolved by adding a disambiguating projection,
        // so that the output consists of distinct symbols.
        // This is necessary to successfully replace the part of the plan with another plan
        // in the next recursion step. The replacement must fit in the output layout of the previous
        // recursion step, without a constraint that certain outputs should be equal.
        assertThat(assertions.query(
                Session.builder(assertions.getDefaultSession())
                        .setSystemProperty(MAX_RECURSION_DEPTH, "4")
                        .build(),
                "WITH RECURSIVE " +
                        "        T(a, orig_a) AS (VALUES (1, 0), (2, 0), (3, 1), (4, 1), (5, 2), (6, 3), (7, 5)), " +
                        "        CTE(a, orig_a, base_id, root_a) AS( " +
                        "                                           SELECT a, orig_a, 'base_entry', a " +
                        "                                               FROM T " +
                        "                                               WHERE orig_a = 0 " +
                        "                                           UNION ALL " +
                        "                                           SELECT T.a, T.orig_a, 'derived', CTE.root_a " +
                        "                                               FROM T " +
                        "                                               INNER JOIN " +
                        "                                               CTE " +
                        "                                               ON CTE.a = T.orig_a " +
                        "                                           ) " +
                        "SELECT * FROM CTE"))
                .matches("VALUES (1, 0, 'base_entry', 1), " +
                        "        (2, 0, 'base_entry', 2), " +
                        "        (3, 1, 'derived',    1), " +
                        "        (4, 1, 'derived',    1), " +
                        "        (5, 2, 'derived',    2), " +
                        "        (6, 3, 'derived',    1), " +
                        "        (7, 5, 'derived',    2)");
    }
}
