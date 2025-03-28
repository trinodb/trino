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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestWindow
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    @Timeout(10)
    public void testManyFunctionsWithSameWindow()
    {
        assertThat(assertions.query(
                """
                SELECT
                          SUM(a) OVER w,
                          COUNT(a) OVER w,
                          MIN(a) OVER w,
                          MAX(a) OVER w,
                          SUM(b) OVER w,
                          COUNT(b) OVER w,
                          MIN(b) OVER w,
                          MAX(b) OVER w,
                          SUM(c) OVER w,
                          COUNT(c) OVER w,
                          MIN(c) OVER w,
                          MAX(c) OVER w,
                          SUM(d) OVER w,
                          COUNT(d) OVER w,
                          MIN(d) OVER w,
                          MAX(d) OVER w,
                          SUM(e) OVER w,
                          COUNT(e) OVER w,
                          MIN(e) OVER w,
                          MAX(e) OVER w,
                          SUM(f) OVER w,
                          COUNT(f) OVER w,
                          MIN(f) OVER w,
                          MAX(f) OVER w
                        FROM (
                            VALUES (1, 1, 1, 1, 1, 1, 1)
                        ) AS t(k, a, b, c, d, e, f)
                        WINDOW w AS (ORDER BY k ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
                """))
                .matches("VALUES (BIGINT '1', BIGINT '1', 1, 1, BIGINT '1', BIGINT '1', 1, 1, BIGINT '1', BIGINT '1', 1, 1, BIGINT '1', BIGINT '1', 1, 1, BIGINT '1', BIGINT '1', 1, 1, BIGINT '1', BIGINT '1', 1, 1)");
    }

    @Test
    public void testWindowWithOrderBy()
    {
        // window and aggregate ordering on different columns
        assertThat(assertions.query(
                """
                SELECT a, ARRAY_AGG(c ORDER BY c) OVER w
                FROM (
                    VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 4, 7), (1, 5, 5),
                           (2, 1, 1), (2, 2, 3), (2, 3, 2), (2, 4, 1)) AS t(a, b, c)
                WINDOW w AS (PARTITION BY a ORDER BY b)
                """))
                .matches(
                        """
                        VALUES
                            (1, ARRAY[1]),
                            (1, ARRAY[1, 2]),
                            (1, ARRAY[1, 2, 3]),
                            (1, ARRAY[1, 2, 3, 4, 7]),
                            (1, ARRAY[1, 2, 3, 4, 7]),
                            (1, ARRAY[1, 2, 3, 4, 5, 7]),
                            (2, ARRAY[1]),
                            (2, ARRAY[1, 3]),
                            (2, ARRAY[1, 2, 3]),
                            (2, ARRAY[1, 1, 2, 3])
                        """);

        // window and aggregate ordering on different columns (different ordering)
        assertThat(assertions.query(
                """
                SELECT a, ARRAY_AGG(c ORDER BY c DESC) OVER w
                FROM (
                    VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 4, 7), (1, 5, 5),
                           (2, 1, 1), (2, 2, 3), (2, 3, 2), (2, 4, 1)) AS t(a, b, c)
                WINDOW w AS (PARTITION BY a ORDER BY b)
                """))
                .matches(
                        """
                        VALUES
                            (1, ARRAY[1]),
                            (1, ARRAY[2, 1]),
                            (1, ARRAY[3, 2, 1]),
                            (1, ARRAY[7, 4, 3, 2, 1]),
                            (1, ARRAY[7, 4, 3, 2, 1]),
                            (1, ARRAY[7, 5, 4, 3, 2, 1]),
                            (2, ARRAY[1]),
                            (2, ARRAY[3, 1]),
                            (2, ARRAY[3, 2, 1]),
                            (2, ARRAY[3, 2, 1, 1])
                        """);

        // aggregate ordering on column not in output
        assertThat(assertions.query(
                """
                SELECT a, ARRAY_AGG(c ORDER BY d) OVER w
                FROM (
                    VALUES (1, 1, 1, 5), (1, 2, 2, 4), (1, 3, 3, 1), (1, 4, 4, 2), (1, 4, 7, 6), (1, 5, 5, 3),
                           (2, 1, 1, 4), (2, 2, 3, 3), (2, 3, 2, 2), (2, 4, 1, 1)) AS t(a, b, c, d)
                WINDOW w AS (PARTITION BY a ORDER BY b)
                """))
                .matches(
                        """
                        VALUES
                            (1, ARRAY[1]),
                            (1, ARRAY[2, 1]),
                            (1, ARRAY[3, 2, 1]),
                            (1, ARRAY[3, 4, 2, 1, 7]),
                            (1, ARRAY[3, 4, 2, 1, 7]),
                            (1, ARRAY[3, 4, 5, 2, 1, 7]),
                            (2, ARRAY[1]),
                            (2, ARRAY[3, 1]),
                            (2, ARRAY[2, 3, 1]),
                            (2, ARRAY[1, 2, 3, 1])
                        """);

        // window and aggregate ordering on the same column
        assertThat(assertions.query(
                """
                SELECT a, ARRAY_AGG(c ORDER BY b) OVER w
                FROM (
                    VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 5, 5),
                           (2, 1, 1), (2, 2, 3), (2, 3, 2), (2, 4, 1)) AS t(a, b, c)
                WINDOW w AS (PARTITION BY a ORDER BY b)
                """))
                .matches(
                        """
                        VALUES
                            (1, ARRAY[1]),
                            (1, ARRAY[1, 2]),
                            (1, ARRAY[1, 2, 3]),
                            (1, ARRAY[1, 2, 3, 4]),
                            (1, ARRAY[1, 2, 3, 4, 5]),
                            (2, ARRAY[1]),
                            (2, ARRAY[1, 3]),
                            (2, ARRAY[1, 3, 2]),
                            (2, ARRAY[1, 3, 2, 1])
                        """);

        // window and aggregate ordering on same column (different sort order)
        assertThat(assertions.query(
                """
                SELECT a, ARRAY_AGG(c ORDER BY b DESC) OVER w
                FROM (
                    VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 5, 5),
                           (2, 1, 1), (2, 2, 3), (2, 3, 2), (2, 4, 1)) AS t(a, b, c)
                WINDOW w AS (PARTITION BY a ORDER BY b)
                """))
                .matches(
                        """
                        VALUES
                            (1, ARRAY[1]),
                            (1, ARRAY[2, 1]),
                            (1, ARRAY[3, 2, 1]),
                            (1, ARRAY[4, 3, 2, 1]),
                            (1, ARRAY[5, 4, 3, 2, 1]),
                            (2, ARRAY[1]),
                            (2, ARRAY[3, 1]),
                            (2, ARRAY[2, 3, 1]),
                            (2, ARRAY[1, 2, 3, 1])
                        """);

        // aggregate ordering on two columns (tiebreaker ASC)
        assertThat(assertions.query(
                """
                SELECT a, ARRAY_AGG(c ORDER BY d, c) OVER w
                FROM (
                    VALUES (1, 1, 1, 5), (1, 2, 2, 4), (1, 3, 3, 4), (1, 4, 4, 5), (1, 4, 7, 1), (1, 5, 5, 2)) AS t(a, b, c, d)
                WINDOW w AS (PARTITION BY a ORDER BY b)
                """))
                .matches(
                        """
                        VALUES
                            (1, ARRAY[1]),
                            (1, ARRAY[2, 1]),
                            (1, ARRAY[2, 3, 1]),
                            (1, ARRAY[7, 2, 3, 1, 4]),
                            (1, ARRAY[7, 2, 3, 1, 4]),
                            (1, ARRAY[7, 5, 2, 3, 1, 4])
                        """);

        // aggregate ordering on two columns (tiebreaker DESC)
        assertThat(assertions.query(
                """
                SELECT a, ARRAY_AGG(c ORDER BY d, c DESC) OVER w
                FROM (
                    VALUES (1, 1, 1, 5), (1, 2, 2, 4), (1, 3, 3, 4), (1, 4, 4, 5), (1, 4, 7, 1), (1, 5, 5, 2)) AS t(a, b, c, d)
                WINDOW w AS (PARTITION BY a ORDER BY b)
                """))
                .matches(
                        """
                        VALUES
                            (1, ARRAY[1]),
                            (1, ARRAY[2, 1]),
                            (1, ARRAY[3, 2, 1]),
                            (1, ARRAY[7, 3, 2, 4, 1]),
                            (1, ARRAY[7, 3, 2, 4, 1]),
                            (1, ARRAY[7, 5, 3, 2, 4, 1])
                        """);

        // multiple aggregate functions
        assertThat(assertions.query(
                """
                SELECT a,
                       ARRAY_AGG(c ORDER BY c) OVER w,
                       ARRAY_AGG(c ORDER BY c DESC) OVER w,
                       ARRAY_AGG(c ORDER BY d) OVER w
                FROM (
                    VALUES (1, 1, 1, 5), (1, 2, 2, 4), (1, 3, 3, 1), (1, 4, 4, 2), (1, 4, 7, 6), (1, 5, 5, 3),
                           (2, 1, 1, 4), (2, 2, 3, 3), (2, 3, 2, 2), (2, 4, 1, 1)) AS t(a, b, c, d)
                WINDOW w AS (PARTITION BY a ORDER BY b)
                """))
                .matches(
                        """
                        VALUES
                            (1, ARRAY[1], ARRAY[1], ARRAY[1]),
                            (1, ARRAY[1, 2], ARRAY[2, 1], ARRAY[2, 1]),
                            (1, ARRAY[1, 2, 3], ARRAY[3, 2, 1], ARRAY[3, 2, 1]),
                            (1, ARRAY[1, 2, 3, 4, 7], ARRAY[7, 4, 3, 2, 1], ARRAY[3, 4, 2, 1, 7]),
                            (1, ARRAY[1, 2, 3, 4, 7], ARRAY[7, 4, 3, 2, 1], ARRAY[3, 4, 2, 1, 7]),
                            (1, ARRAY[1, 2, 3, 4, 5, 7], ARRAY[7, 5, 4, 3, 2, 1], ARRAY[3, 4, 5, 2, 1, 7]),
                            (2, ARRAY[1], ARRAY[1], ARRAY[1]),
                            (2, ARRAY[1, 3], ARRAY[3, 1], ARRAY[3, 1]),
                            (2, ARRAY[1, 2, 3], ARRAY[3, 2, 1], ARRAY[2, 3, 1]),
                            (2, ARRAY[1, 1, 2, 3], ARRAY[3, 2, 1, 1], ARRAY[1, 2, 3, 1])
                        """);

        // test empty frames
        assertThat(assertions.query(
                """
                SELECT a, ARRAY_AGG(c ORDER BY b DESC) OVER w
                FROM (
                    VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 5, 5)) t(a, b, c)
                WINDOW w AS (PARTITION BY a ORDER BY b GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)
                """))
                .matches(
                        """
                        VALUES
                            (1, ARRAY[3, 2]),
                            (1, ARRAY[4, 3]),
                            (1, ARRAY[5, 4]),
                            (1, ARRAY[5]),
                            (1, NULL)
                        """);
    }

    @Test
    public void testWindowWithDistinct()
    {
        // just distinct
        assertThat(assertions.query(
                """
                SELECT a, ARRAY_AGG(DISTINCT c) OVER w
                FROM (
                    VALUES (1, 1, 1), (1, 2, 2), (1, 3, 1), (1, 4, 4), (1, 4, 2), (1, 5, 5),
                           (2, 1, 1), (2, 2, 3), (2, 3, 2), (2, 4, 3)) AS t(a, b, c)
                WINDOW w AS (PARTITION BY a ORDER BY b)
                """))
                .matches(
                        """
                        VALUES
                            (1, ARRAY[1]),
                            (1, ARRAY[1, 2]),
                            (1, ARRAY[1, 2]),
                            (1, ARRAY[1, 2, 4]),
                            (1, ARRAY[1, 2, 4]),
                            (1, ARRAY[1, 2, 4, 5]),
                            (2, ARRAY[1]),
                            (2, ARRAY[1, 3]),
                            (2, ARRAY[1, 3, 2]),
                            (2, ARRAY[1, 3, 2])
                        """);

        assertThat(assertions.query(
                """
                SELECT *, COUNT(DISTINCT b) OVER w
                FROM (VALUES ('x', true), ('x', false)) AS t(a, b)
                WINDOW w AS (PARTITION BY a)
                """))
                .matches(
                        """
                        VALUES
                            ('x', true, BIGINT '2'),
                            ('x', false, BIGINT '2')
                        """);

        // distinct with order by
        assertThat(assertions.query(
                """
                SELECT a, ARRAY_AGG(DISTINCT c ORDER BY c DESC) OVER w
                FROM (
                    VALUES (1, 1, 1), (1, 2, 2), (1, 3, 1), (1, 4, 4), (1, 4, 2), (1, 5, 5),
                           (2, 1, 1), (2, 2, 3), (2, 3, 2), (2, 4, 3)) AS t(a, b, c)
                WINDOW w AS (PARTITION BY a ORDER BY b)
                """))
                .matches(
                        """
                        VALUES
                            (1, ARRAY[1]),
                            (1, ARRAY[2, 1]),
                            (1, ARRAY[2, 1]),
                            (1, ARRAY[4, 2, 1]),
                            (1, ARRAY[4, 2, 1]),
                            (1, ARRAY[5, 4, 2, 1]),
                            (2, ARRAY[1]),
                            (2, ARRAY[3, 1]),
                            (2, ARRAY[3, 2, 1]),
                            (2, ARRAY[3, 2, 1])
                        """);
    }
}
