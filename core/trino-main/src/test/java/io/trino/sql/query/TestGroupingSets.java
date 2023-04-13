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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestGroupingSets
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
    public void testPredicateOverGroupingKeysWithEmptyGroupingSet()
    {
        assertThat(assertions.query(
                "WITH t AS (" +
                        "    SELECT a" +
                        "    FROM (" +
                        "        VALUES 1, 2" +
                        "    ) AS u(a)" +
                        "    GROUP BY GROUPING SETS ((), (a))" +
                        ")" +
                        "SELECT * " +
                        "FROM t " +
                        "WHERE a IS NOT NULL"))
                .matches("VALUES 1, 2");
    }

    @Test
    public void testDistinctWithMixedReferences()
    {
        assertThat(assertions.query("" +
                "SELECT a " +
                "FROM (VALUES 1) t(a) " +
                "GROUP BY DISTINCT ROLLUP(a, t.a)"))
                .matches("VALUES (1), (NULL)");

        assertThat(assertions.query("" +
                "SELECT a " +
                "FROM (VALUES 1) t(a) " +
                "GROUP BY DISTINCT GROUPING SETS ((a), (t.a))"))
                .matches("VALUES 1");

        assertThat(assertions.query("" +
                "SELECT a " +
                "FROM (VALUES 1) t(a) " +
                "GROUP BY DISTINCT a, GROUPING SETS ((), (t.a))"))
                .matches("VALUES 1");
    }

    @Test
    public void testRollupAggregationWithOrderedLimit()
    {
        assertThat(assertions.query("" +
                "SELECT a " +
                "FROM (VALUES 3, 2, 1) t(a) " +
                "GROUP BY ROLLUP (a) " +
                "ORDER BY a LIMIT 2"))
                .matches("VALUES 1, 2");
    }

    @Test
    public void testComplexCube()
    {
        assertThat(assertions.query("""
                SELECT a, b, c, count(*)
                FROM (VALUES (1, 1, 1), (1, 2, 2), (1, 2, 2)) t(a, b, c)
                GROUP BY CUBE (a, (b, c))
                """))
                .matches("""
                        VALUES
                            (   1,    1,    1, BIGINT '1'),
                            (   1,    2,    2, 2),
                            (   1, NULL, NULL, 3),
                            (NULL, NULL, NULL, 3),
                            (NULL,    1,    1, 1),
                            (NULL,    2,    2, 2)
                        """);
    }

    @Test
    public void testComplexRollup()
    {
        assertThat(assertions.query("""
                SELECT a, b, c, count(*)
                FROM (VALUES (1, 1, 1), (1, 2, 2), (1, 2, 2)) t(a, b, c)
                GROUP BY ROLLUP (a, (b, c))
                """))
                .matches("""
                         VALUES
                         (   1,    1,    1, BIGINT '1'),
                         (NULL, NULL, NULL, 3),
                         (   1, NULL, NULL, 3),
                         (   1,    2,    2, 2)
                        """);
    }
}
