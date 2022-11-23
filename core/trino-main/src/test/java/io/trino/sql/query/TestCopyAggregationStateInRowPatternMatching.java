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

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCopyAggregationStateInRowPatternMatching
{
    // at each step of matching, the threads are forked because of the alternation.
    // at each fork, the aggregation state is copied
    // every thread is validated at the final step by the defining condition for label `X`
    private static final String QUERY = """
            SELECT m.id, m.classy
            FROM (VALUES (1), (2), (3)) t(id)
               MATCH_RECOGNIZE (
                 ORDER BY id
                 MEASURES CLASSIFIER() AS classy
                 ALL ROWS PER MATCH
                 PATTERN ((A | B)* X)
                 %s
              ) AS m
            """;

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
    public void testArrayAgg()
    {
        // test SingleArrayAggregationState.copy()
        assertThat(assertions.query(format(QUERY, "DEFINE X AS array_join(array_agg(CLASSIFIER()), '', '') = 'BAX' ")))
                .matches("""
                        VALUES
                             (1, VARCHAR 'B'),
                             (2, 'A'),
                             (3, 'X')
                        """);
    }

    @Test
    public void testMinByN()
    {
        // test MinMaxByNStateFactory.SingleMinMaxByNState.copy()
        assertThat(assertions.query(format(QUERY, "DEFINE X AS array_join(min_by(CLASSIFIER(), id, 3), '', '') = 'BAX' ")))
                .matches("""
                        VALUES
                             (1, VARCHAR 'B'),
                             (2, 'A'),
                             (3, 'X')
                        """);
    }

    @Test
    public void testMaxByN()
    {
        // test MinMaxByNStateFactory.SingleMinMaxByNState.copy()
        assertThat(assertions.query(format(QUERY, "DEFINE X AS array_join(max_by(CLASSIFIER(), id, 3), '', '') = 'XAB' ")))
                .matches("""
                        VALUES
                             (1, VARCHAR 'B'),
                             (2, 'A'),
                             (3, 'X')
                        """);
    }

    @Test
    public void testMinN()
    {
        // test MinMaxNStateFactory.SingleMinMaxNState.copy()
        assertThat(assertions.query(format(QUERY, "DEFINE X AS array_join(min(CLASSIFIER(), 3), '', '') = 'ABX' ")))
                .matches("""
                        VALUES
                             (1, VARCHAR 'A'),
                             (2, 'B'),
                             (3, 'X')
                        """);
    }

    @Test
    public void testMaxN()
    {
        // test MinMaxNStateFactory.SingleMinMaxNState.copy()
        assertThat(assertions.query(format(QUERY, "DEFINE X AS array_join(max(CLASSIFIER(), 3), '', '') = 'XBA' ")))
                .matches("""
                        VALUES
                             (1, VARCHAR 'A'),
                             (2, 'B'),
                             (3, 'X')
                        """);
    }

    @Test
    public void testMultimapAgg()
    {
        // test SingleMultimapAggregationState.copy()
        assertThat(assertions.query(format(QUERY, "DEFINE X AS array_join(element_at(multimap_agg(id, CLASSIFIER()), 1), '', '') = 'B' ")))
                .matches("""
                        VALUES
                             (1, VARCHAR 'B'),
                             (2, 'A'),
                             (3, 'X')
                        """);
    }

    @Test
    public void testMapAgg()
    {
        // test KeyValuePairsStateFactory.SingleState.copy()
        assertThat(assertions.query(format(QUERY, "DEFINE X AS element_at(map_agg(id, CLASSIFIER()), 1) = 'B' ")))
                .matches("""
                        VALUES
                             (1, VARCHAR 'B'),
                             (2, 'A'),
                             (3, 'X')
                        """);
    }

    @Test
    public void testMapUnion()
    {
        String query = """
                SELECT m.id, m.classy
                FROM (VALUES ('B'), ('C'), ('D')) t(id)
                   MATCH_RECOGNIZE (
                     ORDER BY id
                     MEASURES CLASSIFIER() AS classy
                     ALL ROWS PER MATCH
                     PATTERN ((A | B)* X)
                     %s
                  ) AS m
                """;

        // test KeyValuePairsStateFactory.SingleState.copy()
        assertThat(assertions.query(format(query, "DEFINE X AS element_at(map_union(MAP(ARRAY[id], ARRAY[id])), 'B') = FIRST(CLASSIFIER()) ")))
                .matches("""
                        VALUES
                             ('B', VARCHAR 'B'),
                             ('C', 'A'),
                             ('D', 'X')
                        """);
    }

    @Test
    public void testDecimalAvg()
    {
        // test LongDecimalWithOverflowAndLongStateFactory.SingleLongDecimalWithOverflowAndLongState.copy()
        assertThat(assertions.query(format(QUERY, "DEFINE X AS avg(CAST(B.id AS decimal(2, 1))) = 1e0 ")))
                .matches("""
                        VALUES
                             (1, VARCHAR 'B'),
                             (2, 'A'),
                             (3, 'X')
                        """);

        assertThat(assertions.query(format(QUERY, "DEFINE X AS avg(CAST(B.id AS decimal(30, 20))) = 1e0 ")))
                .matches("""
                        VALUES
                             (1, VARCHAR 'B'),
                             (2, 'A'),
                             (3, 'X')
                        """);
    }

    @Test
    public void testDecimalSum()
    {
        // test LongDecimalWithOverflowStateFactory.SingleLongDecimalWithOverflowState.copy()
        assertThat(assertions.query(format(QUERY, "DEFINE X AS sum(CAST(B.id AS decimal(2, 1))) = 1.0 ")))
                .matches("""
                        VALUES
                             (1, VARCHAR 'B'),
                             (2, 'A'),
                             (3, 'X')
                        """);

        assertThat(assertions.query(format(QUERY, "DEFINE X AS sum(CAST(B.id AS decimal(30, 20))) = 1.0 ")))
                .matches("""
                        VALUES
                             (1, VARCHAR 'B'),
                             (2, 'A'),
                             (3, 'X')
                        """);
    }
}
