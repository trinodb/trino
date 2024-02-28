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
public class TestExpressionRewriteInRowPatternMatching
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testDesugarArrayConstructor()
    {
        assertThat(assertions.query("SELECT m.id, m.classy, m.array, m.sum " +
                "          FROM (VALUES (1), (2), (3)) t(id) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                           CLASSIFIER() AS classy, " +
                "                           ARRAY['foo', CLASSIFIER()] AS array, " + // array constructor in top-level expression
                "                           sum(array_max(ARRAY[MATCH_NUMBER()])) AS sum " + // array constructor in aggregation argument
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN ((A | B)* X) " +
                "                   DEFINE X AS array_agg(CLASSIFIER()) = ARRAY['B', 'A', 'X'] AND " + // array constructor in top-level expression
                "                               sum(array_max(ARRAY[MATCH_NUMBER()])) = 3 " + // array constructor in aggregation argument
                "                ) AS m"))
                .matches("VALUES " +
                        "     (1, VARCHAR 'B', ARRAY[VARCHAR 'foo', 'B'], BIGINT '1'), " +
                        "     (2, 'A',         ARRAY['foo', 'A'],         2), " +
                        "     (3, 'X',         ARRAY['foo', 'X'],         3)");
    }

    @Test
    public void testDesugarLike()
    {
        assertThat(assertions.query("SELECT m.id, m.classy, m.measure_1, m.measure_2 " +
                "          FROM (VALUES (1), (2), (3)) t(id) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                           CLASSIFIER() AS classy, " +
                "                           CLASSIFIER() LIKE '%X' AS measure_1, " + // LIKE predicate in top-level expression
                "                           bool_or(CLASSIFIER() LIKE '%B') AS measure_2 " + // LIKE predicate in aggregation argument
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN ((A | B)* X) " +
                "                   DEFINE X AS PREV(CLASSIFIER()) LIKE '%B' AND " + // LIKE predicate in top-level expression
                "                               bool_or(CLASSIFIER() LIKE '%A') " + // LIKE predicate in aggregation argument
                "                ) AS m"))
                .matches("VALUES " +
                        "     (1, VARCHAR 'A', false, false), " +
                        "     (2, 'B',         false, true), " +
                        "     (3, 'X',         true , true)");
    }

    @Test
    public void testSimplifyExpressions()
    {
        assertThat(assertions.query("SELECT m.id, m.classy, m.measure_1, m.measure_2 " +
                "          FROM (VALUES (1), (2), (3)) t(id) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                           CLASSIFIER() AS classy, " +
                "                           true OR MATCH_NUMBER() / 0 > 0 AS measure_1, " + // potential division by zero in top-level expression
                "                           bool_and(MATCH_NUMBER() / 0 > 0 AND false) AS measure_2 " + // potential division by zero in aggregation argument
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN ((A | B)* X) " +
                "                   DEFINE X AS IF(false, 0 / 0, MATCH_NUMBER()) = 1 OR " + // potential division by zero in top-level expression
                "                                bool_and(MATCH_NUMBER() / 0 > 0 AND false) " + // potential division by zero in aggregation argument
                "                ) AS m"))
                .matches("VALUES " +
                        "     (1, VARCHAR 'A', true, false), " +
                        "     (2, 'A',         true, false), " +
                        "     (3, 'X',         true, false)");
    }
}
