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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestRowPatternMatchingInWindow
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
    public void testSimpleQuery()
    {
        assertThat(assertions.query("SELECT id, val OVER w, label OVER w " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 80), " +
                "                   (5, 90), " +
                "                   (6, 50), " +
                "                   (7, 40), " +
                "                   (8, 60) " +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            RUNNING LAST(value) AS val, " +
                "                            CLASSIFIER() AS label " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (A B+ C+) " +
                "                   DEFINE /* A defaults to True, matches any row */ " +
                "                          B AS B.value < PREV (B.value), " +
                "                          C AS C.value > PREV (C.value) " +
                "                )"))
                .matches("VALUES " +
                        "     (1, 90, CAST('C' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null), " +
                        "     (5, null, null), " +
                        "     (6, 60, 'C'), " +
                        "     (7, null, null), " +
                        "     (8, null, null) ");
    }

    @Test
    public void testRowPattern()
    {
        String query = "SELECT id, val OVER w, label OVER w " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 70) " +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            RUNNING LAST(value) AS val, " +
                "                            classifier() AS label " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   %s " + // PATTERN and DEFINE
                "                )";

        // empty pattern (need to declare at least one pattern variable. It is in non-preferred branch of pattern alternation and will not be matched)
        // matched empty pattern at every row.
        assertThat(assertions.query(format(query, "PATTERN (() | A) " +
                "                  DEFINE A AS true ")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // pattern concatenation
        // row 1 is unmatched. matched `A B C` starting from row 2. rows 3, 4 are skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B C) " +
                "                  DEFINE " +
                "                          B AS B.value < PREV (B.value), " +
                "                          C AS C.value = PREV (C.value) ")))
                .matches("VALUES " +
                        "     (1, null, CAST(null AS varchar)), " +
                        "     (2, 70, 'C'), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // pattern alternation: when multiple alternatives match, the first in order of declaration is preferred
        // because frame always starts from the current row, we cannot access the preceding value, so B and C never match.
        assertThat(assertions.query(format(query, "PATTERN (B | C | A) " +
                "                  DEFINE " +
                "                          B AS B.value < PREV (B.value), " +
                "                          C AS C.value <= PREV (C.value) ")))
                .matches("VALUES " +
                        "     (1, 90, CAST('A' AS varchar)), " +
                        "     (2, 80, 'A'), " +
                        "     (3, 70, 'A'), " +
                        "     (4, 70, 'A') ");

        // pattern permutation: when multiple permutations match, the first in lexicographical order is preferred
        // matched `A B C` starting from row 1. rows 2, 3 are skipped. row 4 is unmatched.
        assertThat(assertions.query(format(query, "PATTERN (PERMUTE(B, C, A)) " +
                "                  DEFINE " +
                "                          B AS B.value < PREV (B.value), " +
                "                          C AS C.value < PREV (C.value) ")))
                .matches("VALUES " +
                        "     (1, 70, CAST('C' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // grouped pattern
        // row 1 is unmatched. matched `A B C` starting from row 2. rows 3, 4 are skipped.
        assertThat(assertions.query(format(query, "PATTERN (((A) (B (C)))) " +
                "                  DEFINE " +
                "                          B AS B.value < PREV (B.value), " +
                "                          C AS C.value = PREV (C.value) ")))
                .matches("VALUES " +
                        "     (1, null, CAST(null AS varchar)), " +
                        "     (2, 70, 'C'), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");
    }

    @Test
    public void testPatternQuantifiers()
    {
        String query = "SELECT id, val OVER w, label OVER w " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 70) " +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            RUNNING LAST(value) AS val, " +
                "                            classifier() AS label " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   %s " + // PATTERN
                "                  DEFINE B AS B.value <= PREV (B.value) " +
                "                )";

        // matched `A B B B` starting from row 1. rows 2, 3, 4 are skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B*) ")))
                .matches("VALUES " +
                        "     (1, 70, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // matched empty pattern at every row.
        assertThat(assertions.query(format(query, "PATTERN (B*?) ")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // matched `A B B B` starting from row 1. rows 2, 3, 4 are skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B+) ")))
                .matches("VALUES " +
                        "     (1, 70, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // matched `A B` starting from row 1. row 2 is skipped. matched `A B` starting from row 3. row 4 is skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B+?) ")))
                .matches("VALUES " +
                        "     (1, 80, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, 70, 'B'), " +
                        "     (4, null, null) ");

        // matched `A B` starting from row 1. row 2 is skipped. matched `A B` starting from row 3. row 4 is skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B?) ")))
                .matches("VALUES " +
                        "     (1, 80, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, 70, 'B'), " +
                        "     (4, null, null) ");

        // matched `A` at every row.
        assertThat(assertions.query(format(query, "PATTERN (A B??) ")))
                .matches("VALUES " +
                        "     (1, 90, CAST('A' AS varchar)), " +
                        "     (2, 80, 'A'), " +
                        "     (3, 70, 'A'), " +
                        "     (4, 70, 'A') ");

        // matched `A B B B` starting from row 1. rows 2, 3, 4 are skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B{,}) ")))
                .matches("VALUES " +
                        "     (1, 70, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // matched `A` at every row.
        assertThat(assertions.query(format(query, "PATTERN (A B{,}?) ")))
                .matches("VALUES " +
                        "     (1, 90, CAST('A' AS varchar)), " +
                        "     (2, 80, 'A'), " +
                        "     (3, 70, 'A'), " +
                        "     (4, 70, 'A') ");

        // matched `A B B B` starting from row 1. rows 2, 3, 4 are skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B{1,}) ")))
                .matches("VALUES " +
                        "     (1, 70, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // matched `A B` starting from row 1. row 2 is skipped. matched `A B` starting from row 3. row 4 is skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B{1,}?) ")))
                .matches("VALUES " +
                        "     (1, 80, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, 70, 'B'), " +
                        "     (4, null, null) ");

        // matched `A B B B` starting from row 1. rows 2, 3, 4 are skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B{2,}) ")))
                .matches("VALUES " +
                        "     (1, 70, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // matched `A B B` starting from row 1. rows 2, 3 are skipped. matched `C` at row 4.
        assertThat(assertions.query(format(query, "PATTERN (A B{2,}? | C) ")))
                .matches("VALUES " +
                        "     (1, 70, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, 70, 'C') ");

        // every row is unmatched.
        assertThat(assertions.query(format(query, "PATTERN (B{5,}) ")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // every row is unmatched.
        assertThat(assertions.query(format(query, "PATTERN (B{5,}?) ")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // matched `A B` starting from row 1. row 2 is skipped. matched `A B` starting from row 3. row 4 is skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B{,1}) ")))
                .matches("VALUES " +
                        "     (1, 80, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, 70, 'B'), " +
                        "     (4, null, null) ");

        // matched `A` at every row.
        assertThat(assertions.query(format(query, "PATTERN (A B{,1}?) ")))
                .matches("VALUES " +
                        "     (1, 90, CAST('A' AS varchar)), " +
                        "     (2, 80, 'A'), " +
                        "     (3, 70, 'A'), " +
                        "     (4, 70, 'A') ");

        // matched `A B B` starting from row 1. rows 2, 3 are skipped. matched `A` at row 4.
        assertThat(assertions.query(format(query, "PATTERN (A B{,2}) ")))
                .matches("VALUES " +
                        "     (1, 70, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, 70, 'A') ");

        // matched `A` at every row.
        assertThat(assertions.query(format(query, "PATTERN (A B{,2}?) ")))
                .matches("VALUES " +
                        "     (1, 90, CAST('A' AS varchar)), " +
                        "     (2, 80, 'A'), " +
                        "     (3, 70, 'A'), " +
                        "     (4, 70, 'A') ");

        // matched `A B B B` starting from row 1. rows 2, 3, 4 are skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B{,5}) ")))
                .matches("VALUES " +
                        "     (1, 70, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // matched `A` at every row.
        assertThat(assertions.query(format(query, "PATTERN (A B{,5}?) ")))
                .matches("VALUES " +
                        "     (1, 90, CAST('A' AS varchar)), " +
                        "     (2, 80, 'A'), " +
                        "     (3, 70, 'A'), " +
                        "     (4, 70, 'A') ");

        // matched `A B` starting from row 1. row 2 is skipped. matched `A B` starting from row 3. row 4 is skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B{1,1}) ")))
                .matches("VALUES " +
                        "     (1, 80, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, 70, 'B'), " +
                        "     (4, null, null) ");

        // matched `A B` starting from row 1. row 2 is skipped. matched `A B` starting from row 3. row 4 is skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B{1,1}?) ")))
                .matches("VALUES " +
                        "     (1, 80, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, 70, 'B'), " +
                        "     (4, null, null) ");

        // matched `A B B B` starting from row 1. rows 2, 3, 4 are skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B{1,5}) ")))
                .matches("VALUES " +
                        "     (1, 70, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // matched `A B` starting from row 1. row 2 is skipped. matched `A B` starting from row 3. row 4 is skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B{1,5}?) ")))
                .matches("VALUES " +
                        "     (1, 80, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, 70, 'B'), " +
                        "     (4, null, null) ");

        // every row is unmatched.
        assertThat(assertions.query(format(query, "PATTERN (A B{5,7}) ")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // every row is unmatched.
        assertThat(assertions.query(format(query, "PATTERN (A B{5,7}?) ")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // matched `A B` starting from row 1. row 2 is skipped. matched `A B` starting from row 3. row 4 is skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B{1}) ")))
                .matches("VALUES " +
                        "     (1, 80, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, 70, 'B'), " +
                        "     (4, null, null) ");

        // matched `A B` starting from row 1. row 2 is skipped. matched `A B` starting from row 3. row 4 is skipped.
        assertThat(assertions.query(format(query, "PATTERN (A B{1}?) ")))
                .matches("VALUES " +
                        "     (1, 80, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, 70, 'B'), " +
                        "     (4, null, null) ");

        // matched `A B B` starting from row 1. rows 2, 3 are skipped. row 4 is unmatched.
        assertThat(assertions.query(format(query, "PATTERN (A B{2}) ")))
                .matches("VALUES " +
                        "     (1, 70, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // matched `A B B` starting from row 1. rows 2, 3 are skipped. row 4 is unmatched.
        assertThat(assertions.query(format(query, "PATTERN (A B{2}?) ")))
                .matches("VALUES " +
                        "     (1, 70, CAST('B' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // every row is unmatched.
        assertThat(assertions.query(format(query, "PATTERN (A B{5}) ")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        // every row is unmatched.
        assertThat(assertions.query(format(query, "PATTERN (A B{5}?) ")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");
    }

    @Test
    public void testExclusionSyntax()
    {
        // exclusion syntax is allowed in window but it has no effect
        assertThat(assertions.query("SELECT id, val OVER w, label OVER w " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 80), " +
                "                   (5, 90), " +
                "                   (6, 50), " +
                "                   (7, 40), " +
                "                   (8, 60) " +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            RUNNING LAST(value) AS val, " +
                "                            CLASSIFIER() AS label " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN ({- A B+ C+ -}) " +
                "                   DEFINE /* A defaults to True, matches any row */ " +
                "                          B AS B.value < PREV (B.value), " +
                "                          C AS C.value > PREV (C.value) " +
                "                )"))
                .matches("VALUES " +
                        "     (1, 90, CAST('C' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null), " +
                        "     (5, null, null), " +
                        "     (6, 60, 'C'), " +
                        "     (7, null, null), " +
                        "     (8, null, null) ");
    }

    @Test
    public void testEmptyCycle()
    {
        String query = "SELECT id, val OVER w, label OVER w " +
                "          FROM (VALUES " +
                "                   (1, 70), " +
                "                   (2, 80), " +
                "                   (3, 80), " +
                "                   (4, 70) " +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            RUNNING LAST(value) AS val, " +
                "                            classifier() AS label " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   %s " + // PATTERN
                "                  DEFINE B AS B.value >= NEXT (B.value) " +
                "                )";

        assertThat(assertions.query(format(query, "PATTERN (()* | B) ")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN (()+ | B) ")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN ((){5,} | B) ")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN (B | ()*) ")))
                .matches("VALUES " +
                        "     (1, null, CAST(null AS varchar)), " +
                        "     (2, 80, 'B'), " +
                        "     (3, 80, 'B'), " +
                        "     (4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN ((B ()*)*) ")))
                .matches("VALUES " +
                        "     (1, null, CAST(null AS varchar)), " +
                        "     (2, 80, 'B'), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN ((B ()*)*?) ")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null) ");
    }

    @Test
    public void testAfterMatchSkipToPosition()
    {
        String query = "SELECT id, val OVER w, label OVER w " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 80), " +
                "                   (5, 70), " +
                "                   (6, 100) " +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            RUNNING LAST(value) AS val, " +
                "                            classifier() AS label " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   %s" + // AFTER MATCH SKIP
                "                   PATTERN (A B+ C+ | E) " +
                "                   DEFINE " +
                "                            B AS B.value < PREV (B.value), " +
                "                            C AS C.value > PREV (C.value) " +
                "                )";

        // `A B B C` is matched starting at row 1. rows 2, 3, 4 are skipped. `E` is matched at row 5. `E` is matched at row 6.
        assertThat(assertions.query(format(query, "AFTER MATCH SKIP PAST LAST ROW")))
                .matches("VALUES " +
                        "     (1, 80, CAST('C' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, null, null), " +
                        "     (5, 70, 'E'), " +
                        "     (6, 100, 'E') ");

        // `A B B C` is matched starting at row 1.
        // `A B C` is matched starting at row 2.
        // `E` is matched at row 3.
        // `A B C` is matched at row 4.
        // `E` is matched at row 5.
        // `E` is matched at row 6.
        assertThat(assertions.query(format(query, "AFTER MATCH SKIP TO NEXT ROW")))
                .matches("VALUES " +
                        "     (1, 80, CAST('C' AS varchar)), " +
                        "     (2, 80, 'C'), " +
                        "     (3, 70, 'E'), " +
                        "     (4, 100, 'C'), " +
                        "     (5, 70, 'E'), " +
                        "     (6, 100, 'E') ");
    }

    @Test
    public void testAfterMatchSkipToLabel()
    {
        String query = "SELECT id, val OVER w, label OVER w " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 80), " +
                "                   (5, 70), " +
                "                   (6, 100) " +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            RUNNING LAST(value) AS val, " +
                "                            classifier() AS label " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   %s" + // AFTER MATCH SKIP
                "                   PATTERN (A B+ C+ D?) " +
                "                   SUBSET U = (C, D) " +
                "                   DEFINE " +
                "                            B AS B.value < PREV (B.value), " +
                "                            C AS C.value > PREV (C.value), " +
                "                            D AS false " + // never matches
                "                )";

        // `A B B C` is matched starting at row 1. rows 2, 3 are skipped
        // `A B C` is matched at row 4. row 5 is skipped.
        // row 6 is unmatched.
        assertThat(assertions.query(format(query, "AFTER MATCH SKIP TO FIRST C")))
                .matches("VALUES " +
                        "     (1, 80, CAST('C' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, 100, 'C'), " +
                        "     (5, null, null), " +
                        "     (6, null, null) ");

        // `A B B C` is matched starting at row 1. rows 2, is skipped
        // row 3 is unmatched.
        // `A B C` is matched at row 4.
        // row 5 is unmatched.
        // row 6 is unmatched.
        assertThat(assertions.query(format(query, "AFTER MATCH SKIP TO LAST B")))
                .matches("VALUES " +
                        "     (1, 80, CAST('C' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, 100, 'C'), " +
                        "     (5, null, null), " +
                        "     (6, null, null) ");

        // 'SKIP TO B' defaults to 'SKIP TO LAST B', which is the same as above
        assertThat(assertions.query(format(query, "AFTER MATCH SKIP TO B")))
                .matches("VALUES " +
                        "     (1, 80, CAST('C' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, 100, 'C'), " +
                        "     (5, null, null), " +
                        "     (6, null, null) ");

        // 'SKIP TO U' skips to last C or D. D never matches, so it skips to the last C, which is the last row of the match.
        // `A B B C` is matched starting at row 1. rows 2, 3 are skipped
        // `A B C` is matched at row 4. row 5 is skipped.
        // row 6 is unmatched.
        assertThat(assertions.query(format(query, "AFTER MATCH SKIP TO U")))
                .matches("VALUES " +
                        "     (1, 80, CAST('C' AS varchar)), " +
                        "     (2, null, null), " +
                        "     (3, null, null), " +
                        "     (4, 100, 'C'), " +
                        "     (5, null, null), " +
                        "     (6, null, null) ");

        // Exception: trying to resume matching from the first row of the match. If uncaught, it would cause an infinite loop.
        assertThatThrownBy(() -> assertions.query(format(query, "AFTER MATCH SKIP TO A")))
                .hasMessage("AFTER MATCH SKIP failed: cannot skip to first row of match");

        // Exception: trying to skip to label which was not matched
        assertThatThrownBy(() -> assertions.query(format(query, "AFTER MATCH SKIP TO D")))
                .hasMessage("AFTER MATCH SKIP failed: pattern variable is not present in match");
    }

    @Test
    public void testUnionVariable()
    {
        // union variable U refers to rows matched with L and to rows matched with H
        // union variable W refers to all rows (it is a union of all available labels)
        assertThat(assertions.query("SELECT id, val OVER w, lower_or_higher OVER w, label OVER w " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 80) " +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            RUNNING LAST(U.value) AS val, " +
                "                            classifier(U) AS lower_or_higher, " +
                "                            classifier(W) AS label " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   PATTERN ((L | H) A) " +
                "                   SUBSET " +
                "                            U = (L, H), " +
                "                            W = (A, L, H) " +
                "                   DEFINE " +
                "                            A AS A.value = 80, " +
                "                            L AS L.value < 80, " +
                "                            H AS H.value > 80 " +
                "                )"))
                .matches("VALUES " +
                        "     (1, 90, CAST('H' AS varchar), CAST('A' AS varchar)), " +
                        "     (2, null, null, null), " +
                        "     (3, 70, 'L', 'A'), " +
                        "     (4, null, null, null) ");
    }

    @Test
    public void testNavigationFunctions()
    {
        String query = "SELECT id, measure OVER w " +
                "          FROM (VALUES " +
                "                   (1, 10), " +
                "                   (2, 20), " +
                "                   (3, 30)" +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES %s AS measure " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   PATTERN (A+) " +
                "                   DEFINE A AS true " +
                "                )";

        // defaults to RUNNING LAST(value)
        // the semantics is effectively FINAL (computation is positioned at the last row of the match)
        assertThat(assertions.query(format(query, "value")))
                .matches("VALUES " +
                        "     (1, 30), " +
                        "     (2, null), " +
                        "     (3, null) ");

        // defaults to RUNNING LAST(value)
        // the semantics is effectively FINAL (computation is positioned at the last row of the match)
        assertThat(assertions.query(format(query, "LAST(value)")))
                .matches("VALUES " +
                        "     (1, 30), " +
                        "     (2, null), " +
                        "     (3, null) ");

        // the semantics is effectively FINAL (computation is positioned at the last row of the match)
        assertThat(assertions.query(format(query, "RUNNING LAST(value)")))
                .matches("VALUES " +
                        "     (1, 30), " +
                        "     (2, null), " +
                        "     (3, null) ");

        assertThat(assertions.query(format(query, "FINAL LAST(value)")))
                .matches("VALUES " +
                        "     (1, 30), " +
                        "     (2, null), " +
                        "     (3, null) ");

        // defaults to RUNNING FIRST(value)
        // the semantics is effectively FINAL (computation is positioned at the last row of the match)
        assertThat(assertions.query(format(query, "FIRST(value)")))
                .matches("VALUES " +
                        "     (1, 10), " +
                        "     (2, null), " +
                        "     (3, null) ");

        assertThat(assertions.query(format(query, "RUNNING FIRST(value)")))
                .matches("VALUES " +
                        "     (1, 10), " +
                        "     (2, null), " +
                        "     (3, null) ");

        assertThat(assertions.query(format(query, "FINAL FIRST(value)")))
                .matches("VALUES " +
                        "     (1, 10), " +
                        "     (2, null), " +
                        "     (3, null) ");

        // with logical offset
        assertThat(assertions.query(format(query, "FINAL LAST(value, 2)")))
                .matches("VALUES " +
                        "     (1, 10), " +
                        "     (2, null), " +
                        "     (3, null) ");

        assertThat(assertions.query(format(query, "FIRST(value, 2)")))
                .matches("VALUES " +
                        "     (1, 30), " +
                        "     (2, null), " +
                        "     (3, null) ");

        assertThat(assertions.query(format(query, "LAST(value, 10)")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer)), " +
                        "     (2, null), " +
                        "     (3, null) ");

        assertThat(assertions.query(format(query, "FIRST(value, 10)")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer)), " +
                        "     (2, null), " +
                        "     (3, null) ");

        // with physical offset
        // defaults to PREV(RUNNING LAST(value), 1)
        assertThat(assertions.query(format(query, "PREV(value)")))
                .matches("VALUES " +
                        "     (1, 20), " +
                        "     (2, null), " +
                        "     (3, null) ");

        // defaults to NEXT(RUNNING LAST(value), 1)
        assertThat(assertions.query(format(query, "NEXT(value)")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer)), " +
                        "     (2, null), " +
                        "     (3, null) ");

        assertThat(assertions.query(format(query, "NEXT(FIRST(value), 2)")))
                .matches("VALUES " +
                        "     (1, 30), " +
                        "     (2, null), " +
                        "     (3, null) ");

        assertThat(assertions.query(format(query, "NEXT(FIRST(value), 10)")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer)), " +
                        "     (2, null), " +
                        "     (3, null) ");

        assertThat(assertions.query(format(query, "PREV(FIRST(value), 10)")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer)), " +
                        "     (2, null), " +
                        "     (3, null) ");

        assertThat(assertions.query(format(query, "PREV(FIRST(value, 10), 2)")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer)), " +
                        "     (2, null), " +
                        "     (3, null) ");

        assertThat(assertions.query(format(query, "PREV(LAST(value, 10), 2)")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer)), " +
                        "     (2, null), " +
                        "     (3, null) ");
    }

    @Test
    public void testNavigationPastFrameBounds()
    {
        // in window, only the rows within the base frame are accessible.
        // rows 3, 4 are rows of the only match. Row 4 is matched to 'B' and it is the starting point to following navigation expressions
        // for this match, the base frame encloses rows [3, 5]
        String query = "SELECT measure OVER w " +
                "          FROM (VALUES " +
                "                   (1, 10), " +
                "                   (2, 20), " +
                "                   (3, 30), " +
                "                   (4, 30), " +
                "                   (5, 40)" +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES %s AS measure " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   PATTERN (A B) " +
                "                   DEFINE A AS A.value = NEXT(A.value) " +
                "                )";

        // row 0 - out of frame bounds and partition bounds (before partition start)
        assertThat(assertions.query(format(query, "PREV(B.value, 4)")))
                .matches("VALUES " +
                        "     (CAST(null AS integer)), " +
                        "     (null), " +
                        "     (null), " +
                        "     (null), " +
                        "     (null) ");
        // row 1 - out of frame bounds
        assertThat(assertions.query(format(query, "PREV(B.value, 3)")))
                .matches("VALUES " +
                        "     (CAST(null AS integer)), " +
                        "     (null), " +
                        "     (null), " +
                        "     (null), " +
                        "     (null) ");

        // row 2 - out of frame bounds
        assertThat(assertions.query(format(query, "PREV(B.value, 2)")))
                .matches("VALUES " +
                        "     (CAST(null AS integer)), " +
                        "     (null), " +
                        "     (null), " +
                        "     (null), " +
                        "     (null) ");

        // row 5 (out of match, but within frame - value can be accessed)
        assertThat(assertions.query(format(query, "NEXT(B.value, 1)")))
                .matches("VALUES " +
                        "     (null), " +
                        "     (null), " +
                        "     (40), " +
                        "     (null), " +
                        "     (null) ");

        // row 6 - out frame bounds and partition bounds (after partition end)
        assertThat(assertions.query(format(query, "NEXT(B.value, 2)")))
                .matches("VALUES " +
                        "     (CAST(null AS integer)), " +
                        "     (null), " +
                        "     (null), " +
                        "     (null), " +
                        "     (null) ");

        // reduce the base frame so that for the only match it only contains rows 3 and 4
        query = "SELECT measure OVER w " +
                "          FROM (VALUES " +
                "                   (1, 10), " +
                "                   (2, 20), " +
                "                   (3, 30), " +
                "                   (4, 30), " +
                "                   (5, 40)" +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES %s AS measure " +
                "                   ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING " +
                "                   PATTERN (A B) " +
                "                   DEFINE A AS A.value = NEXT(A.value) " +
                "                )";

        // row 5 - out of frame bounds
        assertThat(assertions.query(format(query, "NEXT(B.value, 1)")))
                .matches("VALUES " +
                        "     (CAST(null AS integer)), " +
                        "     (null), " +
                        "     (null), " +
                        "     (null), " +
                        "     (null) ");
    }

    @Test
    public void testFrameBounds()
    {
        assertThat(assertions.query("SELECT id, last_matched_row OVER w " +
                "          FROM (VALUES " +
                "                   (1, 1), " +
                "                   (2, 2), " +
                "                   (3, 6), " +
                "                   (4, 0), " +
                "                   (5, 2)" +
                "               ) t(id, rows) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES LAST(id) AS last_matched_row " +
                "                   ROWS BETWEEN CURRENT ROW AND rows FOLLOWING " +
                "                   AFTER MATCH SKIP TO NEXT ROW " +
                "                   PATTERN (A+) " +
                "                   DEFINE A AS true " +
                "                )"))
                .matches("VALUES " +
                        "     (1, 2), " +
                        "     (2, 4), " +
                        "     (3, 5), " +
                        "     (4, 4), " +
                        "     (5, 5) ");
    }

    @Test
    public void testCaseSensitiveLabels()
    {
        assertThat(assertions.query("SELECT id, label OVER w " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 80) " +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES CLASSIFIER() AS label " +
                "                   ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING " +
                "                   AFTER MATCH SKIP TO NEXT ROW " +
                "                   PATTERN (a b* C*) " +
                "                   DEFINE " +
                "                            \"B\" AS B.value < PREV(b.value), " +
                "                            \"C\" AS C.value > PREV(c.value) " +
                "                )"))
                .matches("VALUES " +
                        "     (1, CAST('C' AS varchar)), " +
                        "     (2, 'C'), " +
                        "     (3, 'C'), " +
                        "     (4, 'A') ");
    }

    @Test
    public void testScalarFunctions()
    {
        // scalar functions and operators used in MEASURES and DEFINE clauses
        assertThat(assertions.query("SELECT id, label OVER w " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 60) " +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES CAST(lower(LAST(CLASSIFIER())) || '_label' AS varchar(7)) AS label " +
                "                   ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS B.value + 10 < abs(PREV (B.value)) " +
                "                )"))
                .matches("VALUES " +
                        "     (1, null), " +
                        "     (2, 'b_label'), " +
                        "     (3, null) ");
    }

    @Test
    public void testPartitioningAndOrdering()
    {
        // multiple partitions, unordered input
        assertThat(assertions.query("SELECT part as partition, id AS row_id, val OVER w, label OVER w " +
                "          FROM (VALUES " +
                "                   (1, 'p1', 90), " +
                "                   (2, 'p1', 80), " +
                "                   (6, 'p1', 80), " +
                "                   (2, 'p2', 20), " +
                "                   (2, 'p3', 60), " +
                "                   (1, 'p3', 50), " +
                "                   (3, 'p1', 70), " +
                "                   (4, 'p1', 80), " +
                "                   (5, 'p1', 90), " +
                "                   (1, 'p2', 20), " +
                "                   (3, 'p3', 70), " +
                "                   (3, 'p2', 10) " +
                "               ) t(id, part, value) " +
                "                 WINDOW w AS ( " +
                "                   PARTITION BY part " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            RUNNING LAST(value) AS val, " +
                "                            classifier() AS label " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (B+) " +
                "                   DEFINE B AS B.value > NEXT (B.value) " +
                "                )"))
                .matches("VALUES " +
                        "     ('p1', 1, 80, CAST('B' AS varchar)), " +
                        "     ('p1', 2, null, null), " +
                        "     ('p1', 3, null, null), " +
                        "     ('p1', 4, null, null), " +
                        "     ('p1', 5, 90, 'B'), " +
                        "     ('p1', 6, null, null), " +
                        "     ('p2', 1, null, null), " +
                        "     ('p2', 2, 20, 'B'), " +
                        "     ('p2', 3, null, null), " +
                        "     ('p3', 1, null, null), " +
                        "     ('p3', 2, null, null), " +
                        "     ('p3', 3, null, null) ");

        // empty input
        assertThat(assertions.query("SELECT part as partition, id AS row_id, val OVER w, label OVER w " +
                "          FROM (SELECT * FROM (VALUES (1, 'p1', 90)) WHERE false) t(id, part, value) " +
                "                 WINDOW w AS ( " +
                "                   PARTITION BY part " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            PREV(RUNNING LAST(value)) AS val, " +
                "                            classifier() AS label " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (B+) " +
                "                   DEFINE B AS B.value < PREV (B.value) " +
                "                )"))
                .returnsEmptyResult();

        // no partitioning, unordered input
        assertThat(assertions.query("SELECT id AS row_id, val OVER w, label OVER w " +
                "          FROM (VALUES " +
                "                   (5, 10), " +
                "                   (2, 90), " +
                "                   (1, 80), " +
                "                   (4, 20), " +
                "                   (3, 30) " +
                "                )t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            RUNNING LAST(value) AS val, " +
                "                            classifier() AS label " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (B+) " +
                "                   DEFINE B AS B.value > NEXT (B.value) " +
                "                )"))
                .matches("VALUES " +
                        "     (1, null, CAST(null AS varchar)), " +
                        "     (2, 20, 'B'), " +
                        "     (3, null, null), " +
                        "     (4, null, null), " +
                        "     (5, null, null) ");

        // no measures
        assertThat(assertions.query("SELECT id AS row_id, part as partition" +
                "          FROM (VALUES " +
                "                   (5, 'p2', 10), " +
                "                   (2, 'p1', 90), " +
                "                   (1, 'p1', 80), " +
                "                   (4, 'p2', 20), " +
                "                   (3, 'p1', 30) " +
                "                )t(id, part, value) " +
                "                 WINDOW w AS ( " +
                "                   PARTITION BY part " +
                "                   ORDER BY id " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   PATTERN (B+) " +
                "                   DEFINE B AS B.value < PREV (B.value) " +
                "                )"))
                .matches("VALUES " +
                        "     (1, 'p1'), " +
                        "     (2, 'p1'), " +
                        "     (3, 'p1'), " +
                        "     (4, 'p2'), " +
                        "     (5, 'p2') ");
    }

    @Test
    public void testWindowFunctions()
    {
        // multiple partitions, unordered input
        // function row_number() ignores frame and numbers rows within partition
        // function array_agg() respects frame, which is reduced to the match (and empty in case of unmatched or skipped rows)
        assertThat(assertions.query("SELECT part as partition, id AS row_id, val OVER w, label OVER w, row_number() OVER w, array_agg(value) OVER w " +
                "          FROM (VALUES " +
                "                   (1, 'p1', 90), " +
                "                   (2, 'p1', 80), " +
                "                   (6, 'p1', 80), " +
                "                   (2, 'p2', 20), " +
                "                   (2, 'p3', 60), " +
                "                   (1, 'p3', 50), " +
                "                   (3, 'p1', 70), " +
                "                   (4, 'p1', 80), " +
                "                   (5, 'p1', 90), " +
                "                   (1, 'p2', 20), " +
                "                   (3, 'p3', 70), " +
                "                   (3, 'p2', 10) " +
                "               ) t(id, part, value) " +
                "                 WINDOW w AS ( " +
                "                   PARTITION BY part " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            RUNNING LAST(value) AS val, " +
                "                            classifier() AS label " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (B+) " +
                "                   DEFINE B AS B.value > NEXT (B.value) " +
                "                )"))
                .matches("VALUES " +
                        "     ('p1', 1, 80, CAST('B' AS varchar), BIGINT '1', ARRAY[90, 80]), " +
                        "     ('p1', 2, null, null, 2, null), " +
                        "     ('p1', 3, null, null, 3, null), " +
                        "     ('p1', 4, null, null, 4, null), " +
                        "     ('p1', 5, 90,   'B',  5, ARRAY[90]), " +
                        "     ('p1', 6, null, null, 6, null), " +
                        "     ('p2', 1, null, null, 1, null), " +
                        "     ('p2', 2, 20,   'B',  2, ARRAY[20]), " +
                        "     ('p2', 3, null, null, 3, null), " +
                        "     ('p3', 1, null, null, 1, null), " +
                        "     ('p3', 2, null, null, 2, null), " +
                        "     ('p3', 3, null, null, 3, null) ");

        // with the option SEEK, pattern for the current row can be found starting from some following row within the base frame
        // e.g. for row 1, the match starts at row 2.
        // window frame for the array_agg() function is reduced to the match
        assertThat(assertions.query("SELECT part as partition, id AS row_id, array_agg(value) OVER w " +
                "          FROM (VALUES " +
                "                   (1, 'p1', 50), " +
                "                   (2, 'p1', 80), " +
                "                   (3, 'p1', 70), " +
                "                   (4, 'p1', 60), " +
                "                   (5, 'p1', 50), " +
                "                   (1, 'p2', 10), " +
                "                   (2, 'p2', 20), " +
                "                   (3, 'p2', 30), " +
                "                   (4, 'p2', 40), " +
                "                   (5, 'p2', 10), " +
                "                   (1, 'p3', 100), " +
                "                   (2, 'p3', 100) " +
                "               ) t(id, part, value) " +
                "                 WINDOW w AS ( " +
                "                   PARTITION BY part " +
                "                   ORDER BY id " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   AFTER MATCH SKIP TO NEXT ROW " +
                "                   SEEK " +
                "                   PATTERN (B+) " +
                "                   DEFINE B AS B.value > NEXT (B.value) " +
                "                )"))
                .matches("VALUES " +
                        "     ('p1', 1, ARRAY[80, 70, 60]), " +
                        "     ('p1', 2, ARRAY[80, 70, 60]), " +
                        "     ('p1', 3, ARRAY[70, 60]), " +
                        "     ('p1', 4, ARRAY[60]), " +
                        "     ('p1', 5, null), " +
                        "     ('p2', 1, ARRAY[40]), " +
                        "     ('p2', 2, ARRAY[40]), " +
                        "     ('p2', 3, ARRAY[40]), " +
                        "     ('p2', 4, ARRAY[40]), " +
                        "     ('p2', 5, null), " +
                        "     ('p3', 1, null), " +
                        "     ('p3', 2, null) ");

        // with the option AFTER MATCH SKIP PAST LAST ROW
        assertThat(assertions.query("SELECT part as partition, id AS row_id, array_agg(value) OVER w " +
                "          FROM (VALUES " +
                "                   (1, 'p1', 50), " +
                "                   (2, 'p1', 80), " +
                "                   (3, 'p1', 70), " +
                "                   (4, 'p1', 60), " +
                "                   (5, 'p1', 50), " +
                "                   (1, 'p2', 10), " +
                "                   (2, 'p2', 20), " +
                "                   (3, 'p2', 30), " +
                "                   (4, 'p2', 40), " +
                "                   (5, 'p2', 10), " +
                "                   (1, 'p3', 100), " +
                "                   (2, 'p3', 100) " +
                "               ) t(id, part, value) " +
                "                 WINDOW w AS ( " +
                "                   PARTITION BY part " +
                "                   ORDER BY id " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   SEEK " +
                "                   PATTERN (B+) " +
                "                   DEFINE B AS B.value > NEXT (B.value) " +
                "                )"))
                .matches("VALUES " +
                        "     ('p1', 1, ARRAY[80, 70, 60]), " +
                        "     ('p1', 2, null), " +
                        "     ('p1', 3, null), " +
                        "     ('p1', 4, null), " +
                        "     ('p1', 5, null), " +
                        "     ('p2', 1, ARRAY[40]), " +
                        "     ('p2', 2, null), " +
                        "     ('p2', 3, null), " +
                        "     ('p2', 4, null), " +
                        "     ('p2', 5, null), " +
                        "     ('p3', 1, null), " +
                        "     ('p3', 2, null) ");
    }

    @Test
    public void testAccessingValuesOutsideMatch()
    {
        // in window, only the rows within the base frame are accessible.
        // in the following example, rows 3, 4 are rows of the only match.
        // row 4 is matched to variable `B`, and it is the base for computing measures.
        String query = "SELECT id, row_0 OVER w, row_1 OVER w, row_2 OVER w, row_3 OVER w, row_4 OVER w, row_5 OVER w, row_6 OVER w, row_7 OVER w " +
                "          FROM (VALUES " +
                "                   (1, 10), " +
                "                   (2, 20), " +
                "                   (3, 30), " +
                "                   (4, 30), " +
                "                   (5, 40), " +
                "                   (6, 50)" +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                           PREV(B.id, 4) AS row_0, " +
                "                           PREV(B.id, 3) AS row_1, " +
                "                           PREV(B.id, 2) AS row_2, " +
                "                           PREV(B.id, 1) AS row_3, " +
                "                           PREV(B.id, 0) AS row_4, " +
                "                           NEXT(B.id, 1) AS row_5, " +
                "                           NEXT(B.id, 2) AS row_6, " +
                "                           NEXT(B.id, 3) AS row_7 " +
                "                   %s " + // frame
                "                   %s " + // AFTER MATCH SKIP
                "                   %s " + // INITIAL / SEEK
                "                   PATTERN (A B) " +
                "                   DEFINE A AS A.value = NEXT(A.value) " +
                "                )";

        // match is found at row 1 with the option SEEK
        assertThat(assertions.query(format(
                query,
                "ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING",
                "AFTER MATCH SKIP PAST LAST ROW",
                "SEEK")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer), 1, 2, 3, 4, 5, CAST(null AS integer), CAST(null AS integer)), " +
                        "     (2, null, null, null, null, null, null, null, null), " +
                        "     (3, null, null, null, null, null, null, null, null), " +
                        "     (4, null, null, null, null, null, null, null, null), " +
                        "     (5, null, null, null, null, null, null, null, null), " +
                        "     (6, null, null, null, null, null, null, null, null) ");

        // match is found at row 2 with the option SEEK
        assertThat(assertions.query(format(
                query,
                "ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING",
                "AFTER MATCH SKIP PAST LAST ROW",
                "SEEK")))
                .matches("VALUES " +
                        "     (1, null, null, null, null, null, null, null, null), " +
                        "     (2, CAST(null AS integer), CAST(null AS integer), 2, 3, 4, CAST(null AS integer), CAST(null AS integer), CAST(null AS integer)), " +
                        "     (3, null, null, null, null, null, null, null, null), " +
                        "     (4, null, null, null, null, null, null, null, null), " +
                        "     (5, null, null, null, null, null, null, null, null), " +
                        "     (6, null, null, null, null, null, null, null, null) ");

        // match is found at row 2 with the option SEEK, and again at row 3
        assertThat(assertions.query(format(
                query,
                "ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING",
                "AFTER MATCH SKIP TO NEXT ROW",
                "SEEK")))
                .matches("VALUES " +
                        "     (1, null, null, null, null, null, null, null, null), " +
                        "     (2, CAST(null AS integer), CAST(null AS integer), 2, 3, 4, CAST(null AS integer), CAST(null AS integer), CAST(null AS integer)), " +
                        "     (3, null, null, null, 3, 4, 5, null, null), " +
                        "     (4, null, null, null, null, null, null, null, null), " +
                        "     (5, null, null, null, null, null, null, null, null), " +
                        "     (6, null, null, null, null, null, null, null, null) ");

        // match is found at row 3 with the option INITIAL
        assertThat(assertions.query(format(
                query,
                "ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING",
                "AFTER MATCH SKIP PAST LAST ROW",
                "INITIAL")))
                .matches("VALUES " +
                        "     (1, null, null, null, null, null, null, null, null), " +
                        "     (2, null, null, null, null, null, null, null, null), " +
                        "     (3, CAST(null AS integer), CAST(null AS integer), CAST(null AS integer), 3, 4, 5, CAST(null AS integer), CAST(null AS integer)), " +
                        "     (4, null, null, null, null, null, null, null, null), " +
                        "     (5, null, null, null, null, null, null, null, null), " +
                        "     (6, null, null, null, null, null, null, null, null) ");

        // match is not found because the frame only encloses a single row, and the match requires two rows
        assertThat(assertions.query(format(
                query,
                "ROWS BETWEEN CURRENT ROW AND CURRENT ROW",
                "AFTER MATCH SKIP PAST LAST ROW",
                "SEEK")))
                .matches("VALUES " +
                        "     (1, CAST(null AS integer), CAST(null AS integer), CAST(null AS integer), CAST(null AS integer), CAST(null AS integer), CAST(null AS integer), CAST(null AS integer), CAST(null AS integer)), " +
                        "     (2, null, null, null, null, null, null, null, null), " +
                        "     (3, null, null, null, null, null, null, null, null), " +
                        "     (4, null, null, null, null, null, null, null, null), " +
                        "     (5, null, null, null, null, null, null, null, null), " +
                        "     (6, null, null, null, null, null, null, null, null) ");
    }

    @Test
    public void testRowPatternMatchingInOrderBy()
    {
        // WINDOW clause
        // rows 1, 2, 3, 4, 5 are matched to X, Y, A, B, C. row 6 is unmatched, so its label is null.
        assertThat(assertions.query("SELECT id " +
                "          FROM (VALUES 1, 2, 3, 4, 5, 6) t(id) " +
                "                WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES CLASSIFIER() AS label " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   PATTERN (X Y A B C) " +
                "                   DEFINE X AS true " +
                "                ) " +
                "          ORDER BY label OVER w ASC NULLS FIRST"))
                .matches("VALUES 6, 3, 4, 5, 1, 2");

        // in-line window specification
        // rows 1, 2, 3, 4, 5 are matched to X, Y, A, B, C. row 6 is unmatched, so its label is null.
        assertThat(assertions.query("SELECT id " +
                "          FROM (VALUES 1, 2, 3, 4, 5, 6) t(id) " +
                "          ORDER BY label OVER (" +
                "                               ORDER BY id " +
                "                               MEASURES CLASSIFIER() AS label " +
                "                               ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                               PATTERN (X Y A B C) " +
                "                               DEFINE X AS true " +
                "                              ) ASC NULLS FIRST"))
                .matches("VALUES 6, 3, 4, 5, 1, 2");

        // mixed window specification
        // rows 1, 2, 3, 4, 5 are matched to X, Y, A, B, C. row 6 is unmatched, so its label is null.
        assertThat(assertions.query("SELECT id " +
                "          FROM (VALUES 1, 2, 3, 4, 5, 6) t(id) " +
                "                WINDOW w AS (ORDER BY id) " +
                "          ORDER BY label OVER (w " +
                "                               MEASURES CLASSIFIER() AS label " +
                "                               ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                               PATTERN (X Y A B C) " +
                "                               DEFINE X AS true " +
                "                              ) ASC NULLS FIRST"))
                .matches("VALUES 6, 3, 4, 5, 1, 2");
    }

    @Test
    public void testSubqueries()
    {
        // NOTE: Subquery in pattern recognition context can always be handled prior to pattern recognition planning, because
        // by analysis it cannot contain labeled column references, navigations, `CLASSIFIER()`, or `MATCH_NUMBER()` calls,
        // which mustn't be pre-projected.
        String query = "SELECT id, val OVER w " +
                "          FROM (VALUES " +
                "                   (1, 100), " +
                "                   (2, 200), " +
                "                   (3, 300), " +
                "                   (4, 400) " +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES %s AS val " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   AFTER MATCH SKIP TO NEXT ROW " +
                "                   PATTERN (A+) " +
                "                   DEFINE A AS %s " +
                "                )";

        assertThat(assertions.query(format(query, "(SELECT 'x')", "(SELECT true)")))
                .matches("VALUES " +
                        "     (1, 'x'), " +
                        "     (2, 'x'), " +
                        "     (3, 'x'), " +
                        "     (4, 'x') ");

        // subquery nested in navigation
        assertThat(assertions.query(format(query, "LAST(A.value + (SELECT 1000))", "FIRST(A.value < 0 OR (SELECT true))")))
                .matches("VALUES " +
                        "     (1, 1400), " +
                        "     (2, 1400), " +
                        "     (3, 1400), " +
                        "     (4, 1400) ");

        // IN-predicate: value and value list without column references
        assertThat(assertions.query(format(query, "LAST(A.id < 0 OR 1 IN (SELECT 1))", "FIRST(A.id > 0 AND 1 IN (SELECT 1))")))
                .matches("VALUES " +
                        "     (1, true), " +
                        "     (2, true), " +
                        "     (3, true), " +
                        "     (4, true) ");

        // IN-predicate: unlabeled column reference in value
        assertThat(assertions.query(format(query, "LAST(id + 50 IN (SELECT 100))", "LAST(id NOT IN (SELECT 100))")))
                .matches("VALUES " +
                        "     (1, false), " +
                        "     (2, false), " +
                        "     (3, false), " +
                        "     (4, false) ");

        // EXISTS-predicate
        assertThat(assertions.query(format(query, "LAST(A.value < 0 OR EXISTS(SELECT 1))", "FIRST(A.value < 0 OR EXISTS(SELECT 1))")))
                .matches("VALUES " +
                        "     (1, true), " +
                        "     (2, true), " +
                        "     (3, true), " +
                        "     (4, true) ");

        // no pattern measures
        assertThat(assertions.query("SELECT id, max(value) OVER w " +
                "          FROM (VALUES " +
                "                   (1, 100), " +
                "                   (2, 200), " +
                "                   (3, 300), " +
                "                   (4, 400) " +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   AFTER MATCH SKIP TO NEXT ROW " +
                "                   PATTERN (A+) " +
                "                   DEFINE A AS (SELECT true) " +
                "                )"))
                .matches("VALUES " +
                        "     (1, 400), " +
                        "     (2, 400), " +
                        "     (3, 400), " +
                        "     (4, 400) ");
    }

    @Test
    public void testInPredicateWithoutSubquery()
    {
        String query = "SELECT id, val OVER w " +
                "          FROM (VALUES " +
                "                   (1, 100), " +
                "                   (2, 200), " +
                "                   (3, 300), " +
                "                   (4, 400) " +
                "               ) t(id, value) " +
                "                 WINDOW w AS ( " +
                "                   ORDER BY id " +
                "                   MEASURES %s AS val " +
                "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                "                   AFTER MATCH SKIP TO NEXT ROW " +
                "                   PATTERN (A+) " +
                "                   DEFINE A AS true " +
                "                )";

        // navigations and labeled column references
        assertThat(assertions.query(format(query, "FIRST(A.value) IN (300, LAST(A.value))")))
                .matches("VALUES " +
                        "     (1, false), " +
                        "     (2, false), " +
                        "     (3, true), " +
                        "     (4, true) ");

        // CLASSIFIER()
        assertThat(assertions.query(format(query, "CLASSIFIER() IN ('X', lower(CLASSIFIER()))")))
                .matches("VALUES " +
                        "     (1, false), " +
                        "     (2, false), " +
                        "     (3, false), " +
                        "     (4, false) ");
    }
}
