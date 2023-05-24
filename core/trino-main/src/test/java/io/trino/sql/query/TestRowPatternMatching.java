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
public class TestRowPatternMatching
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
        assertThat(assertions.query("SELECT m.id, m.match, m.val, m.label " +
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
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            MATCH_NUMBER() AS match, " +
                "                            RUNNING LAST(value) AS val, " +
                "                            CLASSIFIER() AS label " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (A B+ C+) " +
                "                   DEFINE /* A defaults to True, matches any row */ " +
                "                          B AS B.value < PREV (B.value), " +
                "                          C AS C.value > PREV (C.value) " +
                "                ) AS m"))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'A'), " +
                        "     (2, 1, 80, 'B'), " +
                        "     (3, 1, 70, 'B'), " +
                        "     (4, 1, 80, 'C'), " +
                        "     (5, 1, 90, 'C'), " +
                        "     (6, 2, 50, 'A'), " +
                        "     (7, 2, 40, 'B'), " +
                        "     (8, 2, 60, 'C') ");
    }

    @Test
    public void testRowPattern()
    {
        String query = "SELECT m.id AS row_id, m.match, m.val, m.label " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 70) " +
                "               ) t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            match_number() AS match, " +
                "                            RUNNING LAST(value) AS val, " +
                "                            classifier() AS label " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   %s " + // PATTERN and DEFINE
                "                ) AS m";

        // empty pattern (need to declare at least one pattern variable. It is in non-preferred branch of pattern alternation and will not be matched)
        assertThat(assertions.query(format(query, "PATTERN (() | A) " +
                "                  DEFINE A AS true ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 2, null, null), " +
                        "     (3, 3, null, null), " +
                        "     (4, 4, null, null) ");

        // anchor pattern: partition start
        assertThat(assertions.query(format(query, "PATTERN (^A) " +
                "                  DEFINE A AS true ")))
                .matches("VALUES (1, CAST(1 AS bigint), 90, VARCHAR 'A') ");
        assertThat(assertions.query(format(query, "PATTERN (A^) " +
                "                  DEFINE A AS true ")))
                .returnsEmptyResult();
        assertThat(assertions.query(format(query, "PATTERN (^A^) " +
                "                  DEFINE A AS true ")))
                .returnsEmptyResult();

        // anchor pattern: partition end
        assertThat(assertions.query(format(query, "PATTERN (A$) " +
                "                  DEFINE A AS true ")))
                .matches("VALUES (4, CAST(1 AS bigint), 70, VARCHAR 'A') ");
        assertThat(assertions.query(format(query, "PATTERN ($A) " +
                "                  DEFINE A AS true ")))
                .returnsEmptyResult();
        assertThat(assertions.query(format(query, "PATTERN ($A$) " +
                "                  DEFINE A AS true ")))
                .returnsEmptyResult();

        // pattern concatenation
        assertThat(assertions.query(format(query, "PATTERN (A B C) " +
                "                  DEFINE " +
                "                          B AS B.value < PREV (B.value), " +
                "                          C AS C.value = PREV (C.value) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'A'), " +
                        "     (3, 1, 70, 'B'), " +
                        "     (4, 1, 70, 'C') ");

        // pattern alternation: when multiple alternatives match, the first in order of declaration is preferred
        assertThat(assertions.query(format(query, "PATTERN (B | C | A) " +
                "                  DEFINE " +
                "                          B AS B.value < PREV (B.value), " +
                "                          C AS C.value <= PREV (C.value) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'A'), " +
                        "     (2, 2, 80, 'B'), " +
                        "     (3, 3, 70, 'B'), " +
                        "     (4, 4, 70, 'C') ");

        //pattern permutation: when multiple permutations match, the first in lexicographical order is preferred
        assertThat(assertions.query(format(query, "PATTERN (PERMUTE(B, C)) " +
                "                  DEFINE " +
                "                          B AS B.value < PREV (B.value), " +
                "                          C AS C.value < PREV (C.value) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (3, 1, 70, 'C') ");

        // grouped pattern
        assertThat(assertions.query(format(query, "PATTERN (((A) (B (C)))) " +
                "                  DEFINE " +
                "                          B AS B.value < PREV (B.value), " +
                "                          C AS C.value = PREV (C.value) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'A'), " +
                        "     (3, 1, 70, 'B'), " +
                        "     (4, 1, 70, 'C') ");
    }

    @Test
    public void testPatternQuantifiers()
    {
        String query = "SELECT m.id AS row_id, m.match, m.val, m.label " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 70) " +
                "               ) t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            match_number() AS match, " +
                "                            RUNNING LAST(value) AS val, " +
                "                            classifier() AS label " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   %s " + // PATTERN
                "                  DEFINE B AS B.value <= PREV (B.value) " +
                "                ) AS m";

        assertThat(assertions.query(format(query, "PATTERN (B*) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), null, CAST(null AS varchar)), " +
                        "     (2, 2, 80, 'B'), " +
                        "     (3, 2, 70, 'B'), " +
                        "     (4, 2, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B*?) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 2, null, null), " +
                        "     (3, 3, null, null), " +
                        "     (4, 4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN (B+) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (3, 1, 70, 'B'), " +
                        "     (4, 1, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B+?) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (3, 2, 70, 'B'), " +
                        "     (4, 3, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B?) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), null, CAST(null AS varchar)), " +
                        "     (2, 2, 80, 'B'), " +
                        "     (3, 3, 70, 'B'), " +
                        "     (4, 4, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B??) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 2, null, null), " +
                        "     (3, 3, null, null), " +
                        "     (4, 4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN (B{,}) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), null, CAST(null AS varchar)), " +
                        "     (2, 2, 80, 'B'), " +
                        "     (3, 2, 70, 'B'), " +
                        "     (4, 2, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B{,}?) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 2, null, null), " +
                        "     (3, 3, null, null), " +
                        "     (4, 4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN (B{1,}) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (3, 1, 70, 'B'), " +
                        "     (4, 1, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B{1,}?) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (3, 2, 70, 'B'), " +
                        "     (4, 3, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B{2,}) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (3, 1, 70, 'B'), " +
                        "     (4, 1, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B{2,}?) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (3, 1, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B{5,}) ")))
                .returnsEmptyResult();

        assertThat(assertions.query(format(query, "PATTERN (B{5,}?) ")))
                .returnsEmptyResult();

        assertThat(assertions.query(format(query, "PATTERN (B{,1}) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), null, CAST(null AS varchar)), " +
                        "     (2, 2, 80, 'B'), " +
                        "     (3, 3, 70, 'B'), " +
                        "     (4, 4, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B{,1}?) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 2, null, null), " +
                        "     (3, 3, null, null), " +
                        "     (4, 4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN (B{,2}) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), null, CAST(null AS varchar)), " +
                        "     (2, 2, 80, 'B'), " +
                        "     (3, 2, 70, 'B'), " +
                        "     (4, 3, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B{,2}?) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 2, null, null), " +
                        "     (3, 3, null, null), " +
                        "     (4, 4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN (B{,5}) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), null, CAST(null AS varchar)), " +
                        "     (2, 2, 80, 'B'), " +
                        "     (3, 2, 70, 'B'), " +
                        "     (4, 2, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B{,5}?) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 2, null, null), " +
                        "     (3, 3, null, null), " +
                        "     (4, 4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN (B{1,1}) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (3, 2, 70, 'B'), " +
                        "     (4, 3, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B{1,1}?) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (3, 2, 70, 'B'), " +
                        "     (4, 3, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B{1,5}) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (3, 1, 70, 'B'), " +
                        "     (4, 1, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B{1,5}?) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (3, 2, 70, 'B'), " +
                        "     (4, 3, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B{5,7}) ")))
                .returnsEmptyResult();

        assertThat(assertions.query(format(query, "PATTERN (B{5,7}?) ")))
                .returnsEmptyResult();

        assertThat(assertions.query(format(query, "PATTERN (B{1}) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (3, 2, 70, 'B'), " +
                        "     (4, 3, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B{1}?) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (3, 2, 70, 'B'), " +
                        "     (4, 3, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B{2}) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (3, 1, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B{2}?) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (3, 1, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B{5}) ")))
                .returnsEmptyResult();

        assertThat(assertions.query(format(query, "PATTERN (B{5}?) ")))
                .returnsEmptyResult();
    }

    @Test
    public void testExclusionSyntax()
    {
        String query = "SELECT m.id AS row_id, m.match, m.val, m.label " +
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
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            match_number() AS match, " +
                "                            RUNNING LAST(value) AS val, " +
                "                            classifier() AS label " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   %s " + // PATTERN
                "                  DEFINE " +
                "                            B AS B.value < PREV (B.value), " +
                "                            C AS C.value > PREV (C.value) " +
                "                ) AS m";

        // no exclusion -- outputting all matched rows
        assertThat(assertions.query(format(query, "PATTERN (A B+ C+) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'A'), " +
                        "     (2, 1, 80, 'B'), " +
                        "     (3, 1, 70, 'B'), " +
                        "     (4, 1, 80, 'C'), " +
                        "     (5, 1, 90, 'C'), " +
                        "     (6, 2, 50, 'A'), " +
                        "     (7, 2, 40, 'B'), " +
                        "     (8, 2, 60, 'C') ");

        // exclude rows matched to 'B'
        assertThat(assertions.query(format(query, "PATTERN (A {- B+ -} C+) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'A'), " +
                        "     (4, 1, 80, 'C'), " +
                        "     (5, 1, 90, 'C'), " +
                        "     (6, 2, 50, 'A'), " +
                        "     (8, 2, 60, 'C') ");

        // adjacent exclusions: exclude rows matched to 'A' and 'B'
        assertThat(assertions.query(format(query, "PATTERN ({- A -} {- B+ -} C+) ")))
                .matches("VALUES " +
                        "     (4, CAST(1 AS bigint), 80, VARCHAR 'C'), " +
                        "     (5, 1, 90, 'C'), " +
                        "     (8, 2, 60, 'C') ");

        // nested exclusions: exclude all rows from outermost exclusion
        assertThat(assertions.query(format(query, "PATTERN (A {- {- B+ -} C+ -}) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'A'), " +
                        "     (6, 2, 50, 'A') ");

        // exclude all rows
        assertThat(assertions.query(format(query, "PATTERN ({- A B+ C+ -}) ")))
                .returnsEmptyResult();

        // exclude empty pattern: effectively does not exclude any rows
        assertThat(assertions.query(format(query, "PATTERN (A B+ {- ()* -} C+) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'A'), " +
                        "     (2, 1, 80, 'B'), " +
                        "     (3, 1, 70, 'B'), " +
                        "     (4, 1, 80, 'C'), " +
                        "     (5, 1, 90, 'C'), " +
                        "     (6, 2, 50, 'A'), " +
                        "     (7, 2, 40, 'B'), " +
                        "     (8, 2, 60, 'C') ");

        // quantified exclusion
        assertThat(assertions.query(format(query, "PATTERN ( A {- B -}+ {- C -}+) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'A'), " +
                        "     (6, 2, 50, 'A') ");

        assertThat(assertions.query(format(query, "PATTERN ( A {- B -}* {- C -}*) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'A'), " +
                        "     (6, 2, 50, 'A') ");

        assertThat(assertions.query(format(query, "PATTERN ( A {- B -}{1,2} {- C -}{1,2}) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'A'), " +
                        "     (6, 2, 50, 'A') ");

        assertThat(assertions.query(format(query, "PATTERN ( A {- C -}{2,3} {- B -}{2,3}) ")))
                .matches("VALUES (3, CAST(1 AS bigint), 70, VARCHAR 'A') ");
    }

    @Test
    public void testBackReference()
    {
        // defining condition of `X` refers to input values at matched label `A`
        assertThat(assertions.query("SELECT value, classy " +
                "       FROM (VALUES 1, 1) t(value) " +
                "       MATCH_RECOGNIZE ( " +
                "       MEASURES CLASSIFIER() AS classy " +
                "       ALL ROWS PER MATCH " +
                "       PATTERN ((A | B)* X) " +
                "       DEFINE X AS value = A.value " +
                "     )"))
                .matches("VALUES " +
                        "(1, VARCHAR 'A'), " +
                        "(1, 'X') ");

        // defining condition of `X` refers to input values at matched label `B`
        assertThat(assertions.query("SELECT value, classy " +
                "       FROM (VALUES 1, 1) t(value) " +
                "       MATCH_RECOGNIZE ( " +
                "       MEASURES CLASSIFIER() AS classy " +
                "       ALL ROWS PER MATCH " +
                "       PATTERN ((A | B)* X) " +
                "       DEFINE X AS value = B.value " +
                "     )"))
                .matches("VALUES " +
                        "(1, VARCHAR 'B'), " +
                        "(1, 'X') ");

        // defining condition of `X` refers to input values at matched labels `A` and `B`
        assertThat(assertions.query("SELECT value, classy, defining_condition " +
                "       FROM (VALUES 1, 2, 3, 4, 5, 6) t(value) " +
                "       MATCH_RECOGNIZE ( " +
                "       ORDER BY value " +
                "       MEASURES " +
                "               CLASSIFIER() AS classy, " +
                "               PREV(LAST(A.value), 3) + FIRST(A.value) + PREV(LAST(B.value), 2) AS defining_condition" +
                "       ALL ROWS PER MATCH " +
                "       PATTERN ((A | B)* X) " +
                "       DEFINE X AS value = PREV(LAST(A.value), 3) + FIRST(A.value) + PREV(LAST(B.value), 2) " +
                "     )"))
                .matches("VALUES " +
                        "(1, VARCHAR 'B',   null), " +
                        "(2,         'A',   null), " +
                        "(3,         'A',   null), " +
                        "(4,         'A',   null), " +
                        "(5,         'B',      6), " +
                        "(6,         'X',      6) ");

        // defining condition of `X` refers to matched labels `A` and `B`
        assertThat(assertions.query("SELECT value, classy, defining_condition " +
                "       FROM (VALUES 1, 2, 3, 4, 5) t(value) " +
                "       MATCH_RECOGNIZE ( " +
                "       ORDER BY value " +
                "       MEASURES " +
                "               CLASSIFIER() AS classy, " +
                "               PREV(CLASSIFIER(U), 1) = 'A' AND LAST(CLASSIFIER(), 3) = 'B' AND FIRST(CLASSIFIER(U)) = 'B' AS defining_condition" +
                "       ALL ROWS PER MATCH " +
                "       PATTERN ((A | B)* X $) " +
                "       SUBSET U = (A, B) " +
                "       DEFINE X AS PREV(CLASSIFIER(U), 1) = 'A' AND LAST(CLASSIFIER(), 3) = 'B' AND FIRST(CLASSIFIER(U)) = 'B' " +
                "     )"))
                .matches("VALUES " +
                        "(1, VARCHAR 'B',   null), " +
                        "(2,         'B',   false), " +
                        "(3,         'A',   false), " +
                        "(4,         'A',   true), " +
                        "(5,         'X',   true) ");
    }

    @Test
    public void testEmptyCycle()
    {
        String query = "SELECT m.id AS row_id, m.match, m.val, m.label " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 70) " +
                "               ) t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            match_number() AS match, " +
                "                            RUNNING LAST(value) AS val, " +
                "                            classifier() AS label " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   %s " + // PATTERN
                "                  DEFINE B AS B.value < PREV (B.value) " +
                "                ) AS m";

        assertThat(assertions.query(format(query, "PATTERN (()* | B) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 2, null, null), " +
                        "     (3, 3, null, null), " +
                        "     (4, 4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN (()+ | B) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 2, null, null), " +
                        "     (3, 3, null, null), " +
                        "     (4, 4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN ((){5,} | B) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 2, null, null), " +
                        "     (3, 3, null, null), " +
                        "     (4, 4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN (B | ()*) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 2, 80, 'B'), " +
                        "     (3, 3, 70, 'B'), " +
                        "     (4, 4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN ((B ()*)*) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 2, 80, 'B'), " +
                        "     (3, 2, 70, 'B'), " +
                        "     (4, 3, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN ((B ()*)*?) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 2, null, null), " +
                        "     (3, 3, null, null), " +
                        "     (4, 4, null, null) ");

        // quantified anchor pattern
        assertThat(assertions.query(format(query, "PATTERN (^* | B) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 2, null, null), " +
                        "     (3, 3, null, null), " +
                        "     (4, 4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN (^+ | B) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), null, CAST(null AS varchar)), " +
                        "     (2, 2, 80, 'B'), " +
                        "     (3, 3, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (^* A B) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'A'), " +
                        "     (2, 1, 80, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN ($* | B) ")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 2, null, null), " +
                        "     (3, 3, null, null), " +
                        "     (4, 4, null, null) ");

        assertThat(assertions.query(format(query, "PATTERN ($+ | B) ")))
                .matches("VALUES " +
                        "     (2, CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (3, 2, 70, 'B') ");

        assertThat(assertions.query(format(query, "PATTERN (B A $+) ")))
                .matches("VALUES " +
                        "     (3, CAST(1 AS bigint), 70, VARCHAR 'B'), " +
                        "     (4, 1, 70, 'A') ");
    }

    @Test
    public void testOutputModes()
    {
        String query = "SELECT m.match, m.val, m.label " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 70) " +
                "               ) t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            match_number() AS match, " +
                "                            RUNNING LAST(value) AS val, " +
                "                            classifier() AS label " +
                "                   %s " + // ROWS PER MATCH
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   %s " + // PATTERN
                "                  DEFINE B AS B.value < PREV (B.value) " +
                "                ) AS m";

        // ONE ROW PER MATCH shows empty matches by default
        assertThat(assertions.query(format(query, "ONE ROW PER MATCH", "PATTERN (B*) ")))
                .matches("VALUES " +
                        "     (CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 70, 'B'), " +
                        "     (3, null, null) ");

        // defaults to ONE ROW PER MATCH
        assertThat(assertions.query(format(query, "", "PATTERN (B*) ")))
                .matches("VALUES " +
                        "     (CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 70, 'B'), " +
                        "     (3, null, null) ");

        // ONE ROW PER MATCH omits unmatched rows
        assertThat(assertions.query(format(query, "ONE ROW PER MATCH", "PATTERN (B+) ")))
                .matches("VALUES (CAST(1 AS bigint), 70, VARCHAR 'B') ");

        // ALL ROWS PER MATCH shows empty matches by default
        assertThat(assertions.query(format(query, "ALL ROWS PER MATCH", "PATTERN (B*) ")))
                .matches("VALUES " +
                        "     (CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 80, 'B'), " +
                        "     (2, 70, 'B'), " +
                        "     (3, null, null) ");

        // ALL ROWS PER MATCH omits unmatched rows by default
        assertThat(assertions.query(format(query, "ALL ROWS PER MATCH", "PATTERN (B+) ")))
                .matches("VALUES " +
                        "     (CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (1, 70, 'B') ");

        assertThat(assertions.query(format(query, "ALL ROWS PER MATCH SHOW EMPTY MATCHES", "PATTERN (B*) ")))
                .matches("VALUES " +
                        "     (CAST(1 AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (2, 80, 'B'), " +
                        "     (2, 70, 'B'), " +
                        "     (3, null, null) ");

        assertThat(assertions.query(format(query, "ALL ROWS PER MATCH OMIT EMPTY MATCHES", "PATTERN (B*) ")))
                .matches("VALUES " +
                        "     (CAST(2 AS bigint), 80, VARCHAR 'B'), " +
                        "     (2, 70, 'B') ");

        // ALL ROWS PER MATCH OMIT EMPTY MATCHES omits unmatched rows
        assertThat(assertions.query(format(query, "ALL ROWS PER MATCH OMIT EMPTY MATCHES", "PATTERN (B+) ")))
                .matches("VALUES " +
                        "     (CAST(1 AS bigint), 80, VARCHAR 'B'), " +
                        "     (1, 70, 'B') ");

        assertThat(assertions.query(format(query, "ALL ROWS PER MATCH WITH UNMATCHED ROWS", "PATTERN (B+) ")))
                .matches("VALUES " +
                        "     (CAST(null AS bigint), CAST(null AS integer), CAST(null AS varchar)), " +
                        "     (1, 80, 'B'), " +
                        "     (1, 70, 'B'), " +
                        "     (null, null, null) ");
    }

    @Test
    public void testAfterMatchSkip()
    {
        String query = "SELECT m.id, m.match, m.val, m.label " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 80), " +
                "                   (5, 70), " +
                "                   (6, 80) " +
                "               ) t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            match_number() AS match, " +
                "                            RUNNING LAST(value) AS val, " +
                "                            classifier() AS label " +
                "                   ALL ROWS PER MATCH " +
                "                   %s" + // AFTER MATCH SKIP
                "                   PATTERN (A B+ C+ D?) " +
                "                   SUBSET U = (C, D) " +
                "                   DEFINE " +
                "                            B AS B.value < PREV (B.value), " +
                "                            C AS C.value > PREV (C.value), " +
                "                            D AS false " + // never matches
                "                ) AS m";

        // after rows 1, 2, 3, 4 are matched, matching starts at row 5 and no other match is found
        assertThat(assertions.query(format(query, "AFTER MATCH SKIP PAST LAST ROW")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'A'), " +
                        "     (2, 1, 80, 'B'), " +
                        "     (3, 1, 70, 'B'), " +
                        "     (4, 1, 80, 'C') ");

        // after rows 1, 2, 3, 4 are matched, matching starts at row 2. Two more matches are found starting at rows 2 and 4.
        assertThat(assertions.query(format(query, "AFTER MATCH SKIP TO NEXT ROW")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'A'), " +
                        "     (2, 1, 80, 'B'), " +
                        "     (3, 1, 70, 'B'), " +
                        "     (4, 1, 80, 'C'), " +
                        "     (2, 2, 80, 'A'), " +
                        "     (3, 2, 70, 'B'), " +
                        "     (4, 2, 80, 'C'), " +
                        "     (4, 3, 80, 'A'), " +
                        "     (5, 3, 70, 'B'), " +
                        "     (6, 3, 80, 'C') ");

        // after rows 1, 2, 3, 4 are matched, matching starts at row 4. Another match is found starting at row 4.
        assertThat(assertions.query(format(query, "AFTER MATCH SKIP TO FIRST C")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'A'), " +
                        "     (2, 1, 80, 'B'), " +
                        "     (3, 1, 70, 'B'), " +
                        "     (4, 1, 80, 'C'), " +
                        "     (4, 2, 80, 'A'), " +
                        "     (5, 2, 70, 'B'), " +
                        "     (6, 2, 80, 'C') ");

        // after rows 1, 2, 3, 4 are matched, matching starts at row 3. Another match is found starting at row 4.
        assertThat(assertions.query(format(query, "AFTER MATCH SKIP TO LAST B")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'A'), " +
                        "     (2, 1, 80, 'B'), " +
                        "     (3, 1, 70, 'B'), " +
                        "     (4, 1, 80, 'C'), " +
                        "     (4, 2, 80, 'A'), " +
                        "     (5, 2, 70, 'B'), " +
                        "     (6, 2, 80, 'C') ");

        // 'SKIP TO B' defaults to 'SKIP TO LAST B', which is the same as above
        assertThat(assertions.query(format(query, "AFTER MATCH SKIP TO B")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'A'), " +
                        "     (2, 1, 80, 'B'), " +
                        "     (3, 1, 70, 'B'), " +
                        "     (4, 1, 80, 'C'), " +
                        "     (4, 2, 80, 'A'), " +
                        "     (5, 2, 70, 'B'), " +
                        "     (6, 2, 80, 'C') ");

        // 'SKIP TO U' skips to last C or D. D never matches, so it skips to the last C, which is the last row of the match.
        // after rows 1, 2, 3, 4 are matched, matching starts at row 4. Another match is found starting at row 4.
        assertThat(assertions.query(format(query, "AFTER MATCH SKIP TO U")))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'A'), " +
                        "     (2, 1, 80, 'B'), " +
                        "     (3, 1, 70, 'B'), " +
                        "     (4, 1, 80, 'C'), " +
                        "     (4, 2, 80, 'A'), " +
                        "     (5, 2, 70, 'B'), " +
                        "     (6, 2, 80, 'C') ");

        // Exception: trying to resume matching from the first row of the match. If uncaught, it would cause an infinite loop.
        assertThatThrownBy(() -> assertions.query(format(query, "AFTER MATCH SKIP TO A")))
                .hasMessage("AFTER MATCH SKIP failed: cannot skip to first row of match");

        // Exception: trying to skip to label which was not matched
        assertThatThrownBy(() -> assertions.query(format(query, "AFTER MATCH SKIP TO D")))
                .hasMessage("AFTER MATCH SKIP failed: pattern variable is not present in match");
    }

    @Test
    public void testEmptyMatches()
    {
        // row 1 is unmatched and output row is produced
        // rows 2, 3, 4 are a match
        // rows 3, 4, 5 are a match
        // Because AFTER MATCH SKIP TO NEX ROW is specified, rows 4 and 5 are examined. No match is found for them,
        //   but output is not produced, because they were formerly matched by a non-empty match
        // row 6 is unmatched and output row is produced
        assertThat(assertions.query("SELECT m.id, m.match, m.value, m.label " +
                "          FROM (VALUES " +
                "                   (1, 100), " +
                "                   (2, 100), " +
                "                   (3, 90), " +
                "                   (4, 80), " +
                "                   (5, 70), " +
                "                   (6, 100) " +
                "               ) t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            match_number() AS match, " +
                "                            classifier() AS label " +
                "                   ALL ROWS PER MATCH WITH UNMATCHED ROWS " +
                "                   AFTER MATCH SKIP TO NEXT ROW " +
                "                   PATTERN (A B{2}) " +
                "                   DEFINE B AS B.value < PREV (B.value) " +
                "                ) AS m"))
                .matches("VALUES " +
                        "     (1, CAST(null AS bigint), 100, CAST(null AS varchar)), " +
                        "     (2, 1, 100, 'A'), " +
                        "     (3, 1,  90, 'B'), " +
                        "     (4, 1,  80, 'B'), " +
                        "     (3, 2,  90, 'A'), " +
                        "     (4, 2,  80, 'B'), " +
                        "     (5, 2,  70, 'B'), " +
                        "     (6, null,  100, null) ");
    }

    @Test
    public void testUnionVariable()
    {
        // union variable U refers to rows matched with L and to rows matched with H
        // union variable W refers to all rows (it is a union of all available labels)
        assertThat(assertions.query("SELECT m.id, m.match, m.val, m.lower_or_higher, m.label " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 80) " +
                "               ) t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            match_number() AS match, " +
                "                            RUNNING LAST(value) AS val, " +
                "                            classifier(U) AS lower_or_higher, " +
                "                            classifier(W) AS label " +
                "                   ALL ROWS PER MATCH " +
                "                   PATTERN ((L | H) A) " +
                "                   SUBSET " +
                "                            U = (L, H), " +
                "                            W = (A, L, H) " +
                "                   DEFINE " +
                "                            A AS A.value = 80, " +
                "                            L AS L.value < 80, " +
                "                            H AS H.value > 80 " +
                "                ) AS m"))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint), 90, VARCHAR 'H', VARCHAR 'H'), " +
                        "     (2, 1, 80, 'H', 'A'), " +
                        "     (3, 2, 70, 'L', 'L'), " +
                        "     (4, 2, 80, 'L', 'A') ");
    }

    @Test
    public void testNavigationFunctions()
    {
        String query = "SELECT m.id, m.measure " +
                "          FROM (VALUES " +
                "                   (1, 10), " +
                "                   (2, 20), " +
                "                   (3, 30)" +
                "               ) t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES %s AS measure " +
                "                   ALL ROWS PER MATCH " +
                "                   PATTERN (A+) " +
                "                   DEFINE A AS true " +
                "                ) AS m";

        // defaults to RUNNING LAST(value)
        assertThat(assertions.query(format(query, "value")))
                .matches("VALUES " +
                        "     (1, 10), " +
                        "     (2, 20), " +
                        "     (3, 30) ");

        // defaults to RUNNING LAST(value)
        assertThat(assertions.query(format(query, "LAST(value)")))
                .matches("VALUES " +
                        "     (1, 10), " +
                        "     (2, 20), " +
                        "     (3, 30) ");

        assertThat(assertions.query(format(query, "RUNNING LAST(value)")))
                .matches("VALUES " +
                        "     (1, 10), " +
                        "     (2, 20), " +
                        "     (3, 30) ");

        assertThat(assertions.query(format(query, "FINAL LAST(value)")))
                .matches("VALUES " +
                        "     (1, 30), " +
                        "     (2, 30), " +
                        "     (3, 30) ");

        // defaults to RUNNING FIRST(value)
        assertThat(assertions.query(format(query, "FIRST(value)")))
                .matches("VALUES " +
                        "     (1, 10), " +
                        "     (2, 10), " +
                        "     (3, 10) ");

        assertThat(assertions.query(format(query, "RUNNING FIRST(value)")))
                .matches("VALUES " +
                        "     (1, 10), " +
                        "     (2, 10), " +
                        "     (3, 10) ");

        assertThat(assertions.query(format(query, "FINAL FIRST(value)")))
                .matches("VALUES " +
                        "     (1, 10), " +
                        "     (2, 10), " +
                        "     (3, 10) ");

        // with logical offset
        assertThat(assertions.query(format(query, "FINAL LAST(value, 2)")))
                .matches("VALUES " +
                        "     (1, 10), " +
                        "     (2, 10), " +
                        "     (3, 10) ");

        assertThat(assertions.query(format(query, "FIRST(value, 2)")))
                .matches("VALUES " +
                        "     (1, 30), " +
                        "     (2, 30), " +
                        "     (3, 30) ");

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
                        "     (1, null), " +
                        "     (2, 10), " +
                        "     (3, 20) ");

        // defaults to NEXT(RUNNING LAST(value), 1)
        assertThat(assertions.query(format(query, "NEXT(value)")))
                .matches("VALUES " +
                        "     (1, 20), " +
                        "     (2, 30), " +
                        "     (3, null) ");

        assertThat(assertions.query(format(query, "NEXT(FIRST(value), 2)")))
                .matches("VALUES " +
                        "     (1, 30), " +
                        "     (2, 30), " +
                        "     (3, 30) ");

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

        // lookup to rows not included in match, but within partition
        // rows 3, 4 are rows of the only match. Row 4 is matched to 'B' and it is the starting point to following navigation expressions
        query = "SELECT m.measure " +
                "          FROM (VALUES " +
                "                   (1, 10), " +
                "                   (2, 20), " +
                "                   (3, 30), " +
                "                   (4, 30), " +
                "                   (5, 40)" +
                "               ) t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES %s AS measure " +
                "                   ONE ROW PER MATCH " +
                "                   PATTERN (A B) " +
                "                   DEFINE B AS B.value = PREV(B.value) " +
                "                ) AS m";

        // row 0 - out of partition bounds (before partition start)
        assertThat(assertions.query(format(query, "PREV(B.value, 4)")))
                .matches("VALUES CAST(null AS integer) ");

        // row 1 (out of match, but within partition - value can be accessed)
        assertThat(assertions.query(format(query, "PREV(B.value, 3)")))
                .matches("VALUES 10 ");

        // row 2 (out of match, but within partition - value can be accessed)
        assertThat(assertions.query(format(query, "PREV(B.value, 2)")))
                .matches("VALUES 20 ");

        // row 5 (out of match, but within partition - value can be accessed)
        assertThat(assertions.query(format(query, "NEXT(B.value, 1)")))
                .matches("VALUES 40 ");

        // row 6 - out of partition bounds (after partition end)
        assertThat(assertions.query(format(query, "NEXT(B.value, 2)")))
                .matches("VALUES CAST(null AS integer) ");
    }

    @Test
    public void testClassifierFunctionPastCurrentRow()
    {
        // NEXT(CLASSIFIER()) gets the label mapped to the row after current row in running semantics.
        // in MEASURES clause, labels for rows exceeding current row can be accessed, because the match is complete.
        assertThat(assertions.query("SELECT m.id, m.value, m.label, m.next_label " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 80) " +
                "               ) t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            CLASSIFIER() AS label, " +
                "                            NEXT(CLASSIFIER()) AS next_label " +
                "                   ALL ROWS PER MATCH " +
                "                   PATTERN (A B+ C+) " +
                "                   DEFINE " +
                "                            B AS B.value < PREV(B.value), " +
                "                            C AS C.value > PREV(C.value) " +
                "                ) AS m"))
                .matches("VALUES " +
                        "     (1, 90, VARCHAR 'A', VARCHAR 'B'), " +
                        "     (2, 80, 'B', 'B'), " +
                        "     (3, 70, 'B', 'C'), " +
                        "     (4, 80, 'C', null) ");

        // in DEFINE clause, labels for rows exceeding current row are not available, because no rows are matched beyond current row.
        // when such labels are accessed, the result is null.
        // in the following example, NEXT(CLASSIFIER()) always returns null, and so the matching condition for label 'A' is never true
        assertThat(assertions.query("SELECT m.id, m.val " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 80) " +
                "               ) t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES value AS val " +
                "                   ALL ROWS PER MATCH " +
                "                   PATTERN (A+) " +
                "                   DEFINE A AS NEXT(CLASSIFIER()) = 'A' " +
                "                ) AS m"))
                .returnsEmptyResult();
    }

    @Test
    public void testCaseSensitiveLabels()
    {
        assertThat(assertions.query("SELECT m.id, m.label " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 80) " +
                "               ) t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES CLASSIFIER() AS label " +
                "                   ALL ROWS PER MATCH " +
                "                   PATTERN (a \"b\"+ C+) " +
                "                   DEFINE " +
                "                            \"b\" AS \"b\".value < PREV(\"b\".value), " +
                "                            C AS C.value > PREV(C.value) " +
                "                ) AS m"))
                .matches("VALUES " +
                        "     (1, VARCHAR 'A'), " +
                        "     (2, 'b'), " +
                        "     (3, 'b'), " +
                        "     (4, 'C') ");
    }

    @Test
    public void testScalarFunctions()
    {
        // scalar functions and operators used in MEASURES and DEFINE clauses
        assertThat(assertions.query("SELECT m.id, m.label " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 60) " +
                "               ) t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES CAST(lower(LAST(CLASSIFIER())) || '_label' AS varchar(7)) AS label " +
                "                   ALL ROWS PER MATCH " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS B.value + 10 < abs(PREV (B.value)) " +
                "                ) AS m"))
                .matches("VALUES " +
                        "     (2, 'a_label'), " +
                        "     (3, 'b_label') ");
    }

    @Test
    public void testRunningAndFinal()
    {
        assertThat(assertions.query("SELECT id, label, final_label, running_value,final_value, A_running_value, A_final_value, B_running_value, B_final_value, C_running_value, C_final_value " +
                "          FROM (VALUES " +
                "                   (1, 90), " +
                "                   (2, 80), " +
                "                   (3, 70), " +
                "                   (4, 100), " +
                "                   (5, 200) " +
                "               ) t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            CLASSIFIER() AS label, " +
                "                            FINAL LAST(CLASSIFIER()) AS final_label, " +
                "                            RUNNING LAST(value) AS running_value, " +
                "                            FINAL LAST(value) AS final_value," +
                "                            RUNNING LAST(A.value) AS A_running_value, " +
                "                            FINAL LAST(A.value) AS A_final_value, " +
                "                            RUNNING LAST(B.value) AS B_running_value, " +
                "                            FINAL LAST(B.value) AS B_final_value, " +
                "                            RUNNING LAST(C.value) AS C_running_value, " +
                "                            FINAL LAST(C.value) AS C_final_value " +
                "                   ALL ROWS PER MATCH " +
                "                   PATTERN (A B+ C+) " +
                "                   DEFINE " +
                "                            B AS B.value < PREV (B.value), " +
                "                            C AS C.value > PREV (C.value) " +
                "                ) "))
                .matches("VALUES " +
                        "     (1, VARCHAR 'A', VARCHAR 'C', 90, 200, 90, 90, null, 70, null, 200), " +
                        "     (2, 'B', 'C',  80, 200, 90, 90, 80, 70, null, 200), " +
                        "     (3, 'B', 'C',  70, 200, 90, 90, 70, 70, null, 200), " +
                        "     (4, 'C', 'C', 100, 200, 90, 90, 70, 70,  100, 200), " +
                        "     (5, 'C', 'C', 200, 200, 90, 90, 70, 70,  200, 200) ");
    }

    @Test
    public void testPartitioningAndOrdering()
    {
        // multiple partitions, unordered input
        assertThat(assertions.query("SELECT m.part as partition, m.id AS row_id, m.match, m.val, m.label " +
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
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY part " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            match_number() AS match, " +
                "                            PREV(RUNNING LAST(value)) AS val, " +
                "                            classifier() AS label " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (B+) " +
                "                   DEFINE B AS B.value < PREV (B.value) " +
                "                ) AS m"))
                .matches("VALUES " +
                        "     ('p1', 2, CAST(1 AS bigint), 90, VARCHAR 'B'), " +
                        "     ('p1', 3, 1, 80, 'B'), " +
                        "     ('p1', 6, 2, 90, 'B'), " +
                        "     ('p2', 3, 1, 20, 'B') ");

        // empty input
        assertThat(assertions.query("SELECT m.part as partition, m.id AS row_id, m.match, m.val, m.label " +
                "          FROM (SELECT * FROM (VALUES (1, 'p1', 90)) WHERE false) t(id, part, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY part " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            match_number() AS match, " +
                "                            PREV(RUNNING LAST(value)) AS val, " +
                "                            classifier() AS label " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (B+) " +
                "                   DEFINE B AS B.value < PREV (B.value) " +
                "                ) AS m"))
                .returnsEmptyResult();

        // no partitioning, unordered input
        assertThat(assertions.query("SELECT m.id AS row_id, m.match, m.val, m.label " +
                "          FROM (VALUES " +
                "                   (5, 10), " +
                "                   (2, 90), " +
                "                   (1, 80), " +
                "                   (4, 20), " +
                "                   (3, 30) " +
                "                )t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES " +
                "                            match_number() AS match, " +
                "                            RUNNING LAST(value) AS val, " +
                "                            classifier() AS label " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (B+) " +
                "                   DEFINE B AS B.value < PREV (B.value) " +
                "                ) AS m"))
                .matches("VALUES " +
                        "     (3, CAST(1 AS bigint), 30, VARCHAR 'B'), " +
                        "     (4, 1, 20, 'B'), " +
                        "     (5, 1, 10, 'B') ");

        // no measures
        assertThat(assertions.query("SELECT m.id AS row_id, m.part as partition" +
                "          FROM (VALUES " +
                "                   (5, 'p2', 10), " +
                "                   (2, 'p1', 90), " +
                "                   (1, 'p1', 80), " +
                "                   (4, 'p2', 20), " +
                "                   (3, 'p1', 30) " +
                "                )t(id, part, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY part " +
                "                   ORDER BY id " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (B+) " +
                "                   DEFINE B AS B.value < PREV (B.value) " +
                "                ) AS m"))
                .matches("VALUES " +
                        "     (3, 'p1'), " +
                        "     (5, 'p2') ");
    }

    @Test
    public void testOutputLayout()
    {
        String query = "SELECT * " +
                "          FROM (VALUES ('ordering', 'partitioning', 90) " +
                "               ) t(id, part, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY part " +
                "                   ORDER BY id " +
                "                   MEASURES CLASSIFIER() AS classy " +
                "                   %s " + // ROWS PER MATCH
                "                   PATTERN (A) " +
                "                   DEFINE A AS true " +
                "                ) AS m";

        // ALL ROWS PER MATCH: PARTITION BY columns, ORDER BY columns, measures, remaining input columns
        assertThat(assertions.query(format(query, "ALL ROWS PER MATCH")))
                .matches("VALUES ('partitioning', 'ordering', VARCHAR 'A', 90) ");

        // ONE ROW PER MATCH: PARTITION BY columns, measures
        assertThat(assertions.query(format(query, "ONE ROW PER MATCH")))
                .matches("VALUES ('partitioning', VARCHAR 'A') ");

        // duplicate ORDER BY symbol
        assertThat(assertions.query("SELECT * " +
                "          FROM (VALUES ('ordering', 'partitioning', 90) " +
                "               ) t(id, part, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY part " +
                "                   ORDER BY id ASC, id DESC " +
                "                   MEASURES CLASSIFIER() AS classy " +
                "                   ALL ROWS PER MATCH " +
                "                   PATTERN (A) " +
                "                   DEFINE A AS true " +
                "                ) AS m"))
                .matches("VALUES ('partitioning', 'ordering', 'ordering', VARCHAR 'A', 90) ");
    }

    @Test
    public void testMultipleMatchRecognize()
    {
        assertThat(assertions.query("SELECT first.classy, second.classy, third.classy " +
                "          FROM (VALUES (1), (2), (3)) t(id) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES CLASSIFIER() AS classy " +
                "                   ALL ROWS PER MATCH " +
                "                   PATTERN (A) " +
                "                   DEFINE A AS true " +
                "                ) AS first, " +
                "                (VALUES (10), (20) ) t(id) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES CLASSIFIER() AS classy " +
                "                   ALL ROWS PER MATCH " +
                "                   PATTERN (B) " +
                "                   DEFINE B AS true " +
                "                ) AS second, " +
                "                (VALUES 100) t(id) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES CLASSIFIER() AS classy " +
                "                   ALL ROWS PER MATCH " +
                "                   PATTERN (C) " +
                "                   DEFINE C AS true " +
                "                ) AS third "))
                .matches("VALUES " +
                        "     (VARCHAR 'A', VARCHAR 'B', VARCHAR 'C')," +
                        "     ('A', 'B', 'C')," +
                        "     ('A', 'B', 'C')," +
                        "     ('A', 'B', 'C')," +
                        "     ('A', 'B', 'C')," +
                        "     ('A', 'B', 'C') ");
    }

    @Test
    public void testSubqueries()
    {
        String query = "SELECT m.val " +
                "          FROM (VALUES " +
                "                   (1, 100), " +
                "                   (2, 200), " +
                "                   (3, 300), " +
                "                   (4, 400) " +
                "               ) t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES %s AS val " +
                "                   ONE ROW PER MATCH " +
                "                   AFTER MATCH SKIP TO NEXT ROW " +
                "                   PATTERN (A+) " +
                "                   DEFINE A AS %s " +
                "                ) AS m";

        assertThat(assertions.query(format(query, "(SELECT 'x')", "(SELECT true)")))
                .matches("VALUES " +
                        "     ('x'), " +
                        "     ('x'), " +
                        "     ('x'), " +
                        "     ('x') ");

        // subquery nested in navigation
        assertThat(assertions.query(format(query, "FINAL LAST(A.value + (SELECT 1000))", "FIRST(A.value < 0 OR (SELECT true))")))
                .matches("VALUES " +
                        "     (1400), " +
                        "     (1400), " +
                        "     (1400), " +
                        "     (1400) ");

        // IN-predicate: value and value list without column references
        assertThat(assertions.query(format(query, "LAST(A.id < 0 OR 1 IN (SELECT 1))", "FIRST(A.id > 0 AND 1 IN (SELECT 1))")))
                .matches("VALUES " +
                        "     (true), " +
                        "     (true), " +
                        "     (true), " +
                        "     (true) ");

        // IN-predicate: unlabeled column reference in value
        assertThat(assertions.query(format(query, "FIRST(id % 2 IN (SELECT 0))", "FIRST(value * 0 IN (SELECT 0))")))
                .matches("VALUES " +
                        "     (false), " +
                        "     (true), " +
                        "     (false), " +
                        "     (true) ");

        // EXISTS-predicate
        assertThat(assertions.query(format(query, "LAST(A.value < 0 OR EXISTS(SELECT 1))", "FIRST(A.value < 0 OR EXISTS(SELECT 1))")))
                .matches("VALUES " +
                        "     (true), " +
                        "     (true), " +
                        "     (true), " +
                        "     (true) ");
    }

    @Test
    public void testInPredicateWithoutSubquery()
    {
        String query = "SELECT m.val " +
                "          FROM (VALUES " +
                "                   (1, 100), " +
                "                   (2, 200), " +
                "                   (3, 300), " +
                "                   (4, 400) " +
                "               ) t(id, value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY id " +
                "                   MEASURES %s AS val " +
                "                   ONE ROW PER MATCH " +
                "                   AFTER MATCH SKIP TO NEXT ROW " +
                "                   PATTERN (A+) " +
                "                   DEFINE A AS true " +
                "                ) AS m";

        // navigations and labeled column references
        assertThat(assertions.query(format(query, "FIRST(A.value) IN (300, LAST(A.value))")))
                .matches("VALUES " +
                        "     (false), " +
                        "     (false), " +
                        "     (true), " +
                        "     (true) ");

        // CLASSIFIER()
        assertThat(assertions.query(format(query, "CLASSIFIER() IN ('X', lower(CLASSIFIER()))")))
                .matches("VALUES " +
                        "     (false), " +
                        "     (false), " +
                        "     (false), " +
                        "     (false) ");

        // MATCH_NUMBER()
        assertThat(assertions.query(format(query, "MATCH_NUMBER() IN (0, MATCH_NUMBER())")))
                .matches("VALUES " +
                        "     (true), " +
                        "     (true), " +
                        "     (true), " +
                        "     (true) ");
    }

    @Test
    public void testPotentiallyExponentialMatch()
    {
        // the following example can produce exponential number of different match attempts.
        // because equivalent threads in Matcher are detected and pruned, it is solved in linear time.
        assertThat(assertions.query("SELECT m.classy " +
                "          FROM (VALUES (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1)) t(value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES CLASSIFIER() AS classy " +
                "                   PATTERN ((A+)+ B) " +
                "                   DEFINE " +
                "                           A AS value = 1, " +
                "                           B AS value = 2 " +
                "                ) AS m"))
                .returnsEmptyResult();
    }

    @Test
    public void testExponentialMatch()
    {
        // the match `B B B B B B B B LAST` is the last one checked out of over 2^9 possible matches
        assertThat(assertions.query("SELECT m.classy " +
                "          FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9)) t(value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY value " +
                "                   MEASURES CLASSIFIER() AS classy " +
                "                   ALL ROWS PER MATCH " +
                "                   PATTERN ((A | B)+ LAST) " +
                "                   DEFINE LAST AS FIRST(CLASSIFIER()) = 'B' AND " +
                "                                  FIRST(CLASSIFIER(), 1) = 'B' AND " +
                "                                  FIRST(CLASSIFIER(), 2) = 'B' AND " +
                "                                  FIRST(CLASSIFIER(), 3) = 'B' AND " +
                "                                  FIRST(CLASSIFIER(), 4) = 'B' AND " +
                "                                  FIRST(CLASSIFIER(), 5) = 'B' AND " +
                "                                  FIRST(CLASSIFIER(), 6) = 'B' AND " +
                "                                  FIRST(CLASSIFIER(), 7) = 'B' " +

                "                ) AS m"))
                .matches("VALUES (VARCHAR 'B'), ('B'), ('B'), ('B'), ('B'), ('B'), ('B'), ('B'), ('LAST') ");
    }

    @Test
    public void testProperties()
    {
        assertThat(assertions.query("""
                WITH
                    t(a, b) AS (VALUES (1, 1)),
                    u AS (SELECT * FROM t WHERE b = 1)
                SELECT *
                FROM u
                  MATCH_RECOGNIZE (
                   PARTITION BY a
                   PATTERN (X)
                   DEFINE X AS (b = 1))
                """))
                .matches("VALUES 1");
    }

    @Test
    public void testKillThread()
    {
        assertThat(assertions.query("""
                SELECT *
                FROM (VALUES 1, 2, 3, 4, 5)
                  MATCH_RECOGNIZE (
                      MEASURES 'foo' AS foo
                      PATTERN ((Y?){2,})
                      DEFINE Y AS true)
                """))
                .matches("VALUES 'foo'");
    }
}
