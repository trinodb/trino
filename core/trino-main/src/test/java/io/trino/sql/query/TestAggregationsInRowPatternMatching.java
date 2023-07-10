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

import static org.assertj.core.api.Assertions.assertThat;

public class TestAggregationsInRowPatternMatching
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
    public void testSimpleQuery()
    {
        // aggregation argument coerced to BIGINT
        assertThat(assertions.query("""
                SELECT m.id, m.running_sum
                FROM (VALUES
                         (1),
                         (2),
                         (3),
                         (4),
                         (5),
                         (6),
                         (7),
                         (8)
                     ) t(id)
                       MATCH_RECOGNIZE (
                         ORDER BY id
                         MEASURES RUNNING sum(id) AS running_sum
                         ALL ROWS PER MATCH
                         AFTER MATCH SKIP PAST LAST ROW
                         PATTERN (A*)
                         DEFINE A AS true
                      ) AS m
                """))
                .matches("""
                        VALUES
                             (1, BIGINT '1'),
                             (2, 3),
                             (3, 6),
                             (4, 10),
                             (5, 15),
                             (6, 21),
                             (7, 28),
                             (8, 36)
                        """);

        assertThat(assertions.query("""
                SELECT m.id, m.running_labels
                FROM (VALUES
                         (1),
                         (2),
                         (3),
                         (4),
                         (5),
                         (6),
                         (7),
                         (8)
                     ) t(id)
                       MATCH_RECOGNIZE (
                         ORDER BY id
                         MEASURES RUNNING array_agg(CLASSIFIER(A)) AS running_labels
                         ALL ROWS PER MATCH
                         AFTER MATCH SKIP PAST LAST ROW
                         PATTERN (A*)
                         DEFINE A AS true
                      ) AS m
                """))
                .matches("""
                        VALUES
                             (1, CAST(ARRAY['A'] AS array(varchar))),
                             (2, ARRAY['A', 'A']),
                             (3, ARRAY['A', 'A', 'A']),
                             (4, ARRAY['A', 'A', 'A', 'A']),
                             (5, ARRAY['A', 'A', 'A', 'A', 'A']),
                             (6, ARRAY['A', 'A', 'A', 'A', 'A', 'A']),
                             (7, ARRAY['A', 'A', 'A', 'A', 'A', 'A', 'A']),
                             (8, ARRAY['A', 'A', 'A', 'A', 'A', 'A', 'A', 'A'])
                        """);

        assertThat(assertions.query("""
                SELECT m.id, m.running_labels
                FROM (VALUES
                         (1),
                         (2),
                         (3),
                         (4),
                         (5),
                         (6),
                         (7),
                         (8)
                     ) t(id)
                       MATCH_RECOGNIZE (
                         ORDER BY id
                         MEASURES concat_ws('', RUNNING array_agg(lower(CLASSIFIER(U)))) AS running_labels
                         ALL ROWS PER MATCH
                         AFTER MATCH SKIP PAST LAST ROW
                         PATTERN (M A X X T C H "!")
                         SUBSET U = (M, A, T, C, H, "!")
                         DEFINE M AS true
                      ) AS m
                """))
                .matches("""
                        VALUES
                             (1, CAST('m' AS varchar)),
                             (2, 'ma'),
                             (3, 'ma'),
                             (4, 'ma'),
                             (5, 'mat'),
                             (6, 'matc'),
                             (7, 'match'),
                             (8, 'match!')
                        """);
    }

    @Test
    public void testPartitioning()
    {
        // multiple partitions, unordered input. computing rolling sum for each partition
        assertThat(assertions.query("""
                SELECT m.part as partition, m.id AS row_id, m.running_sum
                FROM (VALUES
                         (1, 'p1', 1),
                         (2, 'p1', 1),
                         (6, 'p1', 1),
                         (2, 'p2', 10),
                         (2, 'p3', 100),
                         (1, 'p3', 100),
                         (3, 'p1', 1),
                         (4, 'p1', 1),
                         (5, 'p1', 1),
                         (1, 'p2', 10),
                         (3, 'p3', 100),
                         (3, 'p2', 10)
                     ) t(id, part, value)
                       MATCH_RECOGNIZE (
                         PARTITION BY part
                         ORDER BY id
                         MEASURES RUNNING sum(value) AS running_sum
                         ALL ROWS PER MATCH
                         AFTER MATCH SKIP PAST LAST ROW
                         PATTERN (B+)
                         DEFINE B AS true
                      ) AS m
                """))
                .matches("""
                        VALUES
                             ('p1', 1, BIGINT '1'),
                             ('p1', 2, 2),
                             ('p1', 3, 3),
                             ('p1', 4, 4),
                             ('p1', 5, 5),
                             ('p1', 6, 6),
                             ('p2', 1, 10),
                             ('p2', 2, 20),
                             ('p2', 3, 30),
                             ('p3', 1, 100),
                             ('p3', 2, 200),
                             ('p3', 3, 300)
                        """);

        // multiple partitions, unordered input. multiple matches in each partition. computing rolling sum for each match
        assertThat(assertions.query("""
                SELECT m.part as partition, m.match_no, m.id AS row_id, m.running_sum
                FROM (VALUES
                   (1, 'p1', 1),
                   (2, 'p1', 1),
                   (6, 'p1', 1),
                   (2, 'p2', 10),
                   (2, 'p3', 100),
                   (1, 'p3', 100),
                   (3, 'p1', 1),
                   (4, 'p1', 1),
                   (5, 'p1', 1),
                   (1, 'p2', 10),
                   (3, 'p3', 100),
                   (3, 'p2', 10)
                ) t(id, part, value)
                 MATCH_RECOGNIZE (
                   PARTITION BY part
                   ORDER BY id
                   MEASURES
                           RUNNING sum(value) AS running_sum,
                           MATCH_NUMBER() AS match_no
                   ALL ROWS PER MATCH
                   AFTER MATCH SKIP TO NEXT ROW
                   PATTERN (B+)
                   DEFINE B AS true
                ) AS m
                """))
                .matches("""
                        VALUES
                             ('p1', BIGINT '1', 1, BIGINT '1'),
                             ('p1', 1, 2, 2),
                             ('p1', 1, 3, 3),
                             ('p1', 1, 4, 4),
                             ('p1', 1, 5, 5),
                             ('p1', 1, 6, 6),
                             ('p1', 2, 2, 1),
                             ('p1', 2, 3, 2),
                             ('p1', 2, 4, 3),
                             ('p1', 2, 5, 4),
                             ('p1', 2, 6, 5),
                             ('p1', 3, 3, 1),
                             ('p1', 3, 4, 2),
                             ('p1', 3, 5, 3),
                             ('p1', 3, 6, 4),
                             ('p1', 4, 4, 1),
                             ('p1', 4, 5, 2),
                             ('p1', 4, 6, 3),
                             ('p1', 5, 5, 1),
                             ('p1', 5, 6, 2),
                             ('p1', 6, 6, 1),
                             ('p2', 1, 1, 10),
                             ('p2', 1, 2, 20),
                             ('p2', 1, 3, 30),
                             ('p2', 2, 2, 10),
                             ('p2', 2, 3, 20),
                             ('p2', 3, 3, 10),
                             ('p3', 1, 1, 100),
                             ('p3', 1, 2, 200),
                             ('p3', 1, 3, 300),
                             ('p3', 2, 2, 100),
                             ('p3', 2, 3, 200),
                             ('p3', 3, 3, 100)
                        """);
    }

    @Test
    public void testTentativeLabelMatch()
    {
        assertThat(assertions.query("""
                SELECT m.id, m.classy, m.running_avg_B
                FROM (VALUES
                         (1, 4),
                         (2, 6),
                         (3, 0)
                     ) t(id, value)
                       MATCH_RECOGNIZE (
                         ORDER BY id
                         MEASURES
                                 RUNNING avg(B.value) AS running_avg_B,
                                 CLASSIFIER() AS classy
                         ALL ROWS PER MATCH
                         AFTER MATCH SKIP PAST LAST ROW
                         PATTERN ((A | B)*)
                         DEFINE A AS avg(B.value) = 5
                      ) AS m
                """))
                .matches("""
                        VALUES
                             (1, VARCHAR 'B', 4e0),
                             (2, 'B', 5e0),
                             (3, 'A', 5e0)
                        """);

        assertThat(assertions.query("""
                SELECT m.id, m.classy, m.running_avg_A
                FROM (VALUES
                         (1, 4),
                         (2, 6),
                         (3, 0),
                         (4, 5)
                ) t(id, value)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES
                            RUNNING avg(A.value) AS running_avg_A,
                            CLASSIFIER() AS classy
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN ((A | B)*)
                    DEFINE A AS avg(A.value) = 5
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (1, VARCHAR 'B', null),
                             (2, 'B', null),
                             (3, 'B', null),
                             (4, 'A', 5e0)
                        """);

        // need to drop the tentative match of label `A`.
        assertThat(assertions.query("""
                SELECT m.part as partition, m.id AS row_id, m.running_sum
                FROM (VALUES
                         (1, 'p1', 1),
                         (2, 'p1', 1),
                         (6, 'p1', 1),
                         (2, 'p2', 10),
                         (2, 'p3', 100),
                         (1, 'p3', 100),
                         (3, 'p1', 1),
                         (4, 'p1', 1),
                         (5, 'p1', 1),
                         (1, 'p2', 10),
                         (3, 'p3', 100),
                         (3, 'p2', 10)
                ) t(id, part, value)
                  MATCH_RECOGNIZE (
                    PARTITION BY part
                    ORDER BY id
                    MEASURES RUNNING sum(value) AS running_sum
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (B (A | B) B)
                    DEFINE A AS sum(value) > 1000
                 ) AS m
                """))
                .matches("""
                        VALUES
                             ('p1', 1, BIGINT '1'),
                             ('p1', 2, 2),
                             ('p1', 3, 3),
                             ('p1', 4, 1),
                             ('p1', 5, 2),
                             ('p1', 6, 3),
                             ('p2', 1, 10),
                             ('p2', 2, 20),
                             ('p2', 3, 30),
                             ('p3', 1, 100),
                             ('p3', 2, 200),
                             ('p3', 3, 300)
                        """);

        // need to drop the tentative match of label `A`.
        assertThat(assertions.query("""
                SELECT m.part as partition, m.id AS row_id, m.classy, m.running_sum
                FROM (VALUES
                         (1, 'p1', 1),
                         (2, 'p1', 1),
                         (3, 'p1', 1),
                         (4, 'p1', 1),
                         (5, 'p1', 1),
                         (1, 'p2', 2),
                         (2, 'p2', 2),
                         (3, 'p2', 2),
                         (4, 'p2', 2),
                         (5, 'p2', 2)
                ) t(id, part, value)
                  MATCH_RECOGNIZE (
                    PARTITION BY part
                    ORDER BY id
                    MEASURES
                            RUNNING sum(value) AS running_sum,
                            CLASSIFIER() AS classy
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN ((A | B)*)
                    DEFINE A AS sum(value) > 4
                 ) AS m
                """))
                .matches("""
                        VALUES
                             ('p1', 1, VARCHAR 'B', BIGINT '1'),
                             ('p1', 2, 'B', 2),
                             ('p1', 3, 'B', 3),
                             ('p1', 4, 'B', 4),
                             ('p1', 5, 'A', 5),
                             ('p2', 1, 'B', 2),
                             ('p2', 2, 'B', 4),
                             ('p2', 3, 'A', 6),
                             ('p2', 4, 'A', 8),
                             ('p2', 5, 'A', 10)
                        """);

        // need to drop the tentative match of label `A`.
        assertThat(assertions.query("""
                SELECT m.part as partition, m.id AS row_id, m.running_sum
                FROM (VALUES
                         (1, 'p1', 1),
                         (2, 'p1', 1),
                         (6, 'p1', 1),
                         (2, 'p2', 10),
                         (2, 'p3', 100),
                         (1, 'p3', 100),
                         (3, 'p1', 1),
                         (4, 'p1', 1),
                         (5, 'p1', 1),
                         (1, 'p2', 10),
                         (3, 'p3', 100),
                         (3, 'p2', 10)
                ) t(id, part, value)
                  MATCH_RECOGNIZE (
                    PARTITION BY part
                    ORDER BY id
                    MEASURES RUNNING sum(value) AS running_sum
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (B (A | B) B)
                    DEFINE A AS arbitrary(value) > 1000
                 ) AS m
                """))
                .matches("""
                        VALUES
                             ('p1', 1, BIGINT '1'),
                             ('p1', 2, 2),
                             ('p1', 3, 3),
                             ('p1', 4, 1),
                             ('p1', 5, 2),
                             ('p1', 6, 3),
                             ('p2', 1, 10),
                             ('p2', 2, 20),
                             ('p2', 3, 30),
                             ('p3', 1, 100),
                             ('p3', 2, 200),
                             ('p3', 3, 300)
                        """);

        // need to drop the tentative match of label `A`.
        assertThat(assertions.query("""
                SELECT m.part as partition, m.id AS row_id, m.classy, m.running_max
                FROM (VALUES
                         (1, 'p1', 1),
                         (2, 'p1', 2),
                         (3, 'p1', 3),
                         (4, 'p1', 4),
                         (5, 'p1', 5),
                         (1, 'p2', 2),
                         (2, 'p2', 4),
                         (3, 'p2', 6),
                         (4, 'p2', 8),
                         (5, 'p2', 10)
                ) t(id, part, value)
                  MATCH_RECOGNIZE (
                    PARTITION BY part
                    ORDER BY id
                    MEASURES
                            RUNNING max(value) AS running_max,
                            CLASSIFIER() AS classy
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN ((A | B)*)
                    DEFINE A AS max(value) > 4
                 ) AS m
                """))
                .matches("""
                        VALUES
                             ('p1', 1, VARCHAR 'B', 1),
                             ('p1', 2, 'B', 2),
                             ('p1', 3, 'B', 3),
                             ('p1', 4, 'B', 4),
                             ('p1', 5, 'A', 5),
                             ('p2', 1, 'B', 2),
                             ('p2', 2, 'B', 4),
                             ('p2', 3, 'A', 6),
                             ('p2', 4, 'A', 8),
                             ('p2', 5, 'A', 10)
                        """);
    }

    @Test
    public void testTentativeLabelMatchWithRuntimeEvaluatedAggregationArgument()
    {
        // need to drop the tentative match of label `A`.
        assertThat(assertions.query("""
                SELECT m.part as partition, m.id AS row_id, m.classy, m.running_max
                FROM (VALUES
                         (1, 'p1', 1),
                         (2, 'p1', 2),
                         (3, 'p1', 3),
                         (4, 'p1', 4),
                         (5, 'p1', 5),
                         (1, 'p2', 2),
                         (2, 'p2', 4),
                         (3, 'p2', 6),
                         (4, 'p2', 8),
                         (5, 'p2', 10)
                ) t(id, part, value)
                  MATCH_RECOGNIZE (
                    PARTITION BY part
                    ORDER BY id
                    MEASURES
                            RUNNING max(value) AS running_max,
                            CLASSIFIER() AS classy
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN ((A | B)*)
                    DEFINE A AS max(value + MATCH_NUMBER()) > 5
                 ) AS m
                """))
                .matches("""
                        VALUES
                             ('p1', 1, VARCHAR 'B', 1),
                             ('p1', 2, 'B', 2),
                             ('p1', 3, 'B', 3),
                             ('p1', 4, 'B', 4),
                             ('p1', 5, 'A', 5),
                             ('p2', 1, 'B', 2),
                             ('p2', 2, 'B', 4),
                             ('p2', 3, 'A', 6),
                             ('p2', 4, 'A', 8),
                             ('p2', 5, 'A', 10)
                        """);
    }

    @Test
    public void testAggregationArguments()
    {
        // aggregation argument combining source data(`value`) and runtime-evaluated data (`CLASSIFIER()`)
        assertThat(assertions.query("""
                SELECT m.part, m.id, m.measure
                FROM (VALUES
                         ('p1', 1, 'a'),
                         ('p1', 2, 'b'),
                         ('p1', 3, 'c'),
                         ('p1', 4, 'd'),
                         ('p1', 5, 'e'),
                         ('p1', 6, 'f'),
                         ('p2', 1, 'g'),
                         ('p2', 2, 'h'),
                         ('p2', 3, 'i'),
                         ('p3', 1, 'j'),
                         ('p3', 2, 'k'),
                         ('p3', 3, 'l')
                ) t(part, id, value)
                  MATCH_RECOGNIZE (
                    PARTITION BY part
                    ORDER BY id
                    MEASURES array_agg(value || CLASSIFIER()) AS measure
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (X Y Z+)
                    DEFINE X AS true
                 ) AS m
                """))
                .matches("""
                        VALUES
                             ('p1', 1, ARRAY[VARCHAR 'aX']),
                             ('p1', 2, ARRAY['aX', 'bY']),
                             ('p1', 3, ARRAY['aX', 'bY', 'cZ']),
                             ('p1', 4, ARRAY['aX', 'bY', 'cZ', 'dZ']),
                             ('p1', 5, ARRAY['aX', 'bY', 'cZ', 'dZ', 'eZ']),
                             ('p1', 6, ARRAY['aX', 'bY', 'cZ', 'dZ', 'eZ', 'fZ']),
                             ('p2', 1, ARRAY['gX']),
                             ('p2', 2, ARRAY['gX', 'hY']),
                             ('p2', 3, ARRAY['gX', 'hY', 'iZ']),
                             ('p3', 1, ARRAY['jX']),
                             ('p3', 2, ARRAY['jX', 'kY']),
                             ('p3', 3, ARRAY['jX', 'kY', 'lZ'])
                        """);

        // duplicate input symbol (`value`) in runtime-evaluated aggregation argument
        assertThat(assertions.query("""
                SELECT m.id, m.measure
                FROM (VALUES
                         (1, 'a'),
                         (2, 'b'),
                         (3, 'c')
                ) t(id, value)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES array_agg(value || value || CLASSIFIER()) AS measure
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (X Y Z)
                    DEFINE X AS true
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (1, ARRAY[VARCHAR 'aaX']),
                             (2, ARRAY['aaX', 'bbY']),
                             (3, ARRAY['aaX', 'bbY', 'ccZ'])
                        """);

        // subquery in aggregation argument
        assertThat(assertions.query("""
                SELECT m.id, m.measure_1, m.measure_2, m.measure_3
                FROM (VALUES
                         (1, 'a'),
                         (2, 'b'),
                         (3, 'c')
                ) t(id, value)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES
                            array_agg('X' || (SELECT 'Y')) AS measure_1,
                            array_agg('X' IN (SELECT 'Y')) AS measure_2,
                            array_agg(EXISTS (SELECT 'Y')) AS measure_3
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (X Y Z)
                    DEFINE X AS true
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (1, ARRAY[VARCHAR 'XY'], ARRAY[false], ARRAY[true]),
                             (2, ARRAY['XY', 'XY'], ARRAY[false, false], ARRAY[true, true]),
                             (3, ARRAY['XY', 'XY', 'XY'], ARRAY[false, false, false], ARRAY[true, true, true])
                        """);

        // subquery in runtime-evaluated aggregation argument
        assertThat(assertions.query("""
                SELECT m.id, m.measure_1, m.measure_2, m.measure_3
                FROM (VALUES
                         (1, 'a'),
                         (2, 'b'),
                         (3, 'c')
                ) t(id, value)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES
                            array_agg(CLASSIFIER() || (SELECT 'A')) AS measure_1,
                            array_agg(MATCH_NUMBER() = 10 AND 0 IN (SELECT 1)) AS measure_2,
                            array_agg(MATCH_NUMBER() = 1 AND EXISTS (SELECT 'Y')) AS measure_3
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (X Y Z)
                    DEFINE X AS true
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (1, ARRAY[VARCHAR 'XA'], ARRAY[false], ARRAY[true]),
                             (2, ARRAY['XA', 'YA'], ARRAY[false, false], ARRAY[true, true]),
                             (3, ARRAY['XA', 'YA', 'ZA'], ARRAY[false, false, false], ARRAY[true, true, true])
                        """);

        // second argument of the aggregation is runtime-evaluated
        assertThat(assertions.query("""
                SELECT m.id, m.measure
                FROM (VALUES
                         (1, 'p'),
                         (2, 'q'),
                         (3, 'r'),
                         (4, 's')
                ) t(id, value)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES max_by(value, CLASSIFIER()) AS measure
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (A B D C)
                    DEFINE A AS true
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (1, 'p'),
                             (2, 'q'),
                             (3, 'r'),
                             (4, 'r')
                        """);
    }

    @Test
    public void testSelectiveAggregation()
    {
        // each of the aggregations is applied only to rows with labels (X, Z)
        assertThat(assertions.query("""
                SELECT m.id, m.measure_1, m.measure_2, m.measure_3
                FROM (VALUES
                         (1, 'a'),
                         (2, 'b'),
                         (3, 'c'),
                         (4, 'd')
                ) t(id, value)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES
                            array_agg(U.id) AS measure_1,
                            array_agg(CLASSIFIER(U)) AS measure_2,
                            array_agg(U.value || CLASSIFIER(U)) AS measure_3
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (X Y Z Y)
                    SUBSET U = (X, Z)
                    DEFINE X AS true
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (1, ARRAY[1], ARRAY[VARCHAR 'X'], ARRAY[VARCHAR 'aX']),
                             (2, ARRAY[1], ARRAY['X'], ARRAY['aX']),
                             (3, ARRAY[1, 3], ARRAY['X', 'Z'], ARRAY['aX', 'cZ']),
                             (4, ARRAY[1, 3], ARRAY['X', 'Z'], ARRAY['aX', 'cZ'])
                        """);
    }

    @Test
    public void testCountAggregation()
    {
        assertThat(assertions.query("""
                SELECT m.id, m.measure_1, m.measure_2
                FROM (VALUES
                         (1, 'a'),
                         (2, 'b'),
                         (3, 'c'),
                         (4, 'd')
                ) t(id, value)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES
                            count(*) AS measure_1,
                            count() AS measure_2
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (X Y Z)
                    DEFINE X AS id > 1
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (2, BIGINT '1', BIGINT '1'),
                             (3, 2, 2),
                             (4, 3, 3)
                        """);

        // explicit RUNNING or FINAL semantics
        assertThat(assertions.query("""
                SELECT m.id, m.measure_1, m.measure_2, m.measure_3, m.measure_4
                FROM (VALUES
                         (1, 'a'),
                         (2, 'b'),
                         (3, 'c'),
                         (4, 'd')
                ) t(id, value)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES
                            RUNNING count(*) AS measure_1,
                            FINAL count(*) AS measure_2,
                            RUNNING count() AS measure_3,
                            FINAL count() AS measure_4
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (A B C D)
                    DEFINE A AS true
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (1, BIGINT '1', BIGINT '4', BIGINT '1', BIGINT '4'),
                             (2, 2, 4, 2, 4),
                             (3, 3, 4, 3, 4),
                             (4, 4, 4, 4, 4)
                        """);

        assertThat(assertions.query("""
                SELECT m.id, m.measure_1, m.measure_2
                FROM (VALUES
                         (1, 'a'),
                         (2, 'b'),
                         (3, 'c'),
                         (4, 'd')
                ) t(id, value)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES
                            count(C.*) AS measure_1,
                            count(U.*) AS measure_2
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (A B C D)
                    SUBSET U = (B, D)
                    DEFINE A AS true
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (1, BIGINT '0', BIGINT '0'),
                             (2, 0, 1),
                             (3, 1, 1),
                             (4, 1, 2)
                        """);
    }

    @Test
    public void testLabelAndColumnNames()
    {
        // column `A` and label `A`
        // count non-null values in column `A`, in rows matched to label `A`
        // count non-null values in column `A`, regardless of matched label
        // count rows matched to label `A`
        assertThat(assertions.query("""
                SELECT m.id, m.classy, m.measure_1, m.measure_2, m.measure_3
                FROM (VALUES
                         (1, 'p'),
                         (2, 'q'),
                         (3, null),
                         (4, 's')
                ) t(id, A)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES
                            CLASSIFIER() AS classy,
                            count(A.A) AS measure_1,
                            count(A) AS measure_2,
                            count(A.*) AS measure_3
                    ALL ROWS PER MATCH
                    PATTERN (A B A A)
                    DEFINE A AS true
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (1, VARCHAR 'A', BIGINT '1', BIGINT '1', BIGINT '1'),
                             (2, 'B', 1, 2, 1),
                             (3, 'A', 1, 2, 2),
                             (4, 'A', 2, 3, 3)
                        """);
    }

    @Test
    public void testOneRowPerMatch()
    {
        assertThat(assertions.query("""
                SELECT m.part, m.measure
                FROM (VALUES
                         ('p1', 1, 'a'),
                         ('p1', 2, 'b'),
                         ('p1', 3, 'c'),
                         ('p1', 4, 'd'),
                         ('p1', 5, 'e'),
                         ('p1', 6, 'f'),
                         ('p2', 1, 'g'),
                         ('p2', 2, 'h'),
                         ('p2', 3, 'i'),
                         ('p2', 4, 'j'),
                         ('p2', 5, 'k'),
                         ('p2', 6, 'l')
                ) t(part, id, value)
                  MATCH_RECOGNIZE (
                    PARTITION BY part
                    ORDER BY id
                    MEASURES array_agg(value || CLASSIFIER()) AS measure
                    ONE ROW PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (X Y Z)
                    DEFINE X AS true
                 ) AS m
                """))
                .matches("""
                        VALUES
                             ('p1', ARRAY[VARCHAR 'aX', 'bY', 'cZ']),
                             ('p1', ARRAY['dX', 'eY', 'fZ']),
                             ('p2', ARRAY['gX', 'hY', 'iZ']),
                             ('p2', ARRAY['jX', 'kY', 'lZ'])
                        """);
    }

    @Test
    public void testSeek()
    {
        // in `measure_2`, the aggregation argument `value || CLASSIFIER()` is runtime-evaluated
        assertThat(assertions.query("""
                SELECT part, id, measure_1 OVER w, measure_2 OVER w
                FROM (VALUES
                         (1, 'p1', 'A'),
                         (2, 'p1', 'B'),
                         (3, 'p1', 'C'),
                         (4, 'p1', 'D'),
                         (5, 'p1', 'E'),
                         (1, 'p2', 'A'),
                         (2, 'p2', 'B'),
                         (3, 'p2', 'C'),
                         (4, 'p2', 'D'),
                         (5, 'p2', 'E')
                ) t(id, part, value)
                  WINDOW w AS (
                    PARTITION BY part
                    ORDER BY id
                    MEASURES
                            array_agg(value) AS measure_1,
                            array_agg(value || CLASSIFIER()) AS measure_2
                    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                    AFTER MATCH SKIP TO NEXT ROW
                    SEEK
                    PATTERN (X+)
                    DEFINE X AS X.value > 'B')
                """))
                .matches("""
                        VALUES
                             ('p1', 1, ARRAY['C', 'D', 'E'], ARRAY[VARCHAR 'CX', 'DX', 'EX']),
                             ('p1', 2, ARRAY['C', 'D', 'E'], ARRAY['CX', 'DX', 'EX']),
                             ('p1', 3, ARRAY[ 'C', 'D', 'E'], ARRAY['CX', 'DX', 'EX']),
                             ('p1', 4, ARRAY['D', 'E'], ARRAY['DX', 'EX']),
                             ('p1', 5, ARRAY['E'], ARRAY['EX']),
                             ('p2', 1, ARRAY['C', 'D', 'E'], ARRAY['CX', 'DX', 'EX']),
                             ('p2', 2, ARRAY['C', 'D', 'E'], ARRAY['CX', 'DX', 'EX']),
                             ('p2', 3, ARRAY['C', 'D', 'E'], ARRAY['CX', 'DX', 'EX']),
                             ('p2', 4, ARRAY['D', 'E'], ARRAY['DX', 'EX']),
                             ('p2', 5, ARRAY['E'], ARRAY['EX'])
                        """);
    }

    @Test
    public void testExclusions()
    {
        assertThat(assertions.query("""
                SELECT m.part, m.measure_1, m.measure_2
                FROM (VALUES
                         ('p1', 1, '1a'),
                         ('p1', 2, '1b'),
                         ('p1', 3, '1c'),
                         ('p1', 4, '1d'),
                         ('p1', 5, '1e'),
                         ('p2', 1, '2a'),
                         ('p2', 2, '2b'),
                         ('p2', 3, '2c'),
                         ('p2', 4, '2d'),
                         ('p2', 5, '2e')
                ) t(part, id, value)
                  MATCH_RECOGNIZE (
                    PARTITION BY part
                    ORDER BY id
                    MEASURES
                            array_agg(value) AS measure_1,
                            array_agg(value || CLASSIFIER()) AS measure_2
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (P {- Q R -} S)
                    DEFINE P AS id > 1
                 ) AS m
                """))
                .matches("""
                        VALUES
                             ('p1', ARRAY['1b'], ARRAY[VARCHAR '1bP']),
                             ('p1', ARRAY['1b', '1c', '1d', '1e'], ARRAY['1bP', '1cQ', '1dR', '1eS']),
                             ('p2', ARRAY['2b'], ARRAY['2bP']),
                             ('p2', ARRAY['2b', '2c', '2d', '2e'], ARRAY['2bP', '2cQ', '2dR', '2eS'])
                        """);
    }

    @Test
    public void testBalancingSums()
    {
        assertThat(assertions.query("""
                SELECT m.id, m.classy, m.running_sum_A, m.running_sum_B
                FROM (VALUES
                         (1, 4),
                         (2, 6),
                         (3, 10),
                         (4, 1),
                         (5, 1),
                         (6, 1),
                         (7, 10),
                         (8, 5),
                         (9, 1)
                ) t(id, value)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES
                            RUNNING sum(A.value) AS running_sum_A,
                            RUNNING sum(B.value) AS running_sum_B,
                            CLASSIFIER() AS classy
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN ((A | B)*)
                    DEFINE A AS sum(A.value) - A.value <= sum(B.value)
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (1, VARCHAR 'B', null, BIGINT '4'),
                             (2, 'A', BIGINT '6', BIGINT '4'),
                             (3, 'B', BIGINT '6', BIGINT '14'),
                             (4, 'A', BIGINT '7', BIGINT '14'),
                             (5, 'A', BIGINT '8', BIGINT '14'),
                             (6, 'A', BIGINT '9', BIGINT '14'),
                             (7, 'A', BIGINT '19', BIGINT '14'),
                             (8, 'B', BIGINT '19', BIGINT '19'),
                             (9, 'A', BIGINT '20', BIGINT '19')
                        """);
    }

    @Test
    public void testPeriodLength() //https://stackoverflow.com/questions/68448694/how-can-i-calculate-user-session-time-from-heart-beat-data-in-presto-sql
    {
        // D is for 1-element sequences; A B* C is for longer sequences
        assertThat(assertions.query("""
                SELECT user_id, CAST(periods_total AS integer)
                FROM (VALUES
                         (1, 3),
                         (1, 4),
                         (1, 5),
                         (1, 8),
                         (1, 9),
                         (2, 2),
                         (2, 3),
                         (2, 4)
                ) t(user_id, minute_of_the_day)
                  MATCH_RECOGNIZE (
                    PARTITION BY user_id
                    ORDER BY minute_of_the_day
                    MEASURES COALESCE(sum(C.minute_of_the_day) - sum(A.minute_of_the_day), 0) AS periods_total
                    ONE ROW PER MATCH
                    PATTERN ((A B* C | D)*)
                    DEFINE
                           B AS minute_of_the_day = PREV(minute_of_the_day) + 1,
                           C AS minute_of_the_day = PREV(minute_of_the_day) + 1)
                """))
                .matches("""
                        VALUES
                             (1, 3),
                             (2, 2)
                        """);
    }

    @Test
    public void testSetPartitioning()
    {
        // partition into 2 subsets of equal sums
        assertThat(assertions.query("""
                SELECT m.id, m.running_labels
                FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8)) t(id)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES RUNNING array_agg(CLASSIFIER()) AS running_labels
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (^(A | B)* (LAST_A | LAST_B)$)
                    DEFINE
                            LAST_A AS sum(A.id) + id = sum(B.id),
                            LAST_B AS sum(B.id) + id = sum(A.id)
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (1, CAST(ARRAY['A'] AS array(varchar))),
                             (2, ARRAY['A', 'A']),
                             (3, ARRAY['A', 'A', 'A']),
                             (4, ARRAY['A', 'A', 'A', 'A']),
                             (5, ARRAY['A', 'A', 'A', 'A', 'B']),
                             (6, ARRAY['A', 'A', 'A', 'A', 'B', 'B']),
                             (7, ARRAY['A', 'A', 'A', 'A', 'B', 'B', 'B']),
                             (8, ARRAY['A', 'A', 'A', 'A', 'B', 'B', 'B', 'LAST_A'])
                        """);

        // partition into 3 subsets of equal sums
        assertThat(assertions.query("""
                SELECT m.id, m.running_labels
                FROM (VALUES (1), (2), (3), (4), (5), (6)) t(id)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES RUNNING array_agg(CLASSIFIER()) AS running_labels
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (^(A | B | C)* (LAST_A | LAST_B | LAST_C)$)
                    DEFINE
                            LAST_A AS sum(A.id) + id = sum(B.id) AND sum(B.id) = sum(C.id),
                            LAST_B AS sum(B.id) + id = sum(A.id)  AND sum(A.id) = sum(C.id),
                            LAST_C AS sum(C.id) + id = sum(A.id)  AND sum(A.id) = sum(B.id)
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (1, CAST(ARRAY['A'] AS array(varchar))),
                             (2, ARRAY['A', 'B']),
                             (3, ARRAY['A', 'B', 'C']),
                             (4, ARRAY['A', 'B', 'C', 'C']),
                             (5, ARRAY['A', 'B', 'C', 'C', 'B']),
                             (6, ARRAY['A', 'B', 'C', 'C', 'B', 'LAST_A'])
                        """);
    }

    @Test
    public void testForkingThreads()
    {
        // at each step of matching, the threads are forked because of the alternation. every thread is validated at the final step by the defining condition for label `X`
        assertThat(assertions.query("""
                SELECT m.id, m.running_labels
                FROM (VALUES (1), (2), (3), (4)) t(id)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES RUNNING array_agg(CLASSIFIER()) AS running_labels
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN ((A | B | C)* X)
                    DEFINE X AS array_agg(CLASSIFIER()) = ARRAY['C', 'A', 'B', 'X']
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (1, CAST(ARRAY['C'] AS array(varchar))),
                             (2, ARRAY['C', 'A']),
                             (3, ARRAY['C', 'A', 'B']),
                             (4, ARRAY['C', 'A', 'B', 'X'])
                        """);
    }

    @Test
    public void testMultipleAggregationsInDefine()
    {
        // the defining conditions for `A` and `B` involve two aggregations each, and all of the aggregations have runtime-evaluated arguments (dependent on `CLASSIFIER` or `MATCH_NUMBER`)
        assertThat(assertions.query("""
                SELECT m.match_no, m.labels
                FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8)) t(id)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES
                            MATCH_NUMBER() AS match_no,
                            array_agg(CLASSIFIER()) AS labels
                    ONE ROW PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN ((A | B){4})
                    DEFINE
                            A AS max(id - 2 * MATCH_NUMBER()) > 1 AND max(CLASSIFIER()) = 'B',
                            B AS min(lower(CLASSIFIER())) = 'b' OR min(MATCH_NUMBER() + 100) < 0
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (BIGINT '1', CAST(ARRAY['B', 'B', 'B', 'A'] AS array(varchar))),
                             (2, ARRAY['B', 'A', 'A', 'A'])
                        """);
    }

    @Test
    public void testRunningAndFinalAggregations()
    {
        // all aggregations in `MEASURES` have runtime-evaluated arguments (dependent on `CLASSIFIER` or `MATCH_NUMBER`)
        assertThat(assertions.query("""
                SELECT m.id, m.match, m.running_labels, m.final_labels, m.running_match, m.final_match
                FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8)) t(id)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES
                            MATCH_NUMBER() AS match,
                            RUNNING array_agg(CLASSIFIER()) AS running_labels,
                            FINAL array_agg(lower(CLASSIFIER())) AS final_labels,
                            RUNNING sum(MATCH_NUMBER() * 100) AS running_match,
                            FINAL sum(-MATCH_NUMBER()) AS final_match
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (A B C D)
                    DEFINE A AS true
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (1, BIGINT '1', CAST(ARRAY['A'] AS array(varchar)), CAST(ARRAY['a', 'b', 'c', 'd'] AS array(varchar)), BIGINT '100', BIGINT '-4'),
                             (2,         1,       ARRAY['A', 'B'],                    ARRAY['a', 'b', 'c', 'd'],                            200,          -4),
                             (3,         1,       ARRAY['A', 'B', 'C'],               ARRAY['a', 'b', 'c', 'd'],                            300,          -4),
                             (4,         1,       ARRAY['A', 'B', 'C', 'D'],          ARRAY['a', 'b', 'c', 'd'],                            400,          -4),
                             (5,         2,       ARRAY['A'],                         ARRAY['a', 'b', 'c', 'd'],                            200,          -8),
                             (6,         2,       ARRAY['A', 'B'],                    ARRAY['a', 'b', 'c', 'd'],                            400,          -8),
                             (7,         2,       ARRAY['A', 'B', 'C'],               ARRAY['a', 'b', 'c', 'd'],                            600,          -8),
                             (8,         2,       ARRAY['A', 'B', 'C', 'D'],          ARRAY['a', 'b', 'c', 'd'],                            800,          -8)
                        """);
    }

    @Test
    public void testMultipleAggregationArguments()
    {
        // all aggregations in `MEASURES` and `DEFINE` have 2 arguments, both runtime-evaluated (dependent on `CLASSIFIER` or `MATCH_NUMBER`)
        assertThat(assertions.query("""
                SELECT m.id, m.classy, m.match, m.running_measure, m.final_measure
                FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8)) t(id)
                  MATCH_RECOGNIZE (
                    ORDER BY id
                    MEASURES
                            MATCH_NUMBER() AS match,
                            CLASSIFIER() AS classy,
                            RUNNING max_by(MATCH_NUMBER() * 100 + id, CLASSIFIER()) AS running_measure,
                            FINAL max_by(-MATCH_NUMBER() - id, lower(CLASSIFIER())) AS final_measure
                    ALL ROWS PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (A B C D)
                    DEFINE A AS max_by(MATCH_NUMBER(), CLASSIFIER()) > 0
                 ) AS m
                """))
                .matches("""
                        VALUES
                             (1, VARCHAR 'A', BIGINT '1', BIGINT '101', BIGINT '-5'),
                             (2,         'B',         1,          102,          -5),
                             (3,         'C',         1,          103,          -5),
                             (4,         'D',         1,          104,          -5),
                             (5,         'A',         2,          205,          -10),
                             (6,         'B',         2,          206,          -10),
                             (7,         'C',         2,          207,          -10),
                             (8,         'D',         2,          208,          -10)
                        """);
    }
}
