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

/// End-to-end tests for SQL:2023 F262 (Extended `CASE` expression). Each WHEN may carry a
/// predicate fragment whose LHS is supplied by the surrounding CASE operand. The operand evaluates
/// once across all clauses, which we exercise here with non-deterministic operands.
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestExtendedCase
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testComparison()
    {
        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN > 100 THEN 1
                            WHEN < 0 THEN 2
                            ELSE 3
                       END
                FROM (VALUES 200, -1, 50) t(x)
                """))
                .matches("VALUES 1, 2, 3");
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN BETWEEN 1 AND 10 THEN 1
                            WHEN BETWEEN 11 AND 20 THEN 2
                            ELSE 3
                       END
                FROM (VALUES 5, 15, 25) t(x)
                """))
                .matches("VALUES 1, 2, 3");

        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN NOT BETWEEN 1 AND 10 THEN 1
                            ELSE 0
                       END
                FROM (VALUES 5, 11) t(x)
                """))
                .matches("VALUES 0, 1");
    }

    @Test
    public void testInList()
    {
        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN IN (1, 2, 3) THEN 1
                            WHEN IN (4, 5) THEN 2
                            ELSE 3
                       END
                FROM (VALUES 2, 5, 9) t(x)
                """))
                .matches("VALUES 1, 2, 3");

        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN NOT IN (1, 2, 3) THEN 1
                            ELSE 0
                       END
                FROM (VALUES 2, 9) t(x)
                """))
                .matches("VALUES 0, 1");

        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN IN (1, 2, 3) THEN 1
                            ELSE 0
                       END
                FROM (VALUES CAST(NULL AS integer), 2) t(x)
                """))
                .matches("VALUES 0, 1");

        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN IN (1, NULL, 3) THEN 1
                            ELSE 0
                       END
                FROM (VALUES 1, 2, 3) t(x)
                """))
                .matches("VALUES 1, 0, 1");

        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN NOT IN (1, NULL, 3) THEN 1
                            ELSE 0
                       END
                FROM (VALUES 1, 4) t(x)
                """))
                .matches("VALUES 0, 0");
    }

    @Test
    public void testIsNull()
    {
        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN IS NULL THEN 1
                            WHEN IS NOT NULL THEN 2
                       END
                FROM (VALUES CAST(NULL AS integer), 1) t(x)
                """))
                .matches("VALUES 1, 2");
    }

    @Test
    public void testBooleanTest()
    {
        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN IS TRUE THEN 1
                            WHEN IS FALSE THEN 2
                            WHEN IS UNKNOWN THEN 3
                       END
                FROM (VALUES true, false, CAST(NULL AS boolean)) t(x)
                """))
                .matches("VALUES 1, 2, 3");

        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN IS NOT TRUE THEN 1
                            ELSE 0
                       END
                FROM (VALUES true, false, CAST(NULL AS boolean)) t(x)
                """))
                .matches("VALUES 0, 1, 1");
    }

    @Test
    public void testLike()
    {
        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN LIKE 'a%' THEN 1
                            WHEN LIKE 'b%' THEN 2
                            ELSE 3
                       END
                FROM (VALUES VARCHAR 'apple', 'banana', 'cherry') t(x)
                """))
                .matches("VALUES 1, 2, 3");

        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN NOT LIKE 'a%' THEN 1
                            ELSE 0
                       END
                FROM (VALUES VARCHAR 'apple', 'banana') t(x)
                """))
                .matches("VALUES 0, 1");
    }

    @Test
    public void testDistinctFrom()
    {
        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN IS NOT DISTINCT FROM CAST(NULL AS integer) THEN 1
                            ELSE 2
                       END
                FROM (VALUES CAST(NULL AS integer), 1) t(x)
                """))
                .matches("VALUES 1, 2");

        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN IS DISTINCT FROM 1 THEN 1
                            ELSE 2
                       END
                FROM (VALUES CAST(NULL AS integer), 1, 5) t(x)
                """))
                .matches("VALUES 1, 2, 1");
    }

    @Test
    public void testTypeReconciliation()
    {
        // The operand is evaluated once, so all clauses must agree on a single operand type. The
        // decimal literal in one WHEN widens the integer operand for every clause — including the
        // bare-equality and BETWEEN clauses, whose values are coerced to the common type too.
        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN 1 THEN 1
                            WHEN BETWEEN 10 AND 20 THEN 3
                            WHEN > 2.5 THEN 2
                            ELSE 4
                       END
                FROM (VALUES 1, 3, 15, 0) t(x)
                """))
                .matches("VALUES 1, 2, 3, 4");
    }

    @Test
    public void testQuantifiedComparison()
    {
        // A predicate-fragment WHEN may carry a quantified comparison against a subquery; the case
        // operand supplies the comparison's left-hand side and is still evaluated only once.
        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN > ALL (VALUES 1, 2, 3) THEN 1
                            WHEN <= ANY (VALUES 1, 2, 3) THEN 2
                            ELSE 3
                       END
                FROM (VALUES 0, 5, 2) t(x)
                """))
                .matches("VALUES 2, 1, 2");

        // The operand is evaluated once even when a clause is a relational quantified comparison:
        // every random() value is either > 0.5 or <= 0.5, so no row falls through to ELSE.
        assertThat(assertions.query(
                """
                SELECT count(*)
                FROM (
                    SELECT CASE random()
                                WHEN > ALL (VALUES 0.5e0) THEN 1
                                WHEN <= 0.5e0 THEN 2
                                ELSE 3
                           END AS bucket
                    FROM unnest(sequence(1, 200)) AS t(x))
                WHERE bucket = 3
                """))
                .matches("VALUES BIGINT '0'");
    }

    @Test
    public void testQuantifiedComparisonEqualsAllNotEqualsAny()
    {
        // Exercise the remaining quantifier/operator combinations: `= ALL` matches when the
        // operand equals every subquery row, `<> ANY` matches when it differs from at least one.
        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN = ALL (VALUES 3, 3) THEN 1
                            WHEN <> ANY (VALUES 2) THEN 2
                            ELSE 3
                       END
                FROM (VALUES 3, 5, 2) t(x)
                """))
                .matches("VALUES 1, 2, 3");
    }

    @Test
    public void testInSubquery()
    {
        // The IN value list of a predicate-fragment WHEN may be a subquery; the case operand
        // supplies the IN predicate's left-hand side, evaluated once like every other fragment.
        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN IN (VALUES 1, 2, 3) THEN 1
                            WHEN NOT IN (VALUES 7, 8, 9) THEN 2
                            ELSE 3
                       END
                FROM (VALUES 2, 5, 8) t(x)
                """))
                .matches("VALUES 1, 2, 3");
    }

    @Test
    public void testCorrelatedInSubquery()
    {
        // The subquery in a predicate-fragment WHEN may correlate to the enclosing row. Here the
        // IN-subquery references t.k, so each row is tested against a different value set.
        assertThat(assertions.query(
                """
                SELECT CASE t.x
                            WHEN IN (SELECT u.v FROM (VALUES (1, 10), (1, 20), (2, 99)) u(k, v) WHERE u.k = t.k) THEN 1
                            ELSE 0
                       END
                FROM (VALUES (1, 10), (1, 99), (2, 10)) t(k, x)
                """))
                .matches("VALUES 1, 0, 0");
    }

    @Test
    public void testNullOperandThroughSubqueryFragment()
    {
        // A NULL case operand fed through a relational IN fragment yields NULL (not TRUE), so the
        // clause does not match and the row falls through to ELSE.
        assertThat(assertions.query(
                """
                SELECT CASE CAST(NULL AS integer)
                            WHEN IN (VALUES 1) THEN 1
                            ELSE 2
                       END
                """))
                .matches("VALUES 2");
    }

    @Test
    public void testSubqueryFragmentInJoinCondition()
    {
        // A predicate-fragment WHEN carrying a subquery may appear in a JOIN ON expression. The
        // fragment's subquery is planned on the join branch that resolves its operand, and the
        // resulting predicate mapping is propagated into the join's translation map.
        assertThat(assertions.query(
                """
                SELECT t.x, u.y
                FROM (VALUES 1, 2, 8) t(x)
                JOIN (VALUES 1, 2) u(y)
                  ON u.y = CASE t.x
                                WHEN IN (VALUES 1, 2, 3) THEN 1
                                ELSE 2
                           END
                """))
                .matches("VALUES (1, 1), (2, 1), (8, 2)");
    }

    @Test
    public void testMixedWithBareEquality()
    {
        // The legacy bare-equality WHEN form coexists with F262 predicate-fragment WHENs.
        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN 0 THEN 1
                            WHEN > 0 THEN 2
                            ELSE 3
                       END
                FROM (VALUES 0, 5, -3) t(x)
                """))
                .matches("VALUES 1, 2, 3");
    }

    @Test
    public void testFirstMatchWins()
    {
        // Clause order matters: a value matching multiple predicates returns the first clause's
        // result, identical to the legacy simple-CASE semantics.
        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN < 100 THEN 1
                            WHEN < 1000 THEN 2
                            ELSE 3
                       END
                FROM (VALUES 5) t(x)
                """))
                .matches("VALUES 1");
    }

    @Test
    public void testSingleOperandEvaluation()
    {
        // The operand evaluates exactly once per row. With no ELSE and an exhaustive two-way
        // partition, single evaluation gives every row a non-NULL bucket. Re-evaluating the
        // operand per clause would let one row's draw land below 0.5 for the first WHEN and
        // at-or-above 0.5 for the second — matching neither and yielding a NULL bucket. So a
        // NULL bucket can only appear if the operand is evaluated more than once.
        assertThat(assertions.query(
                """
                SELECT count(*)
                FROM (
                    SELECT CASE random()
                                WHEN < 0.5e0 THEN 'lo'
                                WHEN >= 0.5e0 THEN 'hi'
                           END AS bucket
                    FROM unnest(sequence(1, 200)) AS t(x))
                WHERE bucket IS NULL
                """))
                .matches("VALUES BIGINT '0'");
    }

    @Test
    public void testRejectsInSearchedCase()
    {
        // Searched CASE has no implicit LHS for a predicate fragment to attach to, so the
        // grammar accepts only a boolean expression after WHEN. A predicate fragment is
        // rejected at parse time.
        assertThat(assertions.query(
                """
                SELECT CASE
                            WHEN > 5 THEN 1
                            ELSE 0
                       END
                FROM (VALUES 7) t(x)
                """))
                .failure()
                .hasMessageMatching("(?s).*mismatched input '>'.*");
    }

    @Test
    public void testRejectsTypeMismatchInSubqueryFragment()
    {
        // The operand and the subquery column of a relational fragment must share a common type.
        // An integer operand against a varchar subquery column is rejected during analysis.
        assertThat(assertions.query(
                """
                SELECT CASE x
                            WHEN IN (VALUES 'a', 'b') THEN 1
                            ELSE 0
                       END
                FROM (VALUES 1) t(x)
                """))
                .failure()
                .hasMessageMatching("(?s).*same type.*");
    }
}
