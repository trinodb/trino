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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.plan.JoinNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJoin
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testCrossJoinEliminationWithOuterJoin()
    {
        assertThat(assertions.query(
                """
                WITH
                  a AS (SELECT id FROM (VALUES (1)) AS t(id)),
                  b AS (SELECT id FROM (VALUES (1)) AS t(id)),
                  c AS (SELECT id FROM (VALUES ('1')) AS t(id)),
                  d as (SELECT id FROM (VALUES (1)) AS t(id))
                SELECT a.id
                FROM a
                LEFT JOIN b ON a.id = b.id
                JOIN c ON a.id = CAST(c.id AS bigint)
                JOIN d ON d.id = a.id
                """))
                .matches("VALUES 1");
    }

    @Test
    public void testSingleRowNonDeterministicSource()
    {
        assertThat(assertions.query(
                """
                WITH data(id) AS (SELECT uuid())
                SELECT COUNT(DISTINCT id)
                FROM (VALUES 1, 2, 3, 4, 5, 6, 7, 8)
                CROSS JOIN data
                """))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    public void testJoinOnNan()
    {
        assertThat(assertions.query(
                """
                WITH t(x) AS (VALUES nan())
                SELECT * FROM t t1 JOIN t t2 ON NOT t1.x < t2.x
                """))
                .matches("VALUES (nan(), nan())");
    }

    @Test
    public void testJoinWithComplexCriteria()
    {
        // Test for https://github.com/trinodb/trino/issues/13145
        // The issue happens because ReorderJoins evaluates candidates for equality inference
        // based on one form of the join criteria (i.e., CAST(...) = CASE ... END)) and then
        // attempts to make reformulate the join criteria based on another form of the expression
        // with the terms flipped (i.e., CASE ... END = CAST(...)). Because NullabilityAnalyzer.mayReturnNullOnNonNullInput
        // could return an inconsistent result for both forms, the expression ended being dropped
        // from the join clause.
        assertThat(assertions.query(
                """
                WITH
                    t1 (id, v) as (
                        VALUES
                            (1, 100),
                            (2, 200)),
                    t2 (id, x, y) AS (
                        VALUES
                            (1, 10, 'a'),
                            (2, 10, 'b'))
                SELECT x, y
                FROM t1 JOIN t2 ON (t1.id = t2.id)
                WHERE IF(t1.v = 0, 'cc', y) = 'b'
                """))
                .matches("VALUES (10, 'b')");
    }

    @Test
    public void testAliasingOfNullCasts()
    {
        // Test for https://github.com/trinodb/trino/issues/13565
        assertThat(assertions.query(
                """
                WITH t AS (
                    SELECT CAST(null AS varchar) AS x, CAST(null AS varchar) AS y
                    FROM (VALUES 1) t(a) JOIN (VALUES 1) u(a) USING (a))
                SELECT * FROM t
                WHERE CAST(x AS bigint) IS NOT NULL AND y = 'hello'
                """))
                .result()
                .hasTypes(List.of(VARCHAR, VARCHAR))
                .isEmpty();
    }

    @Test
    public void testAsofJoinExecution()
    {
        // <= direction
        assertThat(assertions.query(
                """
                WITH
                    a(t, k) AS (VALUES (1, 1), (2, 1), (3, 2)),
                    b(t, k, v) AS (VALUES (1, 1, 'x'), (2, 1, 'y'), (4, 1, 'z'), (1, 2, 'q'))
                SELECT a.t, a.k, b.v
                FROM a ASOF JOIN b ON a.k = b.k AND b.t <= a.t
                ORDER BY 1
                """))
                .matches("VALUES (1, 1, 'x'), (2, 1, 'y'), (3, 2, 'q')");

        // >= direction
        assertThat(assertions.query(
                """
                WITH
                    a(t, k) AS (VALUES (1, 1), (2, 1), (3, 2)),
                    b(t, k, v) AS (VALUES (1, 1, 'x'), (2, 1, 'y'), (4, 1, 'z'), (1, 2, 'q'))
                SELECT a.t, a.k, b.v
                FROM a ASOF JOIN b ON a.k = b.k AND b.t >= a.t
                ORDER BY 1
                """))
                .matches("VALUES (1, 1, 'x'), (2, 1, 'y')");
    }

    @Test
    public void testAsofJoinWithNullInequalityColumn()
    {
        assertThat(assertions.query(
                """
                WITH
                    a(t, k) AS (VALUES (2, 1), (4, 1)),
                    b(t, k, v) AS (VALUES (CAST(NULL AS INTEGER), 1, 'null'), (1, 1, 'one'), (3, 1, 'three'))
                SELECT a.t, a.k, b.v
                FROM a ASOF JOIN b ON a.k = b.k AND b.t <= a.t
                ORDER BY 1
                """))
                .matches("VALUES (2, 1, 'one'), (4, 1, 'three')");
    }

    @Test
    public void testAsofLeftJoinExecution()
    {
        // <= direction
        assertThat(assertions.query(
                """
                WITH
                    a(t, k) AS (VALUES (1, 1), (5, 1), (3, 3)),
                    b(t, k, v) AS (VALUES (2, 1, 'm'), (4, 1, 'n'))
                SELECT a.t, a.k, b.v
                FROM a ASOF LEFT JOIN b ON a.k = b.k AND b.t <= a.t
                ORDER BY 1
                """))
                .matches("VALUES (1, 1, NULL), (3, 3, NULL), (5, 1, 'n')");

        // >= direction
        assertThat(assertions.query(
                """
                WITH
                    a(t, k) AS (VALUES (1, 1), (5, 1), (3, 3)),
                    b(t, k, v) AS (VALUES (2, 1, 'm'), (4, 1, 'n'))
                SELECT a.t, a.k, b.v
                FROM a ASOF LEFT JOIN b ON a.k = b.k AND b.t >= a.t
                ORDER BY 1
                """))
                .matches("VALUES (1, 1, 'm'), (3, 3, NULL), (5, 1, NULL)");
    }

    @Test
    public void testAsofLeftJoinWithNullInequalityColumn()
    {
        assertThat(assertions.query(
                """
                WITH
                    a(t, k) AS (VALUES (2, 1), (5, 1)),
                    b(t, k, v) AS (VALUES (CAST(NULL AS INTEGER), 1, 'null'), (3, 1, 'three'))
                SELECT a.t, a.k, b.v
                FROM a ASOF LEFT JOIN b ON a.k = b.k AND b.t <= a.t
                ORDER BY 1
                """))
                .matches("VALUES (2, 1, NULL), (5, 1, 'three')");
    }

    @Test
    public void testAsofJoinStrictInequality()
    {
        // strict < direction
        assertThat(assertions.query(
                """
                WITH
                    a(t, k) AS (VALUES (2, 1)),
                    b(t, k, v) AS (VALUES (1, 1, 'less_'), (2, 1, 'equal'))
                SELECT b.v
                FROM a ASOF JOIN b ON a.k = b.k AND b.t < a.t
                """))
                .matches("VALUES 'less_'");

        // strict > direction
        assertThat(assertions.query(
                """
                WITH
                    a(t, k) AS (VALUES (2, 1)),
                    b(t, k, v) AS (VALUES (1, 1, 'less_'), (2, 1, 'equal'))
                SELECT b.v
                FROM a ASOF JOIN b ON a.k = b.k AND b.t > a.t
                """))
                .returnsEmptyResult();
    }

    @Test
    public void testAsofJoinWithAdditionalBuildFilter()
    {
        // Additional build-side predicate (b.t % 2 = 0) filters out closest candidate (odd timestamp),
        // so the join selects the next valid candidate satisfying the inequality.
        assertThat(assertions.query(
                """
                WITH
                    a(t, k) AS (VALUES (3, 1), (5, 1)),
                    b(t, k, v) AS (VALUES (2, 1, 'x2'), (4, 1, 'x4'), (5, 1, 'x5'))
                SELECT a.t, a.k, b.v
                FROM a ASOF JOIN b ON a.k = b.k AND b.t <= a.t AND (b.t % 2) = 0
                ORDER BY 1
                """))
                .matches("VALUES (3, 1, 'x2'), (5, 1, 'x4')");
    }

    @Test
    public void testAsofJoinWithoutEquiCondition()
    {
        // ASOF join using only inequality (no equi-conjunct)
        assertThat(assertions.query(
                """
                WITH
                    a(t) AS (VALUES 0, 1, 3),
                    b(t, v) AS (VALUES (1, 'x1'), (2, 'x2'))
                SELECT a.t, b.v
                FROM a ASOF JOIN b ON b.t <= a.t
                ORDER BY 1
                """))
                .matches("VALUES (1, 'x1'), (3, 'x2')");

        // Reverse direction with only inequality (>=)
        assertThat(assertions.query(
                """
                WITH
                    a(t) AS (VALUES 0, 1, 3),
                    b(t, v) AS (VALUES (1, 'x1'), (2, 'x2'))
                SELECT a.t, b.v
                FROM a ASOF JOIN b ON b.t >= a.t
                ORDER BY 1
                """))
                .matches("VALUES (0, 'x1'), (1, 'x1')");
    }

    @Test
    public void testAsofJoinWithExtraInequalities()
    {
        // Extra right-only inequality (b.t < 3) excludes the closest candidate for a.t = 3,
        // changing the chosen match from b.t = 3 to b.t = 2.
        assertThat(assertions.query(
                """
                WITH
                    a(t, k) AS (VALUES (2, 1), (3, 1)),
                    b(t, k, v) AS (VALUES (1, 1, 'v1'), (2, 1, 'v2'), (2, 1, 'v2'), (3, 1, 'v3'))
                SELECT a.t, a.k, b.v
                FROM a ASOF JOIN b
                  ON a.k = b.k AND b.t <= a.t AND b.t < 3 AND a.t > 2
                ORDER BY 1
                """))
                .matches("VALUES (3, 1, 'v2')");
    }

    @Test
    public void testAsofJoinOrdersLikeWithValuesUsingCustkeyBound()
    {
        assertThat(assertions.query(
                """
                WITH
                  o1(orderkey, custkey) AS (VALUES (1, 1), (2, 1), (3, 2)),
                  o2(orderkey, custkey, v) AS (VALUES (1, 1, 'x'), (2, 1, 'y'), (4, 1, 'z'), (1, 2, 'q'), (2, 2, 'v'))
                SELECT o1.custkey, o2.v
                FROM o1 ASOF JOIN o2
                  ON o1.custkey = o2.custkey AND o2.orderkey <= o1.custkey
                ORDER BY 1, 2
                """))
                .matches("VALUES (1, 'x'), (1, 'x'), (2, 'v')");
    }

    @Test
    public void testCorrelatedSubqueryInAsofJoinClause()
    {
        // Correlation in ASOF join clause is not allowed (treated like outer join for correlation purposes)
        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2) t(x) ASOF JOIN (VALUES 1, 3) u(x) ON t.x IN (SELECT v.x FROM (VALUES 1, 2) v(x) WHERE u.x = v.x)"))
                .failure()
                .hasMessageContaining("Reference to column 'u.x' from outer scope not allowed in this context");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2) t(x) ASOF JOIN (VALUES 1, 3) u(x) ON u.x IN (SELECT v.x FROM (VALUES 1, 2) v(x) WHERE t.x = v.x)"))
                .failure()
                .hasMessageContaining("Reference to column 't.x' from outer scope not allowed in this context");
    }

    @Test
    public void testAsofJoinBuildSideComplexExpressionPushdown()
    {
        // Verify correctness when inequality uses complex build-side expression
        assertThat(assertions.query(
                """
                WITH
                    a(t, k) AS (VALUES (3, 1), (2, 1)),
                    b(t, k) AS (VALUES (1, 1), (2, 1))
                SELECT a.t, a.k, b.t
                FROM a ASOF JOIN b ON a.k = b.k AND (b.t + 1) <= a.t
                ORDER BY 1
                """))
                .matches("VALUES (2, 1, 1), (3, 1, 2)");
    }

    @Test
    public void testAsofJoinBuildSideComplexExpressionPushdownNoBuildOutputs()
    {
        // Same as above, but ensure no build-side columns are projected in the output
        assertThat(assertions.query(
                """
                WITH
                    a(t, k) AS (VALUES (3, 1), (2, 1)),
                    b(t, k) AS (VALUES (1, 1), (2, 1))
                SELECT a.t, a.k
                FROM a ASOF JOIN b ON a.k = b.k AND (b.t + 1) <= a.t
                ORDER BY 1
                """))
                .matches("VALUES (2, 1), (3, 1)");
    }

    @Test
    public void testAsofJoinBuildSideCastExpression()
    {
        // Build-side inequality uses CAST on the build column
        assertThat(assertions.query(
                """
                WITH
                    a(t, k) AS (VALUES (3, 1), (2, 1), (5, 1)),
                    b(t, k) AS (VALUES (CAST('1' AS varchar), 1), (CAST('2' AS varchar), 1), (CAST('4' AS varchar), 1))
                SELECT a.t, a.k, b.t
                FROM a ASOF JOIN b ON a.k = b.k AND (CAST(b.t AS integer) + 1) <= a.t
                ORDER BY 1
                """))
                .matches("VALUES (2, 1, CAST('1' AS VARCHAR)), (3, 1, CAST('2' AS VARCHAR)), (5, 1, CAST('4' AS VARCHAR))");
    }

    @Test
    public void testCorrelatedSubqueryInAsofLeftJoinClause()
    {
        // Correlation in ASOF LEFT join clause is not allowed
        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2) t(x) ASOF LEFT JOIN (VALUES 1, 3) u(x) ON t.x IN (SELECT v.x FROM (VALUES 1, 2) v(x) WHERE u.x = v.x)"))
                .failure()
                .hasMessageContaining("Reference to column 'u.x' from outer scope not allowed in this context");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2) t(x) ASOF LEFT JOIN (VALUES 1, 3) u(x) ON u.x IN (SELECT v.x FROM (VALUES 1, 2) v(x) WHERE t.x = v.x)"))
                .failure()
                .hasMessageContaining("Reference to column 't.x' from outer scope not allowed in this context");
    }

    @Test
    public void testInPredicateInJoinCriteria()
    {
        // IN with subquery containing column references
        assertThat(assertions.query(
                """
                WITH
                    t(x, y) AS (VALUES (1, 10), (2, 20)),
                    u(x) AS (VALUES 1, 2),
                    w(z) AS (VALUES 10, 20)
                SELECT *
                FROM t LEFT JOIN u ON t.x = u.x AND t.y IN (SELECT z FROM w)
                """))
                .matches("VALUES (2, 20, 2), (1, 10, 1)");

        assertThat(assertions.query("SELECT * FROM (VALUES 1, 2, NULL) t(x) JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES 1)"))
                .matches("VALUES (1, 1), (1, 3), (1, NULL)");

        assertThat(assertions.query("SELECT * FROM (VALUES 1, 2, NULL) t(x) LEFT JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES 1)"))
                .matches("VALUES (1, 1), (1, 3), (1, NULL), (2, NULL), (NULL, NULL)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) RIGHT JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES 1)"))
                .matches("VALUES (1, 1), (1, 3), (1, NULL)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) FULL JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES 1)"))
                .matches("VALUES (1, 1), (1, 3), (1, NULL), (2, NULL), (NULL, NULL)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES 1)"))
                .matches("VALUES (1, 1), (2, 1), (NULL, 1)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) LEFT JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES 1)"))
                .matches("VALUES (1, 1), (2, 1), (NULL, 1)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) RIGHT JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES 1)"))
                .matches("VALUES (1, 1), (2, 1), (NULL, 1), (NULL, 3), (NULL, NULL)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) FULL JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES 1)"))
                .matches("VALUES (1, 1), (2, 1), (NULL, 1), (NULL, 3), (NULL, NULL)");

        // correlated subquery in inner join clause
        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (SELECT v.x FROM (VALUES 1, 2) v(x) WHERE u.x = v.x)"))
                .matches("VALUES (1,1)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (SELECT v.x FROM (VALUES 1, 2) v(x) WHERE t.x = v.x)"))
                .matches("VALUES (1,1)");

        // correlation in join clause not allowed for outer join
        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) FULL JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES t.x)"))
                .failure().hasMessage("line 1:93: Reference to column 't.x' from outer scope not allowed in this context");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) FULL JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES u.x)"))
                .failure().hasMessage("line 1:93: Reference to column 'u.x' from outer scope not allowed in this context");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) FULL JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES t.x)"))
                .failure().hasMessage("line 1:93: Reference to column 't.x' from outer scope not allowed in this context");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) FULL JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES u.x)"))
                .failure().hasMessage("line 1:93: Reference to column 'u.x' from outer scope not allowed in this context");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) LEFT JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES t.x)"))
                .failure().hasMessage("line 1:93: Reference to column 't.x' from outer scope not allowed in this context");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) LEFT JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES u.x)"))
                .failure().hasMessage("line 1:93: Reference to column 'u.x' from outer scope not allowed in this context");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) LEFT JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES t.x)"))
                .failure().hasMessage("line 1:93: Reference to column 't.x' from outer scope not allowed in this context");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) LEFT JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES u.x)"))
                .failure().hasMessage("line 1:93: Reference to column 'u.x' from outer scope not allowed in this context");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) RIGHT JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES t.x)"))
                .failure().hasMessage("line 1:94: Reference to column 't.x' from outer scope not allowed in this context");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) RIGHT JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES u.x)"))
                .failure().hasMessage("line 1:94: Reference to column 'u.x' from outer scope not allowed in this context");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) RIGHT JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES t.x)"))
                .failure().hasMessage("line 1:94: Reference to column 't.x' from outer scope not allowed in this context");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) RIGHT JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES u.x)"))
                .failure().hasMessage("line 1:94: Reference to column 'u.x' from outer scope not allowed in this context");
    }

    @Test
    public void testQuantifiedComparisonInJoinCriteria()
    {
        assertThat(assertions.query("SELECT * FROM (VALUES 1, 2, NULL) t(x) RIGHT JOIN (VALUES 1, 3, NULL) u(x) ON u.x > ALL (VALUES 1)"))
                .matches("VALUES (1, 3), (2, 3), (NULL, 3), (NULL, 1), (NULL, NULL)");

        assertThat(assertions.query("SELECT * FROM (VALUES 1, 2, NULL) t(x) JOIN (VALUES 1, 3, NULL) u(x) ON t.x + u.x > ALL (VALUES 2)"))
                .matches("VALUES (1, 3), (2, 1), (2, 3)");

        // TODO: this should fail during analysis, but currently fails during planning
        //   StatementAnalyzer.visitJoin needs to be updated to check whether the join criteria is an InPredicate or QualifiedComparison
        //   with mixed references to both sides of the join. For that, the Expression needs to be analyzed against a hybrid scope made of both branches
        //   of the join, instead of using the output scope of the Join node. This, in turn requires adding support for multiple scopes in ExpressionAnalyzer
        assertThat(assertions.query("SELECT * FROM (VALUES 1, 2, NULL) t(x) RIGHT JOIN (VALUES 1, 3, NULL) u(x) ON t.x + u.x > ALL (VALUES 1)"))
                .nonTrinoExceptionFailure().hasMessageMatching("Invalid node. Expression dependencies .* not in source plan output .*");
    }

    @Test
    public void testOutputDuplicatesInsensitiveJoin()
    {
        assertions.assertQueryAndPlan(
                "SELECT t.x, count(*) FROM (VALUES random(), 2) t(x) JOIN (VALUES -random(), 2, 2) u(x) ON t.x = u.x GROUP BY t.x",
                "VALUES (DOUBLE '2', BIGINT '2')",
                anyTree(
                        aggregation(
                                ImmutableMap.of("COUNT", aggregationFunction("count", ImmutableList.of())),
                                join(INNER, builder -> builder
                                        .ignoreEquiCriteria()
                                        .left(anyTree(values("t_x")))
                                        .right(anyTree(values("u_x"))))
                                        .with(JoinNode.class, not(JoinNode::isMaySkipOutputDuplicates)))));

        assertions.assertQueryAndPlan(
                "SELECT t.x FROM (VALUES random(), 2) t(x) JOIN (VALUES -random(), 2, 2) u(x) ON t.x = u.x GROUP BY t.x",
                "VALUES DOUBLE '2'",
                anyTree(
                        aggregation(
                                ImmutableMap.of(),
                                FINAL,
                                anyTree(
                                        join(INNER, builder -> builder
                                                .ignoreEquiCriteria()
                                                .left(anyTree(values("t_x")))
                                                .right(anyTree(values("u_x"))))
                                                .with(JoinNode.class, JoinNode::isMaySkipOutputDuplicates)))));
    }

    @Test
    public void testPredicateOverOuterJoin()
    {
        assertThat(assertions.query(
                """
                SELECT 5
                FROM (VALUES (1,'foo')) l(l1, l2)
                LEFT JOIN (VALUES (2,'bar')) r(r1, r2)
                ON l2 = r2
                WHERE l1 >= COALESCE(r1, 0)
                """))
                .matches("VALUES 5");

        assertThat(assertions.query(
                """
                SELECT 5
                FROM (VALUES (2,'foo')) l(l1, l2)
                RIGHT JOIN (VALUES (1,'bar')) r(r1, r2)
                ON l2 = r2
                WHERE r1 >= COALESCE(l1, 0)
                """))
                .matches("VALUES 5");
    }

    @Test
    void testFilterThatMayFail()
    {
        assertThat(assertions.query(
                """
                WITH
                    t(x,y) AS (
                        VALUES
                            ('a', '1'),
                            ('b', 'x'),
                            (null, 'y')
                    ),
                    u(x,y) AS (
                        VALUES
                            ('a', '1'),
                            ('c', 'x'),
                            (null, 'y')
                    )
                SELECT *
                FROM t JOIN u ON t.x = u.x
                WHERE CAST(t.y AS int) = 1
                """))
                .matches("VALUES ('a', '1', 'a', '1')");

        assertThat(assertions.query(
                """
                WITH
                    a(k, v) AS (VALUES if(random() >= 0, (1, CAST('10' AS varchar)))),
                    b(k, v) AS (VALUES if(random() >= 0, (1, CAST('foo' AS varchar)))),
                    t AS (
                        SELECT k, CAST(v AS BIGINT) v1, v
                        FROM a)
                SELECT t.k, b.k
                FROM t JOIN b ON t.k = b.k AND t.v1 = 10 AND t.v = b.v
                """))
                .returnsEmptyResult();
    }
}
