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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSubqueries
{
    private static final String UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG = "line .*: Given correlated subquery is not supported";

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
    public void testCorrelatedExistsSubqueriesWithOrPredicateAndNull()
    {
        assertions.assertQueryAndPlan(
                "SELECT EXISTS(SELECT 1 FROM (VALUES null, 10) t(x) WHERE y > x OR y + 10 > x) FROM (values 11 + if(rand() >= 0, 0)) t2(y)",
                "VALUES true",
                anyTree(
                        aggregation(
                                ImmutableMap.of("COUNT", functionCall("count", ImmutableList.of())),
                                aggregation -> aggregation.isStreamable() && aggregation.getStep() == SINGLE,
                                node(JoinNode.class,
                                        anyTree(
                                                values("y")),
                                        project(
                                                ImmutableMap.of("NON_NULL", expression("true")),
                                                values("x"))))));

        assertions.assertQueryAndPlan(
                "SELECT EXISTS(SELECT 1 FROM (VALUES null) t(x) WHERE y > x OR y + 10 > x) FROM (VALUES 11 + if(rand() >= 0, 0)) t2(y)",
                "VALUES false",
                anyTree(
                        aggregation(
                                ImmutableMap.of("COUNT", functionCall("count", ImmutableList.of())),
                                aggregation -> aggregation.isStreamable() && aggregation.getStep() == SINGLE,
                                node(JoinNode.class,
                                        anyTree(
                                                values("y")),
                                        project(
                                                ImmutableMap.of("NON_NULL", expression("true")),
                                                values("x"))))));
    }

    @Test
    public void testUnsupportedSubqueriesWithCoercions()
    {
        // coercion FROM subquery symbol type to correlation type
        assertThatThrownBy(() -> assertions.query(
                "SELECT EXISTS(SELECT 1 FROM (VALUES (1, null)) t(a, b) WHERE t.a=t2.b GROUP BY t.b) FROM (VALUES 1.0, 2.0) t2(b)"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        // coercion from t.a (null) to integer
        assertThatThrownBy(() -> assertions.query(
                "SELECT EXISTS(SELECT 1 FROM (VALUES (null, null)) t(a, b) WHERE t.a=t2.b GROUP BY t.b) FROM (VALUES 1, 2) t2(b)"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Test
    public void testCorrelatedSubqueriesWithLimit()
    {
        assertThat(assertions.query(
                "SELECT (SELECT t.a FROM (VALUES 1, 2) t(a) WHERE t.a=t2.b LIMIT 1) FROM (VALUES 1) t2(b)"))
                .matches("VALUES 1");
        assertThat(assertions.query(
                "SELECT (SELECT t.a FROM (VALUES 1, 2) t(a) WHERE t.a=t2.b LIMIT 2) FROM (VALUES 1) t2(b)"))
                .matches("VALUES 1");
        assertThat(assertions.query(
                "SELECT (SELECT t.a FROM (VALUES 1, 2, 3) t(a) WHERE t.a = t2.b LIMIT 2) FROM (VALUES 1) t2(b)"))
                .matches("VALUES 1");
        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT t.a FROM (VALUES 1, 1, 2, 3) t(a) WHERE t.a = t2.b LIMIT 2) FROM (VALUES 1) t2(b)"))
                .hasMessageMatching("Scalar sub-query has returned multiple rows");
        // Limit(1) and non-constant output symbol of the subquery
        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT count(*) FROM (VALUES (1, 0), (1, 1)) t(a, b) WHERE a = c GROUP BY b LIMIT 1) FROM (VALUES (1)) t2(c)"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        // Limit(1) and non-constant output symbol of the subquery
        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT a + b FROM (VALUES (1, 1), (1, 1)) t(a, b) WHERE a = c LIMIT 1) FROM (VALUES (1)) t2(c)"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        // Limit and correlated non-equality predicate in the subquery
        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT t.b FROM (VALUES (1, 2), (1, 3)) t(a, b) WHERE t.a = t2.a AND t.b > t2.b LIMIT 1) FROM (VALUES (1, 2)) t2(a, b)"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertThat(assertions.query(
                "SELECT (SELECT t.a FROM (VALUES (1, 2), (1, 3)) t(a, b) WHERE t.a = t2.a AND t2.b > 1 LIMIT 1) FROM (VALUES (1, 2)) t2(a, b)"))
                .matches("VALUES 1");
        // TopN and correlated non-equality predicate in the subquery
        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT t.b FROM (VALUES (1, 2), (1, 3)) t(a, b) WHERE t.a = t2.a AND t.b > t2.b ORDER BY t.b LIMIT 1) FROM (VALUES (1, 2)) t2(a, b)"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertThat(assertions.query(
                "SELECT (SELECT t.b FROM (VALUES (1, 2), (1, 3)) t(a, b) WHERE t.a = t2.a AND t2.b > 1 ORDER BY t.b LIMIT 1) FROM (VALUES (1, 2)) t2(a, b)"))
                .matches("VALUES 2");
        assertThat(assertions.query(
                "SELECT (SELECT sum(t.a) FROM (VALUES 1, 2) t(a) WHERE t.a=t2.b group by t.a LIMIT 2) FROM (VALUES 1) t2(b)"))
                .matches("VALUES BIGINT '1'");
        assertThat(assertions.query(
                "SELECT (SELECT count(*) FROM (SELECT t.a FROM (VALUES 1, 1, null, 3) t(a) WHERE t.a=t2.b LIMIT 1)) FROM (VALUES 1, 2) t2(b)"))
                .matches("VALUES BIGINT '1', BIGINT '0'");

        assertions.assertQueryAndPlan(
                "SELECT EXISTS(SELECT 1 FROM (VALUES 1, 1, 3) t(a) WHERE t.a=t2.b LIMIT 1) FROM (VALUES 1, 2) t2(b)",
                "VALUES true, false",
                anyTree(
                        node(JoinNode.class,
                                anyTree(
                                        values("b")),
                                anyTree(
                                        aggregation(ImmutableMap.of(), FINAL,
                                                anyTree(
                                                        aggregation(ImmutableMap.of(), PARTIAL,
                                                                anyTree(
                                                                        values("a")))))))));

        assertThat(assertions.query(
                "SELECT (SELECT count(*) FROM (VALUES 1, 1, 3) t(a) WHERE t.a=t2.b LIMIT 1) FROM (VALUES 1) t2(b)"))
                .matches("VALUES BIGINT '2'");

        assertions.assertQueryAndPlan(
                "SELECT EXISTS(SELECT 1 FROM (VALUES ('x', 1)) u(x, cid) WHERE x = 'x' AND t.cid = cid LIMIT 1) FROM (VALUES 1) t(cid)",
                "VALUES true",
                anyTree(
                        node(JoinNode.class,
                                anyTree(
                                        values("t_cid")),
                                anyTree(
                                        aggregation(ImmutableMap.of(), FINAL,
                                                anyTree(
                                                        aggregation(ImmutableMap.of(), PARTIAL,
                                                                anyTree(
                                                                        values("u_x", "u_cid")))))))));

        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT t.a FROM (VALUES 1, 2, 3) t(a) WHERE t.a = t2.b ORDER BY a FETCH FIRST ROW WITH TIES) FROM (VALUES 1) t2(b)"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertThat(assertions.query(
                "SELECT * " +
                        "FROM (VALUES 1, 2, 3, null) outer_relation(id) " +
                        "LEFT JOIN LATERAL " +
                        "(SELECT value FROM " +
                        "(VALUES " +
                        "(1, 'a'), " +
                        "(1, 'a'), " +
                        "(1, 'a'), " +
                        "(1, 'a'), " +
                        "(2, 'b'), " +
                        "(null, 'c')) inner_relation(id, value) " +
                        "WHERE outer_relation.id = inner_relation.id " +
                        "LIMIT 2) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(1, 'a'), " +
                        "(1, 'a'), " +
                        "(2, 'b'), " +
                        "(3, null), " +
                        "(null, null)");
        // TopN in correlated subquery
        assertThat(assertions.query(
                "SELECT * " +
                        "FROM (VALUES 1, 2, 3, null) outer_relation(id) " +
                        "LEFT JOIN LATERAL " +
                        "(SELECT value FROM " +
                        "(VALUES " +
                        "(1, 'd'), " +
                        "(1, 'c'), " +
                        "(1, 'b'), " +
                        "(1, 'a'), " +
                        "(2, 'w'), " +
                        "(null, 'x')) inner_relation(id, value) " +
                        "WHERE outer_relation.id = inner_relation.id " +
                        "ORDER BY inner_relation.value LIMIT 2) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(1, 'a'), " +
                        "(1, 'b'), " +
                        "(2, 'w'), " +
                        "(3, null), " +
                        "(null, null)");
        // correlated symbol in predicate not bound to inner relation + Limit
        assertThat(assertions.query(
                "SELECT * " +
                        "FROM (VALUES 1, 2, 3, null) outer_relation(id) " +
                        "LEFT JOIN LATERAL " +
                        "(SELECT value FROM (VALUES 'a', 'a', 'a') inner_relation(value) " +
                        "   WHERE outer_relation.id = 3 LIMIT 2) " +
                        "ON TRUE"))
                .matches("VALUES (1, null), (2, null), (3, 'a'), (3, 'a'), (null, null)");
        assertThat(assertions.query(
                "SELECT * " +
                        "FROM (VALUES 1, 2, 3, null) outer_relation(id) " +
                        "LEFT JOIN LATERAL " +
                        "(SELECT 1 FROM (VALUES 'a', 'a', 'a') inner_relation(value) " +
                        "   WHERE outer_relation.id = 3 LIMIT 2) " +
                        "ON TRUE"))
                .matches("VALUES (1, null), (2, null), (3, 1), (3, 1), (null, null)");
        // correlated symbol in predicate not bound to inner relation + TopN
        assertThat(assertions.query(
                "SELECT * " +
                        "FROM (VALUES 1, 2, 3, null) outer_relation(id) " +
                        "LEFT JOIN LATERAL " +
                        "(SELECT value FROM (VALUES 'c', 'a', 'b') inner_relation(value) " +
                        "   WHERE outer_relation.id = 3 ORDER BY value LIMIT 2) " +
                        "ON TRUE"))
                .matches("VALUES (1, null), (2, null), (3, 'a'), (3, 'b'), (null, null)");
        // TopN with ordering not decorrelating
        assertThatThrownBy(() -> assertions.query(
                "SELECT * " +
                        "FROM (VALUES 1, 2, 3, null) outer_relation(id) " +
                        "LEFT JOIN LATERAL " +
                        "(SELECT value FROM (VALUES 'c', 'a', 'b') inner_relation(value) " +
                        "   WHERE outer_relation.id = 3 ORDER BY outer_relation.id LIMIT 2) " +
                        "ON TRUE"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        // TopN with ordering only by constants
        assertThat(assertions.query(
                "SELECT * " +
                        "FROM (VALUES 1, 2, 3, null) outer_relation(id) " +
                        "LEFT JOIN LATERAL " +
                        "(SELECT value FROM (VALUES (3, 'b'), (3, 'a'), (null, 'b')) inner_relation(id, value) " +
                        "   WHERE outer_relation.id = inner_relation.id ORDER BY id LIMIT 2) " +
                        "ON TRUE"))
                .matches("VALUES (1, null), (2, null), (3, 'a'), (3, 'b'), (null, null)");
        // TopN with ordering by constants and non-constant local symbols
        assertThat(assertions.query(
                "SELECT * " +
                        "FROM (VALUES 1, 2, 3, null) outer_relation(id) " +
                        "LEFT JOIN LATERAL " +
                        "(SELECT value FROM (VALUES (3, 'b'), (3, 'a'), (null, 'b')) inner_relation(id, value) " +
                        "   WHERE outer_relation.id = inner_relation.id ORDER BY id, value LIMIT 2) " +
                        "ON TRUE"))
                .matches("VALUES (1, null), (2, null), (3, 'a'), (3, 'b'), (null, null)");
        // TopN with ordering by non-constant local symbols
        assertThat(assertions.query(
                "SELECT * " +
                        "FROM (VALUES 1, 2, 3, null) outer_relation(id) " +
                        "LEFT JOIN LATERAL " +
                        "(SELECT value FROM (VALUES (3, 'b'), (3, 'a'), (null, 'b')) inner_relation(id, value) " +
                        "   WHERE outer_relation.id = inner_relation.id ORDER BY value LIMIT 2) " +
                        "ON TRUE"))
                .matches("VALUES (1, null), (2, null), (3, 'a'), (3, 'b'), (null, null)");
    }

    @Test
    public void testNestedUncorrelatedSubqueryInCorrelatedSubquery()
    {
        // aggregation with empty grouping set
        assertThat(assertions.query(
                "SELECT((SELECT b FROM (SELECT array_agg(a) FROM (VALUES 1) A(a)) B(b) WHERE b = c)) FROM (VALUES ARRAY[1], ARRAY[2]) C(c)"))
                .matches("VALUES ARRAY[1], null");
        // aggregation with multiple grouping sets
        assertThat(assertions.query(
                "SELECT((SELECT b FROM (SELECT count(a) FROM (VALUES (1, 2, 3)) A(a, key_1, key_2) GROUP BY GROUPING SETS ((key_1), (key_2)) LIMIT 1) B(b) WHERE b = c)) FROM (VALUES 1, 2) C(c)"))
                .matches("VALUES BIGINT '1', null");
        // limit 1
        assertThat(assertions.query(
                "SELECT((SELECT c FROM (SELECT b FROM (VALUES (1, 2), (1, 2)) inner_relation(a, b) WHERE a = 1 LIMIT 1) C(c) WHERE c = d)) FROM (VALUES 2) D(d)"))
                .matches("VALUES 2");
    }

    @Test
    public void testCorrelatedSubqueriesWithGroupBy()
    {
        // t.a is not a "constant" column, group by does not guarantee single row per correlated subquery
        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT count(*) FROM (VALUES 1, 2, 3, null) t(a) WHERE t.a<t2.b GROUP BY t.a) FROM (VALUES 1, 2, 3) t2(b)"))
                .hasMessageMatching("Scalar sub-query has returned multiple rows");
        assertThat(assertions.query(
                "SELECT (SELECT count(*) FROM (VALUES 1, 1, 2, 3, null) t(a) WHERE t.a<t2.b GROUP BY t.a HAVING count(*) > 1) FROM (VALUES 1, 2) t2(b)"))
                .matches("VALUES null, BIGINT '2'");

        assertions.assertQueryAndPlan(
                "SELECT EXISTS(SELECT 1 FROM (VALUES 1, 1, 3) t(a) WHERE t.a=t2.b GROUP BY t.a) FROM (VALUES 1, 2) t2(b)",
                "VALUES true, false",
                anyTree(
                        node(JoinNode.class,
                                anyTree(
                                        values("b")),
                                anyTree(
                                        aggregation(ImmutableMap.of(), FINAL,
                                                anyTree(
                                                        aggregation(ImmutableMap.of(), PARTIAL,
                                                                anyTree(
                                                                        values("a")))))))));

        assertions.assertQueryAndPlan(
                "SELECT EXISTS(SELECT 1 FROM (VALUES (1, 2), (1, 2), (null, null), (3, 3)) t(a, b) WHERE t.a=t2.b GROUP BY t.a, t.b) FROM (VALUES 1, 2) t2(b)",
                "VALUES true, false",
                anyTree(
                        node(JoinNode.class,
                                anyTree(
                                        values("t2_b")),
                                anyTree(
                                        aggregation(ImmutableMap.of(), FINAL,
                                                anyTree(
                                                        aggregation(ImmutableMap.of(), PARTIAL,
                                                                anyTree(
                                                                        aggregation(ImmutableMap.of(), FINAL,
                                                                                anyTree(
                                                                                        aggregation(ImmutableMap.of(), PARTIAL,
                                                                                                anyTree(
                                                                                                        values("t_a", "t_b")))))))))))));

        assertions.assertQueryAndPlan(
                "SELECT EXISTS(SELECT 1 FROM (VALUES (1, 2), (1, 2), (null, null), (3, 3)) t(a, b) WHERE t.a<t2.b GROUP BY t.a, t.b) FROM (VALUES 1, 2) t2(b)",
                "VALUES false, true",
                anyTree(
                        aggregation(
                                ImmutableMap.of("COUNT", functionCall("count", ImmutableList.of())),
                                aggregation -> aggregation.isStreamable() && aggregation.getStep() == SINGLE,
                                node(JoinNode.class,
                                        anyTree(
                                                values("t2_b")),
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("NON_NULL", expression("true")),
                                                        anyTree(
                                                                aggregation(
                                                                        ImmutableMap.of(),
                                                                        FINAL,
                                                                        anyTree(
                                                                                values("t_a", "t_b"))))))))));

        // t.b is not a "constant" column, cannot be pushed above aggregation
        assertThatThrownBy(() -> assertions.query(
                "SELECT EXISTS(SELECT 1 FROM (VALUES (1, 1), (1, 1), (null, null), (3, 3)) t(a, b) WHERE t.a+t.b<t2.b GROUP BY t.a) FROM (VALUES 1, 2) t2(b)"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        assertions.assertQueryAndPlan(
                "SELECT EXISTS(SELECT 1 FROM (VALUES (1, 1), (1, 1), (null, null), (3, 3)) t(a, b) WHERE t.a+t.b<t2.b GROUP BY t.a, t.b) FROM (VALUES 1, 4) t2(b)",
                "VALUES false, true",
                anyTree(
                        aggregation(
                                ImmutableMap.of("COUNT", functionCall("count", ImmutableList.of())),
                                aggregation -> aggregation.isStreamable() && aggregation.getStep() == SINGLE,
                                node(JoinNode.class,
                                        anyTree(
                                                values("t2_b")),
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("NON_NULL", expression("true")),
                                                        aggregation(
                                                                ImmutableMap.of(),
                                                                FINAL,
                                                                anyTree(
                                                                        values("t_a", "t_b")))))))));

        assertions.assertQueryAndPlan(
                "SELECT EXISTS(SELECT 1 FROM (VALUES (1, 2), (1, 2), (null, null), (3, 3)) t(a, b) WHERE t.a=t2.b GROUP BY t.b) FROM (VALUES 1, 2) t2(b)",
                "VALUES true, false",
                anyTree(
                        node(JoinNode.class,
                                anyTree(
                                        values("t2_b")),
                                anyTree(
                                        aggregation(ImmutableMap.of(), FINAL,
                                                anyTree(
                                                        aggregation(ImmutableMap.of(), PARTIAL,
                                                                anyTree(
                                                                        aggregation(ImmutableMap.of(), FINAL,
                                                                                anyTree(
                                                                                        aggregation(ImmutableMap.of(), PARTIAL,
                                                                                                anyTree(
                                                                                                        values("t_a", "t_b")))))))))))));

        assertions.assertQueryAndPlan(
                "SELECT EXISTS(SELECT * FROM (VALUES 1, 1, 2, 3) t(a) WHERE t.a=t2.b GROUP BY t.a HAVING count(*) > 1) FROM (VALUES 1, 2) t2(b)",
                "VALUES true, false",
                anyTree(
                        node(JoinNode.class,
                                anyTree(
                                        values("b")),
                                anyTree(
                                        aggregation(ImmutableMap.of(), FINAL,
                                                anyTree(
                                                        aggregation(ImmutableMap.of(), PARTIAL,
                                                                anyTree(
                                                                        values("a")))))))));

        assertThat(assertions.query(
                "SELECT EXISTS(SELECT * FROM (SELECT t.a FROM (VALUES (1, 1), (1, 1), (1, 2), (1, 2), (3, 3)) t(a, b) WHERE t.b=t2.b GROUP BY t.a HAVING count(*) > 1) t WHERE t.a=t2.b)" +
                        " FROM (VALUES 1, 2) t2(b)"))
                .matches("VALUES true, false");

        assertions.assertQueryAndPlan(
                "SELECT EXISTS(SELECT * FROM (VALUES 1, 1, 2, 3) t(a) WHERE t.a=t2.b GROUP BY (t.a) HAVING count(*) > 1) FROM (VALUES 1, 2) t2(b)",
                "VALUES true, false",
                anyTree(
                        node(JoinNode.class,
                                anyTree(
                                        values("b")),
                                anyTree(
                                        aggregation(ImmutableMap.of(), FINAL,
                                                anyTree(
                                                        aggregation(ImmutableMap.of(), PARTIAL,
                                                                anyTree(
                                                                        values("a")))))))));
    }

    @Test
    public void testCorrelatedLateralWithGroupBy()
    {
        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2) t2(b), LATERAL (SELECT t.a FROM (VALUES 1, 1, 3) t(a) WHERE t.a=t2.b GROUP BY t.a)"))
                .matches("VALUES (1, 1)");
        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2) t2(b), LATERAL (SELECT count(*) FROM (VALUES 1, 1, 2, 3) t(a) WHERE t.a=t2.b GROUP BY t.a HAVING count(*) > 1)"))
                .matches("VALUES (1, BIGINT '2')");
        // correlated subqueries with grouping sets are not supported
        assertThatThrownBy(() -> assertions.query(
                "SELECT * FROM (VALUES 1, 2) t2(b), LATERAL (SELECT t.a, t.b, count(*) FROM (VALUES (1, 1), (1, 2), (2, 2), (3, 3)) t(a, b) WHERE t.a=t2.b GROUP BY GROUPING SETS ((t.a, t.b), (t.a)))"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Test
    public void testUncorrelatedSubquery()
    {
        assertThat(assertions.query("SELECT * FROM (VALUES 1, 3, null) t(a) INNER JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a < b")).matches("SELECT 1, 2");
        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (VALUES 1, 3, null) t(a) INNER JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a * 8 < b");
        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (SELECT 1 WHERE 0 = 1) t(a) INNER JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a < b");
        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (VALUES 1, 3, null) t(a) INNER JOIN LATERAL (SELECT 1 WHERE 0 = 1) t2(b) ON a < b");

        assertThat(assertions.query("SELECT * FROM (VALUES 1, 3, null) t(a) LEFT JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a < b")).matches("VALUES (1, 2), (3, null), (null, null)");
        assertThat(assertions.query("SELECT * FROM (VALUES 1, 3, null) t(a) LEFT JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a * 8 < b")).matches("VALUES (1, CAST(null AS INTEGER)), (3, null), (null, null)");
        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (SELECT 1 WHERE 0 = 1) t(a) LEFT JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a < b");
        assertThat(assertions.query("SELECT * FROM (VALUES 1, 3, null) t(a) LEFT JOIN LATERAL (SELECT 1 WHERE 0 = 1) t2(b) ON a < b")).matches("VALUES (1, CAST(null AS INTEGER)), (3, null), (null, null)");

        assertThat(assertions.query("SELECT * FROM (VALUES 1, null) t(a) RIGHT JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a < b")).matches("VALUES (1, 2), (null, null), (null, 2), (null, null)");
        assertThat(assertions.query("SELECT * FROM (VALUES 1, null) t(a) RIGHT JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a * 8 < b")).matches("VALUES (CAST(null AS INTEGER), 2), (null, null), (null, 2), (null, null)");
        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (SELECT 1 WHERE 0 = 1) t(a) RIGHT JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a < b");
        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (VALUES 1, 3, null) t(a) RIGHT JOIN LATERAL (SELECT 1 WHERE 0 = 1) t2(b) ON a < b");
        assertThat(assertions.query("SELECT * FROM (VALUES 1, null) t(a) RIGHT JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON TRUE")).matches("VALUES (1, 2), (1, null), (null, 2), (null, null)");

        assertThat(assertions.query("SELECT * FROM (VALUES 1, null) t(a) FULL JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON TRUE")).matches("VALUES (1, 2), (1, null), (null, 2), (null, null)");
        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (SELECT 1 WHERE 0 = 1) t(a) FULL JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON TRUE");
        assertThat(assertions.query("SELECT * FROM (VALUES 1, null) t(a) FULL JOIN LATERAL (SELECT 1 WHERE 0 = 1) t2(b) ON TRUE")).matches("VALUES (1, CAST(null AS INTEGER)), (null, null)");
        assertThatThrownBy(() -> assertions.query(
                "SELECT * FROM (VALUES 1, null) t(a) FULL JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a < b"))
                .hasMessageMatching(".* FULL JOIN involving LATERAL relation is only supported with condition ON TRUE");
    }

    @Test
    public void testLateralWithUnnest()
    {
        assertThatThrownBy(() -> assertions.query(
                "SELECT * FROM (VALUES ARRAY[1]) t(x), LATERAL (SELECT * FROM UNNEST(x))"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Test
    public void testCorrelatedScalarSubquery()
    {
        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2) t2(b) WHERE (SELECT b) = 2"))
                .matches("VALUES 2");
    }

    @Test
    public void testRemoveUnreferencedScalarSubqueryOrInput()
    {
        // scalar unreferenced input, empty subquery, nonempty correlation
        assertions.assertQueryReturnsEmptyResult("SELECT b FROM (VALUES 1) t(a) INNER JOIN LATERAL (SELECT 2 WHERE a = 2) t2(b) ON true");
        assertThat(assertions.query("SELECT b FROM (VALUES 1) t(a) LEFT JOIN LATERAL (SELECT 2 WHERE a = 2) t2(b) ON true")).matches("VALUES CAST(null AS INTEGER)");

        // scalar unreferenced input, empty subquery, empty correlation
        assertions.assertQueryReturnsEmptyResult("SELECT b FROM (VALUES 1) t(a) INNER JOIN LATERAL (SELECT 2 WHERE 0 = 1) t2(b) ON true");
        assertThat(assertions.query("SELECT b FROM (VALUES 1) t(a) LEFT JOIN LATERAL (SELECT 2 WHERE 0 = 1) t2(b) ON true")).matches("VALUES CAST(null AS INTEGER)");
        assertions.assertQueryReturnsEmptyResult("SELECT b FROM (VALUES 1) t(a) RIGHT JOIN LATERAL (SELECT 2 WHERE 0 = 1) t2(b) ON true");
        assertThat(assertions.query("SELECT b FROM (VALUES 1) t(a) FULL JOIN LATERAL (SELECT 2 WHERE 0 = 1) t2(b) ON true")).matches("VALUES CAST(null AS INTEGER)");

        // scalar unreferenced subquery, at least scalar input
        assertThat(assertions.query("SELECT a FROM (VALUES 1, 2) t(a) INNER JOIN LATERAL (VALUES a) t2(b) ON true")).matches("VALUES 1, 2");
        assertThat(assertions.query("SELECT a FROM (VALUES 1, 2) t(a) LEFT JOIN LATERAL (VALUES a) t2(b) ON true")).matches("VALUES 1, 2");
        assertThat(assertions.query("SELECT a FROM (VALUES 1, 2) t(a) RIGHT JOIN LATERAL (VALUES 3) t2(b) ON true")).matches("VALUES 1, 2");
        assertThat(assertions.query("SELECT a FROM (VALUES 1, 2) t(a) FULL JOIN LATERAL (VALUES 3) t2(b) ON true")).matches("VALUES 1, 2");

        // scalar unreferenced subquery, empty input
        assertions.assertQueryReturnsEmptyResult("SELECT a FROM (SELECT 1 where 0 = 1) t(a) INNER JOIN LATERAL (VALUES a) t2(b) ON true");
        assertions.assertQueryReturnsEmptyResult("SELECT a FROM (SELECT 1 where 0 = 1) t(a) LEFT JOIN LATERAL (VALUES a) t2(b) ON true");
        assertions.assertQueryReturnsEmptyResult("SELECT a FROM (SELECT 1 where 0 = 1) t(a) RIGHT JOIN LATERAL (VALUES 2) t2(b) ON true");
        assertions.assertQueryReturnsEmptyResult("SELECT a FROM (SELECT 1 where 0 = 1) t(a) FULL JOIN LATERAL (VALUES 2) t2(b) ON true");
    }

    @Test
    public void testCorrelatedSubqueryWithExplicitCoercion()
    {
        assertThat(assertions.query(
                "SELECT 1 FROM (VALUES 1, 2) t1(b) WHERE 1 = (SELECT cast(b as decimal(7,2)))"))
                .matches("VALUES 1");
    }

    @Test
    public void testCorrelation()
    {
        // unqualified reference
        assertThat(assertions.query(
                "SELECT * " +
                        "FROM (VALUES 1) t(x) " +
                        "WHERE EXISTS (" +
                        "    SELECT count(*)" +
                        "    FROM (VALUES 1, 2) u(y)" +
                        "    GROUP BY y" +
                        "    HAVING y = x)"))
                .matches("VALUES 1");

        // qualified reference
        assertThat(assertions.query(
                "SELECT * " +
                        "FROM (VALUES 1) t(x) " +
                        "WHERE EXISTS (" +
                        "    SELECT count(*)" +
                        "    FROM (VALUES 1, 2) u(y)" +
                        "    GROUP BY y" +
                        "    HAVING y = t.x)"))
                .matches("VALUES 1");
    }

    @Test
    public void testCorrelatedSubqueryWithoutFilter()
    {
        assertThat(assertions.query(
                "SELECT (SELECT outer_relation.b FROM (VALUES 1) inner_relation) FROM (values 2) outer_relation(b)"))
                .matches("VALUES 2");
        assertThatThrownBy(() -> assertions.query(
                "SELECT (VALUES b) FROM (VALUES 2) outer_relation(b)"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT a + b FROM (VALUES 1) inner_relation(a)) FROM (VALUES 2) outer_relation(b)"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT rank() OVER(partition by b) FROM (VALUES 1) inner_relation(a)) FROM (VALUES 2) outer_relation(b)"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Test
    public void testCorrelatedJoin()
    {
        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, 3, null) t1(a) INNER JOIN LATERAL (SELECT b FROM (VALUES 2, 3, null) t2(b) WHERE b > a) ON TRUE"))
                .matches("VALUES (1, 2), (1, 3), (2, 3)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, 3, null) t1(a) INNER JOIN LATERAL (SELECT b FROM (VALUES 2, 3, null) t2(b) WHERE b > a) ON b < 3"))
                .matches("VALUES (1, 2)");

        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (SELECT 1 where 0 = 1) t(a) INNER JOIN LATERAL (SELECT 2 WHERE a = 1 ) t2(b) ON TRUE");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, 3, null) t1(a) LEFT JOIN LATERAL (SELECT b FROM (VALUES 2, 3, null) t2(b) WHERE b > a) ON TRUE"))
                .matches("VALUES (1, 2), (1, 3), (2, 3), (3, null), (null, null)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, 3, null) t1(a) LEFT JOIN LATERAL (SELECT b FROM (VALUES 2, 3, null) t2(b) WHERE b > a) ON b < 3"))
                .matches("VALUES (1, 2), (2, null), (3, null), (null, null)");

        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (SELECT 1 where 0 = 1) t(a) LEFT JOIN LATERAL (SELECT 2 WHERE a = 1 ) t2(b) ON TRUE");
    }
}
