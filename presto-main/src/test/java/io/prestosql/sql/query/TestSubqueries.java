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
package io.prestosql.sql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.plan.JoinNode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.SINGLE;

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
                                ImmutableMap.of("COUNT", functionCall("count", ImmutableList.of("NON_NULL"))),
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
                                ImmutableMap.of("COUNT", functionCall("count", ImmutableList.of("NON_NULL"))),
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
        assertions.assertFails(
                "SELECT EXISTS(SELECT 1 FROM (VALUES (1, null)) t(a, b) WHERE t.a=t2.b GROUP BY t.b) FROM (VALUES 1.0, 2.0) t2(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        // coercion from t.a (null) to integer
        assertions.assertFails(
                "SELECT EXISTS(SELECT 1 FROM (VALUES (null, null)) t(a, b) WHERE t.a=t2.b GROUP BY t.b) FROM (VALUES 1, 2) t2(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Test
    public void testCorrelatedSubqueriesWithLimit()
    {
        assertions.assertQuery(
                "SELECT (SELECT t.a FROM (VALUES 1, 2) t(a) WHERE t.a=t2.b LIMIT 1) FROM (VALUES 1) t2(b)",
                "VALUES 1");
        assertions.assertQuery(
                "SELECT (SELECT t.a FROM (VALUES 1, 2) t(a) WHERE t.a=t2.b LIMIT 2) FROM (VALUES 1) t2(b)",
                "VALUES 1");
        assertions.assertQuery(
                "SELECT (SELECT t.a FROM (VALUES 1, 2, 3) t(a) WHERE t.a = t2.b LIMIT 2) FROM (VALUES 1) t2(b)",
                "VALUES 1");
        assertions.assertFails(
                "SELECT (SELECT t.a FROM (VALUES 1, 1, 2, 3) t(a) WHERE t.a = t2.b LIMIT 2) FROM (VALUES 1) t2(b)",
                "Scalar sub-query has returned multiple rows");
        // Limit(1) and non-constant output symbol of the subquery
        assertions.assertFails(
                "SELECT (SELECT count(*) FROM (VALUES (1, 0), (1, 1)) t(a, b) WHERE a = c GROUP BY b LIMIT 1) FROM (VALUES (1)) t2(c)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        // Limit(1) and non-constant output symbol of the subquery
        assertions.assertFails(
                "SELECT (SELECT a + b FROM (VALUES (1, 1), (1, 1)) t(a, b) WHERE a = c LIMIT 1) FROM (VALUES (1)) t2(c)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        // Limit and correlated non-equality predicate in the subquery
        assertions.assertFails(
                "SELECT (SELECT t.b FROM (VALUES (1, 2), (1, 3)) t(a, b) WHERE t.a = t2.a AND t.b > t2.b LIMIT 1) FROM (VALUES (1, 2)) t2(a, b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertions.assertQuery(
                "SELECT (SELECT t.a FROM (VALUES (1, 2), (1, 3)) t(a, b) WHERE t.a = t2.a AND t2.b > 1 LIMIT 1) FROM (VALUES (1, 2)) t2(a, b)",
                "VALUES 1");
        // TopN and correlated non-equality predicate in the subquery
        assertions.assertFails(
                "SELECT (SELECT t.b FROM (VALUES (1, 2), (1, 3)) t(a, b) WHERE t.a = t2.a AND t.b > t2.b ORDER BY t.b LIMIT 1) FROM (VALUES (1, 2)) t2(a, b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertions.assertQuery(
                "SELECT (SELECT t.b FROM (VALUES (1, 2), (1, 3)) t(a, b) WHERE t.a = t2.a AND t2.b > 1 ORDER BY t.b LIMIT 1) FROM (VALUES (1, 2)) t2(a, b)",
                "VALUES 2");
        assertions.assertQuery(
                "SELECT (SELECT sum(t.a) FROM (VALUES 1, 2) t(a) WHERE t.a=t2.b group by t.a LIMIT 2) FROM (VALUES 1) t2(b)",
                "VALUES BIGINT '1'");
        assertions.assertQuery(
                "SELECT (SELECT count(*) FROM (SELECT t.a FROM (VALUES 1, 1, null, 3) t(a) WHERE t.a=t2.b LIMIT 1)) FROM (VALUES 1, 2) t2(b)",
                "VALUES BIGINT '1', BIGINT '0'");

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

        assertions.assertQuery(
                "SELECT (SELECT count(*) FROM (VALUES 1, 1, 3) t(a) WHERE t.a=t2.b LIMIT 1) FROM (VALUES 1) t2(b)",
                "VALUES BIGINT '2'");

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

        assertions.assertFails(
                "SELECT (SELECT t.a FROM (VALUES 1, 2, 3) t(a) WHERE t.a = t2.b ORDER BY a FETCH FIRST ROW WITH TIES) FROM (VALUES 1) t2(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertions.assertQuery(
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
                        "ON TRUE",
                "VALUES " +
                        "(1, 'a'), " +
                        "(1, 'a'), " +
                        "(2, 'b'), " +
                        "(3, null), " +
                        "(null, null)");
        // TopN in correlated subquery
        assertions.assertQuery(
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
                        "ON TRUE",
                "VALUES " +
                        "(1, 'a'), " +
                        "(1, 'b'), " +
                        "(2, 'w'), " +
                        "(3, null), " +
                        "(null, null)");
        // correlated symbol in predicate not bound to inner relation + Limit
        assertions.assertQuery(
                "SELECT * " +
                        "FROM (VALUES 1, 2, 3, null) outer_relation(id) " +
                        "LEFT JOIN LATERAL " +
                        "(SELECT value FROM (VALUES 'a', 'a', 'a') inner_relation(value) " +
                        "   WHERE outer_relation.id = 3 LIMIT 2) " +
                        "ON TRUE",
                "VALUES (1, null), (2, null), (3, 'a'), (3, 'a'), (null, null)");
        assertions.assertQuery(
                "SELECT * " +
                        "FROM (VALUES 1, 2, 3, null) outer_relation(id) " +
                        "LEFT JOIN LATERAL " +
                        "(SELECT 1 FROM (VALUES 'a', 'a', 'a') inner_relation(value) " +
                        "   WHERE outer_relation.id = 3 LIMIT 2) " +
                        "ON TRUE",
                "VALUES (1, null), (2, null), (3, 1), (3, 1), (null, null)");
        // correlated symbol in predicate not bound to inner relation + TopN
        assertions.assertQuery(
                "SELECT * " +
                        "FROM (VALUES 1, 2, 3, null) outer_relation(id) " +
                        "LEFT JOIN LATERAL " +
                        "(SELECT value FROM (VALUES 'c', 'a', 'b') inner_relation(value) " +
                        "   WHERE outer_relation.id = 3 ORDER BY value LIMIT 2) " +
                        "ON TRUE",
                "VALUES (1, null), (2, null), (3, 'a'), (3, 'b'), (null, null)");
        // TopN with ordering not decorrelating
        assertions.assertFails(
                "SELECT * " +
                        "FROM (VALUES 1, 2, 3, null) outer_relation(id) " +
                        "LEFT JOIN LATERAL " +
                        "(SELECT value FROM (VALUES 'c', 'a', 'b') inner_relation(value) " +
                        "   WHERE outer_relation.id = 3 ORDER BY outer_relation.id LIMIT 2) " +
                        "ON TRUE",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        // TopN with ordering only by constants
        assertions.assertQuery(
                "SELECT * " +
                        "FROM (VALUES 1, 2, 3, null) outer_relation(id) " +
                        "LEFT JOIN LATERAL " +
                        "(SELECT value FROM (VALUES (3, 'b'), (3, 'a'), (null, 'b')) inner_relation(id, value) " +
                        "   WHERE outer_relation.id = inner_relation.id ORDER BY id LIMIT 2) " +
                        "ON TRUE",
                "VALUES (1, null), (2, null), (3, 'a'), (3, 'b'), (null, null)");
        // TopN with ordering by constants and non-constant local symbols
        assertions.assertQuery(
                "SELECT * " +
                        "FROM (VALUES 1, 2, 3, null) outer_relation(id) " +
                        "LEFT JOIN LATERAL " +
                        "(SELECT value FROM (VALUES (3, 'b'), (3, 'a'), (null, 'b')) inner_relation(id, value) " +
                        "   WHERE outer_relation.id = inner_relation.id ORDER BY id, value LIMIT 2) " +
                        "ON TRUE",
                "VALUES (1, null), (2, null), (3, 'a'), (3, 'b'), (null, null)");
        // TopN with ordering by non-constant local symbols
        assertions.assertQuery(
                "SELECT * " +
                        "FROM (VALUES 1, 2, 3, null) outer_relation(id) " +
                        "LEFT JOIN LATERAL " +
                        "(SELECT value FROM (VALUES (3, 'b'), (3, 'a'), (null, 'b')) inner_relation(id, value) " +
                        "   WHERE outer_relation.id = inner_relation.id ORDER BY value LIMIT 2) " +
                        "ON TRUE",
                "VALUES (1, null), (2, null), (3, 'a'), (3, 'b'), (null, null)");
    }

    @Test
    public void testNestedUncorrelatedSubqueryInCorrelatedSubquery()
    {
        // aggregation with empty grouping set
        assertions.assertQuery(
                "SELECT((SELECT b FROM (SELECT array_agg(a) FROM (VALUES 1) A(a)) B(b) WHERE b = c)) FROM (VALUES ARRAY[1], ARRAY[2]) C(c)",
                "VALUES ARRAY[1], null");
        // aggregation with multiple grouping sets
        assertions.assertQuery(
                "SELECT((SELECT b FROM (SELECT count(a) FROM (VALUES (1, 2, 3)) A(a, key_1, key_2) GROUP BY GROUPING SETS ((key_1), (key_2)) LIMIT 1) B(b) WHERE b = c)) FROM (VALUES 1, 2) C(c)",
                "VALUES BIGINT '1', null");
        // limit 1
        assertions.assertQuery(
                "SELECT((SELECT c FROM (SELECT b FROM (VALUES (1, 2), (1, 2)) inner_relation(a, b) WHERE a = 1 LIMIT 1) C(c) WHERE c = d)) FROM (VALUES 2) D(d)",
                "VALUES 2");
    }

    @Test
    public void testCorrelatedSubqueriesWithGroupBy()
    {
        // t.a is not a "constant" column, group by does not guarantee single row per correlated subquery
        assertions.assertFails(
                "SELECT (SELECT count(*) FROM (VALUES 1, 2, 3, null) t(a) WHERE t.a<t2.b GROUP BY t.a) FROM (VALUES 1, 2, 3) t2(b)",
                "Scalar sub-query has returned multiple rows");
        assertions.assertQuery(
                "SELECT (SELECT count(*) FROM (VALUES 1, 1, 2, 3, null) t(a) WHERE t.a<t2.b GROUP BY t.a HAVING count(*) > 1) FROM (VALUES 1, 2) t2(b)",
                "VALUES null, BIGINT '2'");

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
                                ImmutableMap.of("COUNT", functionCall("count", ImmutableList.of("NON_NULL"))),
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
        assertions.assertFails(
                "SELECT EXISTS(SELECT 1 FROM (VALUES (1, 1), (1, 1), (null, null), (3, 3)) t(a, b) WHERE t.a+t.b<t2.b GROUP BY t.a) FROM (VALUES 1, 2) t2(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        assertions.assertQueryAndPlan(
                "SELECT EXISTS(SELECT 1 FROM (VALUES (1, 1), (1, 1), (null, null), (3, 3)) t(a, b) WHERE t.a+t.b<t2.b GROUP BY t.a, t.b) FROM (VALUES 1, 4) t2(b)",
                "VALUES false, true",
                anyTree(
                        aggregation(
                                ImmutableMap.of("COUNT", functionCall("count", ImmutableList.of("NON_NULL"))),
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

        assertions.assertQuery(
                "SELECT EXISTS(SELECT * FROM (SELECT t.a FROM (VALUES (1, 1), (1, 1), (1, 2), (1, 2), (3, 3)) t(a, b) WHERE t.b=t2.b GROUP BY t.a HAVING count(*) > 1) t WHERE t.a=t2.b)" +
                        " FROM (VALUES 1, 2) t2(b)",
                "VALUES true, false");

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
        assertions.assertQuery(
                "SELECT * FROM (VALUES 1, 2) t2(b), LATERAL (SELECT t.a FROM (VALUES 1, 1, 3) t(a) WHERE t.a=t2.b GROUP BY t.a)",
                "VALUES (1, 1)");
        assertions.assertQuery(
                "SELECT * FROM (VALUES 1, 2) t2(b), LATERAL (SELECT count(*) FROM (VALUES 1, 1, 2, 3) t(a) WHERE t.a=t2.b GROUP BY t.a HAVING count(*) > 1)",
                "VALUES (1, BIGINT '2')");
        // correlated subqueries with grouping sets are not supported
        assertions.assertFails(
                "SELECT * FROM (VALUES 1, 2) t2(b), LATERAL (SELECT t.a, t.b, count(*) FROM (VALUES (1, 1), (1, 2), (2, 2), (3, 3)) t(a, b) WHERE t.a=t2.b GROUP BY GROUPING SETS ((t.a, t.b), (t.a)))",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Test
    public void testUncorrelatedSubquery()
    {
        assertions.assertQuery("SELECT * FROM (VALUES 1, 3, null) t(a) INNER JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a < b", "SELECT 1, 2");
        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (VALUES 1, 3, null) t(a) INNER JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a * 8 < b");
        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (SELECT 1 WHERE 0 = 1) t(a) INNER JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a < b");
        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (VALUES 1, 3, null) t(a) INNER JOIN LATERAL (SELECT 1 WHERE 0 = 1) t2(b) ON a < b");

        assertions.assertQuery("SELECT * FROM (VALUES 1, 3, null) t(a) LEFT JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a < b", "VALUES (1, 2), (3, null), (null, null)");
        assertions.assertQuery("SELECT * FROM (VALUES 1, 3, null) t(a) LEFT JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a * 8 < b", "VALUES (1, CAST(null AS INTEGER)), (3, null), (null, null)");
        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (SELECT 1 WHERE 0 = 1) t(a) LEFT JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a < b");
        assertions.assertQuery("SELECT * FROM (VALUES 1, 3, null) t(a) LEFT JOIN LATERAL (SELECT 1 WHERE 0 = 1) t2(b) ON a < b", "VALUES (1, CAST(null AS INTEGER)), (3, null), (null, null)");

        assertions.assertQuery("SELECT * FROM (VALUES 1, null) t(a) RIGHT JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a < b", "VALUES (1, 2), (null, null), (null, 2), (null, null)");
        assertions.assertQuery("SELECT * FROM (VALUES 1, null) t(a) RIGHT JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a * 8 < b", "VALUES (CAST(null AS INTEGER), 2), (null, null), (null, 2), (null, null)");
        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (SELECT 1 WHERE 0 = 1) t(a) RIGHT JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a < b");
        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (VALUES 1, 3, null) t(a) RIGHT JOIN LATERAL (SELECT 1 WHERE 0 = 1) t2(b) ON a < b");
        assertions.assertQuery("SELECT * FROM (VALUES 1, null) t(a) RIGHT JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON TRUE", "VALUES (1, 2), (1, null), (null, 2), (null, null)");

        assertions.assertQuery("SELECT * FROM (VALUES 1, null) t(a) FULL JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON TRUE", "VALUES (1, 2), (1, null), (null, 2), (null, null)");
        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (SELECT 1 WHERE 0 = 1) t(a) FULL JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON TRUE");
        assertions.assertQuery("SELECT * FROM (VALUES 1, null) t(a) FULL JOIN LATERAL (SELECT 1 WHERE 0 = 1) t2(b) ON TRUE", "VALUES (1, CAST(null AS INTEGER)), (null, null)");
        assertions.assertFails(
                "SELECT * FROM (VALUES 1, null) t(a) FULL JOIN LATERAL (SELECT * FROM (VALUES 2, null)) t2(b) ON a < b",
                ".* FULL JOIN involving LATERAL relation is only supported with condition ON TRUE");
    }

    @Test
    public void testLateralWithUnnest()
    {
        assertions.assertFails(
                "SELECT * FROM (VALUES ARRAY[1]) t(x), LATERAL (SELECT * FROM UNNEST(x))",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Test
    public void testCorrelatedScalarSubquery()
    {
        assertions.assertQuery(
                "SELECT * FROM (VALUES 1, 2) t2(b) WHERE (SELECT b) = 2",
                "VALUES 2");
    }

    @Test
    public void testRemoveUnreferencedScalarSubqueryOrInput()
    {
        // scalar unreferenced input, empty subquery, nonempty correlation
        assertions.assertQueryReturnsEmptyResult("SELECT b FROM (VALUES 1) t(a) INNER JOIN LATERAL (SELECT 2 WHERE a = 2) t2(b) ON true");
        assertions.assertQuery("SELECT b FROM (VALUES 1) t(a) LEFT JOIN LATERAL (SELECT 2 WHERE a = 2) t2(b) ON true", "VALUES CAST(null AS INTEGER)");

        // scalar unreferenced input, empty subquery, empty correlation
        assertions.assertQueryReturnsEmptyResult("SELECT b FROM (VALUES 1) t(a) INNER JOIN LATERAL (SELECT 2 WHERE 0 = 1) t2(b) ON true");
        assertions.assertQuery("SELECT b FROM (VALUES 1) t(a) LEFT JOIN LATERAL (SELECT 2 WHERE 0 = 1) t2(b) ON true", "VALUES CAST(null AS INTEGER)");
        assertions.assertQueryReturnsEmptyResult("SELECT b FROM (VALUES 1) t(a) RIGHT JOIN LATERAL (SELECT 2 WHERE 0 = 1) t2(b) ON true");
        assertions.assertQuery("SELECT b FROM (VALUES 1) t(a) FULL JOIN LATERAL (SELECT 2 WHERE 0 = 1) t2(b) ON true", "VALUES CAST(null AS INTEGER)");

        // scalar unreferenced subquery, at least scalar input
        assertions.assertQuery("SELECT a FROM (VALUES 1, 2) t(a) INNER JOIN LATERAL (VALUES a) t2(b) ON true", "VALUES 1, 2");
        assertions.assertQuery("SELECT a FROM (VALUES 1, 2) t(a) LEFT JOIN LATERAL (VALUES a) t2(b) ON true", "VALUES 1, 2");
        assertions.assertQuery("SELECT a FROM (VALUES 1, 2) t(a) RIGHT JOIN LATERAL (VALUES 3) t2(b) ON true", "VALUES 1, 2");
        assertions.assertQuery("SELECT a FROM (VALUES 1, 2) t(a) FULL JOIN LATERAL (VALUES 3) t2(b) ON true", "VALUES 1, 2");

        // scalar unreferenced subquery, empty input
        assertions.assertQueryReturnsEmptyResult("SELECT a FROM (SELECT 1 where 0 = 1) t(a) INNER JOIN LATERAL (VALUES a) t2(b) ON true");
        assertions.assertQueryReturnsEmptyResult("SELECT a FROM (SELECT 1 where 0 = 1) t(a) LEFT JOIN LATERAL (VALUES a) t2(b) ON true");
        assertions.assertQueryReturnsEmptyResult("SELECT a FROM (SELECT 1 where 0 = 1) t(a) RIGHT JOIN LATERAL (VALUES 2) t2(b) ON true");
        assertions.assertQueryReturnsEmptyResult("SELECT a FROM (SELECT 1 where 0 = 1) t(a) FULL JOIN LATERAL (VALUES 2) t2(b) ON true");
    }

    @Test
    public void testCorrelatedSubqueryWithExplicitCoercion()
    {
        assertions.assertQuery(
                "SELECT 1 FROM (VALUES 1, 2) t1(b) WHERE 1 = (SELECT cast(b as decimal(7,2)))",
                "VALUES 1");
    }

    @Test
    public void testCorrelation()
    {
        // unqualified reference
        assertions.assertQuery(
                "SELECT * " +
                        "FROM (VALUES 1) t(x) " +
                        "WHERE EXISTS (" +
                        "    SELECT count(*)" +
                        "    FROM (VALUES 1, 2) u(y)" +
                        "    GROUP BY y" +
                        "    HAVING y = x)",
                "VALUES 1");

        // qualified reference
        assertions.assertQuery(
                "SELECT * " +
                        "FROM (VALUES 1) t(x) " +
                        "WHERE EXISTS (" +
                        "    SELECT count(*)" +
                        "    FROM (VALUES 1, 2) u(y)" +
                        "    GROUP BY y" +
                        "    HAVING y = t.x)",
                "VALUES 1");
    }

    @Test
    public void testCorrelatedSubqueryWithoutFilter()
    {
        assertions.assertQuery(
                "SELECT (SELECT outer_relation.b FROM (VALUES 1) inner_relation) FROM (values 2) outer_relation(b)",
                "VALUES 2");
        assertions.assertFails(
                "SELECT (VALUES b) FROM (VALUES 2) outer_relation(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertions.assertFails(
                "SELECT (SELECT a + b FROM (VALUES 1) inner_relation(a)) FROM (VALUES 2) outer_relation(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertions.assertFails(
                "SELECT (SELECT rank() OVER(partition by b) FROM (VALUES 1) inner_relation(a)) FROM (VALUES 2) outer_relation(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Test
    public void testCorrelatedJoin()
    {
        assertions.assertQuery(
                "SELECT * FROM (VALUES 1, 2, 3, null) t1(a) INNER JOIN LATERAL (SELECT b FROM (VALUES 2, 3, null) t2(b) WHERE b > a) ON TRUE",
                "VALUES (1, 2), (1, 3), (2, 3)");

        assertions.assertQuery(
                "SELECT * FROM (VALUES 1, 2, 3, null) t1(a) INNER JOIN LATERAL (SELECT b FROM (VALUES 2, 3, null) t2(b) WHERE b > a) ON b < 3",
                "VALUES (1, 2)");

        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (SELECT 1 where 0 = 1) t(a) INNER JOIN LATERAL (SELECT 2 WHERE a = 1 ) t2(b) ON TRUE");

        assertions.assertQuery(
                "SELECT * FROM (VALUES 1, 2, 3, null) t1(a) LEFT JOIN LATERAL (SELECT b FROM (VALUES 2, 3, null) t2(b) WHERE b > a) ON TRUE",
                "VALUES (1, 2), (1, 3), (2, 3), (3, null), (null, null)");

        assertions.assertQuery(
                "SELECT * FROM (VALUES 1, 2, 3, null) t1(a) LEFT JOIN LATERAL (SELECT b FROM (VALUES 2, 3, null) t2(b) WHERE b > a) ON b < 3",
                "VALUES (1, 2), (2, null), (3, null), (null, null)");

        assertions.assertQueryReturnsEmptyResult("SELECT * FROM (SELECT 1 where 0 = 1) t(a) LEFT JOIN LATERAL (SELECT 2 WHERE a = 1 ) t2(b) ON TRUE");
    }
}
