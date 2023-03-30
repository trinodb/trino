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
import io.trino.Session;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.testing.LocalQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestSubqueries
{
    private static final String UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG = "line .*: Given correlated subquery is not supported";

    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        Session session = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        LocalQueryRunner runner = LocalQueryRunner.builder(session)
                .build();

        runner.createCatalog(TEST_CATALOG_NAME, new TpchConnectorFactory(1), ImmutableMap.of());

        assertions = new QueryAssertions(runner);
    }

    @AfterAll
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
    public void testSubqueriesWithGroupByAndCoercions()
    {
        // In the following examples, t.a is determined to be constant
        // coercion FROM subquery symbol type to correlation type
        assertThat(assertions.query(
                "SELECT EXISTS(SELECT 1 FROM (VALUES (1, null)) t(a, b) WHERE t.a=t2.b GROUP BY t.b) FROM (VALUES 1.0, 2.0) t2(b)"))
                .matches("VALUES true, false");
        // coercion from t.a (null) to integer
        assertThat(assertions.query(
                "SELECT EXISTS(SELECT 1 FROM (VALUES (null, null)) t(a, b) WHERE t.a=t2.b GROUP BY t.b) FROM (VALUES 1, 2) t2(b)"))
                .matches("VALUES false, false");
        // coercion FROM subquery symbol type to correlation type. Aggregation grouped by a constant.
        assertThat(assertions.query(
                "SELECT (SELECT count(*) FROM (VALUES 1, 2, 2) t(a) WHERE t.a=t2.b GROUP BY t.a LIMIT 1) FROM (VALUES 1.0) t2(b)"))
                .matches("VALUES BIGINT '1'");
        // non-injective coercion bigint -> double
        assertThatThrownBy(() -> assertions.query(
                "SELECT EXISTS(SELECT 1 FROM (VALUES (BIGINT '1', null)) t(a, b) WHERE t.a=t2.b GROUP BY t.b) FROM (VALUES 1e0, 2e0) t2(b)"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Test
    public void testSubqueriesWithGroupByAndConstantExpression()
    {
        // In the following examples, t.a is determined to be constant
        assertThat(assertions.query(
                "SELECT EXISTS(SELECT 1 FROM (VALUES (1, null)) t(a, b) WHERE t.a = t2.b * t2.c - 1 GROUP BY t.b) FROM (VALUES (1, 2), (2, 3)) t2(b, c)"))
                .matches("VALUES true, false");

        assertThat(assertions.query(
                "SELECT EXISTS(SELECT 1 FROM (VALUES (null, null)) t(a, b) WHERE t.a = t2.b * t2.c - 1 GROUP BY t.b) FROM (VALUES (1, 2), (2, 3)) t2(b, c)"))
                .matches("VALUES false, false");

        assertThat(assertions.query(
                "SELECT (SELECT count(*) FROM (VALUES 1, 3, 3) t(a) WHERE t.a = t2.b * t2.c - 1 GROUP BY t.a LIMIT 1) FROM (VALUES (1, 2), (2, 2)) t2(b, c)"))
                .matches("VALUES BIGINT '1', BIGINT '2'");
    }

    @Test
    public void testCorrelatedSubqueriesWithLimitOne()
    {
        // In the following examples, t.a is determined to be constant
        // coercion of subquery symbol
        assertThat(assertions.query(
                "SELECT (SELECT t.a FROM (VALUES 1, 2, 3) t(a) WHERE t.a = t2.b LIMIT 1) FROM (VALUES 1.0, 2.0) t2(b)"))
                .matches("VALUES 1, 2");

        // subquery symbol is equal to constant expression
        assertThat(assertions.query(
                "SELECT (SELECT t.a FROM (VALUES 1, 2, 3, 4, 5, 6) t(a) WHERE t.a = t2.b * t2.c - 1 LIMIT 1) FROM (VALUES (1, 2), (2, 3)) t2(b, c)"))
                .matches("VALUES 1, 5");

        // non-injective coercion bigint -> double
        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT t.a FROM (VALUES BIGINT '1', BIGINT '2') t(a) WHERE t.a = t2.b LIMIT 1) FROM (VALUES 1e0, 2e0) t2(b)"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Test
    public void testCorrelatedSubqueriesWithLimitGreaterThanOne()
    {
        // In the following examples, t.a is determined to be constant
        // coercion of subquery symbol
        assertThat(assertions.query(
                "SELECT (SELECT t.a FROM (VALUES 1, 2, 3) t(a) WHERE t.a = t2.b LIMIT 2) FROM (VALUES 1.0, 2.0) t2(b)"))
                .matches("VALUES 1, 2");

        // subquery symbol is equal to constant expression
        assertThat(assertions.query(
                "SELECT (SELECT t.a FROM (VALUES 1, 2, 3, 4, 5, 6) t(a) WHERE t.a = t2.b * t2.c - 1 LIMIT 2) FROM (VALUES (1, 2), (2, 3)) t2(b, c)"))
                .matches("VALUES 1, 5");

        // non-injective coercion bigint -> double
        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT t.a FROM (VALUES BIGINT '1', BIGINT '2', BIGINT '3') t(a) WHERE t.a = t2.b LIMIT 2) FROM (VALUES 1e0, 2e0) t2(b)"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Test
    public void testCorrelatedSubqueriesWithTopN()
    {
        // In the following examples, the ordering symbol is determined to be constant, so correlated TopN is rewritten to RowNumberNode instead of TopNRankingNode
        // coercion of subquery symbol
        assertions.assertQueryAndPlan(
                "SELECT (SELECT t.a FROM (VALUES 1, 2, 3) t(a) WHERE t.a = t2.b ORDER BY a LIMIT 1) FROM (VALUES 1.0, 2.0) t2(b)",
                "VALUES 1, 2",
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("cast_b", "cast_a")
                                .left(
                                        any(
                                                project(
                                                        ImmutableMap.of("cast_b", expression("CAST(b AS decimal(11, 1))")),
                                                        any(
                                                                values("b")))))
                                .right(
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("cast_a", expression("CAST(a AS decimal(11, 1))")),
                                                        any(
                                                                rowNumber(
                                                                        rowBuilder -> rowBuilder
                                                                                .maxRowCountPerPartition(Optional.of(1))
                                                                                .partitionBy(ImmutableList.of("a")),
                                                                        anyTree(
                                                                                values("a"))))))))));

        // subquery symbol is equal to constant expression
        assertions.assertQueryAndPlan(
                "SELECT (SELECT t.a FROM (VALUES 1, 2, 3, 4, 5) t(a) WHERE t.a = t2.b * t2.c - 1 ORDER BY a LIMIT 1) FROM (VALUES (1, 2), (2, 3)) t2(b, c)",
                "VALUES 1, 5",
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("expr", "a")
                                .left(
                                        any(
                                                project(
                                                        ImmutableMap.of("expr", expression("b * c - 1")),
                                                        any(
                                                                values("b", "c")))))
                                .right(
                                        any(
                                                rowNumber(
                                                        rowBuilder -> rowBuilder
                                                                .maxRowCountPerPartition(Optional.of(1))
                                                                .partitionBy(ImmutableList.of("a")),
                                                        anyTree(
                                                                values("a"))))))));

        // non-injective coercion bigint -> double
        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT t.a FROM (VALUES BIGINT '1', BIGINT '2') t(a) WHERE t.a = t2.b ORDER BY a LIMIT 1) FROM (VALUES 1e0, 2e0) t2(b)"))
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
        assertThat(assertions.query(
                "SELECT (VALUES b) FROM (VALUES 2) outer_relation(b)"))
                .matches("VALUES 2");
        assertThat(assertions.query(
                "SELECT (SELECT a + b FROM (VALUES 1) inner_relation(a)) FROM (VALUES 2) outer_relation(b)"))
                .matches("VALUES 3");
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

    @Test
    public void testCorrelatedInnerUnnestWithGlobalAggregation()
    {
        assertThat(assertions.query(
                "SELECT (SELECT array_agg(x) FROM UNNEST(a) u(x)) FROM (VALUES ARRAY[1, 2, 3]) t(a)"))
                .matches("VALUES ARRAY[1, 2, 3]");

        assertThat(assertions.query(
                "SELECT (SELECT count(DISTINCT x) FROM UNNEST(a) u(x)) FROM (VALUES ARRAY[1, 2, 3]) t(a)"))
                .matches("VALUES BIGINT '3'");

        assertThat(assertions.query(
                "SELECT (SELECT count(DISTINCT x * 0e0) - 10e0 FROM UNNEST(a) u(x)) FROM (VALUES ARRAY[1, 2, 3]) t(a)"))
                .matches("VALUES -9e0");

        assertThat(assertions.query(
                "SELECT (SELECT max(count) FROM (SELECT count(v) AS count FROM UNNEST(id, val) u(i, v) GROUP BY i)) " +
                        "FROM (VALUES (ARRAY['a', 'a', 'b'], ARRAY[1, 2, 3])) t(id, val)"))
                .matches("VALUES BIGINT '2'");

        // projection in unnest
        assertThat(assertions.query(
                "SELECT (SELECT histogram(e) FROM UNNEST(regexp_extract_all(x, '.')) u(e)) FROM (VALUES 'abccccdaee') t(x)"))
                .matches("VALUES map(cast(ARRAY['a', 'b', 'c', 'd', 'e'] AS array(varchar(10))), cast(ARRAY[2, 1, 4, 1, 2] AS array(bigint)))");

        // multiple rows in input
        assertThat(assertions.query(
                "SELECT (SELECT array_agg(x) FROM UNNEST(a) u(x)) " +
                        "FROM (VALUES ARRAY[1, 2, 3], ARRAY[4], ARRAY[5, 6]) t(a)"))
                .matches("VALUES ARRAY[1, 2, 3], ARRAY[4], ARRAY[5, 6]");

        assertThat(assertions.query(
                "SELECT (SELECT count(DISTINCT x) FROM UNNEST(a) u(x)) " +
                        "FROM (VALUES ARRAY[1, 2, 3], ARRAY[4], ARRAY[5, 6]) t(a)"))
                .matches("VALUES BIGINT '3', 1, 2");

        assertThat(assertions.query(
                "SELECT (SELECT max(count) FROM (SELECT count(v) AS count FROM UNNEST(id, val) u(i, v) GROUP BY i)) " +
                        "FROM (VALUES " +
                        "(ARRAY['a', 'a', 'b'], ARRAY[1, 2, 3]), " +
                        "(ARRAY['c', 'd'],      ARRAY[4]), " +
                        "(ARRAY['e'],           ARRAY[5, 6, 7, 8])) t(id, val)"))
                .matches("VALUES BIGINT '2', 1, 3");

        // multiple projections / coercions
        assertThat(assertions.query(
                "SELECT (SELECT max(count - 5e0) * -6 FROM (SELECT count(v * 2e0) - 3 AS count FROM UNNEST(id, val) u(i, v) GROUP BY i)) " +
                        "FROM (VALUES (ARRAY['a', 'a', 'b'], ARRAY[1, 2, 3])) t(id, val)"))
                .matches("VALUES 36e0");

        // correlated symbol (id) used in projection in subquery
        assertThat(assertions.query(
                "SELECT (SELECT ROW(id, array_agg(x)) FROM UNNEST(a) u(x)) FROM (VALUES ('a', ARRAY[1, 2, 3]), ('b', ARRAY[4])) t(id, a)"))
                .matches("VALUES ROW(ROW('a', ARRAY[1, 2, 3])), ROW(ROW('b', ARRAY[4]))");

        // with ordinality
        assertThat(assertions.query(
                "SELECT (SELECT max(avg_ord) FROM (SELECT avg(ordinality) AS avg_ord FROM UNNEST(id, val) WITH ORDINALITY u(i, v, ordinality) GROUP BY i)) " +
                        "FROM (VALUES (ARRAY['a', 'b', 'b'], ARRAY[1, 2, 3])) t(id, val)"))
                .matches("VALUES 2.5e0");

        assertThat(assertions.query(
                "SELECT (SELECT ROW(array_agg(x), array_agg(ordinality)) FROM UNNEST(a) WITH ORDINALITY u(x, ordinality)) " +
                        "FROM (VALUES ARRAY[1, 2, 3], ARRAY[4], ARRAY[5, 6]) t(a)"))
                .matches("VALUES ROW(ROW(ARRAY[1, 2, 3], CAST(ARRAY[1, 2, 3] AS array(bigint)))), ROW(ROW(ARRAY[4], ARRAY[1])), ROW(ROW(ARRAY[5, 6], ARRAY[1, 2]))");

        // correlated grouping key - illegal by AggregationAnalyzer
        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT max(count) FROM (SELECT count(v) AS count FROM UNNEST(id, val) u(i, v) GROUP BY id)) " +
                        "FROM (VALUES (ARRAY['a', 'a', 'b'], ARRAY[1, 2, 3])) t(id, val)"))
                .hasMessageMatching("Grouping field .* should originate from .*");

        // aggregation with filter: all aggregations have filter, so filter is pushed down and it is not supported by the correlated unnest rewrite
        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT array_agg(x) FILTER (WHERE x < 3) FROM UNNEST(a) u(x)) FROM (VALUES ARRAY[1, 2, 3]) t(a)"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // aggregation with filter: no filter pushdown
        assertThat(assertions.query(
                "SELECT (SELECT ROW(array_agg(x) FILTER (WHERE x < 3), avg(x)) FROM UNNEST(a) u(x)) FROM (VALUES ARRAY[1, 2, 3]) t(a)"))
                .matches("VALUES ROW(ROW(ARRAY[1, 2], 2e0))");

        // empty input
        assertThat(assertions.query(
                "SELECT (SELECT array_agg(x) FROM UNNEST(a) u(x)) FROM (SELECT ARRAY[1, 2, 3] WHERE false) t(a)"))
                .returnsEmptyResult();

        assertThat(assertions.query(
                "SELECT (SELECT count(DISTINCT x) FROM UNNEST(a) u(x)) FROM (SELECT ARRAY[1, 2, 3] WHERE false) t(a)"))
                .returnsEmptyResult();

        // empty unnest results for some input rows
        assertThat(assertions.query(
                "SELECT (SELECT count(x) FROM UNNEST(a) u(x)) " +
                        "FROM (VALUES ARRAY[1, 2, 3], null, ARRAY[4], ARRAY[]) t(a)"))
                .matches("VALUES BIGINT '3', 0, 1, 0");

        assertThat(assertions.query(
                "SELECT (SELECT max(count) FROM (SELECT count(v) AS count FROM UNNEST(id, val) u(i, v) GROUP BY i)) " +
                        "FROM (VALUES " +
                        "(ARRAY['a', 'a', 'b'], ARRAY[1, 2, 3]), " +
                        "(ARRAY[],              ARRAY[]), " +
                        "(null,                 null)) t(id, val)"))
                .matches("VALUES BIGINT '2', null, null");

        // two levels of decorrelation
        assertThat(assertions.query(
                "SELECT (SELECT array_agg(y) FROM UNNEST(b) u2(y)) " +
                        "FROM (" +
                        "      SELECT (SELECT array_agg(x) FROM UNNEST(a) u(x)) " +
                        "      FROM (VALUES ARRAY[1, 2, 3]) t(a)" +
                        ") t2(b)"))
                .matches("VALUES ARRAY[1, 2, 3]");
    }

    @Test
    public void testCorrelatedLeftUnnestWithGlobalAggregation()
    {
        // in the following queries, the part `(VALUES 1) LEFT JOIN UNNEST(...) ON TRUE` creates an UnnestNode of type LEFT
        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(VALUES ARRAY[1, 2, 3]) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT array_agg(x) FROM (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)) t2(b) " +
                        "ON TRUE"))
                .matches("VALUES ARRAY[1, 2, 3]");

        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(VALUES ARRAY[1, 2, 3]) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT count(DISTINCT x) FROM (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)) t2(b) " +
                        "ON TRUE"))
                .matches("VALUES BIGINT '3'");

        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(VALUES ARRAY[1, 2, 3]) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT count(DISTINCT x * 0e0) - 10e0 FROM (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)) t2(b) " +
                        "ON TRUE"))
                .matches("VALUES -9e0");

        // after the unnest, there is first a grouped aggregation (count(v) ... GROUP BY i), and then a global aggregation (max(count))
        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(VALUES (ARRAY['a', 'a', 'b'], ARRAY[1, 2, 3])) t(id, val) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT max(count) FROM (SELECT count(v) AS count FROM (SELECT i, v FROM (VALUES 1) LEFT JOIN UNNEST(id, val) u(i, v) ON TRUE) GROUP BY i)) t2(b) " +
                        "ON TRUE"))
                .matches("VALUES BIGINT '2'");

        // projection in unnest
        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(VALUES 'abccccdaee') t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT histogram(x) FROM (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(regexp_extract_all(a, '.')) u(x) ON TRUE)) t2(b) " +
                        "ON TRUE"))
                .matches("VALUES map(cast(ARRAY['a', 'b', 'c', 'd', 'e'] AS array(varchar(10))), cast(ARRAY[2, 1, 4, 1, 2] AS array(bigint)))");

        // multiple rows in input
        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(VALUES ARRAY[1, 2, 3], ARRAY[4], ARRAY[5, 6]) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT array_agg(x) FROM (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)) t2(b) " +
                        "ON TRUE"))
                .matches("VALUES ARRAY[1, 2, 3], ARRAY[4], ARRAY[5, 6]");

        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(VALUES ARRAY[1, 2, 3], ARRAY[4], ARRAY[5, 6]) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT count(DISTINCT x) FROM (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)) t2(b) " +
                        "ON TRUE"))
                .matches("VALUES BIGINT '3', 1, 2");

        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(VALUES " +
                        "    (ARRAY['a', 'a', 'b'], ARRAY[1, 2, 3]), " +
                        "    (ARRAY['c', 'd'],      ARRAY[4]), " +
                        "    (ARRAY['e'],           ARRAY[5, 6, 7, 8])" +
                        ") t(id, val) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT max(count) FROM (SELECT count(v) AS count FROM (SELECT i, v FROM (VALUES 1) LEFT JOIN UNNEST(id, val) u(i, v) ON TRUE) GROUP BY i)) t2(b) " +
                        "ON TRUE"))
                .matches("VALUES BIGINT '2', 1, 3");

        // multiple projections / coercions
        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(VALUES (ARRAY['a', 'a', 'b'], ARRAY[1, 2, 3])) t(id, val) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT max(count - 5e0) * -6 FROM (SELECT count(v * 2e0) - 3 AS count FROM (SELECT i, v FROM (VALUES 1) LEFT JOIN UNNEST(id, val) u(i, v) ON TRUE) GROUP BY i)) t2(b) " +
                        "ON TRUE"))
                .matches("VALUES 36e0");

        // correlated symbol (id) used in projection in subquery
        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(VALUES ('a', ARRAY[1, 2, 3]), ('b', ARRAY[4])) t(id, a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT ROW(id, array_agg(x)) FROM (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)) t2(b) " +
                        "ON TRUE"))
                .matches("VALUES ROW(ROW('a', ARRAY[1, 2, 3])), ROW(ROW('b', ARRAY[4]))");

        // with ordinality
        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(VALUES (ARRAY['a', 'b', 'b'], ARRAY[1, 2, 3])) t(id, val) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT max(avg_ord) FROM " +
                        "            (SELECT avg(ordinality) AS avg_ord FROM " +
                        "                (SELECT i, v, ordinality FROM (VALUES 1) LEFT JOIN UNNEST(id, val) WITH ORDINALITY u(i, v, ordinality) ON TRUE)" +
                        "                 GROUP BY i)" +
                        "        ) t2(b) " +
                        "ON TRUE"))
                .matches("VALUES 2.5e0");

        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(VALUES ARRAY[1, 2, 3], ARRAY[4], ARRAY[5, 6]) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT ROW(array_agg(x), array_agg(ordinality)) FROM (SELECT x, ordinality FROM (VALUES 1) LEFT JOIN UNNEST(a) WITH ORDINALITY u(x, ordinality) ON TRUE)) t2(b) " +
                        "ON TRUE"))
                .matches("VALUES ROW(ROW(ARRAY[1, 2, 3], CAST(ARRAY[1, 2, 3] AS array(bigint)))), ROW(ROW(ARRAY[4], ARRAY[1])), ROW(ROW(ARRAY[5, 6], ARRAY[1, 2]))");

        // correlated grouping key - illegal by AggregationAnalyzer
        assertThatThrownBy(() -> assertions.query(
                "SELECT b FROM " +
                        "(VALUES (ARRAY['a', 'a', 'b'], ARRAY[1, 2, 3])) t(id, val) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT max(count) FROM (SELECT count(v) AS count FROM (SELECT i, v FROM (VALUES 1) LEFT JOIN UNNEST(id, val) u(i, v) ON TRUE) GROUP BY id)) t2(b) " +
                        "ON TRUE"))
                .hasMessageMatching("Grouping field .* should originate from .*");

        // aggregation with filter: all aggregations have filter, so filter is pushed down and it is not supported by the correlated unnest rewrite
        assertThatThrownBy(() -> assertions.query(
                "SELECT b FROM " +
                        "(VALUES ARRAY[1, 2, 3]) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT array_agg(x) FILTER (WHERE x < 3) FROM (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)) t2(b) " +
                        "ON TRUE"))
                .hasMessageMatching(UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // aggregation with filter: no filter pushdown
        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(VALUES ARRAY[1, 2, 3]) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT ROW(array_agg(x) FILTER (WHERE x < 3), avg(x)) FROM (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)) t2(b) " +
                        "ON TRUE"))
                .matches("VALUES ROW(ROW(ARRAY[1, 2], 2e0))");

        // empty input
        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(SELECT ARRAY[1, 2, 3] WHERE FALSE) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT array_agg(x) FROM (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)) t2(b) " +
                        "ON TRUE"))
                .returnsEmptyResult();

        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(SELECT ARRAY[1, 2, 3] WHERE FALSE) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT count(DISTINCT x) FROM (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)) t2(b) " +
                        "ON TRUE"))
                .returnsEmptyResult();

        // empty unnest results for some input rows
        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(VALUES ARRAY[1, 2, 3], null, ARRAY[4], ARRAY[]) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT count(x) FROM (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)) t2(b) " +
                        "ON TRUE"))
                .matches("VALUES BIGINT '3', 0, 1, 0");

        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(VALUES ARRAY[1, 2, 3], null, ARRAY[4], ARRAY[]) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT avg(x) FROM (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)) t2(b) " +
                        "ON TRUE"))
                .matches("VALUES 2e0, null, 4, null");

        assertThat(assertions.query(
                "SELECT b FROM " +
                        "(VALUES " +
                        "    (ARRAY['a', 'a', 'b'], ARRAY[1, 2, 3]), " +
                        "    (ARRAY[],              ARRAY[]), " +
                        "    (null,                 null)" +
                        ") t(id, val) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT max(count) FROM (SELECT count(v) AS count FROM (SELECT i, v FROM (VALUES 1) LEFT JOIN UNNEST(id, val) u(i, v) ON TRUE) GROUP BY i)) t2(b) " +
                        "ON TRUE"))
                .matches("VALUES BIGINT '2', 0, 0");

        // two levels of decorrelation
        assertThat(assertions.query(
                "SELECT c FROM (" +
                        "SELECT b FROM " +
                        "     (VALUES ARRAY[1, 2, 3]) t(a) " +
                        "     LEFT JOIN " +
                        "     LATERAL (SELECT array_agg(x) FROM (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)) t2(b) " +
                        "     ON TRUE ) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT array_agg(y) FROM (SELECT y FROM (VALUES 1) LEFT JOIN UNNEST(b) w(y) ON TRUE)) t3(c) " +
                        "ON TRUE "))
                .matches("VALUES ARRAY[1, 2, 3]");
    }

    @Test
    public void testCorrelatedUnnestInScalarSubquery()
    {
        // in the following queries, the part `SELECT * FROM UNNEST(...)` creates an UnnestNode of type INNER,
        // while `(VALUES 1) LEFT JOIN UNNEST(...) ON TRUE` creates an UnnestNode of type LEFT
        assertThat(assertions.query(
                "SELECT (SELECT * FROM UNNEST(a) u(x)) " +
                        "FROM (VALUES ARRAY[1], ARRAY[2]) t(a)"))
                .matches("VALUES 1, 2");

        assertThat(assertions.query(
                "SELECT (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)" +
                        "FROM (VALUES ARRAY[1], ARRAY[2]) t(a)"))
                .matches("VALUES 1, 2");

        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT * FROM UNNEST(a) u(x)) " +
                        "FROM (VALUES ARRAY[1, 2, 3]) t(a)"))
                .hasMessage("Scalar sub-query has returned multiple rows");

        // limit in subquery
        assertThat(assertions.query(
                "SELECT (SELECT * FROM UNNEST(a) u(x) LIMIT 1) " +
                        "FROM (VALUES ARRAY[1, 1, 1], ARRAY[2, 2]) t(a)"))
                .matches("VALUES 1, 2");

        assertThat(assertions.query(
                "SELECT (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE LIMIT 1)" +
                        "FROM (VALUES ARRAY[1, 1, 1], ARRAY[2, 2]) t(a)"))
                .matches("VALUES 1, 2");

        assertThat(assertions.query(
                "SELECT (SELECT * FROM UNNEST(a) u(x) LIMIT 5) " +
                        "FROM (VALUES ARRAY[1], ARRAY[4]) t(a)"))
                .matches("VALUES 1, 4");

        assertThat(assertions.query(
                format("SELECT (SELECT * FROM UNNEST(a) u(x) LIMIT %d) " +
                        "FROM (VALUES ARRAY[1], ARRAY[4]) t(a)", Long.MAX_VALUE)))
                .matches("VALUES 1, 4");

        // limit with ties in subquery
        assertThat(assertions.query(
                "SELECT (SELECT * FROM UNNEST(a) u(x) ORDER BY x FETCH FIRST ROW WITH TIES) " +
                        "FROM (VALUES ARRAY[3, 1, 2], ARRAY[5, 4]) t(a)"))
                .matches("VALUES 1, 4");

        assertThat(assertions.query(
                "SELECT (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE ORDER BY x FETCH FIRST ROW WITH TIES)" +
                        "FROM (VALUES ARRAY[3, 1, 2], ARRAY[5, 4]) t(a)"))
                .matches("VALUES 1, 4");

        assertThat(assertions.query(
                "SELECT (SELECT * FROM UNNEST(a) u(x) ORDER BY x FETCH FIRST 100 ROWS WITH TIES) " +
                        "FROM (VALUES ARRAY[1], ARRAY[4]) t(a)"))
                .matches("VALUES 1, 4");

        // topN in subquery
        assertThat(assertions.query(
                "SELECT (SELECT * FROM UNNEST(a) u(x) ORDER BY x LIMIT 1) " +
                        "FROM (VALUES ARRAY[3, 1, 2], ARRAY[5, 4]) t(a)"))
                .matches("VALUES 1, 4");

        assertThat(assertions.query(
                "SELECT (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE ORDER BY x LIMIT 1)" +
                        "FROM (VALUES ARRAY[3, 1, 2], ARRAY[5, 4]) t(a)"))
                .matches("VALUES 1, 4");

        assertThat(assertions.query(
                "SELECT (SELECT * FROM UNNEST(a) u(x) ORDER BY x LIMIT 100) " +
                        "FROM (VALUES ARRAY[1], ARRAY[4]) t(a)"))
                .matches("VALUES 1, 4");

        // empty result of unnest for some input rows
        assertThat(assertions.query(
                "SELECT (SELECT * FROM UNNEST(a) u(x)) " +
                        "FROM (VALUES ARRAY[1], ARRAY[], null) t(a)"))
                .matches("VALUES 1, null, null");

        assertThat(assertions.query(
                "SELECT (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)" +
                        "FROM (VALUES ARRAY[1], ARRAY[], null) t(a)"))
                .matches("VALUES 1, null, null");

        // projection in subquery
        assertThat(assertions.query(
                "SELECT (SELECT x IS NULL FROM UNNEST(a) u(x)) " +
                        "FROM (VALUES ARRAY[1], ARRAY[], null) t(a)"))
                .matches("VALUES false, null, null");

        assertThat(assertions.query(
                "SELECT (SELECT x IS NULL FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)" +
                        "FROM (VALUES ARRAY[1], ARRAY[], null) t(a)"))
                .matches("VALUES false, true, true");

        // projection in unnest
        assertThat(assertions.query(
                "SELECT (SELECT x FROM UNNEST(regexp_extract_all(a, '.')) u(x) LIMIT 1) " +
                        "FROM (VALUES 'xxxxxxxxxx') t(a)"))
                .matches("VALUES CAST('x' AS varchar(10))");

        // correlated symbol (id) used in projection in the subquery
        assertThat(assertions.query(
                "SELECT (SELECT ROW(id, x) FROM UNNEST(val) u(x)) " +
                        "FROM (VALUES ('a', ARRAY[1]), ('b', ARRAY[2])) t(id, val)"))
                .matches("VALUES ROW(ROW('a', 1)), ROW(ROW('b', 2))");

        // with ordinality
        assertThat(assertions.query(
                "SELECT (SELECT ROW(x, ordinality) FROM UNNEST(a) WITH ORDINALITY u(x, ordinality)) " +
                        "FROM (VALUES ARRAY[1], ARRAY[2]) t(a)"))
                .matches("VALUES ROW(ROW(1, BIGINT '1')), ROW(ROW(2, 1))");

        // empty input
        assertThat(assertions.query(
                "SELECT (SELECT * FROM UNNEST(a) u(x)) " +
                        "FROM (SELECT ARRAY[1] WHERE FALSE) t(a)"))
                .returnsEmptyResult();

        assertThat(assertions.query(
                "SELECT (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE)" +
                        "FROM (SELECT ARRAY[1] WHERE FALSE) t(a)"))
                .returnsEmptyResult();

        // nested correlation
        assertThat(assertions.query(
                "SELECT (SELECT * FROM UNNEST(b)) " +
                        "FROM (SELECT (SELECT * FROM UNNEST(a)) " +
                        "              FROM (VALUES ARRAY[ARRAY[1]], ARRAY[ARRAY[2]]) t(a)" +
                        ") t2(b)"))
                .matches("VALUES 1, 2");
    }

    @Test
    public void testCorrelatedUnnestInLateralSubquery()
    {
        // in the following queries, the part `SELECT * FROM UNNEST(...)` creates an UnnestNode of type INNER,
        // while `(VALUES 1) LEFT JOIN UNNEST(...) ON TRUE` creates an UnnestNode of type LEFT
        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[1, 2], ARRAY[3]) t(a) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT * FROM UNNEST(a)) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[1, 2], 1), " +
                        "(ARRAY[1, 2], 2), " +
                        "(ARRAY[3],    3)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[1, 2], ARRAY[3]) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT * FROM UNNEST(a)) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[1, 2], 1), " +
                        "(ARRAY[1, 2], 2), " +
                        "(ARRAY[3],    3)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[1, 2], ARRAY[3]) t(a) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[1, 2], 1), " +
                        "(ARRAY[1, 2], 2), " +
                        "(ARRAY[3],    3)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[1, 2], ARRAY[3]) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[1, 2], 1), " +
                        "(ARRAY[1, 2], 2), " +
                        "(ARRAY[3],    3)");

        // limit in subquery
        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[1, 1], ARRAY[3]) t(a) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT * FROM UNNEST(a) LIMIT 1) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[1, 1], 1), " +
                        "(ARRAY[3],    3)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[1, 1], ARRAY[3]) t(a) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE LIMIT 1) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[1, 1], 1), " +
                        "(ARRAY[3],    3)");

        assertThat(assertions.query(
                format("SELECT * FROM (VALUES ARRAY[1, 2], ARRAY[3]) t(a) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT * FROM UNNEST(a) LIMIT %d) " +
                        "ON TRUE", Long.MAX_VALUE)))
                .matches("VALUES " +
                        "(ARRAY[1, 2], 1), " +
                        "(ARRAY[1, 2], 2), " +
                        "(ARRAY[3],    3)");

        // limit with ties in subquery
        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[2, 1, 1], ARRAY[3]) t(a) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT * FROM UNNEST(a) u(x) ORDER BY x FETCH FIRST ROW WITH TIES) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[2, 1, 1], 1), " +
                        "(ARRAY[2, 1, 1], 1), " +
                        "(ARRAY[3],    3)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[2, 1, 1], ARRAY[3]) t(a) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE ORDER BY x FETCH FIRST ROW WITH TIES) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[2, 1, 1], 1), " +
                        "(ARRAY[2, 1, 1], 1), " +
                        "(ARRAY[3],    3)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[2, 2, 1, 3, 4], ARRAY[5]) t(a) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT * FROM UNNEST(a) u(x) ORDER BY x FETCH FIRST 2 ROWS WITH TIES) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[2, 2, 1, 3, 4], 1), " +
                        "(ARRAY[2, 2, 1, 3, 4], 2), " +
                        "(ARRAY[2, 2, 1, 3, 4], 2), " +
                        "(ARRAY[5],    5)");

        // topN in subquery
        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[2, 1, 1], ARRAY[3]) t(a) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT * FROM UNNEST(a) u(x) ORDER BY x LIMIT 1) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[2, 1, 1], 1), " +
                        "(ARRAY[3],    3)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[2, 1, 1], ARRAY[3]) t(a) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE ORDER BY x LIMIT 1) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[2, 1, 1], 1), " +
                        "(ARRAY[3],    3)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[2, 2, 1, 3, 4], ARRAY[5]) t(a) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT * FROM UNNEST(a) u(x) ORDER BY x LIMIT 2) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[2, 2, 1, 3, 4], 1), " +
                        "(ARRAY[2, 2, 1, 3, 4], 2), " +
                        "(ARRAY[5],    5)");

        // empty result of unnest for some input rows
        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[1, 2], ARRAY[], null) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT * FROM UNNEST(a)) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[1, 2], 1), " +
                        "(ARRAY[1, 2], 2), " +
                        "(ARRAY[], null), " +
                        "(null, null)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[1, 2], ARRAY[], null) t(a) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT * FROM UNNEST(a)) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[1, 2], 1), " +
                        "(ARRAY[1, 2], 2)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[1, 2], ARRAY[], null) t(a) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT x FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[1, 2], 1), " +
                        "(ARRAY[1, 2], 2), " +
                        "(ARRAY[], null), " +
                        "(null, null)");

        // projection in subquery
        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[1, 2], ARRAY[], null) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT x IS NULL FROM UNNEST(a) u (x)) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[1, 2], false), " +
                        "(ARRAY[1, 2], false), " +
                        "(ARRAY[], null), " +
                        "(null, null)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[1, 2], ARRAY[], null) t(a) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT x IS NULL FROM UNNEST(a) u(x)) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[1, 2], false), " +
                        "(ARRAY[1, 2], false)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[1, 2], ARRAY[], null) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT x IS NULL FROM (VALUES 1) LEFT JOIN UNNEST(a) u(x) ON TRUE) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[1, 2], false), " +
                        "(ARRAY[1, 2], false), " +
                        "(ARRAY[], true), " +
                        "(null, true)");

        // projection in unnest
        assertThat(assertions.query(
                "SELECT * FROM (VALUES 'abc') t(a) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT x FROM UNNEST(regexp_extract_all(a, '.')) u(x)) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "('abc', CAST('a' AS varchar(3))), " +
                        "('abc', 'b'), " +
                        "('abc', 'c')");

        // correlated symbol (id) used in projection in the subquery
        assertThat(assertions.query(
                "SELECT * FROM (VALUES ('a', ARRAY[1, 2]), ('b', ARRAY[3])) t(id, val) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT ROW(id, x) FROM UNNEST(val) u(x)) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "('a', ARRAY[1, 2], ROW('a', 1)), " +
                        "('a', ARRAY[1, 2], ROW('a', 2)), " +
                        "('b', ARRAY[3], ROW('b', 3))");

        // with ordinality
        assertThat(assertions.query(
                "SELECT * FROM (VALUES ARRAY[1, 2], ARRAY[3], null) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT 100 * x + ordinality FROM UNNEST(a) WITH ORDINALITY u(x, ordinality)) " +
                        "ON TRUE"))
                .matches("VALUES " +
                        "(ARRAY[1, 2], BIGINT '101'), " +
                        "(ARRAY[1, 2], 202), " +
                        "(ARRAY[3], 301), " +
                        "(null, null)");

        // empty input
        assertThat(assertions.query(
                "SELECT * FROM (SELECT ARRAY[1] WHERE FALSE) t(a) " +
                        "INNER JOIN " +
                        "LATERAL (SELECT * FROM UNNEST(a)) " +
                        "ON TRUE"))
                .returnsEmptyResult();

        assertThat(assertions.query(
                "SELECT * FROM (SELECT ARRAY[1] WHERE FALSE) t(a) " +
                        "LEFT JOIN " +
                        "LATERAL (SELECT * FROM UNNEST(a)) " +
                        "ON TRUE"))
                .returnsEmptyResult();

        // nested correlation
        assertThat(assertions.query(
                "SELECT y FROM " +
                        "       (SELECT x FROM " +
                        "       (VALUES ARRAY[ARRAY[1, 2, 3]], ARRAY[ARRAY[4, 5]], ARRAY[null]) t(a) " +
                        "       LEFT JOIN " +
                        "       LATERAL (SELECT * FROM UNNEST(a) u(x)) " +
                        "       ON TRUE) t2(b)" +
                        "LEFT JOIN " +
                        "LATERAL (SELECT * FROM UNNEST(b) v(y))" +
                        "ON TRUE "))
                .matches("VALUES 1, 2, 3, 4, 5, null");
    }

    @Test
    public void testQuantifiedComparison()
    {
        assertThat(assertions.query(
                "SELECT (SELECT 2) > ALL (SELECT 1)"))
                .describedAs("Subquery on value side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(2) > ALL (VALUES ROW(0), ROW(1))"))
                .describedAs("Single-element row type, non-null and non-indeterminate values")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(2) > ALL (VALUES ROW(0), ROW(1), ROW(NULL))"))
                .describedAs("Single-element row type, indeterminate value")
                .matches("VALUES CAST(NULL AS boolean)");

        assertThat(assertions.query(
                "SELECT ROW(2) > ALL (VALUES ROW(0), ROW(1), NULL)"))
                .describedAs("Single-element row type, null value")
                .matches("VALUES CAST(NULL AS boolean)");

        assertThat(assertions.query(
                "SELECT ROW(2) > ALL (SELECT 0 WHERE false)"))
                .describedAs("Single-element row type, with empty set")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT NULL > ALL (VALUES ROW(0), ROW(1))"))
                .describedAs("Single-element row type, null value, non-null and non-indeterminate elements")
                .matches("VALUES CAST(NULL AS boolean)");

        assertThat(assertions.query(
                "SELECT NULL > ALL (VALUES ROW(0), ROW(NULL))"))
                .describedAs("Single-element row type, null value, indeterminate element")
                .matches("VALUES CAST(NULL AS boolean)");

        assertThat(assertions.query(
                "SELECT NULL > ALL (VALUES ROW(0), NULL)"))
                .describedAs("Single-element row type, null value, null element")
                .matches("VALUES CAST(NULL AS boolean)");

        assertThat(assertions.query(
                "SELECT NULL > ALL (SELECT 0 WHERE false)"))
                .describedAs("Single-element row type, null value, empty set")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT BIGINT '1' = ALL (SELECT 1)"))
                .describedAs("Implicit row type for value side w/ coercion on subquery side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT 1 = ALL (SELECT BIGINT '1')"))
                .describedAs("Implicit row type for value side w/ coercion on value side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT 1 = ALL (SELECT 1)"))
                .describedAs("Implicit row type for value")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(1) = ALL (SELECT 1)"))
                .describedAs("Explicit row type for value side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(BIGINT '1') = ALL (SELECT 1)"))
                .describedAs("Explicit row type for value side w/ coercion on subquery side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(1) = ALL (SELECT BIGINT '1')"))
                .describedAs("Explicit row type for value side w/ coercion on value side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(1, 2) = ALL (SELECT 1, 2)"))
                .describedAs("Multi-column row type")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(1, 2) = ALL (SELECT BIGINT '1', BIGINT '2')"))
                .describedAs("Multi-column row type with coercion on value side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(BIGINT '1', BIGINT '2') = ALL (SELECT 1, 2)"))
                .describedAs("Multi-column row type with coercion on subquery side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(BIGINT '1', 2) = ALL (SELECT 1, BIGINT '2')"))
                .describedAs("Multi-column row type with coercion on both sides")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT NULL = ALL (SELECT 1, BIGINT '2')"))
                .describedAs("Multi-column row type, null value")
                .matches("VALUES CAST(NULL AS boolean)");
    }

    @Test
    public void testInPredicate()
    {
        assertThat(assertions.query(
                "SELECT (SELECT 1) IN (SELECT 1)"))
                .describedAs("Subquery on value side, implicit row type for value side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT (SELECT 2) IN (SELECT 1)"))
                .describedAs("Subquery on value side, implicit row type for value side")
                .matches("VALUES false");

        assertThat(assertions.query(
                "SELECT 1 IN (SELECT 1)"))
                .describedAs("Implicit row type for value side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT 1 IN (SELECT BIGINT '1')"))
                .describedAs("Implicit row type for value side w/ coercion on value side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT BIGINT '1' IN (SELECT 1)"))
                .describedAs("Implicit row type for value side w/ coercion on subquery side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(1) IN (SELECT 1)"))
                .describedAs("Explicit row type for value side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(1) IN (SELECT BIGINT '1')"))
                .describedAs("Explicit row type for value side w/ coercion on value side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(BIGINT '1') IN (SELECT 1)"))
                .describedAs("Explicit row type for value side with coercion on subquery side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(0) IN (VALUES ROW(0), ROW(1))"))
                .describedAs("Single-element row type, non-null and non-indeterminate values, present")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(0) IN (VALUES ROW(1), ROW(2))"))
                .describedAs("Single-element row type, non-null and non-indeterminate values, not present")
                .matches("VALUES false");

        assertThat(assertions.query(
                "SELECT ROW(0) IN (VALUES ROW(0), ROW(NULL))"))
                .describedAs("Single-element row type, indeterminate element, present")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(0) IN (VALUES ROW(1), ROW(NULL))"))
                .describedAs("Single-element row type, indeterminate element, not present")
                .matches("VALUES CAST(NULL AS boolean)");

        assertThat(assertions.query(
                "SELECT ROW(0) IN (VALUES ROW(0), NULL)"))
                .describedAs("Single-element row type, null element, present")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(0) IN (VALUES ROW(1), NULL)"))
                .describedAs("Single-element row type, null element, not present")
                .matches("VALUES CAST(NULL AS boolean)");

        assertThat(assertions.query(
                "SELECT ROW(0) IN (SELECT 0 WHERE false)"))
                .describedAs("Single-element row type, with empty set")
                .matches("VALUES false");

        assertThat(assertions.query(
                "SELECT NULL IN (VALUES ROW(0))"))
                .describedAs("Single-element row type, null value, non-null and non-indeterminate elements")
                .matches("VALUES CAST(NULL AS boolean)");

        assertThat(assertions.query(
                "SELECT NULL IN (VALUES ROW(0), ROW(NULL))"))
                .describedAs("Single-element row type, null value, indeterminate element")
                .matches("VALUES CAST(NULL AS boolean)");

        assertThat(assertions.query(
                "SELECT NULL IN (VALUES ROW(0), NULL)"))
                .describedAs("Single-element row type, null value, null element")
                .matches("VALUES CAST(NULL AS boolean)");

        assertThat(assertions.query(
                "SELECT NULL IN (SELECT 0 WHERE false)"))
                .describedAs("Single-element row type, null value, empty set")
                .matches("VALUES false");

        assertThat(assertions.query(
                "SELECT ROW(1, 2) IN (SELECT 1, 2)"))
                .describedAs("Multi-column row type")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(1, 2) IN (SELECT BIGINT '1', BIGINT '2')"))
                .describedAs("Multi-column row type with coercion on value side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(BIGINT '1', BIGINT '2') IN (SELECT 1, 2)"))
                .describedAs("Multi-column row type with coercion on subquery side")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT ROW(BIGINT '1', 2) IN (SELECT 1, BIGINT '2')"))
                .describedAs("Multi-column row type with coercion on both sides")
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT NULL IN (SELECT 1, BIGINT '2')"))
                .describedAs("Multi-column row type, null value")
                .matches("VALUES CAST(NULL AS boolean)");
    }

    @Test
    public void testRowSubquery()
    {
        // ISO 9075-2 2016 7.19 <subquery>
        // 2.b) If the cardinality of RRS is 0 (zero), then the value of the <row subquery> is a row whose degree is
        //      D and whose fields are all the null value.
        assertThat(assertions.query(
                "SELECT (SELECT 1 AS a, 2 AS b WHERE false)"))
                .matches("SELECT CAST(ROW(NULL, NULL) AS row(a integer, b integer))");

        assertThat(assertions.query(
                "SELECT (SELECT 1, 2) = (SELECT ROW(1, 2))"))
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT (SELECT 'a', 1)"))
                .hasOutputTypes(List.of(rowType(field(createVarcharType(1)), field(INTEGER))))
                .matches("SELECT ROW('a', 1)");

        assertThat(assertions.query(
                "SELECT (SELECT 'a' AS x, 1 AS y)"))
                .hasOutputTypes(List.of(rowType(field("x", createVarcharType(1)), field("y", INTEGER))))
                .matches("SELECT CAST(ROW('a', 1) AS row(x varchar(1), y integer))");

        assertThat(assertions.query(
                "SELECT * FROM (SELECT (SELECT 1, 2))"))
                .matches("SELECT ROW(1, 2)");

        assertThat(assertions.query(
                "SELECT (SELECT t.* FROM (VALUES 1)) FROM (SELECT 1, 'a') t(a, b)"))
                .matches("SELECT CAST(ROW(1, 'a') AS row(a integer, b varchar(1)))");

        // The quoted identifiers are needed due to some pre-existing inconsistencies
        // in how identifiers are canonicalized (see TypeSignatureTranslator.canonicalize())
        assertThat(assertions.query(
                "SELECT (SELECT t.* AS (x, y) FROM (SELECT 1, 'a') t)"))
                .matches("SELECT CAST(ROW(1, 'a') AS row(\"X\" integer, \"Y\" varchar(1)))");

        // test implicit coercion for the result of the subquery
        assertThat(assertions.query(
                "SELECT (SELECT 1, 2) = CAST(ROW(1,2) AS row(a bigint, b bigint))"))
                .matches("VALUES true");

        // make sure invisible fields are not exposed
        assertThat(assertions.query(
                "SELECT (TABLE region ORDER BY regionkey LIMIT 1)"))
                .matches("SELECT CAST(ROW(0, 'AFRICA', 'lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to ') AS row(regionkey bigint, name varchar(25), \"comment\" varchar(152)))");

        assertThat(assertions.query(
                "SELECT (SELECT 'a' AS x, 1)"))
                .matches("SELECT (SELECT CAST(ROW('a', 1) AS row(x varchar(1), integer)))");
    }
}
