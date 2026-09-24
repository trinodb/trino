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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.SystemSessionProperties.ITERATIVE_OPTIMIZER_TIMEOUT;
import static io.trino.SystemSessionProperties.ITERATIVE_PREDICATE_PUSHDOWN_ENABLED;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestPredicatePushdown
{
    private Map<Boolean, QueryAssertions> assertionsByImplementation;

    @BeforeAll
    public void setup()
    {
        Session session = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TINY_SCHEMA_NAME)
                .build();
        ImmutableMap.Builder<Boolean, QueryAssertions> assertions = ImmutableMap.builder();
        for (boolean iterativePredicatePushdown : new boolean[] {true, false}) {
            QueryRunner runner = new StandaloneQueryRunner(session, builder -> {
                if (!iterativePredicatePushdown) {
                    builder.addProperty("optimizer.iterative-predicate-pushdown.enabled", "false");
                }
            });
            runner.installPlugin(new TpchPlugin());
            runner.createCatalog(TEST_CATALOG_NAME, "tpch", ImmutableMap.of("tpch.splits-per-node", "1"));
            assertions.put(iterativePredicatePushdown, new QueryAssertions(runner));
        }
        assertionsByImplementation = assertions.buildOrThrow();
    }

    @AfterAll
    public void teardown()
    {
        closeAllRuntimeException(assertionsByImplementation.get(true), assertionsByImplementation.get(false));
        assertionsByImplementation = null;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGroupingKeyPredicateThroughJoin(boolean iterativePredicatePushdown)
    {
        QueryAssertions assertions = assertionsByImplementation.get(iterativePredicatePushdown);
        Session session = assertions.sessionBuilder()
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "3s")
                .build();
        assertThat(assertions.query(session,
                """
                SELECT *
                FROM (
                    SELECT l.custkey, l.orderstatus, sum(CAST(l.totalprice AS bigint)) totalprice, max(r.custkey) maxcustkey
                    FROM orders l JOIN orders r ON l.orderkey = r.orderkey
                    GROUP BY l.custkey, l.orderstatus
                )
                WHERE custkey = maxcustkey
                    AND maxcustkey % 2 = 0
                    AND orderstatus = 'F'
                    AND totalprice > 10000
                """))
                .matches(
                        """
                        SELECT custkey, orderstatus, sum(CAST(totalprice AS bigint)), max(custkey)
                        FROM orders
                        WHERE custkey % 2 = 0 AND orderstatus = 'F'
                        GROUP BY custkey, orderstatus
                        HAVING sum(CAST(totalprice AS bigint)) > 10000
                        """);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testResultsThroughOperators(boolean iterativePredicatePushdown)
    {
        QueryAssertions assertions = assertionsByImplementation.get(iterativePredicatePushdown);

        assertThat(assertions.query("SELECT count(*) FROM (SELECT orderkey k FROM orders UNION ALL SELECT custkey FROM customer) WHERE k BETWEEN 10 AND 20"))
                .matches("SELECT (SELECT count(*) FROM orders WHERE orderkey BETWEEN 10 AND 20) + (SELECT count(*) FROM customer WHERE custkey BETWEEN 10 AND 20)");

        assertThat(assertions.query("SELECT count(*) FROM (SELECT orderkey, x FROM orders CROSS JOIN UNNEST(ARRAY[1, 2]) t(x)) WHERE orderkey < 100 AND x = 2"))
                .matches("SELECT count(*) FROM orders WHERE orderkey < 100");

        // orderpriority is only grouped in one of the sets, so the filter on its null stays above the grouping
        assertThat(assertions.query(
                """
                SELECT count(*)
                FROM (SELECT orderstatus, orderpriority, count(*) c FROM orders GROUP BY GROUPING SETS ((orderstatus), (orderstatus, orderpriority)))
                WHERE orderstatus = 'F' AND orderpriority IS NULL
                """))
                .matches("VALUES BIGINT '1'");

        assertThat(assertions.query("SELECT count(*) FROM orders TABLESAMPLE BERNOULLI (100) WHERE orderkey < 100"))
                .matches("SELECT count(*) FROM orders WHERE orderkey < 100");

        assertThat(assertions.query("SELECT count(*) FROM (SELECT orderkey FROM orders ORDER BY orderkey) WHERE orderkey < 100"))
                .matches("SELECT count(*) FROM orders WHERE orderkey < 100");

        // Predicates on the null-extended input of an outer join must see the null rows
        assertThat(assertions.query("SELECT count(*) FROM customer c LEFT JOIN orders o ON c.custkey = o.custkey WHERE coalesce(o.orderkey, -1) = -1"))
                .matches("SELECT count(*) FROM customer WHERE custkey NOT IN (SELECT custkey FROM orders)");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testImplementationSessionProperty(boolean iterativePredicatePushdown)
    {
        QueryAssertions assertions = assertionsByImplementation.get(iterativePredicatePushdown);
        String configuredDefault = Boolean.toString(iterativePredicatePushdown);
        assertThat(assertions.getQueryRunner().execute("SHOW SESSION LIKE 'iterative_predicate_pushdown_enabled'").getMaterializedRows())
                .extracting(row -> row.getField(1), row -> row.getField(2))
                .containsExactly(tuple(configuredDefault, configuredDefault));

        for (boolean enabled : new boolean[] {true, false}) {
            String sessionValue = Boolean.toString(enabled);
            Session session = assertions.sessionBuilder()
                    .setSystemProperty(ITERATIVE_PREDICATE_PUSHDOWN_ENABLED, sessionValue)
                    .build();
            assertThat(assertions.getQueryRunner().execute(session, "SHOW SESSION LIKE 'iterative_predicate_pushdown_enabled'").getMaterializedRows())
                    .extracting(row -> row.getField(1), row -> row.getField(2))
                    .containsExactly(tuple(sessionValue, configuredDefault));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicateMatchingProjectionEquality(boolean iterativePredicatePushdown)
    {
        QueryAssertions assertions = assertionsByImplementation.get(iterativePredicatePushdown);
        // The projection x := y + 1 makes x = y + 1 an effective predicate, but it is NULL rather
        // than TRUE when y is NULL, so it does not make the filter on the same expression redundant
        assertThat(assertions.query(
                """
                SELECT count(*)
                FROM (
                    SELECT x, y
                    FROM (SELECT y, y + 1 AS x FROM (SELECT IF(orderkey % 2 = 0, NULL, orderkey) AS y FROM orders))
                    GROUP BY x, y)
                WHERE x = y + 1
                """))
                .matches("SELECT count(*) FROM orders WHERE orderkey % 2 = 1");

        assertThat(assertions.query(
                """
                SELECT count(*)
                FROM (SELECT custkey, y, y + 1 AS x FROM (SELECT IF(orderkey % 2 = 0, NULL, orderkey) AS y, custkey FROM orders))
                WHERE custkey IN (SELECT custkey FROM customer) AND x = y + 1
                """))
                .matches("SELECT count(*) FROM orders WHERE orderkey % 2 = 1");

        assertThat(assertions.query(
                """
                SELECT count(*)
                FROM (SELECT custkey, y, y + 1 AS x FROM (SELECT IF(orderkey % 2 = 0, NULL, orderkey) AS y, custkey FROM orders)) l
                JOIN customer c ON l.custkey = c.custkey
                WHERE l.x = l.y + 1
                """))
                .matches("SELECT count(*) FROM orders WHERE orderkey % 2 = 1");

        assertThat(assertions.query(
                """
                SELECT count(*)
                FROM (SELECT custkey, y, y + 1 AS x FROM (SELECT IF(orderkey % 2 = 0, NULL, orderkey) AS y, custkey FROM orders)) l
                JOIN customer c ON l.custkey = c.custkey AND l.x = l.y + 1
                """))
                .matches("SELECT count(*) FROM orders WHERE orderkey % 2 = 1");

        // Equality inference rejects x = NULLIF(y, 0), so it is matched directly
        assertThat(assertions.query(
                """
                SELECT count(*)
                FROM (SELECT custkey, y, NULLIF(y, 0) AS x FROM (SELECT IF(orderkey % 2 = 0, NULL, orderkey) AS y, custkey FROM orders)) l
                JOIN customer c ON l.custkey = c.custkey
                WHERE l.x = NULLIF(l.y, 0)
                """))
                .matches("SELECT count(*) FROM orders WHERE orderkey % 2 = 1");

        assertThat(assertions.query(
                """
                SELECT count(*)
                FROM (SELECT custkey, y, length(y) AS x FROM (SELECT IF(orderkey % 2 = 0, NULL, orderstatus) AS y, custkey FROM orders))
                WHERE custkey IN (SELECT custkey FROM customer) AND x = length(y)
                """))
                .matches("SELECT count(*) FROM orders WHERE orderkey % 2 = 1");

        // A join conjunct over the null-extended input stays in the join filter
        assertThat(assertions.query(
                """
                SELECT count(l.custkey)
                FROM customer c
                LEFT JOIN (SELECT custkey, y, length(y) AS x FROM (SELECT IF(orderkey % 2 = 0, NULL, orderstatus) AS y, custkey FROM orders)) l
                ON l.custkey = c.custkey AND l.x = length(l.y)
                """))
                .matches("SELECT count(*) FROM orders WHERE orderkey % 2 = 1");

        // Join equalities rewrite a conjunct into one an input's projection equality states
        assertThat(assertions.query(
                """
                SELECT *
                FROM (SELECT a, NULLIF(a, 0) AS x FROM (VALUES 0, 1) t(a)) l
                JOIN (VALUES 0, 1) r(b) ON l.a = r.b
                WHERE l.x = NULLIF(r.b, 0)
                """))
                .matches("VALUES (1, 1, 1)");

        assertThat(assertions.query(
                """
                SELECT count(*)
                FROM (SELECT a, NULLIF(a, 0) AS x FROM (SELECT IF(nationkey % 2 = 0, 0, nationkey) AS a FROM nation)) l
                JOIN (SELECT IF(nationkey % 2 = 0, 0, nationkey) AS b FROM nation) r ON l.a = r.b
                WHERE l.x = NULLIF(r.b, 0)
                """))
                .matches("SELECT count(*) FROM nation WHERE nationkey % 2 = 1");

        assertThat(assertions.query(
                """
                SELECT count(r.b)
                FROM (SELECT IF(nationkey % 2 = 0, 0, nationkey) AS a FROM nation) l
                LEFT JOIN (SELECT b, NULLIF(b, 0) AS y FROM (SELECT IF(nationkey % 2 = 0, 0, nationkey) AS b FROM nation)) r
                ON l.a = r.b AND r.y = NULLIF(l.a, 0)
                """))
                .matches("SELECT count(*) FROM nation WHERE nationkey % 2 = 1");

        assertThat(assertions.query(
                """
                SELECT count(*)
                FROM (
                    SELECT a, b, x
                    FROM (SELECT a, a AS b, NULLIF(a, 0) AS x FROM (SELECT IF(nationkey % 2 = 0, 0, nationkey) AS a FROM nation))
                    GROUP BY a, b, x
                    HAVING a = b AND x = NULLIF(b, 0))
                """))
                .matches("SELECT count(*) FROM nation WHERE nationkey % 2 = 1");

        // The effective predicate of the null-extended input wraps its projection equality in a disjunction
        assertThat(assertions.query(
                """
                SELECT count(*)
                FROM nation n
                LEFT JOIN (SELECT nationkey, b, NULLIF(b, 0) AS x FROM (SELECT nationkey, IF(nationkey % 2 = 0, 0, nationkey) AS b FROM nation)) o
                ON n.nationkey = o.nationkey
                WHERE o.x = NULLIF(o.b, 0)
                """))
                .matches("SELECT count(*) FROM nation WHERE nationkey % 2 = 1");

        // The conjunct states the projection equality only once simplified
        assertThat(assertions.query(
                """
                SELECT count(*)
                FROM (SELECT k, a, NULLIF(a, 0) AS x FROM (SELECT nationkey AS k, IF(nationkey % 2 = 0, 0, nationkey) AS a FROM nation)) l
                JOIN region r ON l.k % 5 = r.regionkey
                WHERE CASE WHEN true THEN (l.x = NULLIF(l.a, 0) AND r.regionkey >= 0) END
                """))
                .matches("SELECT count(*) FROM nation WHERE nationkey % 2 = 1");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testJoinCriteriaSharingSymbolWithJoinReordering(boolean iterativePredicatePushdown)
    {
        QueryAssertions assertions = assertionsByImplementation.get(iterativePredicatePushdown);
        // Join reordering only keeps the equalities among each input's required outputs, so the implied
        // lineitem.orderkey = lineitem.partkey must be pushed into lineitem before it runs
        Session session = assertions.sessionBuilder()
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "AUTOMATIC")
                .build();
        assertThat(assertions.query(session, "SELECT count(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey"))
                .matches("SELECT count(*) FROM lineitem WHERE orderkey = partkey");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRetainedConjunctWithResidualFilterConverges(boolean iterativePredicatePushdown)
    {
        QueryAssertions assertions = assertionsByImplementation.get(iterativePredicatePushdown);
        Session session = assertions.sessionBuilder()
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "10s")
                .build();
        // NOT IN stays above the semi-join, so the retained x = y + 1 joins that filter instead of a second one above it
        assertThat(assertions.query(session,
                """
                SELECT count(*)
                FROM (SELECT custkey, y, y + 1 AS x FROM (SELECT IF(orderkey % 2 = 0, NULL, orderkey) AS y, custkey FROM orders))
                WHERE custkey NOT IN (SELECT custkey FROM customer WHERE custkey < 100) AND x = y + 1
                """))
                .matches("SELECT count(*) FROM orders WHERE orderkey % 2 = 1 AND custkey >= 100");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInferredProjectionEqualitiesConverge(boolean iterativePredicatePushdown)
    {
        QueryAssertions assertions = assertionsByImplementation.get(iterativePredicatePushdown);
        Session session = assertions.sessionBuilder()
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "10s")
                .build();
        // Join inference derives equalities between projected expressions of both inputs, and
        // pushing them must reach a fixed point
        assertThat(assertions.query(session,
                """
                SELECT count(*)
                FROM (
                    SELECT orderkey % 64 AS a, suppkey % 2 AS b, orderkey AS c, linenumber % 2 AS d
                    FROM lineitem
                    WHERE partkey % 7 = 0) l
                JOIN (
                    SELECT t1.a AS a, t1.b AS b, t2.orderkey AS c, sum(length(t2.comment)) % 2 AS d
                    FROM (SELECT orderkey AS a, custkey % 3 AS b FROM orders) t1
                    JOIN orders t2 ON t1.a = (t2.orderkey % 1000)
                    WHERE t1.a % 1000 = 0
                    GROUP BY t1.a, t1.b, t2.orderkey) o
                ON l.a = o.a AND l.b = o.b AND l.c = o.c AND l.d = o.d
                """))
                .succeeds();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFailingConjunctGuardedByOtherInputOfNormalizedJoin(boolean iterativePredicatePushdown)
    {
        QueryAssertions assertions = assertionsByImplementation.get(iterativePredicatePushdown);
        // r.name = 'ASIA' normalizes the outer join and removes the nation rows for which the cast fails
        assertThat(assertions.query(
                """
                SELECT count(*)
                FROM nation n LEFT JOIN region r ON n.regionkey = r.regionkey AND r.name = 'ASIA'
                WHERE r.name = 'ASIA' AND CAST(CASE WHEN n.regionkey = 2 THEN '7' ELSE n.name END AS bigint) > 0
                """))
                .matches("VALUES BIGINT '5'");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFailingConjunctAfterGuardOnPreservedInput(boolean iterativePredicatePushdown)
    {
        QueryAssertions assertions = assertionsByImplementation.get(iterativePredicatePushdown);
        // The null-rejecting predicate on c converts the outer join to an inner join. The division
        // must still be evaluated after its guard when both are pushed to the preserved input.
        assertThat(assertions.query(
                """
                SELECT count(*)
                FROM orders o LEFT JOIN customer c ON o.custkey = c.custkey
                WHERE c.custkey > 0 AND o.orderkey % 4 <> 0 AND 100 / (o.orderkey % 4) > 1
                """))
                .matches("SELECT count(*) FROM orders WHERE orderkey % 4 IN (1, 2, 3) AND 100 / (orderkey % 4) > 1");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testConditionalExpressionWithFailingExpression(boolean iterativePredicatePushdown)
    {
        QueryAssertions assertions = assertionsByImplementation.get(iterativePredicatePushdown);
        assertThat(assertions.query("" +
                "WITH t (k, a) AS ( " +
                "    VALUES " +
                "        (1, 1), " +
                "        (2, 0) " +
                ")," +
                "u (k, v) AS ( " +
                "    VALUES" +
                "        (1, 'x'), " +
                "        (2, 'y') " +
                ") " +
                "SELECT * " +
                "FROM ( " +
                "    SELECT v, if(v = 'x', 1 / a) AS r" +
                "    FROM t JOIN u ON t.k = u.k " +
                ") " +
                "WHERE v = 'x' AND r IS NOT NULL"))
                .matches("VALUES ('x', 1)");

        assertThat(assertions.query(
                "WITH t (k, v) AS ( " +
                        "    VALUES " +
                        "        (1, 1), " +
                        "        (2, 0) " +
                        "), " +
                        "u (k) AS ( " +
                        "    VALUES 1, 2 " +
                        ") " +
                        "SELECT * " +
                        "FROM ( " +
                        "    SELECT t.k, if(t.k = 1, 1 / t.v) AS r " +
                        "    FROM t JOIN u ON t.k = u.k " +
                        ") " +
                        "WHERE k = 1 AND r <> 0"))
                .matches("VALUES (1, 1)");

        assertThat(assertions.query(
                "WITH t (k, v) AS ( " +
                        "    VALUES " +
                        "        (1, 1), " +
                        "        (2, 0) " +
                        "), " +
                        "u (k) AS ( " +
                        "    VALUES 1, 2 " +
                        ") " +
                        "SELECT * " +
                        "FROM ( " +
                        "    SELECT t.k, if(t.k = 1, 1 / t.v) AS r " +
                        "    FROM t LEFT JOIN u ON t.k = u.k " +
                        ") " +
                        "WHERE k = 1 AND r <> 0"))
                .matches("VALUES (1, 1)");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testNotBetweenOverExpression(boolean iterativePredicatePushdown)
    {
        QueryAssertions assertions = assertionsByImplementation.get(iterativePredicatePushdown);
        // The CAST in the SQLs below keeps the operand non-trivial so BETWEEN lowers to a Let; a bare column would inline and bypass the negation-dropping path under test.
        assertThat(assertions.query(
                """
                SELECT count(*) FROM orders
                WHERE CAST(orderdate AS timestamp(6)) BETWEEN TIMESTAMP '1995-03-01 00:00:00' AND TIMESTAMP '1995-03-31 23:59:59.999999'
                """))
                .matches(
                        """
                        SELECT count(*) FROM orders
                        WHERE CAST(orderdate AS timestamp(6)) >= TIMESTAMP '1995-03-01 00:00:00'
                            AND CAST(orderdate AS timestamp(6)) <= TIMESTAMP '1995-03-31 23:59:59.999999'
                        """);

        assertThat(assertions.query(
                """
                SELECT count(*) FROM orders
                WHERE CAST(orderdate AS timestamp(6)) NOT BETWEEN TIMESTAMP '1995-03-01 00:00:00' AND TIMESTAMP '1995-03-31 23:59:59.999999'
                """))
                .matches(
                        """
                        SELECT count(*) FROM orders
                        WHERE CAST(orderdate AS timestamp(6)) < TIMESTAMP '1995-03-01 00:00:00'
                            OR CAST(orderdate AS timestamp(6)) > TIMESTAMP '1995-03-31 23:59:59.999999'
                        """);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testNotNullIfOverExpression(boolean iterativePredicatePushdown)
    {
        QueryAssertions assertions = assertionsByImplementation.get(iterativePredicatePushdown);
        // The CAST in the SQLs below keeps the operand non-trivial so NULLIF lowers to a Let; a bare column would inline and bypass the negation-dropping path under test.
        assertThat(assertions.query(
                """
                SELECT count(*) FROM orders
                WHERE NULLIF(CAST(orderdate AS timestamp(6)) = TIMESTAMP '1995-03-01 00:00:00', true)
                """))
                .matches(
                        """
                        SELECT count(*) FROM orders
                        WHERE CASE WHEN CAST(orderdate AS timestamp(6)) = TIMESTAMP '1995-03-01 00:00:00' THEN NULL
                            ELSE CAST(orderdate AS timestamp(6)) = TIMESTAMP '1995-03-01 00:00:00' END
                        """);

        assertThat(assertions.query(
                """
                SELECT count(*) FROM orders
                WHERE NOT NULLIF(CAST(orderdate AS timestamp(6)) = TIMESTAMP '1995-03-01 00:00:00', true)
                """))
                .matches(
                        """
                        SELECT count(*) FROM orders
                        WHERE NOT(CASE WHEN CAST(orderdate AS timestamp(6)) = TIMESTAMP '1995-03-01 00:00:00' THEN NULL
                            ELSE CAST(orderdate AS timestamp(6)) = TIMESTAMP '1995-03-01 00:00:00' END)
                        """);
    }
}
