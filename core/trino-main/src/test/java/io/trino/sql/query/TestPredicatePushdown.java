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
