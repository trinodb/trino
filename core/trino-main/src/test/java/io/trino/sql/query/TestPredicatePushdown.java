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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestPredicatePushdown
{
    private final QueryAssertions assertions;

    public TestPredicatePushdown()
    {
        Session session = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TINY_SCHEMA_NAME)
                .build();
        QueryRunner runner = new StandaloneQueryRunner(session);
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog(TEST_CATALOG_NAME, "tpch", ImmutableMap.of("tpch.splits-per-node", "1"));
        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testIsNotDistinctFromJoin()
    {
        // NULL matches NULL, and a NULL is distinct from the value standing in for it in the join keys
        assertThat(assertions.query(
                "SELECT a.id, b.id FROM (VALUES (1, 1), (2, CAST(NULL AS integer)), (3, 0), (4, 5)) a(id, k) " +
                        "JOIN (VALUES (10, 1), (20, CAST(NULL AS integer)), (30, 0), (40, 0)) b(id, k) ON a.k IS NOT DISTINCT FROM b.k"))
                .matches("VALUES (1, 10), (2, 20), (3, 30), (3, 40)");
        assertThat(assertions.query(
                "SELECT a.id, b.id FROM (VALUES (1, 'x'), (2, CAST(NULL AS varchar)), (3, ''), (4, 'y')) a(id, k) " +
                        "LEFT JOIN (VALUES (10, ''), (20, CAST(NULL AS varchar)), (30, 'x')) b(id, k) ON a.k IS NOT DISTINCT FROM b.k"))
                .matches("VALUES (1, 30), (2, 20), (3, 10), (4, CAST(NULL AS integer))");
        assertThat(assertions.query(
                "SELECT a.id, b.id FROM (VALUES (1, false), (2, CAST(NULL AS boolean))) a(id, k) " +
                        "FULL JOIN (VALUES (10, false), (20, CAST(NULL AS boolean)), (30, true)) b(id, k) ON a.k IS NOT DISTINCT FROM b.k"))
                .matches("VALUES (1, 10), (2, 20), (CAST(NULL AS integer), 30)");
        assertThat(assertions.query(
                "SELECT a.id, b.id FROM (VALUES (1, DECIMAL '0.00'), (2, CAST(NULL AS decimal(30, 2)))) a(id, k) " +
                        "JOIN (VALUES (10, DECIMAL '0.00'), (20, CAST(NULL AS decimal(30, 2)))) b(id, k) ON a.k IS NOT DISTINCT FROM b.k"))
                .matches("VALUES (1, 10), (2, 20)");
        // floating-point keys keep IS NOT DISTINCT FROM semantics for NaN
        assertThat(assertions.query(
                "SELECT a.id, b.id FROM (VALUES (1, nan()), (2, CAST(NULL AS double)), (3, 1e0)) a(id, k) " +
                        "JOIN (VALUES (10, nan()), (20, CAST(NULL AS double)), (30, 1e0)) b(id, k) ON a.k IS NOT DISTINCT FROM b.k"))
                .matches("VALUES (1, 10), (2, 20), (3, 30)");
    }

    @Test
    public void testConditionalExpressionWithFailingExpression()
    {
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

    @Test
    public void testNotBetweenOverExpression()
    {
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

    @Test
    public void testNotNullIfOverExpression()
    {
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
