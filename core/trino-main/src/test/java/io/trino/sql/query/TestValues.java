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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestValues
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testRowSpecifications()
    {
        assertThat(assertions.query("VALUES 1, 2"))
                .matches("VALUES ROW(1), ROW(2)");

        assertThat(assertions.query("VALUES (1, 'a')"))
                .matches("VALUES ROW(1, 'a')");

        assertThat(assertions.query("VALUES CAST(ROW(1, 'TruE') AS row(double, boolean))"))
                .matches("VALUES ROW(1e0, true)");

        // mixed specifications
        assertThat(assertions.query("VALUES " +
                "CAST(ROW(1, 'TruE') AS row(double, boolean)), " +
                "ROW(2e0, false), " +
                "(3e0, null)"))
                .matches("VALUES " +
                        "ROW(1e0, true)," +
                        "ROW(2e0, false), " +
                        "ROW(3e0, null)");

        assertThat(assertions.query("SELECT key FROM " +
                "(VALUES " +
                "CAST(ROW(1, 'TruE') AS row(double, boolean)), " +
                "ROW(2e0, false), " +
                "(3e0, null)) T(key, value)"))
                .matches("VALUES 1e0, 2e0, 3e0");
    }

    @Test
    public void testCoercions()
    {
        assertThat(assertions.query("VALUES 1, 2e0"))
                .matches("VALUES ROW(1e0), ROW(2e0)");

        assertThat(assertions.query("VALUES (1, 2), (3, 4e0)"))
                .matches("VALUES ROW(1, 2e0), ROW(3, 4e0)");

        assertThat(assertions.query("VALUES CAST(ROW(1, 2) AS row(integer, smallint)), (3, 4e0)"))
                .matches("VALUES ROW(1, 2e0), ROW(3, 4e0)");

        assertThat(assertions.query("VALUES CAST(ROW(1, 2) AS row(integer, double)), (3, 4)"))
                .matches("VALUES ROW(1, 2e0), ROW(3, 4e0)");

        assertThat(assertions.query("VALUES CAST(ROW(1, null) AS row(integer, double)), (3, 4)"))
                .matches("VALUES ROW(1, null), ROW(3, 4e0)");
    }

    @Test
    public void testNulls()
    {
        assertThat(assertions.query("VALUES null"))
                .matches("VALUES ROW(null)");

        assertThat(assertions.query("VALUES (null, null)"))
                .matches("VALUES ROW(null, null)");

        assertThat(assertions.query("VALUES (null, null), ('a', 'b')"))
                .matches("VALUES ROW(null, null), ROW('a', 'b')");

        assertThat(assertions.query("VALUES " +
                "CAST(ROW(null, null) AS row(real, double)), " +
                "(1, 1)"))
                .matches("VALUES " +
                        "ROW(null, null), " +
                        "ROW(REAL '1', DOUBLE '1')");
    }

    @Test
    public void testSubqueries()
    {
        for (String query : ImmutableList.of(
                "VALUES (SELECT 1)",
                "VALUES (1, (SELECT count(*) FROM (VALUES 1, 2) t(x)))",
                "VALUES 1, (SELECT 2)",
                "SELECT * FROM (VALUES (1 + (SELECT 2))) t(x)",
                "VALUES CAST((SELECT 1) AS BIGINT)",
                "VALUES IF(false, (SELECT 1), 0)",
                "VALUES EXISTS(SELECT 1)",
                "VALUES 1 IN (SELECT 1)",
                "VALUES 1 = ANY(SELECT 1)",
                "SELECT * FROM (VALUES 1) t(x), LATERAL (VALUES (SELECT x))")) {
            assertThat(assertions.query(query))
                    .failure().hasErrorCode(NOT_SUPPORTED)
                    .hasMessageContaining("Subqueries are not supported in VALUES");
        }
    }

    @Test
    public void testFailingExpression()
    {
        assertThat(assertions.query("VALUES 0 / 0"))
                .failure().hasErrorCode(DIVISION_BY_ZERO);

        assertThat(assertions.query("VALUES CASE 1 WHEN 0 THEN true WHEN 0 / 0 THEN false END"))
                .failure().hasErrorCode(DIVISION_BY_ZERO);

        assertThat(assertions.query("VALUES IF(0 / 0 > 0, true, false)"))
                .failure().hasErrorCode(DIVISION_BY_ZERO);
    }
}
