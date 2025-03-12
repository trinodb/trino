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
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestWith
{
    private final QueryAssertions assertions;

    public TestWith()
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
    public void testWithHiddenFields()
    {
        // Ensure WITH works when the subquery exposes hidden fields

        // First, verify the assumption that the nation table contains the expected hidden column
        assertThat(assertions.query(
                format(
                        "SELECT count(*) " +
                                "FROM information_schema.columns " +
                                "WHERE table_catalog = '%s' and table_schema = '%s' and table_name = 'nation' and column_name = 'row_number'", TEST_CATALOG_NAME, TINY_SCHEMA_NAME)))
                .matches("VALUES BIGINT '0'");
        assertions.execute("SELECT min(row_number) FROM nation");

        assertThat(assertions.query(
                "WITH t(a, b, c, d) AS (TABLE nation) " +
                        "SELECT a, b FROM t WHERE a = 1"))
                .matches("VALUES (BIGINT '1', CAST('ARGENTINA' AS VARCHAR(25)))");

        // Test CTE specified as TABLE without columns aliases
        assertThat(assertions.query("WITH t AS (TABLE nation) " +
                "SELECT * FROM t"))
                .matches("SELECT * FROM nation");

        // try access hidden column
        assertThat(assertions.query("WITH t AS (TABLE nation) " +
                "SELECT min(row_number) FROM t"))
                .failure().hasMessage("line 1:37: Column 'row_number' cannot be resolved");
    }
}
