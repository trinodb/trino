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
package io.trino.execution;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.sql.query.QueryAssertions;
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
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestEnforcedFilterColumns
{
    private final QueryAssertions assertions;

    public TestEnforcedFilterColumns()
    {
        Session session = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TINY_SCHEMA_NAME)
                .build();
        QueryRunner runner = new StandaloneQueryRunner(
                session,
                builder -> builder.addProperty("query.enforced-filter-columns", TEST_CATALOG_NAME + "." + TINY_SCHEMA_NAME + ".orders.orderkey"));
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
    public void testEnforcedColumnIsUsedInWhereClause()
    {
        assertThat(assertions.query("SELECT count(*) FROM orders WHERE orderkey = 1"))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    public void testEnforcedColumnIsUsedInSubqueryWhereClause()
    {
        assertThat(assertions.query("SELECT * FROM (SELECT orderkey FROM orders) t WHERE orderkey = 1"))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    public void testEnforcedColumnIsMissingFromWhereClause()
    {
        assertThat(assertions.query("SELECT * FROM orders"))
                .failure()
                .hasMessageContaining("Enforced columns [" + TEST_CATALOG_NAME + "." + TINY_SCHEMA_NAME + ".orders.orderkey] must be referenced in the WHERE clause");
    }

    @Test
    public void testOtherColumnDoesNotSatisfyEnforcedColumn()
    {
        assertThat(assertions.query("SELECT * FROM orders WHERE orderstatus = 'O'"))
                .failure()
                .hasMessageContaining("Enforced columns [" + TEST_CATALOG_NAME + "." + TINY_SCHEMA_NAME + ".orders.orderkey] must be referenced in the WHERE clause");
    }

    @Test
    public void testOtherTableWithSameColumnIsNotEnforced()
    {
        assertThat(assertions.query("SELECT 1 FROM lineitem LIMIT 1"))
                .matches("VALUES 1");
    }

    @Test
    public void testTableWithoutEnforcedColumn()
    {
        assertThat(assertions.query("SELECT count(*) FROM nation"))
                .matches("VALUES BIGINT '25'");
    }
}
