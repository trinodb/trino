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
package io.trino.client.direct;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.concurrent.TimeUnit;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.RETRY_POLICY;
import static io.trino.operator.RetryPolicy.QUERY;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDirectTrinoClient
{
    private QueryRunner queryRunner;
    private QueryRunner queryRunnerWithTaskRetry;

    @BeforeAll
    public void setup()
    {
        queryRunner = new StandaloneQueryRunner(
                TEST_SESSION,
                builder -> builder.overrideProperties(ImmutableMap.of(
                        "query.client.timeout", "1s")));
        queryRunner.installPlugin(new BlackHolePlugin());
        queryRunner.createCatalog("blackhole", "blackhole");
        queryRunner.execute("CREATE SCHEMA blackhole.test_schema");
        queryRunner.execute("CREATE TABLE blackhole.test_schema.slow_test_table (col1 VARCHAR, col2 INTEGER)" +
                "WITH (" +
                "   split_count = 1, " +
                "   pages_per_split = 1, " +
                "   rows_per_page = 1, " +
                "   page_processing_delay = '3s'" +
                ")");
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of("tpch.splits-per-node", "1"));
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory");

        queryRunnerWithTaskRetry = new StandaloneQueryRunner(
                TEST_SESSION,
                builder -> builder.overrideProperties(ImmutableMap.of(
                        "query.client.timeout", "1s",
                        "retry-policy", "TASK")));
        queryRunnerWithTaskRetry.installPlugin(new TpchPlugin());
        queryRunnerWithTaskRetry.createCatalog("tpch", "tpch", ImmutableMap.of("tpch.splits-per-node", "1"));
    }

    @AfterAll
    public void teardown()
    {
        if (queryRunnerWithTaskRetry != null) {
            queryRunnerWithTaskRetry.close();
            queryRunnerWithTaskRetry = null;
        }
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    public void testDirectTrinoClientLongQuery()
    {
        queryRunner.execute(TEST_SESSION, "SELECT * FROM blackhole.test_schema.slow_test_table");
    }

    @Test
    public void testBasicQuery()
    {
        MaterializedResult result = queryRunner.execute(TEST_SESSION, "SELECT 1 AS col");

        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
        assertThat(result.getColumnNames()).containsExactly("col");
    }

    @Test
    public void testEmptyResult()
    {
        MaterializedResult result = queryRunner.execute(TEST_SESSION, "SELECT * FROM (SELECT 'hello' AS col) WHERE 1 = 0");

        assertThat(result.getRowCount()).isEqualTo(0);
        assertThat(result.getColumnNames()).containsExactly("col");
    }

    @Test
    public void testDdlStatement()
    {
        Session session = Session.builder(TEST_SESSION)
                .setCatalog("memory")
                .setSchema("default")
                .build();

        String tableName = "test_table_" + randomNameSuffix();
        queryRunner.execute(session, "CREATE TABLE %s (id BIGINT)".formatted(tableName));
        assertThat(queryRunner.tableExists(session, tableName)).isTrue();

        queryRunner.execute(session, "DROP TABLE %s".formatted(tableName));
        assertThat(queryRunner.tableExists(session, tableName)).isFalse();
    }

    @Test
    public void testQueryFailure()
    {
        assertThatThrownBy(() -> queryRunner.execute(TEST_SESSION, "SELECT * FROM non_existent_table"))
                .isInstanceOf(QueryFailedException.class)
                .hasMessageContaining("non_existent_table");
    }

    @Test
    public void testUpdateStatement()
    {
        Session session = Session.builder(TEST_SESSION)
                .setCatalog("memory")
                .setSchema("default")
                .build();

        String tableName = "test_table_" + randomNameSuffix();
        queryRunner.execute(session, "CREATE TABLE %s (id BIGINT)".formatted(tableName));
        try {
            MaterializedResult result = queryRunner.execute(session, "INSERT INTO %s (id) VALUES (1), (2), (3)".formatted(tableName));
            assertThat(result.getUpdateCount()).hasValue(3L);
        }
        finally {
            queryRunner.execute(session, "DROP TABLE IF EXISTS %s".formatted(tableName));
        }
    }

    @Test
    public void testQueryWithTaskRetryPolicyInSession()
    {
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(RETRY_POLICY, TASK.name())
                .build();

        MaterializedResult result = queryRunner.execute(session, "SELECT 1 AS col");

        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
    }

    @Test
    public void testQueryWithTaskRetryPolicyInConfig()
    {
        MaterializedResult result = queryRunnerWithTaskRetry.execute(TEST_SESSION, "SELECT 1 AS col");

        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
    }

    @Test
    public void testQueryWithQueryRetryPolicy()
    {
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(RETRY_POLICY, QUERY.name())
                .build();

        MaterializedResult result = queryRunner.execute(session, "SELECT 1 AS col");

        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
    }
}
