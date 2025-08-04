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
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_DATA;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestBranching
{
    private final QueryAssertions assertions;

    public TestBranching()
    {
        QueryRunner runner = new StandaloneQueryRunner(TEST_SESSION);

        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withGetTableHandle((_, schemaTableName) -> new MockConnectorTableHandle(schemaTableName))
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    throw new UnsupportedOperationException();
                })
                .withBranches(ImmutableList.of("main"))
                .withData(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                        return TPCH_NATION_DATA;
                    }
                    throw new UnsupportedOperationException();
                })
                .build()));
        runner.createCatalog("mock", "mock", ImmutableMap.of());

        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    void teardown()
    {
        assertions.close();
    }

    @Test
    void testInsert()
    {
        assertThat(assertions.query("INSERT INTO mock.tiny.nation @ main VALUES (101, 'POLAND', 0, 'No comment')"))
                .matches("SELECT BIGINT '1'");

        assertThat(assertions.query("INSERT INTO mock.tiny.nation @ stale VALUES (26, 'POLAND', 11, 'No comment')"))
                .failure().hasMessage("line 1:1: Branch 'stale' does not exist");
    }

    @Test
    void testDelete()
    {
        assertThat(assertions.query("DELETE FROM mock.tiny.nation @ main WHERE nationkey < 3"))
                .matches("SELECT BIGINT '3'");

        assertThat(assertions.query("DELETE FROM mock.tiny.nation @ stale WHERE nationkey IN (1, 2, 3)"))
                .failure().hasMessage("line 1:1: Branch 'stale' does not exist");
    }

    @Test
    void testUpdate()
    {
        assertThat(assertions.query("UPDATE mock.tiny.nation @ main SET regionkey = regionkey + 1"))
                .matches("SELECT BIGINT '25'");

        assertThat(assertions.query("UPDATE mock.tiny.nation @ stale SET regionkey = regionkey * 10"))
                .failure().hasMessage("line 1:1: Branch 'stale' does not exist");
    }

    @Test
    void testMergeInsert()
    {
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation @ main USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT VALUES (101, 'POLAND', 0, 'No comment')
                """))
                .matches("SELECT BIGINT '1'");

        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation @ stale USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT VALUES (101, 'POLAND', 10, 'No comment')
                """))
                .failure().hasMessage("line 1:1: Branch 'stale' does not exist");
    }

    @Test
    void testMergeDelete()
    {
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation @ main USING (VALUES 1,2) t(x) ON nationkey = x
                WHEN MATCHED THEN DELETE
                """))
                .matches("SELECT BIGINT '2'");

        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation @ stale USING (VALUES 1,2) t(x) ON nationkey = x
                WHEN MATCHED THEN DELETE
                """))
                .failure().hasMessage("line 1:1: Branch 'stale' does not exist");
    }

    @Test
    void testMergeUpdate()
    {
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation @ main USING (VALUES 5) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET regionkey = regionkey * 2
                """))
                .matches("SELECT BIGINT '1'");

        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation @ stale USING (VALUES 5) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET regionkey = regionkey * 2
                """))
                .failure().hasMessage("line 1:1: Branch 'stale' does not exist");
    }
}
