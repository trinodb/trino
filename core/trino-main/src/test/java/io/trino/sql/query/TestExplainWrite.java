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

import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingMetadata;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // tests share beginInsertCalls
final class TestExplainWrite
{
    private final AtomicInteger beginInsertCalls = new AtomicInteger();
    private final QueryAssertions assertions = new QueryAssertions(testSessionBuilder()
            .setCatalog("mock")
            .setSchema("default")
            .build());

    TestExplainWrite()
    {
        // TestingMetadata creates the table when the write begins, like some connectors do
        TestingMetadata metadata = new TestingMetadata()
        {
            @Override
            public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
            {
                beginInsertCalls.incrementAndGet();
                return super.beginInsert(session, tableHandle, columns, retryMode);
            }
        };
        QueryRunner runner = assertions.getQueryRunner();
        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withMetadataWrapper(_ -> metadata)
                .build()));
        runner.createCatalog("mock", "mock", Map.of());
        // MockConnector tables support DELETE, UPDATE and MERGE
        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName("mergeable")
                .withGetTableHandle((_, schemaTableName) -> new MockConnectorTableHandle(schemaTableName))
                .withGetColumns(_ -> List.of(new ColumnMetadata("x", INTEGER)))
                .build()));
        runner.createCatalog("mergeable", "mergeable", Map.of());
        metadata.createTable(
                runner.getDefaultSession().toConnectorSession(),
                new ConnectorTableMetadata(new SchemaTableName("default", "existing"), List.of(new ColumnMetadata("x", INTEGER))),
                SaveMode.FAIL);
    }

    @AfterAll
    void teardown()
    {
        assertions.close();
    }

    @Test
    void testExplainCreateTableAsSelect()
    {
        assertThat(explainAllTypes("CREATE TABLE new_table AS SELECT 1 x"))
                .contains("outputTable", "new_table");
        assertThat(tableExists("new_table")).isFalse();
    }

    @Test
    void testExplainInsert()
    {
        int calls = beginInsertCalls.get();
        assertThat(explainAllTypes("INSERT INTO existing VALUES 1"))
                .contains("outputTable", "existing");
        assertThat(beginInsertCalls.get()).isEqualTo(calls);
    }

    @Test
    void testExplainDelete()
    {
        assertThat(explainAllTypesExceptGraphviz("DELETE FROM mergeable.default.merge_target WHERE x = 1"))
                .contains("outputTable", "merge_target");
    }

    @Test
    void testExplainUpdate()
    {
        assertThat(explainAllTypesExceptGraphviz("UPDATE mergeable.default.merge_target SET x = 2"))
                .contains("outputTable", "merge_target");
    }

    @Test
    void testExplainMerge()
    {
        assertThat(explainAllTypesExceptGraphviz(
                """
                MERGE INTO mergeable.default.merge_target USING (VALUES 1) t(y) ON x = y
                WHEN MATCHED THEN UPDATE SET x = 2
                WHEN NOT MATCHED THEN INSERT VALUES (y)
                """))
                .contains("outputTable", "merge_target");
    }

    @Test
    void testExplainAnalyzeExecutesWrite()
    {
        assertions.execute("EXPLAIN ANALYZE CREATE TABLE analyzed_table AS SELECT 1 x");
        assertThat(tableExists("analyzed_table")).isTrue();

        int calls = beginInsertCalls.get();
        assertions.execute("EXPLAIN ANALYZE INSERT INTO existing VALUES 1");
        assertThat(beginInsertCalls.get()).isEqualTo(calls + 1);
    }

    /**
     * @return the IO plan in JSON format
     */
    private String explainAllTypes(@Language("SQL") String statement)
    {
        assertions.execute("EXPLAIN (TYPE DISTRIBUTED, FORMAT GRAPHVIZ) " + statement);
        return explainAllTypesExceptGraphviz(statement);
    }

    /**
     * Use for statements planned with a {@code MergeWriterNode}, which {@code FORMAT GRAPHVIZ} does not support.
     *
     * @return the IO plan in JSON format
     */
    private String explainAllTypesExceptGraphviz(@Language("SQL") String statement)
    {
        assertions.execute("EXPLAIN " + statement);
        assertions.execute("EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON) " + statement);
        return (String) assertions.execute("EXPLAIN (TYPE IO, FORMAT JSON) " + statement).getOnlyValue();
    }

    private boolean tableExists(String table)
    {
        QueryRunner runner = assertions.getQueryRunner();
        return runner.tableExists(runner.getDefaultSession(), table);
    }
}
