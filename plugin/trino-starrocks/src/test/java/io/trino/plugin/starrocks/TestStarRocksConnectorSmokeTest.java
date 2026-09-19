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
package io.trino.plugin.starrocks;

import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
final class TestStarRocksConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    private final TestingStarRocksEnvironment environment = new TestingStarRocksEnvironment(List.of(
            new TestingStarRocksEnvironment.TableDefinition(
                    new SchemaTableName("starrocks_test", "events"),
                    "starrocks_test",
                    "events",
                    StarRocksRelationType.TABLE,
                    List.of(
                            new StarRocksRemoteColumn("id", "id", "BIGINT", Optional.of(20), Optional.empty(), 1, Optional.empty()),
                            new StarRocksRemoteColumn("created_at", "created_at", "DATETIME", Optional.empty(), Optional.empty(), 2, Optional.of("datetimev2(3)")),
                            new StarRocksRemoteColumn("name", "name", "VARCHAR", Optional.of(20), Optional.empty(), 3, Optional.of("varchar(20)")),
                            new StarRocksRemoteColumn("amount", "amount", "DECIMAL", Optional.of(18), Optional.of(2), 4, Optional.of("decimal(18,2)"))),
                    List.of(
                            Map.of("id", 1L, "created_at", "2024-01-01 10:15:30.125", "name", "alpha", "amount", new BigDecimal("12.34")),
                            Map.of("id", 2L, "created_at", "2024-01-02 11:16:31.250", "name", "beta", "amount", new BigDecimal("56.78")),
                            Map.of("id", 3L, "created_at", "2024-01-03 12:17:32.500", "name", "gamma", "amount", new BigDecimal("90.12")))),
            new TestingStarRocksEnvironment.TableDefinition(
                    new SchemaTableName("starrocks_test", "events_view"),
                    "StarRocks_Test",
                    "EventsView",
                    StarRocksRelationType.VIEW,
                    List.of(
                            new StarRocksRemoteColumn("id", "id", "BIGINT", Optional.of(20), Optional.empty(), 1, Optional.empty()),
                            new StarRocksRemoteColumn("name", "name", "VARCHAR", Optional.of(20), Optional.empty(), 2, Optional.of("varchar(20)"))),
                    List.of(
                            Map.of("id", 1L, "name", "alpha"),
                            Map.of("id", 2L, "name", "beta"))),
            new TestingStarRocksEnvironment.TableDefinition(
                    new SchemaTableName("starrocks_test", "starrocks_specific"),
                    "starrocks_test",
                    "starrocks_specific",
                    StarRocksRelationType.TABLE,
                    List.of(
                            new StarRocksRemoteColumn("metric_key", "metric_key", "BIGINT", Optional.of(20), Optional.empty(), 1, Optional.empty()),
                            new StarRocksRemoteColumn("largeint_summary", "largeint_summary", "DECIMAL", Optional.empty(), Optional.empty(), 2, Optional.of("largeint")),
                            new StarRocksRemoteColumn("bitmap_summary", "bitmap_summary", "BITMAP", Optional.empty(), Optional.empty(), 3, Optional.of("bitmap")),
                            new StarRocksRemoteColumn("json_payload", "json_payload", "JSON", Optional.empty(), Optional.empty(), 4, Optional.of("json"))),
                    List.of(
                            Map.of("metric_key", 10L, "largeint_summary", "9223372036854775809", "bitmap_summary", "1,3,5", "json_payload", "{\"kind\":\"bitmap\",\"count\":3}"),
                            Map.of("metric_key", 11L, "largeint_summary", "-1", "bitmap_summary", "2,4", "json_payload", "{\"kind\":\"bitmap\",\"count\":2}")))));

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return StarRocksQueryRunner.builder(environment).build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR -> false;
            case SUPPORTS_AGGREGATION_PUSHDOWN -> true;
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_DELETE,
                 SUPPORTS_DYNAMIC_FILTER_PUSHDOWN,
                 SUPPORTS_INSERT,
                 SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_MERGE,
                 SUPPORTS_NATIVE_QUERY,
                 SUPPORTS_NOT_NULL_CONSTRAINT,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT,
                 SUPPORTS_UPDATE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                 SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    void testStarRocksBasicRead()
    {
        assertQuery(
                "SELECT nationkey, name FROM nation WHERE nationkey <= 2 ORDER BY nationkey",
                "VALUES (0, 'ALGERIA'), (1, 'ARGENTINA'), (2, 'BRAZIL')");
    }

    @Test
    void testStarRocksShowSchemas()
    {
        assertQuery("SHOW SCHEMAS FROM starrocks LIKE 'starrocks_test'", "VALUES 'starrocks_test'");
    }

    @Test
    void testStarRocksShowTablesAndDescribe()
    {
        assertQuery(
                "SHOW TABLES FROM starrocks.starrocks_test",
                "VALUES 'events', 'events_view', 'starrocks_specific'");
        assertQuery(
                "DESCRIBE starrocks.starrocks_test.events",
                """
                VALUES
                    ('id', 'bigint', '', ''),
                    ('created_at', 'timestamp(3)', '', ''),
                    ('name', 'varchar(20)', '', ''),
                    ('amount', 'decimal(18,2)', '', '')
                """);
    }

    @Test
    void testStarRocksShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE starrocks.starrocks_test.events").getOnlyValue())
                .contains("CREATE TABLE starrocks.starrocks_test.events")
                .contains("created_at timestamp(3)")
                .contains("name varchar(20)")
                .contains("amount decimal(18, 2)");
    }

    @Test
    void testStarRocksCountAndDatetimeReads()
    {
        assertQuery("SELECT count(*) FROM starrocks.starrocks_test.events", "VALUES 3");
        assertQuery(
                "SELECT id, created_at FROM starrocks.starrocks_test.events ORDER BY id",
                """
                VALUES
                    (1, TIMESTAMP '2024-01-01 10:15:30.125'),
                    (2, TIMESTAMP '2024-01-02 11:16:31.250'),
                    (3, TIMESTAMP '2024-01-03 12:17:32.500')
                """);
    }

    @Test
    void testStarRocksViewsAndSpecificTypeFallbacks()
    {
        assertQuery(
                "SELECT * FROM starrocks.starrocks_test.events_view ORDER BY id",
                "VALUES (1, 'alpha'), (2, 'beta')");
        assertQuery(
                "SELECT metric_key, largeint_summary, bitmap_summary FROM starrocks.starrocks_test.starrocks_specific ORDER BY metric_key",
                "VALUES (10, '9223372036854775809', '1,3,5'), (11, '-1', '2,4')");
        assertQuery(
                "SELECT metric_key, json_format(json_payload) FROM starrocks.starrocks_test.starrocks_specific ORDER BY metric_key",
                "VALUES (10, '{\"kind\":\"bitmap\",\"count\":3}'), (11, '{\"kind\":\"bitmap\",\"count\":2}')");
    }

    @Test
    void testStarRocksPushdowns()
    {
        assertQuery("SELECT id FROM starrocks.starrocks_test.events WHERE id >= 2 ORDER BY id DESC LIMIT 1", "VALUES 3");
        TestingStarRocksEnvironment.Request topNRequest = environment.getLastRequest().orElseThrow();
        assertThat(topNRequest.tableHandle().constraint().isAll()).isFalse();
        assertThat(topNRequest.tableHandle().sortOrder()).hasSize(1);
        assertThat(topNRequest.tableHandle().limit()).hasValue(1);

        assertQuery("SELECT name FROM starrocks.starrocks_test.events ORDER BY name LIMIT 1", "VALUES 'alpha'");
        TestingStarRocksEnvironment.Request stringTopNRequest = environment.getLastRequest().orElseThrow();
        assertThat(stringTopNRequest.tableHandle().sortOrder()).isEmpty();

        assertQuery("SELECT count(*) FROM starrocks.starrocks_test.events WHERE id >= 2", "VALUES 2");
        TestingStarRocksEnvironment.Request countRequest = environment.getLastRequest().orElseThrow();
        assertThat(countRequest.tableHandle().constraint().isAll()).isFalse();
        assertThat(countRequest.tableHandle().aggregation()).isPresent();
        assertThat(countRequest.tableHandle().projectedColumns()).isPresent();
        assertThat(countRequest.tableHandle().projectedColumns().orElseThrow()).extracting(StarRocksColumnHandle::columnName)
                .containsExactly("_starrocks_agg_0");
        assertThat(countRequest.requestedColumns()).extracting(StarRocksColumnHandle::columnName)
                .containsExactly("_starrocks_agg_0");
    }

    @Test
    void testStarRocksAggregationPushdowns()
    {
        assertQuery(
                "SELECT sum(id), avg(id), min(id), max(created_at) FROM starrocks.starrocks_test.events WHERE id >= 2",
                "VALUES (5, 2.5, 2, TIMESTAMP '2024-01-03 12:17:32.500')");
        TestingStarRocksEnvironment.Request aggregateRequest = environment.getLastRequest().orElseThrow();
        assertThat(aggregateRequest.tableHandle().aggregation()).isPresent();
        assertThat(aggregateRequest.tableHandle().aggregation().orElseThrow().aggregateColumns())
                .extracting(StarRocksAggregateColumn::expression)
                .containsExactly("sum(`id`)", "avg(`id`)", "min(`id`)", "max(`created_at`)");
    }
}
