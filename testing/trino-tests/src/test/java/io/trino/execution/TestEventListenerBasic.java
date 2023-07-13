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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.execution.EventsAwaitingQueries.MaterializedResultWithEvents;
import io.trino.execution.EventsCollector.QueryEvents;
import io.trino.execution.TestEventListenerPlugin.TestingEventListenerPlugin;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.resourcegroups.ResourceGroupManagerPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition.Column;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.eventlistener.ColumnDetail;
import io.trino.spi.eventlistener.ColumnInfo;
import io.trino.spi.eventlistener.OutputColumnMetadata;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.RoutineInfo;
import io.trino.spi.eventlistener.TableInfo;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.security.ViewExpression;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.io.Resources.getResource;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_DATA;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_SCHEMA;
import static io.trino.execution.TestQueues.createResourceGroupId;
import static io.trino.spi.metrics.Metrics.EMPTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.planprinter.JsonRenderer.JsonRenderedNode;
import static io.trino.sql.planner.planprinter.NodeRepresentation.TypedSymbol.typedSymbol;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestEventListenerBasic
        extends AbstractTestQueryFramework
{
    private static final JsonCodec<Map<String, JsonRenderedNode>> ANONYMIZED_PLAN_JSON_CODEC = mapJsonCodec(String.class, JsonRenderedNode.class);
    private static final String IGNORE_EVENT_MARKER = " -- ignore_generated_event";
    private static final String VARCHAR_TYPE = "varchar(15)";
    private static final String BIGINT_TYPE = BIGINT.getDisplayName();
    private static final Metrics TEST_METRICS = new Metrics(ImmutableMap.of("test_metrics", new LongCount(1)));

    private EventsAwaitingQueries queries;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setSystemProperty("task_concurrency", "1")
                .setCatalog("tpch")
                .setSchema("tiny")
                .setClientInfo("{\"clientVersion\":\"testVersion\"}")
                .build();

        EventsCollector generatedEvents = new EventsCollector();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.installPlugin(new TestingEventListenerPlugin(generatedEvents));
        queryRunner.installPlugin(new ResourceGroupManagerPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                        .withListTables((session, schemaName) -> {
                            return switch (schemaName) {
                                case "default" -> List.of("tests_table");
                                case "tiny" -> List.of("nation");
                                default -> List.of();
                            };
                        })
                        .withGetColumns(schemaTableName -> {
                            if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                                return TPCH_NATION_SCHEMA;
                            }
                            return ImmutableList.of(
                                    new ColumnMetadata("test_varchar", createVarcharType(15)),
                                    new ColumnMetadata("test_bigint", BIGINT));
                        })
                        .withGetTableHandle((session, schemaTableName) -> {
                            if (!schemaTableName.getTableName().startsWith("create")) {
                                return new MockConnectorTableHandle(schemaTableName);
                            }
                            return null;
                        })
                        .withApplyProjection((session, handle, projections, assignments) -> {
                            if (((MockConnectorTableHandle) handle).getTableName().getTableName().equals("tests_table")) {
                                throw new RuntimeException("Throw from apply projection");
                            }
                            return Optional.empty();
                        })
                        .withGetViews((connectorSession, prefix) -> {
                            ConnectorViewDefinition definition = new ConnectorViewDefinition(
                                    "SELECT nationkey AS test_column FROM tpch.tiny.nation",
                                    Optional.empty(),
                                    Optional.empty(),
                                    ImmutableList.of(new ConnectorViewDefinition.ViewColumn("test_column", BIGINT.getTypeId(), Optional.empty())),
                                    Optional.empty(),
                                    Optional.empty(),
                                    true);
                            SchemaTableName viewName = new SchemaTableName("default", "test_view");
                            return ImmutableMap.of(viewName, definition);
                        })
                        .withGetMaterializedViews((connectorSession, prefix) -> {
                            ConnectorMaterializedViewDefinition definition = new ConnectorMaterializedViewDefinition(
                                    "SELECT nationkey AS test_column FROM tpch.tiny.nation",
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty(),
                                    ImmutableList.of(new Column("test_column", BIGINT.getTypeId())),
                                    Optional.empty(),
                                    Optional.of("alice"),
                                    ImmutableMap.of());
                            SchemaTableName materializedViewName = new SchemaTableName("default", "test_materialized_view");
                            return ImmutableMap.of(materializedViewName, definition);
                        })
                        .withData(schemaTableName -> {
                            if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                                return TPCH_NATION_DATA;
                            }
                            return ImmutableList.of();
                        })
                        .withMetrics(schemaTableName -> {
                            if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                                return TEST_METRICS;
                            }
                            return EMPTY;
                        })
                        .withRowFilter(schemaTableName -> {
                            if (schemaTableName.getTableName().equals("test_table_with_row_filter")) {
                                return new ViewExpression("user", Optional.of("tpch"), Optional.of("tiny"), "EXISTS (SELECT 1 FROM nation WHERE name = test_varchar)");
                            }
                            return null;
                        })
                        .withColumnMask((schemaTableName, columnName) -> {
                            if (schemaTableName.getTableName().equals("test_table_with_column_mask") && columnName.equals("test_varchar")) {
                                return new ViewExpression("user", Optional.of("tpch"), Optional.of("tiny"), "(SELECT cast(max(orderkey) AS varchar(15)) FROM orders)");
                            }
                            return null;
                        })
                        .build();
                return ImmutableList.of(connectorFactory);
            }
        });
        queryRunner.createCatalog("mock", "mock", ImmutableMap.of());
        queryRunner.getCoordinator().getResourceGroupManager().get()
                .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_simple.json")));

        queries = new EventsAwaitingQueries(generatedEvents, queryRunner);

        return queryRunner;
    }

    private String getResourceFilePath(String fileName)
    {
        try {
            return new File(getResource(fileName).toURI()).getPath();
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private MaterializedResultWithEvents runQueryAndWaitForEvents(@Language("SQL") String sql)
            throws Exception
    {
        return queries.runQueryAndWaitForEvents(sql, getSession());
    }

    @Test
    public void testAnalysisFailure()
            throws Exception
    {
        assertFailedQuery("EXPLAIN (TYPE IO) SELECT sum(bogus) FROM lineitem", "line 1:30: Column 'bogus' cannot be resolved");
    }

    @Test
    public void testParseError()
            throws Exception
    {
        assertFailedQuery("You shall not parse!", "line 1:1: mismatched input 'You'. Expecting: 'ALTER', 'ANALYZE', 'CALL', 'COMMENT', 'COMMIT', 'CREATE', 'DEALLOCATE', 'DELETE', 'DENY', 'DESC', 'DESCRIBE', 'DROP', 'EXECUTE', 'EXPLAIN', 'GRANT', 'INSERT', 'MERGE', 'PREPARE', 'REFRESH', 'RESET', 'REVOKE', 'ROLLBACK', 'SET', 'SHOW', 'START', 'TRUNCATE', 'UPDATE', 'USE', <query>");
    }

    @Test
    public void testPlanningFailure()
            throws Exception
    {
        assertFailedQuery("SELECT lower(test_varchar) FROM mock.default.tests_table", "Throw from apply projection");
    }

    @Test
    public void testAbortedWhileWaitingForResources()
            throws Exception
    {
        Session mySession = Session.builder(getSession())
                .setSystemProperty("required_workers_count", "17")
                .setSystemProperty("required_workers_max_wait_time", "10ms")
                .build();
        assertFailedQuery(mySession, "SELECT * FROM tpch.sf1.nation", "Insufficient active worker nodes. Waited 10.00ms for at least 17 workers, but only 1 workers are active");
    }

    @Test(timeOut = 30_000)
    public void testKilledWhileWaitingForResources()
            throws Exception
    {
        String testQueryMarker = "test_query_id_" + randomUUID().toString().replace("-", "");
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            Session mySession = Session.builder(getSession())
                    .setSystemProperty("required_workers_count", "17")
                    .setSystemProperty("required_workers_max_wait_time", "5m")
                    .build();
            String sql = format("SELECT nationkey AS %s  FROM tpch.sf1.nation", testQueryMarker);

            executorService.submit(
                    // asynchronous call to cancel query which will be stared via `assertFailedQuery` below
                    () -> {
                        Optional<String> queryIdValue = findQueryId(testQueryMarker);
                        assertThat(queryIdValue).as("query id").isPresent();

                        getQueryRunner().execute(format("CALL system.runtime.kill_query('%s', 'because') %s", queryIdValue.get(), IGNORE_EVENT_MARKER));
                        return null;
                    });

            assertFailedQuery(mySession, sql, "Query killed. Message: because");
        }
        finally {
            shutdownAndAwaitTermination(executorService, Duration.ZERO);
        }
    }

    @Test
    public void testWithInvalidExecutionPolicy()
            throws Exception
    {
        Session mySession = Session.builder(getSession())
                .setSystemProperty("execution_policy", "invalid_as_hell")
                .build();
        assertFailedQuery(mySession, "SELECT 1", "No execution policy invalid_as_hell");
    }

    private Optional<String> findQueryId(String queryPattern)
            throws InterruptedException
    {
        Optional<String> queryIdValue = Optional.empty();
        while (queryIdValue.isEmpty()) {
            queryIdValue = computeActual("SELECT query_id FROM system.runtime.queries WHERE query LIKE '%" + queryPattern + "%' AND query NOT LIKE '%system.runtime.queries%'" + IGNORE_EVENT_MARKER)
                    .getOnlyColumn()
                    .map(String.class::cast)
                    .collect(toOptional());
            Thread.sleep(50);
        }
        return queryIdValue;
    }

    private void assertFailedQuery(@Language("SQL") String sql, String expectedFailure)
            throws Exception
    {
        assertFailedQuery(getSession(), sql, expectedFailure);
    }

    private void assertFailedQuery(Session session, @Language("SQL") String sql, String expectedFailure)
            throws Exception
    {
        QueryEvents queryEvents = queries.runQueryAndWaitForEvents(sql, session, Optional.of(expectedFailure)).getQueryEvents();

        QueryCompletedEvent queryCompletedEvent = queryEvents.getQueryCompletedEvent();
        assertEquals(queryCompletedEvent.getMetadata().getQuery(), sql);

        QueryFailureInfo failureInfo = queryCompletedEvent.getFailureInfo()
                .orElseThrow(() -> new AssertionError("Expected query event to be failed"));
        assertEquals(expectedFailure, failureInfo.getFailureMessage().orElse(null));
    }

    @Test
    public void testReferencedTablesAndRoutines()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("SELECT sum(linenumber) FROM lineitem").getQueryEvents();

        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();

        List<TableInfo> tables = event.getMetadata().getTables();
        assertEquals(tables.size(), 1);

        TableInfo table = tables.get(0);
        assertEquals(table.getCatalog(), "tpch");
        assertEquals(table.getSchema(), "tiny");
        assertEquals(table.getTable(), "lineitem");
        assertEquals(table.getAuthorization(), "user");
        assertTrue(table.getFilters().isEmpty());
        assertEquals(table.getColumns().size(), 1);

        ColumnInfo column = table.getColumns().get(0);
        assertEquals(column.getColumn(), "linenumber");
        assertTrue(column.getMask().isEmpty());

        List<RoutineInfo> routines = event.getMetadata().getRoutines();
        assertEquals(tables.size(), 1);

        RoutineInfo routine = routines.get(0);
        assertEquals(routine.getRoutine(), "sum");
        assertEquals(routine.getAuthorization(), "user");
    }

    @Test
    public void testReferencedTablesWithViews()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("SELECT test_column FROM mock.default.test_view").getQueryEvents();

        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();

        List<TableInfo> tables = event.getMetadata().getTables();
        assertThat(tables).hasSize(2);

        TableInfo table = tables.get(0);
        assertThat(table.getCatalog()).isEqualTo("tpch");
        assertThat(table.getSchema()).isEqualTo("tiny");
        assertThat(table.getTable()).isEqualTo("nation");
        assertThat(table.getAuthorization()).isEqualTo("user");
        assertThat(table.isDirectlyReferenced()).isFalse();
        assertThat(table.getFilters()).isEmpty();
        assertThat(table.getColumns()).hasSize(1);

        ColumnInfo column = table.getColumns().get(0);
        assertThat(column.getColumn()).isEqualTo("nationkey");
        assertThat(column.getMask()).isEmpty();

        table = tables.get(1);
        assertThat(table.getCatalog()).isEqualTo("mock");
        assertThat(table.getSchema()).isEqualTo("default");
        assertThat(table.getTable()).isEqualTo("test_view");
        assertThat(table.getAuthorization()).isEqualTo("user");
        assertThat(table.isDirectlyReferenced()).isTrue();
        assertThat(table.getFilters()).isEmpty();
        assertThat(table.getColumns()).hasSize(1);

        column = table.getColumns().get(0);
        assertThat(column.getColumn()).isEqualTo("test_column");
        assertThat(column.getMask()).isEmpty();
    }

    @Test
    public void testReferencedTablesWithMaterializedViews()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("SELECT test_column FROM mock.default.test_materialized_view").getQueryEvents();

        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();

        List<TableInfo> tables = event.getMetadata().getTables();
        assertThat(tables).hasSize(2);
        TableInfo table = tables.get(0);
        assertThat(table.getCatalog()).isEqualTo("tpch");
        assertThat(table.getSchema()).isEqualTo("tiny");
        assertThat(table.getTable()).isEqualTo("nation");
        assertThat(table.getAuthorization()).isEqualTo("alice");
        assertThat(table.isDirectlyReferenced()).isFalse();
        assertThat(table.getFilters()).isEmpty();
        assertThat(table.getColumns()).hasSize(1);

        ColumnInfo column = table.getColumns().get(0);
        assertThat(column.getColumn()).isEqualTo("nationkey");
        assertThat(column.getMask()).isEmpty();

        table = tables.get(1);
        assertThat(table.getCatalog()).isEqualTo("mock");
        assertThat(table.getSchema()).isEqualTo("default");
        assertThat(table.getTable()).isEqualTo("test_materialized_view");
        assertThat(table.getAuthorization()).isEqualTo("user");
        assertThat(table.isDirectlyReferenced()).isTrue();
        assertThat(table.getFilters()).isEmpty();
        assertThat(table.getColumns()).hasSize(1);

        column = table.getColumns().get(0);
        assertThat(column.getColumn()).isEqualTo("test_column");
        assertThat(column.getMask()).isEmpty();
    }

    @Test
    public void testReferencedTablesInCreateView()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("CREATE VIEW mock.default.create_another_test_view AS SELECT * FROM nation").getQueryEvents();

        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();

        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("mock");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("default");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("create_another_test_view");
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("nationkey", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "nationkey"))),
                        new OutputColumnMetadata("name", "varchar(25)", ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "name"))),
                        new OutputColumnMetadata("regionkey", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "regionkey"))),
                        new OutputColumnMetadata("comment", "varchar(152)", ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "comment"))));

        List<TableInfo> tables = event.getMetadata().getTables();
        assertThat(tables).hasSize(1);

        TableInfo table = tables.get(0);
        assertThat(table.getCatalog()).isEqualTo("tpch");
        assertThat(table.getSchema()).isEqualTo("tiny");
        assertThat(table.getTable()).isEqualTo("nation");
        assertThat(table.getAuthorization()).isEqualTo("user");
        assertThat(table.isDirectlyReferenced()).isTrue();
        assertThat(table.getFilters()).isEmpty();
        assertThat(table.getColumns()).hasSize(4);
    }

    @Test
    public void testReferencedTablesInCreateMaterializedView()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("CREATE MATERIALIZED VIEW mock.default.test_view AS SELECT * FROM nation").getQueryEvents();

        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();

        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("mock");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("default");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("test_view");
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("nationkey", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "nationkey"))),
                        new OutputColumnMetadata("name", "varchar(25)", ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "name"))),
                        new OutputColumnMetadata("regionkey", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "regionkey"))),
                        new OutputColumnMetadata("comment", "varchar(152)", ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "comment"))));

        List<TableInfo> tables = event.getMetadata().getTables();
        assertThat(tables).hasSize(1);

        TableInfo table = tables.get(0);
        assertThat(table.getCatalog()).isEqualTo("tpch");
        assertThat(table.getSchema()).isEqualTo("tiny");
        assertThat(table.getTable()).isEqualTo("nation");
        assertThat(table.getAuthorization()).isEqualTo("user");
        assertThat(table.isDirectlyReferenced()).isTrue();
        assertThat(table.getFilters()).isEmpty();
        assertThat(table.getColumns()).hasSize(4);
    }

    @Test
    public void testReferencedTablesWithRowFilter()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("SELECT 1 FROM mock.default.test_table_with_row_filter").getQueryEvents();

        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();

        List<TableInfo> tables = event.getMetadata().getTables();
        assertThat(tables).hasSize(2);

        TableInfo table = tables.get(0);
        assertThat(table.getCatalog()).isEqualTo("tpch");
        assertThat(table.getSchema()).isEqualTo("tiny");
        assertThat(table.getTable()).isEqualTo("nation");
        assertThat(table.getAuthorization()).isEqualTo("user");
        assertThat(table.isDirectlyReferenced()).isFalse();
        assertThat(table.getFilters()).isEmpty();
        assertThat(table.getColumns()).hasSize(1);

        ColumnInfo column = table.getColumns().get(0);
        assertThat(column.getColumn()).isEqualTo("name");
        assertThat(column.getMask()).isEmpty();

        table = tables.get(1);
        assertThat(table.getCatalog()).isEqualTo("mock");
        assertThat(table.getSchema()).isEqualTo("default");
        assertThat(table.getTable()).isEqualTo("test_table_with_row_filter");
        assertThat(table.getAuthorization()).isEqualTo("user");
        assertThat(table.isDirectlyReferenced()).isTrue();
        assertThat(table.getFilters()).hasSize(1);
        assertThat(table.getColumns()).hasSize(1);

        column = table.getColumns().get(0);
        assertThat(column.getColumn()).isEqualTo("test_varchar");
        assertThat(column.getMask()).isEmpty();
    }

    @Test
    public void testReferencedTablesWithColumnMask()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents(
                "CREATE TABLE mock.default.create_table_with_referring_mask AS SELECT * FROM mock.default.test_table_with_column_mask"
        ).getQueryEvents();

        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();

        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("mock");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("default");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("create_table_with_referring_mask");
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("mock", "default", "test_table_with_column_mask", "test_varchar"))),
                        new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("mock", "default", "test_table_with_column_mask", "test_bigint"))));

        List<TableInfo> tables = event.getMetadata().getTables();
        assertThat(tables).hasSize(2);

        TableInfo table = tables.get(0);
        assertThat(table.getCatalog()).isEqualTo("tpch");
        assertThat(table.getSchema()).isEqualTo("tiny");
        assertThat(table.getTable()).isEqualTo("orders");
        assertThat(table.getAuthorization()).isEqualTo("user");
        assertThat(table.isDirectlyReferenced()).isFalse();
        assertThat(table.getFilters()).isEmpty();
        assertThat(table.getColumns()).hasSize(1);

        ColumnInfo column = table.getColumns().get(0);
        assertThat(column.getColumn()).isEqualTo("orderkey");
        assertThat(column.getMask()).isEmpty();

        table = tables.get(1);
        assertThat(table.getCatalog()).isEqualTo("mock");
        assertThat(table.getSchema()).isEqualTo("default");
        assertThat(table.getTable()).isEqualTo("test_table_with_column_mask");
        assertThat(table.getAuthorization()).isEqualTo("user");
        assertThat(table.isDirectlyReferenced()).isTrue();
        assertThat(table.getFilters()).isEmpty();
        assertThat(table.getColumns()).hasSize(2);

        column = table.getColumns().get(0);
        assertThat(column.getColumn()).isEqualTo("test_varchar");
        assertThat(column.getMask()).isPresent();

        column = table.getColumns().get(1);
        assertThat(column.getColumn()).isEqualTo("test_bigint");
        assertThat(column.getMask()).isEmpty();
    }

    @Test
    public void testReferencedColumns()
            throws Exception
    {
        // assert that ColumnInfos for referenced columns are present when the table was not aliased
        QueryEvents queryEvents = runQueryAndWaitForEvents("SELECT name, nationkey FROM nation").getQueryEvents();
        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();
        TableInfo table = getOnlyElement(event.getMetadata().getTables());

        assertEquals(
                table.getColumns().stream()
                        .map(ColumnInfo::getColumn)
                        .collect(toImmutableSet()),
                ImmutableSet.of("name", "nationkey"));

        // assert that ColumnInfos for referenced columns are present when the table was aliased
        queryEvents = runQueryAndWaitForEvents("SELECT name, nationkey FROM nation n").getQueryEvents();
        event = queryEvents.getQueryCompletedEvent();
        table = getOnlyElement(event.getMetadata().getTables());

        assertEquals(
                table.getColumns().stream()
                        .map(ColumnInfo::getColumn)
                        .collect(toImmutableSet()),
                ImmutableSet.of("name", "nationkey"));

        // assert that ColumnInfos for referenced columns are present when the table was aliased and its columns were aliased
        queryEvents = runQueryAndWaitForEvents("SELECT a, b FROM nation n(a, b, c, d)").getQueryEvents();
        event = queryEvents.getQueryCompletedEvent();
        table = getOnlyElement(event.getMetadata().getTables());

        assertEquals(
                table.getColumns().stream()
                        .map(ColumnInfo::getColumn)
                        .collect(toImmutableSet()),
                ImmutableSet.of("name", "nationkey"));
    }

    @Test
    public void testPrepareAndExecute()
            throws Exception
    {
        String selectQuery = "SELECT count(*) FROM lineitem WHERE shipmode = ?";
        String prepareQuery = "PREPARE stmt FROM " + selectQuery;

        // QueryCreated: 1, QueryCompleted: 1, Splits: 0
        QueryEvents queryEvents = runQueryAndWaitForEvents(prepareQuery).getQueryEvents();

        QueryCreatedEvent queryCreatedEvent = queryEvents.getQueryCreatedEvent();
        assertEquals(queryCreatedEvent.getContext().getServerVersion(), "testversion");
        assertEquals(queryCreatedEvent.getContext().getServerAddress(), "127.0.0.1");
        assertEquals(queryCreatedEvent.getContext().getEnvironment(), "testing");
        assertEquals(queryCreatedEvent.getContext().getClientInfo().get(), "{\"clientVersion\":\"testVersion\"}");
        assertEquals(queryCreatedEvent.getMetadata().getQuery(), prepareQuery);
        assertFalse(queryCreatedEvent.getMetadata().getPreparedQuery().isPresent());

        QueryCompletedEvent queryCompletedEvent = queryEvents.getQueryCompletedEvent();
        assertTrue(queryCompletedEvent.getContext().getResourceGroupId().isPresent());
        assertEquals(queryCompletedEvent.getContext().getResourceGroupId().get(), createResourceGroupId("global", "user-user"));
        assertEquals(queryCompletedEvent.getIoMetadata().getOutput(), Optional.empty());
        assertEquals(queryCompletedEvent.getIoMetadata().getInputs().size(), 0);  // Prepare has no inputs
        assertEquals(queryCompletedEvent.getContext().getClientInfo().get(), "{\"clientVersion\":\"testVersion\"}");
        assertEquals(queryCreatedEvent.getMetadata().getQueryId(), queryCompletedEvent.getMetadata().getQueryId());
        assertFalse(queryCompletedEvent.getMetadata().getPreparedQuery().isPresent());
        assertEquals(queryCompletedEvent.getStatistics().getCompletedSplits(), 0); // Prepare has no splits

        // Add prepared statement to a new session to eliminate any impact on other tests in this suite.
        Session sessionWithPrepare = Session.builder(getSession()).addPreparedStatement("stmt", selectQuery).build();

        queryEvents = queries.runQueryAndWaitForEvents("EXECUTE stmt USING 'SHIP'", sessionWithPrepare).getQueryEvents();

        queryCreatedEvent = queryEvents.getQueryCreatedEvent();
        assertEquals(queryCreatedEvent.getContext().getServerVersion(), "testversion");
        assertEquals(queryCreatedEvent.getContext().getServerAddress(), "127.0.0.1");
        assertEquals(queryCreatedEvent.getContext().getEnvironment(), "testing");
        assertEquals(queryCreatedEvent.getContext().getClientInfo().get(), "{\"clientVersion\":\"testVersion\"}");
        assertEquals(queryCreatedEvent.getMetadata().getQuery(), "EXECUTE stmt USING 'SHIP'");
        assertTrue(queryCreatedEvent.getMetadata().getPreparedQuery().isPresent());
        assertEquals(queryCreatedEvent.getMetadata().getPreparedQuery().get(), selectQuery);

        queryCompletedEvent = queryEvents.getQueryCompletedEvent();
        assertTrue(queryCompletedEvent.getContext().getResourceGroupId().isPresent());
        assertEquals(queryCompletedEvent.getContext().getResourceGroupId().get(), createResourceGroupId("global", "user-user"));
        assertEquals(queryCompletedEvent.getIoMetadata().getOutput(), Optional.empty());
        assertEquals(queryCompletedEvent.getIoMetadata().getInputs().size(), 1);
        assertEquals(queryCompletedEvent.getContext().getClientInfo().get(), "{\"clientVersion\":\"testVersion\"}");
        assertEquals(getOnlyElement(queryCompletedEvent.getIoMetadata().getInputs()).getCatalogName(), "tpch");
        assertEquals(queryCreatedEvent.getMetadata().getQueryId(), queryCompletedEvent.getMetadata().getQueryId());
        assertTrue(queryCompletedEvent.getMetadata().getPreparedQuery().isPresent());
        assertEquals(queryCompletedEvent.getMetadata().getPreparedQuery().get(), selectQuery);
    }

    @Test
    public void testOutputStats()
            throws Exception
    {
        MaterializedResultWithEvents result = runQueryAndWaitForEvents("SELECT 1 FROM lineitem");
        QueryCreatedEvent queryCreatedEvent = result.getQueryEvents().getQueryCreatedEvent();
        QueryCompletedEvent queryCompletedEvent = result.getQueryEvents().getQueryCompletedEvent();
        QueryStats queryStats = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(new QueryId(queryCreatedEvent.getMetadata().getQueryId())).getQueryStats();

        assertTrue(queryStats.getOutputDataSize().toBytes() > 0L);
        assertTrue(queryCompletedEvent.getStatistics().getOutputBytes() > 0L);
        assertEquals(result.getMaterializedResult().getRowCount(), queryStats.getOutputPositions());
        assertEquals(result.getMaterializedResult().getRowCount(), queryCompletedEvent.getStatistics().getOutputRows());

        result = runQueryAndWaitForEvents("SELECT COUNT(1) FROM lineitem");
        queryCreatedEvent = result.getQueryEvents().getQueryCreatedEvent();
        queryCompletedEvent = result.getQueryEvents().getQueryCompletedEvent();
        queryStats = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(new QueryId(queryCreatedEvent.getMetadata().getQueryId())).getQueryStats();

        assertTrue(queryStats.getOutputDataSize().toBytes() > 0L);
        assertTrue(queryCompletedEvent.getStatistics().getOutputBytes() > 0L);
        assertEquals(1L, queryStats.getOutputPositions());
        assertEquals(1L, queryCompletedEvent.getStatistics().getOutputRows());

        // Ensure the proper conversion in QueryMonitor#createQueryStatistics
        QueryStatistics statistics = queryCompletedEvent.getStatistics();
        assertEquals(statistics.getCpuTime().toMillis(), queryStats.getTotalCpuTime().toMillis());
        assertEquals(statistics.getWallTime().toMillis(), queryStats.getElapsedTime().toMillis());
        assertEquals(statistics.getQueuedTime().toMillis(), queryStats.getQueuedTime().toMillis());
        assertEquals(statistics.getScheduledTime().get().toMillis(), queryStats.getTotalScheduledTime().toMillis());
        assertEquals(statistics.getResourceWaitingTime().get().toMillis(), queryStats.getResourceWaitingTime().toMillis());
        assertEquals(statistics.getAnalysisTime().get().toMillis(), queryStats.getAnalysisTime().toMillis());
        assertEquals(statistics.getPlanningTime().get().toMillis(), queryStats.getPlanningTime().toMillis());
        assertEquals(statistics.getExecutionTime().get().toMillis(), queryStats.getExecutionTime().toMillis());
        assertEquals(statistics.getPeakUserMemoryBytes(), queryStats.getPeakUserMemoryReservation().toBytes());
        assertEquals(statistics.getPeakTaskUserMemory(), queryStats.getPeakTaskUserMemory().toBytes());
        assertEquals(statistics.getPeakTaskTotalMemory(), queryStats.getPeakTaskTotalMemory().toBytes());
        assertEquals(statistics.getPhysicalInputBytes(), queryStats.getPhysicalInputDataSize().toBytes());
        assertEquals(statistics.getPhysicalInputRows(), queryStats.getPhysicalInputPositions());
        assertEquals(statistics.getInternalNetworkBytes(), queryStats.getInternalNetworkInputDataSize().toBytes());
        assertEquals(statistics.getInternalNetworkRows(), queryStats.getInternalNetworkInputPositions());
        assertEquals(statistics.getTotalBytes(), queryStats.getRawInputDataSize().toBytes());
        assertEquals(statistics.getTotalRows(), queryStats.getRawInputPositions());
        assertEquals(statistics.getOutputBytes(), queryStats.getOutputDataSize().toBytes());
        assertEquals(statistics.getOutputRows(), queryStats.getOutputPositions());
        assertEquals(statistics.getWrittenBytes(), queryStats.getLogicalWrittenDataSize().toBytes());
        assertEquals(statistics.getWrittenRows(), queryStats.getWrittenPositions());
        assertEquals(statistics.getSpilledBytes(), queryStats.getSpilledDataSize().toBytes());
        assertEquals(statistics.getCumulativeMemory(), queryStats.getCumulativeUserMemory());
        assertEquals(statistics.getStageGcStatistics(), queryStats.getStageGcStatistics());
        assertEquals(statistics.getCompletedSplits(), queryStats.getCompletedDrivers());
    }

    @Test
    public void testOutputColumnsForSelect()
            throws Exception
    {
        assertLineage(
                "SELECT clerk AS test_varchar, orderkey AS test_bigint FROM orders",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsForSelectWithConstantExpression()
            throws Exception
    {
        assertLineage(
                "SELECT '4-NOT SPECIFIED' AS test_varchar, orderkey AS test_bigint FROM orders",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of()),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsForCreateTableAsSelectAll()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("CREATE TABLE mock.default.create_new_table AS SELECT * FROM nation").getQueryEvents();
        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("nationkey", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "nationkey"))),
                        new OutputColumnMetadata("name", "varchar(25)", ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "name"))),
                        new OutputColumnMetadata("regionkey", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "regionkey"))),
                        new OutputColumnMetadata("comment", "varchar(152)", ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "comment"))));
    }

    @Test
    public void testOutputColumnsForCreateTableAsSelectAllFromView()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("CREATE TABLE mock.default.create_new_table AS SELECT * FROM mock.default.test_view").getQueryEvents();
        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_column", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("mock", "default", "test_view", "test_column"))));
    }

    @Test
    public void testOutputColumnsForCreateTableAsSelectAllFromMaterializedView()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("CREATE TABLE mock.default.create_new_table AS SELECT * FROM mock.default.test_materialized_view").getQueryEvents();
        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_column", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("mock", "default", "test_materialized_view", "test_column"))));
    }

    @Test
    public void testOutputColumnsForCreateTableAsSelectWithAliasedColumn()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("CREATE TABLE mock.default.create_new_table(aliased_bigint, aliased_varchar) AS SELECT nationkey AS keynation, concat(name, comment) FROM nation").getQueryEvents();
        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("aliased_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "nationkey"))),
                        new OutputColumnMetadata("aliased_varchar", "varchar", ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "name"), new ColumnDetail("tpch", "tiny", "nation", "comment"))));
    }

    @Test
    public void testOutputColumnsWithClause()
            throws Exception
    {
        assertLineage(
                "WITH w AS (SELECT * FROM orders) SELECT lower(clerk) AS test_varchar, orderkey AS test_bigint FROM w",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsColumnAliasInWithClause()
            throws Exception
    {
        assertLineage(
                "WITH w(aliased_clerk, aliased_orderkey) AS (SELECT clerk, orderkey FROM orders) SELECT lower(aliased_clerk) AS test_varchar, aliased_orderkey AS test_bigint FROM w",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsWithAliasedRelation()
            throws Exception
    {
        assertLineage(
                "SELECT lower(clerk) AS test_varchar, orderkey AS test_bigint FROM (SELECT * FROM orders) w",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsWithColumnAliasInAliasedRelation()
            throws Exception
    {
        assertLineage(
                "SELECT lower(aliased_clerk) AS test_varchar, aliased_orderkey AS test_bigint FROM (SELECT clerk, orderkey FROM orders) w(aliased_clerk, aliased_orderkey)",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsWithWhere()
            throws Exception
    {
        assertLineage(
                "SELECT orderpriority AS test_varchar, orderkey AS test_bigint FROM orders WHERE orderdate > DATE '1995-10-03'",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderpriority"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsWithIfExpression()
            throws Exception
    {
        assertLineage(
                "SELECT IF (orderstatus = 'O', orderpriority, clerk) AS test_varchar, orderkey AS test_bigint FROM orders",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata(
                        "test_varchar",
                        VARCHAR_TYPE,
                        ImmutableSet.of(
                                new ColumnDetail("tpch", "tiny", "orders", "orderstatus"),
                                new ColumnDetail("tpch", "tiny", "orders", "orderpriority"),
                                new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsWithCaseExpression()
            throws Exception
    {
        assertLineage(
                "SELECT CASE WHEN custkey = 100 THEN clerk WHEN custkey = 1000 then orderpriority ELSE orderstatus END AS test_varchar, orderkey AS test_bigint FROM orders",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata(
                        "test_varchar",
                        VARCHAR_TYPE,
                        ImmutableSet.of(
                                new ColumnDetail("tpch", "tiny", "orders", "orderstatus"),
                                new ColumnDetail("tpch", "tiny", "orders", "orderpriority"),
                                new ColumnDetail("tpch", "tiny", "orders", "clerk"),
                                new ColumnDetail("tpch", "tiny", "orders", "custkey"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsWithLimit()
            throws Exception
    {
        assertLineage(
                "SELECT orderpriority AS test_varchar, orderkey AS test_bigint FROM orders LIMIT 100",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderpriority"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsWithOrderBy()
            throws Exception
    {
        assertLineage(
                "SELECT clerk AS test_varchar, orderkey AS test_bigint FROM orders ORDER BY orderdate",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsWithAggregation()
            throws Exception
    {
        assertLineage(
                "SELECT max(orderpriority) AS test_varchar, min(custkey) AS test_bigint FROM orders GROUP BY orderstatus",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderpriority"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "custkey"))));
    }

    @Test
    public void testOutputColumnsWithAggregationWithFilter()
            throws Exception
    {
        assertLineage(
                "SELECT max(orderpriority) FILTER(WHERE orderdate > DATE '2000-01-01') AS test_varchar, max(custkey) AS test_bigint FROM orders GROUP BY orderstatus",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata(
                        "test_varchar",
                        VARCHAR_TYPE,
                        ImmutableSet.of(
                                new ColumnDetail("tpch", "tiny", "orders", "orderpriority"),
                                new ColumnDetail("tpch", "tiny", "orders", "orderdate"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "custkey"))));
    }

    @Test
    public void testOutputColumnsWithAggregationAndHaving()
            throws Exception
    {
        assertLineage(
                "SELECT min(orderpriority) AS test_varchar, max(custkey) AS test_bigint FROM orders GROUP BY orderstatus HAVING min(orderdate) > DATE '2000-01-01'",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderpriority"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "custkey"))));
    }

    @Test
    public void testOutputColumnsWithCountAll()
            throws Exception
    {
        assertLineage(
                "SELECT clerk AS test_varchar, count(*) AS test_bigint FROM orders GROUP BY clerk",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of()));
    }

    @Test
    public void testOutputColumnsWithWindowFunction()
            throws Exception
    {
        assertLineage(
                "SELECT clerk AS test_varchar, min(orderkey) OVER (PARTITION BY custkey ORDER BY orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS test_bigint FROM orders",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata(
                        "test_bigint",
                        BIGINT_TYPE,
                        ImmutableSet.of(
                                new ColumnDetail("tpch", "tiny", "orders", "orderkey"),
                                new ColumnDetail("tpch", "tiny", "orders", "custkey"),
                                new ColumnDetail("tpch", "tiny", "orders", "orderdate"))));
    }

    @Test
    public void testOutputColumnsWithPartialWindowClause()
            throws Exception
    {
        assertLineage(
                "SELECT clerk AS test_varchar, max(orderkey) OVER (w ORDER BY orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS test_bigint FROM orders WINDOW w AS (PARTITION BY custkey)",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata(
                        "test_bigint",
                        BIGINT_TYPE,
                        ImmutableSet.of(
                                new ColumnDetail("tpch", "tiny", "orders", "orderkey"),
                                new ColumnDetail("tpch", "tiny", "orders", "orderdate"))));
    }

    @Test
    public void testOutputColumnsWithWindowClause()
            throws Exception
    {
        assertLineage(
                "SELECT clerk AS test_varchar, min(orderkey) OVER w AS test_bigint FROM orders WINDOW w AS (PARTITION BY custkey ORDER BY orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                ImmutableSet.of("tpch.tiny.orders"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsWithUnCorrelatedQueries()
            throws Exception
    {
        assertLineage(
                "SELECT clerk AS test_varchar, (SELECT nationkey FROM nation LIMIT 1) AS test_bigint FROM orders",
                ImmutableSet.of("tpch.tiny.orders", "tpch.tiny.nation"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "nationkey"))));
    }

    @Test
    public void testOutputColumnsWithCorrelatedQueries()
            throws Exception
    {
        assertLineage(
                "SELECT orderpriority AS test_varchar, (SELECT min(nationkey) FROM customer WHERE customer.custkey = orders.custkey) AS test_bigint FROM orders",
                ImmutableSet.of("tpch.tiny.orders", "tpch.tiny.customer"),
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderpriority"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "customer", "nationkey"))));
    }

    @Test
    public void testOutputColumnsForInsertingSingleColumn()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("INSERT INTO mock.default.table_for_output(test_bigint) SELECT nationkey + 1 AS test_bigint FROM nation").getQueryEvents();
        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "nationkey"))));
    }

    @Test
    public void testOutputColumnsForInsertingAliasedColumn()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("INSERT INTO mock.default.table_for_output(test_varchar, test_bigint) SELECT name AS aliased_name, nationkey AS aliased_varchar FROM nation").getQueryEvents();
        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "name"))),
                        new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "nationkey"))));
    }

    @Test
    public void testOutputColumnsForUpdatingAllColumns()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("UPDATE mock.default.table_for_output SET test_varchar = 'reset', test_bigint = 1").getQueryEvents();
        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of()),
                        new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of()));
    }

    @Test
    public void testOutputColumnsForUpdatingSingleColumn()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("UPDATE mock.default.table_for_output SET test_varchar = 're-reset' WHERE test_bigint = 1").getQueryEvents();
        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of()));
    }

    @Test
    public void testCreateTable()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("CREATE TABLE mock.default.create_simple_table (test_column BIGINT)").getQueryEvents();
        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();
        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("mock");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("default");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("create_simple_table");
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(new OutputColumnMetadata("test_column", BIGINT_TYPE, ImmutableSet.of()));
    }

    @Test
    public void testCreateTableLike()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("CREATE TABLE mock.default.create_simple_table (test_column BIGINT, LIKE mock.default.test_table)").getQueryEvents();
        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();
        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("mock");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("default");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("create_simple_table");
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_column", BIGINT_TYPE, ImmutableSet.of()),
                        new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of()),
                        new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of()));
    }

    @Test
    public void testConnectorMetrics()
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents("SELECT * FROM mock.tiny.nation").getQueryEvents();
        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();
        List<Metrics> connectorMetrics = event.getIoMetadata().getInputs().stream()
                .map(QueryInputMetadata::getConnectorMetrics)
                .collect(toImmutableList());
        assertThat(connectorMetrics).containsExactly(TEST_METRICS);
    }

    @Test(dataProvider = "setOperator")
    public void testOutputColumnsForSetOperations(String setOperator)
            throws Exception
    {
        assertLineage(
                format("SELECT orderpriority AS test_varchar, orderkey AS test_bigint FROM orders %s SELECT clerk, custkey FROM sf1.orders", setOperator),
                ImmutableSet.of("tpch.tiny.orders", "tpch.sf1.orders"),
                new OutputColumnMetadata(
                        "test_varchar",
                        VARCHAR_TYPE,
                        ImmutableSet.of(
                                new ColumnDetail("tpch", "tiny", "orders", "orderpriority"),
                                new ColumnDetail("tpch", "sf1", "orders", "clerk"))),
                new OutputColumnMetadata(
                        "test_bigint",
                        BIGINT_TYPE,
                        ImmutableSet.of(
                                new ColumnDetail("tpch", "tiny", "orders", "orderkey"),
                                new ColumnDetail("tpch", "sf1", "orders", "custkey"))));
    }

    @DataProvider
    public Object[][] setOperator()
    {
        return new Object[][]{
                {"UNION"},
                {"UNION ALL"},
                {"INTERSECT"},
                {"INTERSECT ALL"},
                {"EXCEPT"},
                {"EXCEPT ALL"}};
    }

    @Test
    public void testAnonymizedJsonPlan()
            throws Exception
    {
        QueryEvents queryEvents = queries.runQueryAndWaitForEvents("SELECT quantity FROM lineitem LIMIT 10", getSession(), true).getQueryEvents();
        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();
        Map<String, JsonRenderedNode> anonymizedPlan = ImmutableMap.of(
                "0", new JsonRenderedNode(
                        "6",
                        "Output",
                        ImmutableMap.of("columnNames", "[column_1]"),
                        ImmutableList.of(typedSymbol("symbol_1", "double")),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(new JsonRenderedNode(
                                "98",
                                "Limit",
                                ImmutableMap.of("count", "10", "withTies", "", "inputPreSortedBy", "[]"),
                                ImmutableList.of(typedSymbol("symbol_1", "double")),
                                ImmutableList.of(),
                                ImmutableList.of(),
                                ImmutableList.of(new JsonRenderedNode(
                                        "171",
                                        "LocalExchange",
                                        ImmutableMap.of(
                                                "partitioning", "[connectorHandleType = SystemPartitioningHandle, partitioning = SINGLE, function = SINGLE]",
                                                "isReplicateNullsAndAny", "",
                                                "hashColumn", "[]",
                                                "arguments", "[]"),
                                        ImmutableList.of(typedSymbol("symbol_1", "double")),
                                        ImmutableList.of(),
                                        ImmutableList.of(),
                                        ImmutableList.of(new JsonRenderedNode(
                                                "138",
                                                "RemoteSource",
                                                ImmutableMap.of("sourceFragmentIds", "[1]"),
                                                ImmutableList.of(typedSymbol("symbol_1", "double")),
                                                ImmutableList.of(),
                                                ImmutableList.of(),
                                                ImmutableList.of()))))))),
                "1", new JsonRenderedNode(
                        "137",
                        "LimitPartial",
                        ImmutableMap.of(
                                "count", "10",
                                "withTies", "",
                                "inputPreSortedBy", "[]"),
                        ImmutableList.of(typedSymbol("symbol_1", "double")),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(new JsonRenderedNode(
                                "0",
                                "TableScan",
                                ImmutableMap.of(
                                        "table", "[table = catalog_1.schema_1.table_1, connector = tpch]"),
                                ImmutableList.of(typedSymbol("symbol_1", "double")),
                                ImmutableList.of("symbol_1 := column_2"),
                                ImmutableList.of(),
                                ImmutableList.of()))));
        assertThat(event.getMetadata().getJsonPlan())
                .isEqualTo(Optional.of(ANONYMIZED_PLAN_JSON_CODEC.toJson(anonymizedPlan)));
    }

    private void assertLineage(String baseQuery, Set<String> inputTables, OutputColumnMetadata... outputColumnMetadata)
            throws Exception
    {
        assertLineageInternal("CREATE TABLE mock.default.create_new_table AS " + baseQuery, inputTables, outputColumnMetadata);
        assertLineageInternal("CREATE VIEW mock.default.create_new_view AS " + baseQuery, inputTables, outputColumnMetadata);
        assertLineageInternal("CREATE VIEW mock.default.create_new_materialized_view AS " + baseQuery, inputTables, outputColumnMetadata);
        assertLineageInternal("INSERT INTO mock.default.table_for_output(test_varchar, test_bigint) " + baseQuery, inputTables, outputColumnMetadata);
        assertLineageInternal(format("DELETE FROM mock.default.table_for_output WHERE EXISTS (%s) ", baseQuery), inputTables);
    }

    private void assertLineageInternal(String sql, Set<String> inputTables, OutputColumnMetadata... outputColumnMetadata)
            throws Exception
    {
        QueryEvents queryEvents = runQueryAndWaitForEvents(sql).getQueryEvents();
        QueryCompletedEvent event = queryEvents.getQueryCompletedEvent();
        assertThat(event.getMetadata().getTables())
                .map(TestEventListenerBasic::getQualifiedName)
                .containsExactlyInAnyOrderElementsOf(inputTables);
        if (outputColumnMetadata.length != 0) {
            assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                    .containsExactly(outputColumnMetadata);
        }
    }

    private static String getQualifiedName(TableInfo tableInfo)
    {
        return tableInfo.getCatalog() + '.' + tableInfo.getSchema() + '.' + tableInfo.getTable();
    }
}
