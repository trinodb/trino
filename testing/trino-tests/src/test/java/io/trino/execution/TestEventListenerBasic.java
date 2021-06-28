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
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.execution.EventsCollector.EventFilters;
import io.trino.execution.TestEventListenerPlugin.TestingEventListenerPlugin;
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
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.RoutineInfo;
import io.trino.spi.eventlistener.TableInfo;
import io.trino.spi.security.ViewExpression;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static io.trino.execution.TestQueues.createResourceGroupId;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
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
    private static final String IGNORE_EVENT_MARKER = " -- ignore_generated_event";
    private static final String VARCHAR_TYPE = "varchar(15)";
    private static final String BIGINT_TYPE = BIGINT.getDisplayName();

    private final EventsCollector generatedEvents = new EventsCollector(buildEventFilters());
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
                        .withListTables((session, s) -> ImmutableList.of(new SchemaTableName("default", "tests_table")))
                        .withGetColumns(schemaTableName -> ImmutableList.of(
                                new ColumnMetadata("test_varchar", createVarcharType(15)),
                                new ColumnMetadata("test_bigint", BIGINT)))
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
                                    ImmutableList.of(new ConnectorViewDefinition.ViewColumn("test_column", BIGINT.getTypeId())),
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
                                    "alice",
                                    ImmutableMap.of());
                            SchemaTableName materializedViewName = new SchemaTableName("default", "test_materialized_view");
                            return ImmutableMap.of(materializedViewName, definition);
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

        queries = new EventsAwaitingQueries(generatedEvents, queryRunner, Duration.ofSeconds(1));

        return queryRunner;
    }

    private static EventFilters buildEventFilters()
    {
        return EventFilters.builder()
                .setQueryCreatedFilter(event -> !event.getMetadata().getQuery().contains(IGNORE_EVENT_MARKER))
                .setQueryCompletedFilter(event -> !event.getMetadata().getQuery().contains(IGNORE_EVENT_MARKER))
                .setSplitCompletedFilter(event -> false)
                .build();
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

    private MaterializedResult runQueryAndWaitForEvents(@Language("SQL") String sql, int numEventsExpected)
            throws Exception
    {
        return queries.runQueryAndWaitForEvents(sql, numEventsExpected, getSession());
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
        assertFailedQuery("You shall not parse!", "line 1:1: mismatched input 'You'. Expecting: 'ALTER', 'ANALYZE', 'CALL', 'COMMENT', 'COMMIT', 'CREATE', 'DEALLOCATE', 'DELETE', 'DESC', 'DESCRIBE', 'DROP', 'EXECUTE', 'EXPLAIN', 'GRANT', 'INSERT', 'MERGE', 'PREPARE', 'REFRESH', 'RESET', 'REVOKE', 'ROLLBACK', 'SET', 'SHOW', 'START', 'UPDATE', 'USE', <query>");
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
        queries.runQueryAndWaitForEvents(sql, 2, session, Optional.of(expectedFailure));

        QueryCompletedEvent queryCompletedEvent = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        assertEquals(queryCompletedEvent.getMetadata().getQuery(), sql);

        QueryFailureInfo failureInfo = queryCompletedEvent.getFailureInfo()
                .orElseThrow(() -> new AssertionError("Expected query event to be failed"));
        assertEquals(expectedFailure, failureInfo.getFailureMessage().orElse(null));
    }

    @Test
    public void testReferencedTablesAndRoutines()
            throws Exception
    {
        runQueryAndWaitForEvents("SELECT sum(linenumber) FROM lineitem", 2);

        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

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
        assertTrue(column.getMasks().isEmpty());

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
        runQueryAndWaitForEvents("SELECT test_column FROM mock.default.test_view", 2);

        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

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
        assertThat(column.getMasks()).isEmpty();

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
        assertThat(column.getMasks()).isEmpty();
    }

    @Test
    public void testReferencedTablesWithMaterializedViews()
            throws Exception
    {
        runQueryAndWaitForEvents("SELECT test_column FROM mock.default.test_materialized_view", 2);

        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

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
        assertThat(column.getMasks()).isEmpty();

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
        assertThat(column.getMasks()).isEmpty();
    }

    @Test
    public void testReferencedTablesInCreateView()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE VIEW mock.default.test_view AS SELECT * FROM nation", 2);

        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

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
    public void testReferencedTablesInCreateMaterializedView()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE MATERIALIZED VIEW mock.default.test_view AS SELECT * FROM nation", 2);

        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

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
        runQueryAndWaitForEvents("SELECT 1 FROM mock.default.test_table_with_row_filter", 2);

        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

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
        assertThat(column.getMasks()).isEmpty();

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
        assertThat(column.getMasks()).isEmpty();
    }

    @Test
    public void testReferencedTablesWithColumnMask()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE mock.default.create_table_with_referring_mask AS SELECT * FROM mock.default.test_table_with_column_mask", 2);

        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

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
        assertThat(column.getMasks()).isEmpty();

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
        assertThat(column.getMasks()).hasSize(1);

        column = table.getColumns().get(1);
        assertThat(column.getColumn()).isEqualTo("test_bigint");
        assertThat(column.getMasks()).isEmpty();
    }

    @Test
    public void testReferencedColumns()
            throws Exception
    {
        // assert that ColumnInfos for referenced columns are present when the table was not aliased
        runQueryAndWaitForEvents("SELECT name, nationkey FROM nation", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        TableInfo table = getOnlyElement(event.getMetadata().getTables());

        assertEquals(
                table.getColumns().stream()
                        .map(ColumnInfo::getColumn)
                        .collect(toImmutableSet()),
                ImmutableSet.of("name", "nationkey"));

        // assert that ColumnInfos for referenced columns are present when the table was aliased
        runQueryAndWaitForEvents("SELECT name, nationkey FROM nation n", 2);
        event = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        table = getOnlyElement(event.getMetadata().getTables());

        assertEquals(
                table.getColumns().stream()
                        .map(ColumnInfo::getColumn)
                        .collect(toImmutableSet()),
                ImmutableSet.of("name", "nationkey"));

        // assert that ColumnInfos for referenced columns are present when the table was aliased and its columns were aliased
        runQueryAndWaitForEvents("SELECT a, b FROM nation n(a, b, c, d)", 2);
        event = getOnlyElement(generatedEvents.getQueryCompletedEvents());
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
        runQueryAndWaitForEvents(prepareQuery, 2);

        QueryCreatedEvent queryCreatedEvent = getOnlyElement(generatedEvents.getQueryCreatedEvents());
        assertEquals(queryCreatedEvent.getContext().getServerVersion(), "testversion");
        assertEquals(queryCreatedEvent.getContext().getServerAddress(), "127.0.0.1");
        assertEquals(queryCreatedEvent.getContext().getEnvironment(), "testing");
        assertEquals(queryCreatedEvent.getContext().getClientInfo().get(), "{\"clientVersion\":\"testVersion\"}");
        assertEquals(queryCreatedEvent.getMetadata().getQuery(), prepareQuery);
        assertFalse(queryCreatedEvent.getMetadata().getPreparedQuery().isPresent());

        QueryCompletedEvent queryCompletedEvent = getOnlyElement(generatedEvents.getQueryCompletedEvents());
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

        queries.runQueryAndWaitForEvents("EXECUTE stmt USING 'SHIP'", 2, sessionWithPrepare);

        queryCreatedEvent = getOnlyElement(generatedEvents.getQueryCreatedEvents());
        assertEquals(queryCreatedEvent.getContext().getServerVersion(), "testversion");
        assertEquals(queryCreatedEvent.getContext().getServerAddress(), "127.0.0.1");
        assertEquals(queryCreatedEvent.getContext().getEnvironment(), "testing");
        assertEquals(queryCreatedEvent.getContext().getClientInfo().get(), "{\"clientVersion\":\"testVersion\"}");
        assertEquals(queryCreatedEvent.getMetadata().getQuery(), "EXECUTE stmt USING 'SHIP'");
        assertTrue(queryCreatedEvent.getMetadata().getPreparedQuery().isPresent());
        assertEquals(queryCreatedEvent.getMetadata().getPreparedQuery().get(), selectQuery);

        queryCompletedEvent = getOnlyElement(generatedEvents.getQueryCompletedEvents());
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
        MaterializedResult result = runQueryAndWaitForEvents("SELECT 1 FROM lineitem", 2);
        QueryCreatedEvent queryCreatedEvent = getOnlyElement(generatedEvents.getQueryCreatedEvents());
        QueryCompletedEvent queryCompletedEvent = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        QueryStats queryStats = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(new QueryId(queryCreatedEvent.getMetadata().getQueryId())).getQueryStats();

        assertTrue(queryStats.getOutputDataSize().toBytes() > 0L);
        assertTrue(queryCompletedEvent.getStatistics().getOutputBytes() > 0L);
        assertEquals(result.getRowCount(), queryStats.getOutputPositions());
        assertEquals(result.getRowCount(), queryCompletedEvent.getStatistics().getOutputRows());

        runQueryAndWaitForEvents("SELECT COUNT(1) FROM lineitem", 2);
        queryCreatedEvent = getOnlyElement(generatedEvents.getQueryCreatedEvents());
        queryCompletedEvent = getOnlyElement(generatedEvents.getQueryCompletedEvents());
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
        assertEquals(statistics.getPeakTotalNonRevocableMemoryBytes(), queryStats.getPeakNonRevocableMemoryReservation().toBytes());
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
        assertEquals(statistics.getCumulativeMemory(), queryStats.getCumulativeUserMemory());
        assertEquals(statistics.getStageGcStatistics(), queryStats.getStageGcStatistics());
        assertEquals(statistics.getCompletedSplits(), queryStats.getCompletedDrivers());
    }

    @Test
    public void testOutputColumnsForSelect()
            throws Exception
    {
        assertColumnLineage(
                "SELECT clerk AS test_varchar, orderkey AS test_bigint FROM orders",
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsForSelectWithConstantExpression()
            throws Exception
    {
        assertColumnLineage(
                "SELECT '4-NOT SPECIFIED' AS test_varchar, orderkey AS test_bigint FROM orders",
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of()),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsForCreateTableAsSelectAll()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE mock.default.create_new_table AS SELECT * FROM nation", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());
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
        runQueryAndWaitForEvents("CREATE TABLE mock.default.create_new_table AS SELECT * FROM mock.default.test_view", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_column", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("mock", "default", "test_view", "test_column"))));
    }

    @Test
    public void testOutputColumnsForCreateTableAsSelectAllFromMaterializedView()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE mock.default.create_new_table AS SELECT * FROM mock.default.test_materialized_view", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_column", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("mock", "default", "test_materialized_view", "test_column"))));
    }

    @Test
    public void testOutputColumnsForCreateTableAsSelectWithAliasedColumn()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE mock.default.create_new_table(aliased_bigint, aliased_varchar) AS SELECT nationkey AS keynation, concat(name, comment) FROM nation", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("aliased_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "nationkey"))),
                        new OutputColumnMetadata("aliased_varchar", "varchar", ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "name"), new ColumnDetail("tpch", "tiny", "nation", "comment"))));
    }

    @Test
    public void testOutputColumnsWithClause()
            throws Exception
    {
        assertColumnLineage(
                "WITH w AS (SELECT * FROM orders) SELECT clerk AS test_varchar, orderkey AS test_bigint FROM w",
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsWithWhere()
            throws Exception
    {
        assertColumnLineage(
                "SELECT orderpriority AS test_varchar, orderkey AS test_bigint FROM orders WHERE orderdate > DATE '1995-10-03'",
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderpriority"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsWithIfExpression()
            throws Exception
    {
        assertColumnLineage(
                "SELECT IF (orderstatus = 'O', orderpriority, clerk) AS test_varchar, orderkey AS test_bigint FROM orders",
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
        assertColumnLineage(
                "SELECT CASE WHEN custkey = 100 THEN clerk WHEN custkey = 1000 then orderpriority ELSE orderstatus END AS test_varchar, orderkey AS test_bigint FROM orders",
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
        assertColumnLineage(
                "SELECT orderpriority AS test_varchar, orderkey AS test_bigint FROM orders LIMIT 100",
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderpriority"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsWithOrderBy()
            throws Exception
    {
        assertColumnLineage(
                "SELECT clerk AS test_varchar, orderkey AS test_bigint FROM orders ORDER BY orderdate",
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsWithAggregation()
            throws Exception
    {
        assertColumnLineage(
                "SELECT max(orderpriority) AS test_varchar, min(custkey) AS test_bigint FROM orders GROUP BY orderstatus",
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderpriority"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "custkey"))));
    }

    @Test
    public void testOutputColumnsWithAggregationWithFilter()
            throws Exception
    {
        assertColumnLineage(
                "SELECT max(orderpriority) FILTER(WHERE orderdate > DATE '2000-01-01') AS test_varchar, max(custkey) AS test_bigint FROM orders GROUP BY orderstatus",
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
        assertColumnLineage(
                "SELECT min(orderpriority) AS test_varchar, max(custkey) AS test_bigint FROM orders GROUP BY orderstatus HAVING min(orderdate) > DATE '2000-01-01'",
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderpriority"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "custkey"))));
    }

    @Test
    public void testOutputColumnsWithCountAll()
            throws Exception
    {
        assertColumnLineage(
                "SELECT clerk AS test_varchar, count(*) AS test_bigint FROM orders GROUP BY clerk",
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of()));
    }

    @Test
    public void testOutputColumnsWithWindowFunction()
            throws Exception
    {
        assertColumnLineage(
                "SELECT clerk AS test_varchar, min(orderkey) OVER (PARTITION BY custkey ORDER BY orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS test_bigint FROM orders",
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
        assertColumnLineage(
                "SELECT clerk AS test_varchar, max(orderkey) OVER (w ORDER BY orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS test_bigint FROM orders WINDOW w AS (PARTITION BY custkey)",
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
        assertColumnLineage(
                "SELECT clerk AS test_varchar, min(orderkey) OVER w AS test_bigint FROM orders WINDOW w AS (PARTITION BY custkey ORDER BY orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderkey"))));
    }

    @Test
    public void testOutputColumnsWithUnCorrelatedQueries()
            throws Exception
    {
        assertColumnLineage(
                "SELECT clerk AS test_varchar, (SELECT nationkey FROM nation LIMIT 1) AS test_bigint FROM orders",
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "clerk"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "nationkey"))));
    }

    @Test
    public void testOutputColumnsWithCorrelatedQueries()
            throws Exception
    {
        assertColumnLineage(
                "SELECT orderpriority AS test_varchar, (SELECT min(nationkey) FROM customer WHERE customer.custkey = orders.custkey) AS test_bigint FROM orders",
                new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "orders", "orderpriority"))),
                new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "customer", "nationkey"))));
    }

    @Test
    public void testOutputColumnsForInsertingSingleColumn()
            throws Exception
    {
        runQueryAndWaitForEvents("INSERT INTO mock.default.table_for_output(test_bigint) SELECT nationkey + 1 AS test_bigint FROM nation", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "nationkey"))));
    }

    @Test
    public void testOutputColumnsForInsertingAliasedColumn()
            throws Exception
    {
        runQueryAndWaitForEvents("INSERT INTO mock.default.table_for_output(test_varchar, test_bigint) SELECT name AS aliased_name, nationkey AS aliased_varchar FROM nation", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "name"))),
                        new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of(new ColumnDetail("tpch", "tiny", "nation", "nationkey"))));
    }

    @Test
    public void testOutputColumnsForUpdatingAllColumns()
            throws Exception
    {
        runQueryAndWaitForEvents("UPDATE mock.default.table_for_output SET test_varchar = 'reset', test_bigint = 1", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of()),
                        new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of()));
    }

    @Test
    public void testOutputColumnsForUpdatingSingleColumn()
            throws Exception
    {
        runQueryAndWaitForEvents("UPDATE mock.default.table_for_output SET test_varchar = 're-reset' WHERE test_bigint = 1", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of()));
    }

    @Test
    public void testCreateTable()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE mock.default.create_simple_table (test_column BIGINT)", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());
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
        runQueryAndWaitForEvents("CREATE TABLE mock.default.create_simple_table (test_column BIGINT, LIKE mock.default.test_table)", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("mock");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("default");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("create_simple_table");
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_column", BIGINT_TYPE, ImmutableSet.of()),
                        new OutputColumnMetadata("test_varchar", VARCHAR_TYPE, ImmutableSet.of()),
                        new OutputColumnMetadata("test_bigint", BIGINT_TYPE, ImmutableSet.of()));
    }

    @Test(dataProvider = "setOperator")
    public void testOutputColumnsForSetOperations(String setOperator)
            throws Exception
    {
        assertColumnLineage(
                format("SELECT orderpriority AS test_varchar, orderkey AS test_bigint FROM orders %s SELECT clerk, custkey FROM sf1.orders", setOperator),
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

    private void assertColumnLineage(String baseQuery, OutputColumnMetadata... outputColumnMetadata)
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE mock.default.create_new_table AS " + baseQuery, 2);
        assertColumnMetadata(outputColumnMetadata);

        runQueryAndWaitForEvents("CREATE VIEW mock.default.create_new_view AS " + baseQuery, 2);
        assertColumnMetadata(outputColumnMetadata);

        runQueryAndWaitForEvents("CREATE VIEW mock.default.create_new_materialized_view AS " + baseQuery, 2);
        assertColumnMetadata(outputColumnMetadata);

        runQueryAndWaitForEvents("INSERT INTO mock.default.table_for_output(test_varchar, test_bigint) " + baseQuery, 2);
        assertColumnMetadata(outputColumnMetadata);
    }

    private void assertColumnMetadata(OutputColumnMetadata... outputColumnMetadata)
    {
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(outputColumnMetadata);
    }
}
