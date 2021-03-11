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
import io.trino.execution.EventsCollector.EventFilters;
import io.trino.execution.TestEventListenerPlugin.TestingEventListenerPlugin;
import io.trino.plugin.resourcegroups.ResourceGroupManagerPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.eventlistener.ColumnInfo;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.RoutineInfo;
import io.trino.spi.eventlistener.TableInfo;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
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
                        .withListTables((session, s) -> ImmutableList.of(new SchemaTableName("default", "test_table")))
                        .withApplyProjection((session, handle, projections, assignments) -> {
                            throw new RuntimeException("Throw from apply projection");
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
        assertFailedQuery("You shall not parse!", "line 1:1: mismatched input 'You'. Expecting: 'ALTER', 'ANALYZE', 'CALL', 'COMMENT', 'COMMIT', 'CREATE', 'DEALLOCATE', 'DELETE', 'DESC', 'DESCRIBE', 'DROP', 'EXECUTE', 'EXPLAIN', 'GRANT', 'INSERT', 'PREPARE', 'REFRESH', 'RESET', 'REVOKE', 'ROLLBACK', 'SET', 'SHOW', 'START', 'UPDATE', 'USE', <query>");
    }

    @Test
    public void testPlanningFailure()
            throws Exception
    {
        assertFailedQuery("SELECT * FROM mock.default.tests_table", "Throw from apply projection");
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
            String sql = format("SELECT nationkey as %s  FROM tpch.sf1.nation", testQueryMarker);

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
}
