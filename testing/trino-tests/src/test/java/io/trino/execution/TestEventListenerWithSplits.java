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
import io.trino.execution.TestEventListenerPlugin.TestingEventListenerPlugin;
import io.trino.plugin.resourcegroups.ResourceGroupManagerPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.execution.TestQueues.createResourceGroupId;
import static io.trino.plugin.tpch.TpchConnectorFactory.TPCH_SPLITS_PER_NODE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestEventListenerWithSplits
        extends AbstractTestQueryFramework
{
    private static final int SPLITS_PER_NODE = 3;
    private final EventsCollector generatedEvents = new EventsCollector();
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
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of(TPCH_SPLITS_PER_NODE, Integer.toString(SPLITS_PER_NODE)));
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

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

    @Test
    public void testSplitsForNormalQuery()
            throws Exception
    {
        // We expect the following events
        // QueryCreated: 1, QueryCompleted: 1, Splits: SPLITS_PER_NODE (leaf splits) + LocalExchange[SINGLE] split + Aggregation/Output split
        int expectedEvents = 1 + 1 + SPLITS_PER_NODE + 1 + 1;
        runQueryAndWaitForEvents("SELECT sum(linenumber) FROM lineitem", expectedEvents);

        QueryCreatedEvent queryCreatedEvent = getOnlyElement(generatedEvents.getQueryCreatedEvents());
        assertEquals(queryCreatedEvent.getContext().getServerVersion(), "testversion");
        assertEquals(queryCreatedEvent.getContext().getServerAddress(), "127.0.0.1");
        assertEquals(queryCreatedEvent.getContext().getEnvironment(), "testing");
        assertEquals(queryCreatedEvent.getContext().getClientInfo().get(), "{\"clientVersion\":\"testVersion\"}");
        assertEquals(queryCreatedEvent.getMetadata().getQuery(), "SELECT sum(linenumber) FROM lineitem");
        assertFalse(queryCreatedEvent.getMetadata().getPreparedQuery().isPresent());

        QueryCompletedEvent queryCompletedEvent = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        assertTrue(queryCompletedEvent.getContext().getResourceGroupId().isPresent());
        assertEquals(queryCompletedEvent.getContext().getResourceGroupId().get(), createResourceGroupId("global", "user-user"));
        assertEquals(queryCompletedEvent.getIoMetadata().getOutput(), Optional.empty());
        assertEquals(queryCompletedEvent.getIoMetadata().getInputs().size(), 1);
        assertEquals(queryCompletedEvent.getContext().getClientInfo().get(), "{\"clientVersion\":\"testVersion\"}");
        assertEquals(getOnlyElement(queryCompletedEvent.getIoMetadata().getInputs()).getCatalogName(), "tpch");
        assertEquals(queryCreatedEvent.getMetadata().getQueryId(), queryCompletedEvent.getMetadata().getQueryId());
        assertFalse(queryCompletedEvent.getMetadata().getPreparedQuery().isPresent());
        assertEquals(queryCompletedEvent.getStatistics().getCompletedSplits(), SPLITS_PER_NODE + 2);

        List<SplitCompletedEvent> splitCompletedEvents = generatedEvents.getSplitCompletedEvents();
        assertEquals(splitCompletedEvents.size(), SPLITS_PER_NODE + 2); // leaf splits + aggregation split

        // All splits must have the same query ID
        Set<String> actual = splitCompletedEvents.stream()
                .map(SplitCompletedEvent::getQueryId)
                .collect(toSet());
        assertEquals(actual, ImmutableSet.of(queryCompletedEvent.getMetadata().getQueryId()));

        // Sum of row count processed by all leaf stages is equal to the number of rows in the table
        long actualCompletedPositions = splitCompletedEvents.stream()
                .filter(e -> !e.getStageId().endsWith(".0"))    // filter out the root stage
                .mapToLong(e -> e.getStatistics().getCompletedPositions())
                .sum();

        MaterializedResult result = runQueryAndWaitForEvents("SELECT count(*) FROM lineitem", expectedEvents);
        long expectedCompletedPositions = (long) result.getMaterializedRows().get(0).getField(0);
        assertEquals(actualCompletedPositions, expectedCompletedPositions);

        QueryStatistics statistics = queryCompletedEvent.getStatistics();
        // Aggregation can have memory pool usage
        assertTrue(statistics.getPeakUserMemoryBytes() >= 0);
        assertTrue(statistics.getPeakTaskUserMemory() >= 0);
        assertTrue(statistics.getPeakTaskTotalMemory() >= 0);
        assertTrue(statistics.getCumulativeMemory() >= 0);

        // Not a write query
        assertEquals(statistics.getWrittenBytes(), 0);
        assertEquals(statistics.getWrittenRows(), 0);
        assertEquals(statistics.getStageGcStatistics().size(), 2);

        // Deterministic statistics
        assertEquals(statistics.getPhysicalInputBytes(), 0);
        assertEquals(statistics.getPhysicalInputRows(), expectedCompletedPositions);
        assertEquals(statistics.getProcessedInputBytes(), 0);
        assertEquals(statistics.getProcessedInputRows(), expectedCompletedPositions);
        assertEquals(statistics.getInternalNetworkBytes(), 381);
        assertEquals(statistics.getInternalNetworkRows(), 3);
        assertEquals(statistics.getTotalBytes(), 0);
        assertEquals(statistics.getOutputBytes(), 9);
        assertEquals(statistics.getOutputRows(), 1);
        assertTrue(statistics.isComplete());

        // Check only the presence because they are non-deterministic.
        assertTrue(statistics.getScheduledTime().isPresent());
        assertTrue(statistics.getResourceWaitingTime().isPresent());
        assertTrue(statistics.getAnalysisTime().isPresent());
        assertTrue(statistics.getPlanningTime().isPresent());
        assertTrue(statistics.getExecutionTime().isPresent());
        assertTrue(statistics.getPlanNodeStatsAndCosts().isPresent());
        assertTrue(statistics.getCpuTime().getSeconds() >= 0);
        assertTrue(statistics.getWallTime().getSeconds() >= 0);
        assertTrue(statistics.getCpuTimeDistribution().size() > 0);
        assertTrue(statistics.getOperatorSummaries().size() > 0);
    }

    @Test
    public void testSplitsForConstantQuery()
            throws Exception
    {
        // QueryCreated: 1, QueryCompleted: 1, Splits: 1
        runQueryAndWaitForEvents("SELECT 1", 3);

        QueryCreatedEvent queryCreatedEvent = getOnlyElement(generatedEvents.getQueryCreatedEvents());
        assertEquals(queryCreatedEvent.getContext().getServerVersion(), "testversion");
        assertEquals(queryCreatedEvent.getContext().getServerAddress(), "127.0.0.1");
        assertEquals(queryCreatedEvent.getContext().getEnvironment(), "testing");
        assertEquals(queryCreatedEvent.getContext().getClientInfo().get(), "{\"clientVersion\":\"testVersion\"}");
        assertEquals(queryCreatedEvent.getContext().getQueryType().get(), QueryType.SELECT);
        assertEquals(queryCreatedEvent.getMetadata().getQuery(), "SELECT 1");
        assertFalse(queryCreatedEvent.getMetadata().getPreparedQuery().isPresent());

        QueryCompletedEvent queryCompletedEvent = getOnlyElement(generatedEvents.getQueryCompletedEvents());
        assertTrue(queryCompletedEvent.getContext().getResourceGroupId().isPresent());
        assertEquals(queryCompletedEvent.getContext().getResourceGroupId().get(), createResourceGroupId("global", "user-user"));
        assertEquals(queryCompletedEvent.getStatistics().getTotalRows(), 0L);
        assertEquals(queryCompletedEvent.getContext().getClientInfo().get(), "{\"clientVersion\":\"testVersion\"}");
        assertEquals(queryCreatedEvent.getMetadata().getQueryId(), queryCompletedEvent.getMetadata().getQueryId());
        assertFalse(queryCompletedEvent.getMetadata().getPreparedQuery().isPresent());
        assertEquals(queryCompletedEvent.getContext().getQueryType().get(), QueryType.SELECT);

        List<SplitCompletedEvent> splitCompletedEvents = generatedEvents.getSplitCompletedEvents();
        assertEquals(splitCompletedEvents.get(0).getQueryId(), queryCompletedEvent.getMetadata().getQueryId());
        assertEquals(splitCompletedEvents.get(0).getStatistics().getCompletedPositions(), 1);
    }

    private MaterializedResult runQueryAndWaitForEvents(@Language("SQL") String sql, int numEventsExpected)
            throws Exception
    {
        return queries.runQueryAndWaitForEvents(sql, numEventsExpected, getSession());
    }
}
