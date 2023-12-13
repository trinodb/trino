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
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.execution.EventsAwaitingQueries.MaterializedResultWithEvents;
import io.trino.execution.EventsCollector.QueryEvents;
import io.trino.execution.TestEventListenerPlugin.TestingEventListenerPlugin;
import io.trino.plugin.resourcegroups.ResourceGroupManagerPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.execution.TestQueues.createResourceGroupId;
import static io.trino.plugin.tpch.TpchConnectorFactory.TPCH_SPLITS_PER_NODE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

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
                        .withListTables((session, s) -> ImmutableList.of("test_table"))
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

        queries = new EventsAwaitingQueries(generatedEvents, queryRunner);

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
        QueryEvents queryEvents = runQueryAndWaitForEvents("SELECT sum(linenumber) FROM lineitem").getQueryEvents();

        QueryCreatedEvent queryCreatedEvent = queryEvents.getQueryCreatedEvent();
        assertThat(queryCreatedEvent.getContext().getServerVersion()).isEqualTo("testversion");
        assertThat(queryCreatedEvent.getContext().getServerAddress()).isEqualTo("127.0.0.1");
        assertThat(queryCreatedEvent.getContext().getEnvironment()).isEqualTo("testing");
        assertThat(queryCreatedEvent.getContext().getClientInfo().get()).isEqualTo("{\"clientVersion\":\"testVersion\"}");
        assertThat(queryCreatedEvent.getMetadata().getQuery()).isEqualTo("SELECT sum(linenumber) FROM lineitem");
        assertThat(queryCreatedEvent.getMetadata().getPreparedQuery().isPresent()).isFalse();

        QueryCompletedEvent queryCompletedEvent = queryEvents.getQueryCompletedEvent();
        assertThat(queryCompletedEvent.getContext().getResourceGroupId().isPresent()).isTrue();
        assertThat(queryCompletedEvent.getContext().getResourceGroupId().get()).isEqualTo(createResourceGroupId("global", "user-user"));
        assertThat(queryCompletedEvent.getIoMetadata().getOutput()).isEqualTo(Optional.empty());
        assertThat(queryCompletedEvent.getIoMetadata().getInputs().size()).isEqualTo(1);
        assertThat(queryCompletedEvent.getContext().getClientInfo().get()).isEqualTo("{\"clientVersion\":\"testVersion\"}");
        assertThat(getOnlyElement(queryCompletedEvent.getIoMetadata().getInputs()).getCatalogName()).isEqualTo("tpch");
        assertThat(queryCreatedEvent.getMetadata().getQueryId()).isEqualTo(queryCompletedEvent.getMetadata().getQueryId());
        assertThat(queryCompletedEvent.getMetadata().getPreparedQuery().isPresent()).isFalse();
        assertThat(queryCompletedEvent.getStatistics().getCompletedSplits()).isEqualTo(SPLITS_PER_NODE + 2);

        List<SplitCompletedEvent> splitCompletedEvents = queryEvents.waitForSplitCompletedEvents(SPLITS_PER_NODE + 2, new Duration(30, SECONDS));
        assertThat(splitCompletedEvents.size()).isEqualTo(SPLITS_PER_NODE + 2); // leaf splits + aggregation split

        // All splits must have the same query ID
        Set<String> actual = splitCompletedEvents.stream()
                .map(SplitCompletedEvent::getQueryId)
                .collect(toSet());
        assertThat(actual).isEqualTo(ImmutableSet.of(queryCompletedEvent.getMetadata().getQueryId()));

        // Sum of row count processed by all leaf stages is equal to the number of rows in the table
        long actualCompletedPositions = splitCompletedEvents.stream()
                .filter(e -> !e.getStageId().endsWith(".0"))    // filter out the root stage
                .mapToLong(e -> e.getStatistics().getCompletedPositions())
                .sum();

        MaterializedResultWithEvents result = runQueryAndWaitForEvents("SELECT count(*) FROM lineitem");
        long expectedCompletedPositions = (long) result.getMaterializedResult().getMaterializedRows().get(0).getField(0);
        assertThat(actualCompletedPositions).isEqualTo(expectedCompletedPositions);

        QueryStatistics statistics = queryCompletedEvent.getStatistics();
        // Aggregation can have memory pool usage
        assertThat(statistics.getPeakUserMemoryBytes() >= 0).isTrue();
        assertThat(statistics.getPeakTaskUserMemory() >= 0).isTrue();
        assertThat(statistics.getPeakTaskTotalMemory() >= 0).isTrue();
        assertThat(statistics.getCumulativeMemory() >= 0).isTrue();

        // Not a write query
        assertThat(statistics.getWrittenBytes()).isEqualTo(0);
        assertThat(statistics.getWrittenRows()).isEqualTo(0);
        assertThat(statistics.getStageGcStatistics().size()).isEqualTo(2);

        // Deterministic statistics
        assertThat(statistics.getPhysicalInputBytes()).isEqualTo(0);
        assertThat(statistics.getPhysicalInputRows()).isEqualTo(expectedCompletedPositions);
        assertThat(statistics.getProcessedInputBytes()).isEqualTo(0);
        assertThat(statistics.getProcessedInputRows()).isEqualTo(expectedCompletedPositions);
        assertThat(statistics.getInternalNetworkBytes()).isEqualTo(261);
        assertThat(statistics.getInternalNetworkRows()).isEqualTo(3);
        assertThat(statistics.getTotalBytes()).isEqualTo(0);
        assertThat(statistics.getOutputBytes()).isEqualTo(9);
        assertThat(statistics.getOutputRows()).isEqualTo(1);
        assertThat(statistics.isComplete()).isTrue();

        // Check only the presence because they are non-deterministic.
        assertThat(statistics.getScheduledTime().isPresent()).isTrue();
        assertThat(statistics.getResourceWaitingTime().isPresent()).isTrue();
        assertThat(statistics.getAnalysisTime().isPresent()).isTrue();
        assertThat(statistics.getPlanningTime().isPresent()).isTrue();
        assertThat(statistics.getExecutionTime().isPresent()).isTrue();
        assertThat(statistics.getPlanNodeStatsAndCosts().isPresent()).isTrue();
        assertThat(statistics.getCpuTime().getSeconds() >= 0).isTrue();
        assertThat(statistics.getWallTime().getSeconds() >= 0).isTrue();
        assertThat(statistics.getCpuTimeDistribution().size() > 0).isTrue();
        assertThat(statistics.getOperatorSummaries().size() > 0).isTrue();
        assertThat(statistics.getOutputBufferUtilization().size() > 0).isTrue();
    }

    @Test
    public void testSplitsForConstantQuery()
            throws Exception
    {
        // QueryCreated: 1, QueryCompleted: 1, Splits: 1
        QueryEvents queryEvents = runQueryAndWaitForEvents("SELECT 1").getQueryEvents();

        QueryCreatedEvent queryCreatedEvent = queryEvents.getQueryCreatedEvent();
        assertThat(queryCreatedEvent.getContext().getServerVersion()).isEqualTo("testversion");
        assertThat(queryCreatedEvent.getContext().getServerAddress()).isEqualTo("127.0.0.1");
        assertThat(queryCreatedEvent.getContext().getEnvironment()).isEqualTo("testing");
        assertThat(queryCreatedEvent.getContext().getClientInfo().get()).isEqualTo("{\"clientVersion\":\"testVersion\"}");
        assertThat(queryCreatedEvent.getContext().getQueryType().get()).isEqualTo(QueryType.SELECT);
        assertThat(queryCreatedEvent.getMetadata().getQuery()).isEqualTo("SELECT 1");
        assertThat(queryCreatedEvent.getMetadata().getPreparedQuery().isPresent()).isFalse();

        QueryCompletedEvent queryCompletedEvent = queryEvents.getQueryCompletedEvent();
        assertThat(queryCompletedEvent.getContext().getResourceGroupId().isPresent()).isTrue();
        assertThat(queryCompletedEvent.getContext().getResourceGroupId().get()).isEqualTo(createResourceGroupId("global", "user-user"));
        assertThat(queryCompletedEvent.getStatistics().getTotalRows()).isEqualTo(0L);
        assertThat(queryCompletedEvent.getContext().getClientInfo().get()).isEqualTo("{\"clientVersion\":\"testVersion\"}");
        assertThat(queryCreatedEvent.getMetadata().getQueryId()).isEqualTo(queryCompletedEvent.getMetadata().getQueryId());
        assertThat(queryCompletedEvent.getMetadata().getPreparedQuery().isPresent()).isFalse();
        assertThat(queryCompletedEvent.getContext().getQueryType().get()).isEqualTo(QueryType.SELECT);

        List<SplitCompletedEvent> splitCompletedEvents = queryEvents.waitForSplitCompletedEvents(1, new Duration(30, SECONDS));
        assertThat(splitCompletedEvents.get(0).getQueryId()).isEqualTo(queryCompletedEvent.getMetadata().getQueryId());
        assertThat(splitCompletedEvents.get(0).getStatistics().getCompletedPositions()).isEqualTo(1);
    }

    private MaterializedResultWithEvents runQueryAndWaitForEvents(@Language("SQL") String sql)
            throws Exception
    {
        return queries.runQueryAndWaitForEvents(sql, getSession());
    }
}
