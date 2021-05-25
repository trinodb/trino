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
package io.trino.plugin.queryeventlistener;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.queryeventlistener.model.CompletedEvent;
import io.trino.plugin.queryeventlistener.model.CreatedEvent;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.ResourceEstimates;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;

import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryEventListener
{
    private static final QueryEventListener listener = new QueryEventListener(ImmutableMap.of());

    @Test
    void testHandleQueryCreatedEvent()
            throws Exception
    {
        QueryCreatedEvent createdEvent = getNewQueryCreatedEvent();
        String json = listener.handleQueryCreatedEvent(createdEvent);
        CreatedEvent parsedCreatedEvent = QueryEventListener.objectMapper.readValue(json, CreatedEvent.class);

        assertThat(parsedCreatedEvent.getEventType()).isEqualTo("QueryCreated");
        assertThat(parsedCreatedEvent.getQueryId()).isEqualTo(createdEvent.getMetadata().getQueryId());
        assertThat(parsedCreatedEvent.getCreateTime()).isEqualTo(LocalDateTime.ofInstant(createdEvent.getCreateTime(), ZoneId.systemDefault()));
        assertThat(parsedCreatedEvent.getUser()).isEqualTo(createdEvent.getContext().getUser());
        assertThat(parsedCreatedEvent.getPrincipal()).isEqualTo(createdEvent.getContext().getPrincipal().orElse(null));
        assertThat(parsedCreatedEvent.getSource()).isEqualTo(createdEvent.getContext().getSource().orElse(null));
        assertThat(parsedCreatedEvent.getSchema()).isEqualTo(createdEvent.getContext().getSchema().orElse(null));
        assertThat(parsedCreatedEvent.getCatalog()).isEqualTo(createdEvent.getContext().getCatalog().orElse(null));
        assertThat(parsedCreatedEvent.getSql()).isEqualTo(createdEvent.getMetadata().getQuery());
    }

    @Test
    void testHandleQueryCompletedEvent()
            throws Exception
    {
        QueryCompletedEvent completedEvent = getNewQueryCompletedEvent();
        String json = listener.handleQueryCompletedEvent(completedEvent);
        CompletedEvent parsedCompletedEvent = QueryEventListener.objectMapper.readValue(json, CompletedEvent.class);

        assertThat(parsedCompletedEvent.getEventType()).isEqualTo("QueryCompleted");
        assertThat(parsedCompletedEvent.getQueryId()).isEqualTo(completedEvent.getMetadata().getQueryId());
        assertThat(parsedCompletedEvent.getCreateTime()).isEqualTo(LocalDateTime.ofInstant(completedEvent.getCreateTime(), ZoneId.systemDefault()));
        assertThat(parsedCompletedEvent.getQueuedTime()).isEqualTo(completedEvent.getStatistics().getQueuedTime().toMillis());
        assertThat(parsedCompletedEvent.getWallTime()).isEqualTo(completedEvent.getStatistics().getWallTime().toMillis());
        assertThat(parsedCompletedEvent.getCpuTime()).isEqualTo(completedEvent.getStatistics().getCpuTime().toMillis());
        assertThat(parsedCompletedEvent.getEndTime()).isEqualTo(LocalDateTime.ofInstant(completedEvent.getEndTime(), ZoneOffset.systemDefault()));
        assertThat(parsedCompletedEvent.getUser()).isEqualTo(completedEvent.getContext().getUser());
        assertThat(parsedCompletedEvent.getSource()).isEqualTo(completedEvent.getContext().getSource().orElse(null));
        assertThat(parsedCompletedEvent.getSchema()).isEqualTo(completedEvent.getContext().getSchema().orElse(null));
        assertThat(parsedCompletedEvent.getCatalog()).isEqualTo(completedEvent.getContext().getCatalog().orElse(null));
        assertThat(parsedCompletedEvent.getRecords()).isEqualTo(completedEvent.getStatistics().getTotalRows());
        assertThat(parsedCompletedEvent.isCompleted()).isEqualTo(completedEvent.getStatistics().isComplete());
        assertThat(parsedCompletedEvent.getSql()).isEqualTo(completedEvent.getMetadata().getQuery());
    }

    private QueryCreatedEvent getNewQueryCreatedEvent()
    {
        return new QueryCreatedEvent(
                Instant.now(),
                createQueryContext(),
                createQueryMetadata());
    }

    private QueryCompletedEvent getNewQueryCompletedEvent()
    {
        return new QueryCompletedEvent(
                createQueryMetadata(),
                createQueryStatistics(),
                createQueryContext(),
                createQueryIOMetadata(),
                Optional.empty(),
                ImmutableList.of(),
                Instant.ofEpochMilli(100),
                Instant.ofEpochMilli(200),
                Instant.ofEpochMilli(300));
    }

    private QueryMetadata createQueryMetadata()
    {
        return new QueryMetadata(
                "queryId",
                Optional.empty(), // transactionId
                "query",
                Optional.of("updateType"),
                Optional.of("preparedQuery"),
                "queryState",
                ImmutableList.of(), // tables
                ImmutableList.of(), // routines
                URI.create("http://localhost"),
                Optional.empty(), // plan
                Optional.empty()); // payload
    }

    private QueryContext createQueryContext()
    {
        return new QueryContext(
                "user", // user
                Optional.of("principal"), // principal
                ImmutableSet.of(), // groups
                Optional.empty(), // traceToken
                Optional.empty(), // remoteClientAddress
                Optional.empty(), // userAgent
                Optional.empty(), // clientInfo
                ImmutableSet.of(), // clientTags
                ImmutableSet.of(), // clientCapabilities
                Optional.of("source"), // source
                Optional.of("catalog"), // catalog
                Optional.of("schema"), // schema
                Optional.of(new ResourceGroupId("name")), // resourceGroupId
                ImmutableMap.of(), // sessionProperties
                new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.of(1000L)), // resourceEstimates
                "serverAddress", // serverAddress
                "serverVersion", // serverVersion
                "environment", // environment
                Optional.of(QueryType.SELECT)); // QueryType
    }

    private QueryStatistics createQueryStatistics()
    {
        return new QueryStatistics(
                ofMillis(100), // cpuTime
                ofMillis(100), // wallTime
                ofMillis(100), // queuedTime
                Optional.of(Duration.ofMillis(100)), // scheduledTime
                Optional.of(Duration.ofMillis(100)), // waitingTime
                Optional.of(Duration.ofMillis(100)), // analysisTime
                Optional.of(Duration.ofMillis(100)), // planningTime
                Optional.of(Duration.ofMillis(100)), // executionTime
                1, // peakUserMemoryBytes
                1, // peakTotalNonRevocableMemoryBytes
                1, // peakTaskUserMemory
                1, // peakTaskTotalMemory
                1, // physicalInputBytes
                1, // physicalInputRows
                1, // internalNetworkBytes
                1, // internalNetworkRows
                1, // totalBytes
                1, // totalRows
                1, // outputBytes
                1, // outputRows
                1, // writtenBytes
                1, // writtenRows
                1.0, // cumulativeMemory
                ImmutableList.of(), // stageGcStatistics
                1, // completedSplits
                true, // complete
                ImmutableList.of(), // cpuTimeDistribution
                ImmutableList.of(), //operatorSummaries
                Optional.of("value")); // planNodeStatsAndCosts
    }

    private QueryIOMetadata createQueryIOMetadata()
    {
        return new QueryIOMetadata(ImmutableList.of(), Optional.empty());
    }
}
