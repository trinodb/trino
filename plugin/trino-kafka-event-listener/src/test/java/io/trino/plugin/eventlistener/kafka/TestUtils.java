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

package io.trino.plugin.eventlistener.kafka;

import io.trino.operator.RetryPolicy;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoWarning;
import io.trino.spi.WarningCode;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import io.trino.spi.eventlistener.SplitStatistics;
import io.trino.spi.eventlistener.StageOutputBufferUtilization;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.ResourceEstimates;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.time.Duration.ofMillis;

public final class TestUtils
{
    private static final QueryIOMetadata queryIOMetadata;
    private static final QueryContext queryContext;
    private static final QueryMetadata queryMetadata;
    private static final SplitStatistics splitStatistics;
    private static final QueryStatistics queryStatistics;

    private static final Optional<QueryFailureInfo> queryFailureInfo;

    static final SplitCompletedEvent splitCompletedEvent;
    static final QueryCreatedEvent queryCreatedEvent;
    static final QueryCompletedEvent queryCompletedEvent;

    private TestUtils()
    {
    }

    static {
        queryIOMetadata = new QueryIOMetadata(Collections.emptyList(), Optional.empty());

        queryContext = new QueryContext(
                "user",
                "originalUser",
                Optional.of("principal"),
                Set.of(),
                Set.of(), // groups
                Optional.empty(), // traceToken
                Optional.empty(), // remoteClientAddress
                Optional.empty(), // userAgent
                Optional.empty(), // clientInfo
                new HashSet<>(), // clientTags
                new HashSet<>(), // clientCapabilities
                Optional.of("source"),
                UTC_KEY.getId(),
                Optional.of("catalog"),
                Optional.of("schema"),
                Optional.of(new ResourceGroupId("name")),
                new HashMap<>(), // sessionProperties
                new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.of(1000L)),
                "serverAddress", "serverVersion", "environment",
                Optional.of(QueryType.SELECT),
                RetryPolicy.QUERY.toString());

        queryMetadata = new QueryMetadata(
                "queryId",
                Optional.empty(),
                Optional.empty(),
                "query",
                Optional.of("updateType"),
                Optional.of("preparedQuery"),
                "queryState",
                List.of(),
                List.of(),
                URI.create("http://localhost"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        splitStatistics = new SplitStatistics(
                ofMillis(1000),
                ofMillis(2000),
                ofMillis(3000),
                ofMillis(4000),
                1,
                2,
                Optional.of(Duration.ofMillis(100)),
                Optional.of(Duration.ofMillis(200)));

        queryStatistics = new QueryStatistics(
                ofMillis(1000),
                ofMillis(1000),
                ofMillis(1000),
                ofMillis(1000),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0.0f,
                Collections.emptyList(),
                0,
                true,
                Collections.emptyList(),
                List.of(new StageOutputBufferUtilization(0, 10, 0.1, 0.5, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.0, 1.0, Duration.ofSeconds(1234))),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Optional.empty());

        queryFailureInfo = Optional.of(
                new QueryFailureInfo(
                        new ErrorCode(404, "Access Denied", ErrorType.EXTERNAL),
                        Optional.of("EXTERNAL_ERROR"),
                        Optional.of("Access Denied"),
                        Optional.of("task"),
                        Optional.of("host"),
                        "testJson"));

        splitCompletedEvent = new SplitCompletedEvent(
                "queryId",
                "stageId",
                "taskId",
                Optional.of("catalogName"),
                Instant.now(),
                Optional.of(Instant.now()),
                Optional.of(Instant.now()),
                splitStatistics,
                Optional.empty(),
                "payload");

        queryCreatedEvent = new QueryCreatedEvent(
                Instant.now(),
                queryContext,
                queryMetadata);

        queryCompletedEvent = new QueryCompletedEvent(
                queryMetadata,
                queryStatistics,
                queryContext,
                queryIOMetadata,
                queryFailureInfo,
                List.of(new TrinoWarning(new WarningCode(101, "TestCode"), "Test error message")),
                Instant.now(),
                Instant.now(),
                Instant.now());
    }
}
