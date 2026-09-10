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
package io.trino.plugin.openlineage;

import com.google.common.collect.ImmutableMap;
import io.trino.operator.RetryPolicy;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.eventlistener.ColumnDetail;
import io.trino.spi.eventlistener.ColumnTransformationType;
import io.trino.spi.eventlistener.OutputColumnMetadata;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryOutputMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.StageOutputBufferUtilization;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.ResourceEstimates;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.time.Duration.ofSeconds;

public class TrinoEventData
{
    public static final QueryIOMetadata queryIOMetadata;
    public static final QueryContext queryContext;
    public static final QueryMetadata queryMetadata;
    public static final QueryStatistics queryStatistics;
    public static final QueryCompletedEvent queryCompleteEvent;
    public static final QueryCreatedEvent queryCreatedEvent;
    public static final QueryCompletedEvent queryCompleteEventWithColumnLineage;

    private TrinoEventData()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    static {
        queryIOMetadata = new QueryIOMetadata(Collections.emptyList(), Optional.empty());

        queryContext = new QueryContext(
                "user",
                "originalUser",
                Set.of(), // originalRoles
                Optional.of("principal"),
                Set.of(), // enabledRoles
                Set.of(), // groups
                Optional.of("traceToken"),
                Optional.of("127.0.0.1"),
                Optional.of("Some-User-Agent"),
                Optional.of("Some client info"),
                new HashSet<>(), // clientTags
                new HashSet<>(), // clientCapabilities
                Optional.of("some-trino-client"),
                UTC_KEY.getId(),
                Optional.of("catalog"),
                Optional.of("schema"),
                Optional.of(new ResourceGroupId("name")),
                new HashMap<>(), // sessionProperties
                new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.of(1000L)),
                "serverAddress",
                "serverVersion",
                "environment",
                Optional.of(QueryType.INSERT),
                RetryPolicy.QUERY.toString());

        queryMetadata = new QueryMetadata(
                "queryId",
                Optional.of("transactionId"),
                Optional.empty(), // encoding
                "create table b.c as select * from y.z",
                Optional.of("updateType"),
                Optional.of("preparedQuery"),
                "COMPLETED",
                List.of(), // tables
                List.of(), // routines
                URI.create("http://localhost"),
                Optional.of("queryPlan"),
                Optional.empty(), // jsonPlan
                Optional.empty()); // payload

        queryStatistics = new QueryStatistics(
                ofSeconds(1),
                ofSeconds(1),
                ofSeconds(1),
                ofSeconds(1),
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
                0.0f,
                Collections.emptyList(),
                0,
                true,
                Collections.emptyList(),
                List.of(new StageOutputBufferUtilization(0, 10, 0.1, 0.5, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.0, 1.0, ofSeconds(1234))),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                Optional.empty());

        queryCompleteEvent = new QueryCompletedEvent(
                queryMetadata,
                queryStatistics,
                queryContext,
                queryIOMetadata,
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Instant.parse("2025-04-28T11:23:55.384424Z"),
                Instant.parse("2025-04-28T11:24:16.256207Z"),
                Instant.parse("2025-04-28T11:24:26.993340Z"));

        queryCreatedEvent = new QueryCreatedEvent(
                Instant.parse("2025-04-28T11:23:55.384424Z"),
                queryContext,
                queryMetadata);

        // CREATE TABLE marquez.default.target AS
        //   SELECT a AS identity_col, c + 1 AS transformation_col, sum(b) AS aggregation_col FROM base_table GROUP BY a, c
        // Each output column carries its source columns with the transformation subtype the analyzer classified.
        QueryInputMetadata columnLineageInput = new QueryInputMetadata(
                Optional.of("marquez"),
                "marquez",
                new CatalogVersion("default"),
                "default",
                "base_table",
                List.of(
                        new QueryInputMetadata.Column("a", "bigint"),
                        new QueryInputMetadata.Column("b", "bigint"),
                        new QueryInputMetadata.Column("c", "bigint")),
                Optional.empty(),
                Metrics.EMPTY,
                OptionalLong.empty(),
                OptionalLong.empty());
        QueryOutputMetadata columnLineageOutput = new QueryOutputMetadata(
                "marquez",
                new CatalogVersion("default"),
                "default",
                "target",
                Optional.of(List.of(
                        new OutputColumnMetadata("identity_col", "bigint", Set.of(new ColumnDetail("marquez", "default", "base_table", "a", Optional.of(ColumnTransformationType.IDENTITY)))),
                        new OutputColumnMetadata("transformation_col", "bigint", Set.of(new ColumnDetail("marquez", "default", "base_table", "c", Optional.of(ColumnTransformationType.TRANSFORMATION)))),
                        new OutputColumnMetadata("aggregation_col", "bigint", Set.of(new ColumnDetail("marquez", "default", "base_table", "b", Optional.of(ColumnTransformationType.AGGREGATION)))))),
                Optional.empty(),
                Optional.empty());
        queryCompleteEventWithColumnLineage = new QueryCompletedEvent(
                queryMetadata,
                queryStatistics,
                queryContext,
                new QueryIOMetadata(List.of(columnLineageInput), Optional.of(columnLineageOutput)),
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Instant.parse("2025-04-28T11:23:55.384424Z"),
                Instant.parse("2025-04-28T11:24:16.256207Z"),
                Instant.parse("2025-04-28T11:24:26.993340Z"));
    }
}
