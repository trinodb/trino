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
import io.openlineage.client.OpenLineage.Job;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.Run;
import io.openlineage.client.OpenLineage.RunEvent;
import io.trino.plugin.base.eventlistener.testing.TestingEventListenerContext;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryOutputMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
final class TestOpenLineageListener
{
    @Test
    void testGetCompleteEvent()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEvent);

        assertThat(result)
                .extracting(RunEvent::getEventType)
                .isEqualTo(RunEvent.EventType.COMPLETE);

        assertThat(result)
                .extracting(RunEvent::getEventTime)
                .extracting(ZonedDateTime::toInstant)
                .isEqualTo(TrinoEventData.queryCompleteEvent.getEndTime());

        assertThat(result)
                .extracting(RunEvent::getRun)
                .extracting(Run::getRunId)
                // random UUID part may differ, but prefix is timestamp based
                .matches(uuid -> uuid.toString().startsWith("01967c23-ae78-7"));

        assertThat(result)
                .extracting(RunEvent::getJob)
                .extracting(Job::getNamespace)
                .isEqualTo("trino://testhost");

        assertThat(result)
                .extracting(RunEvent::getJob)
                .extracting(Job::getName)
                .isEqualTo("queryId");

        Map<String, Object> trinoQueryMetadata = result
                .getRun()
                .getFacets()
                .getAdditionalProperties()
                .get("trino_metadata")
                .getAdditionalProperties();

        assertThat(trinoQueryMetadata)
                .containsOnly(
                        entry("query_id", "queryId"),
                        entry("transaction_id", "transactionId"),
                        entry("query_plan", "queryPlan"));

        Map<String, Object> trinoQueryContext =
                result
                        .getRun()
                        .getFacets()
                        .getAdditionalProperties()
                        .get("trino_query_context")
                        .getAdditionalProperties();

        assertThat(trinoQueryContext)
                .containsOnly(
                        entry("server_address", "serverAddress"),
                        entry("environment", "environment"),
                        entry("query_type", "INSERT"),
                        entry("user", "user"),
                        entry("original_user", "originalUser"),
                        entry("principal", "principal"),
                        entry("source", "some-trino-client"),
                        entry("client_info", "Some client info"),
                        entry("remote_client_address", "127.0.0.1"),
                        entry("user_agent", "Some-User-Agent"),
                        entry("trace_token", "traceToken"));
    }

    @Test
    void testGetStartEvent()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", OpenLineageTransport.CONSOLE.toString(),
                "openlineage-event-listener.trino.uri", "http://testhost:8080"));

        RunEvent result = listener.getStartEvent(TrinoEventData.queryCreatedEvent);

        assertThat(result)
                .extracting(RunEvent::getEventType)
                .isEqualTo(RunEvent.EventType.START);

        assertThat(result)
                .extracting(RunEvent::getEventTime)
                .extracting(ZonedDateTime::toInstant)
                .isEqualTo(TrinoEventData.queryCreatedEvent.getCreateTime());

        assertThat(result)
                .extracting(RunEvent::getRun)
                .extracting(Run::getRunId)
                // random UUID part may differ, but prefix is timestamp based
                .matches(uuid -> uuid.toString().startsWith("01967c23-ae78-7"));

        assertThat(result)
                .extracting(RunEvent::getJob)
                .extracting(Job::getNamespace)
                .isEqualTo("trino://testhost:8080");

        assertThat(result)
                .extracting(RunEvent::getJob)
                .extracting(Job::getName)
                .isEqualTo("queryId");

        Map<String, Object> trinoQueryMetadata = result
                .getRun()
                .getFacets()
                .getAdditionalProperties()
                .get("trino_metadata")
                .getAdditionalProperties();

        assertThat(trinoQueryMetadata)
                .containsOnly(
                        entry("query_id", "queryId"),
                        entry("transaction_id", "transactionId"),
                        entry("query_plan", "queryPlan"));

        Map<String, Object> trinoQueryContext = result
                .getRun()
                .getFacets()
                .getAdditionalProperties()
                .get("trino_query_context")
                .getAdditionalProperties();

        assertThat(trinoQueryContext)
                .containsOnly(
                        entry("server_address", "serverAddress"),
                        entry("environment", "environment"),
                        entry("query_type", "INSERT"),
                        entry("user", "user"),
                        entry("original_user", "originalUser"),
                        entry("principal", "principal"),
                        entry("source", "some-trino-client"),
                        entry("client_info", "Some client info"),
                        entry("remote_client_address", "127.0.0.1"),
                        entry("user_agent", "Some-User-Agent"),
                        entry("trace_token", "traceToken"));
    }

    @Test
    void testJobNameFormatting()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost:8080",
                "openlineage-event-listener.job.name-format", "$QUERY_ID-$USER-$SOURCE-$CLIENT_IP-abc123"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEvent);

        assertThat(result)
                .extracting(RunEvent::getJob)
                .extracting(Job::getNamespace)
                .isEqualTo("trino://testhost:8080");

        assertThat(result)
                .extracting(RunEvent::getJob)
                .extracting(Job::getName)
                .isEqualTo("queryId-user-some-trino-client-127.0.0.1-abc123");
    }

    @Test
    void testDatasetExcludePatternMustMatchWholeName()
    {
        QueryCompletedEvent event = completedEventWithOutput("catalog", "schema", "table");

        assertThat(outputNames(event, "catalog\\.schema\\..*")).isEmpty();
        assertThat(outputNames(event, "catalog\\.schema")).containsExactly("catalog.schema.table");
        assertThat(outputNames(event, "schema\\.table")).containsExactly("catalog.schema.table");
        assertThat(outputNames(event, "catalog")).containsExactly("catalog.schema.table");
    }

    private static List<String> outputNames(QueryCompletedEvent event, String datasetExcludePattern)
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost",
                "openlineage-event-listener.dataset.exclude-pattern", datasetExcludePattern));

        return listener.getCompletedEvent(event).getOutputs().stream()
                .map(OutputDataset::getName)
                .collect(toImmutableList());
    }

    private static QueryCompletedEvent completedEventWithOutput(String catalog, String schema, String table)
    {
        QueryCompletedEvent template = TrinoEventData.queryCompleteEvent;
        return new QueryCompletedEvent(
                template.getMetadata(),
                template.getStatistics(),
                template.getContext(),
                new QueryIOMetadata(
                        List.of(),
                        Optional.of(new QueryOutputMetadata(catalog, new CatalogVersion("1"), schema, table, Optional.empty(), Optional.empty(), Optional.empty()))),
                Optional.empty(),
                Optional.empty(),
                List.of(),
                template.getCreateTime(),
                template.getExecutionStartTime(),
                template.getEndTime());
    }

    private static EventListener createEventListener(Map<String, String> config)
    {
        return new OpenLineageListenerFactory().create(ImmutableMap.copyOf(config), new TestingEventListenerContext());
    }
}
