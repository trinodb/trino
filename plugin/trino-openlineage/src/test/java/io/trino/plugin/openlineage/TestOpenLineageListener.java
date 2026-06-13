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
import io.openlineage.client.OpenLineage.Run;
import io.openlineage.client.OpenLineage.RunEvent;
import io.trino.plugin.base.eventlistener.testing.TestingEventListenerContext;
import io.trino.spi.eventlistener.EventListener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.ZonedDateTime;
import java.util.Map;

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
    void testIncludeTransitiveInputsDisabled()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEventWithView);

        assertThat(result.getInputs())
                .hasSize(1)
                .satisfiesExactly(input -> {
                    assertThat(input.getName()).isEqualTo("marquez.default.my_view");
                    assertThat(input.getFacets().getSchema().getFields())
                            .extracting(field -> field.getName())
                            .containsExactly("nationkey", "name");
                });
    }

    @Test
    void testIncludeTransitiveInputsEnabled()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost",
                "openlineage-event-listener.include-transitive-inputs", "true"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEventWithView);

        assertThat(result.getInputs())
                .hasSize(2)
                .satisfiesExactlyInAnyOrder(
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.my_view"),
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.base_table"));
    }

    @Test
    void testIncludeTransitiveInputsNestedViewDisabled()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEventWithNestedView);

        assertThat(result.getInputs())
                .hasSize(1)
                .satisfiesExactly(input -> {
                    assertThat(input.getName()).isEqualTo("marquez.default.v2");
                    assertThat(input.getFacets().getSchema().getFields())
                            .extracting(field -> field.getName())
                            .containsExactly("nationkey", "name");
                });
    }

    @Test
    void testIncludeTransitiveInputsNestedViewEnabled()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost",
                "openlineage-event-listener.include-transitive-inputs", "true"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEventWithNestedView);

        assertThat(result.getInputs())
                .hasSize(3)
                .satisfiesExactlyInAnyOrder(
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.v2"),
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.v1"),
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.base_table"));
    }

    @Test
    void testIncludeTransitiveInputsDirectTableDisabled()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEventWithDirectTable);

        assertThat(result.getInputs())
                .hasSize(1)
                .satisfiesExactly(input -> {
                    assertThat(input.getName()).isEqualTo("marquez.default.orders");
                    assertThat(input.getFacets().getSchema().getFields())
                            .extracting(field -> field.getName())
                            .containsExactly("orderkey", "custkey");
                });
    }

    @Test
    void testIncludeTransitiveInputsDirectTableEnabled()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost",
                "openlineage-event-listener.include-transitive-inputs", "true"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEventWithDirectTable);

        assertThat(result.getInputs())
                .hasSize(1)
                .satisfiesExactly(input -> {
                    assertThat(input.getName()).isEqualTo("marquez.default.orders");
                    assertThat(input.getFacets().getSchema().getFields())
                            .extracting(field -> field.getName())
                            .containsExactly("orderkey", "custkey");
                });
    }

    @Test
    void testIncludeTransitiveInputsMixedInputsDisabled()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEventWithMixedInputs);

        assertThat(result.getInputs())
                .hasSize(2)
                .satisfiesExactlyInAnyOrder(
                        input -> {
                            assertThat(input.getName()).isEqualTo("marquez.default.orders");
                            assertThat(input.getFacets().getSchema().getFields())
                                    .extracting(field -> field.getName())
                                    .containsExactly("orderkey");
                        },
                        input -> {
                            assertThat(input.getName()).isEqualTo("marquez.default.my_view");
                            assertThat(input.getFacets().getSchema().getFields())
                                    .extracting(field -> field.getName())
                                    .containsExactly("nationkey");
                        });
    }

    @Test
    void testIncludeTransitiveInputsMixedInputsEnabled()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost",
                "openlineage-event-listener.include-transitive-inputs", "true"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEventWithMixedInputs);

        assertThat(result.getInputs())
                .hasSize(3)
                .satisfiesExactlyInAnyOrder(
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.orders"),
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.my_view"),
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.nation"));
    }

    @Test
    void testIncludeTransitiveInputsViewMultipleBaseTablesDisabled()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEventWithViewMultipleBaseTables);

        assertThat(result.getInputs())
                .hasSize(1)
                .satisfiesExactly(input -> {
                    assertThat(input.getName()).isEqualTo("marquez.default.join_view");
                    assertThat(input.getFacets().getSchema().getFields())
                            .extracting(field -> field.getName())
                            .containsExactly("col_a", "col_b");
                });
    }

    @Test
    void testIncludeTransitiveInputsViewMultipleBaseTablesEnabled()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost",
                "openlineage-event-listener.include-transitive-inputs", "true"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEventWithViewMultipleBaseTables);

        assertThat(result.getInputs())
                .hasSize(3)
                .satisfiesExactlyInAnyOrder(
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.join_view"),
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.t1"),
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.t2"));
    }

    @Test
    void testIncludeTransitiveInputsNestedViewComplexJoinDisabled()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEventWithNestedViewComplexJoin);

        assertThat(result.getInputs())
                .hasSize(1)
                .satisfiesExactly(input -> {
                    assertThat(input.getName()).isEqualTo("marquez.default.v_outer");
                    assertThat(input.getFacets().getSchema().getFields())
                            .extracting(field -> field.getName())
                            .containsExactly("nationkey", "regionkey", "orderkey");
                });
    }

    @Test
    void testIncludeTransitiveInputsNestedViewComplexJoinEnabled()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost",
                "openlineage-event-listener.include-transitive-inputs", "true"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEventWithNestedViewComplexJoin);

        assertThat(result.getInputs())
                .hasSize(5)
                .satisfiesExactlyInAnyOrder(
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.v_outer"),
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.v_inner"),
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.orders"),
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.nation"),
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.region"));
    }

    @Test
    void testIncludeTransitiveInputsFreshMaterializedViewDisabled()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEventWithFreshMaterializedView);

        assertThat(result.getInputs())
                .hasSize(1)
                .satisfiesExactly(input -> {
                    assertThat(input.getName()).isEqualTo("marquez.default.mv_fresh");
                    assertThat(input.getFacets().getSchema().getFields())
                            .extracting(field -> field.getName())
                            .containsExactly("nationkey", "name");
                });
    }

    @Test
    void testIncludeTransitiveInputsFreshMaterializedViewEnabled()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost",
                "openlineage-event-listener.include-transitive-inputs", "true"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEventWithFreshMaterializedView);

        assertThat(result.getInputs())
                .hasSize(1)
                .satisfiesExactly(input -> {
                    assertThat(input.getName()).isEqualTo("marquez.default.mv_fresh");
                    assertThat(input.getFacets().getSchema().getFields())
                            .extracting(field -> field.getName())
                            .containsExactly("nationkey", "name");
                });
    }

    @Test
    void testIncludeTransitiveInputsStaleMaterializedViewDisabled()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEventWithStaleMaterializedView);

        assertThat(result.getInputs())
                .hasSize(1)
                .satisfiesExactly(input -> {
                    assertThat(input.getName()).isEqualTo("marquez.default.mv_stale");
                    assertThat(input.getFacets().getSchema().getFields())
                            .extracting(field -> field.getName())
                            .containsExactly("nationkey", "name");
                });
    }

    @Test
    void testIncludeTransitiveInputsStaleMaterializedViewEnabled()
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost",
                "openlineage-event-listener.include-transitive-inputs", "true"));

        RunEvent result = listener.getCompletedEvent(TrinoEventData.queryCompleteEventWithStaleMaterializedView);

        assertThat(result.getInputs())
                .hasSize(2)
                .satisfiesExactlyInAnyOrder(
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.mv_stale"),
                        input -> assertThat(input.getName()).isEqualTo("marquez.default.nation"));
    }

    private static EventListener createEventListener(Map<String, String> config)
    {
        return new OpenLineageListenerFactory().create(ImmutableMap.copyOf(config), new TestingEventListenerContext());
    }
}
