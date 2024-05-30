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
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@SuppressWarnings("FieldNamingConvention")
@TestInstance(PER_METHOD)
class TestOpenLineageListener
{
    private final EventListenerFactory factory = new OpenLineageListenerFactory();

    @Test
    void testGetCompleteEvent()
            throws IllegalAccessException
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", "CONSOLE",
                "openlineage-event-listener.trino.uri", "http://testhost"));

        UUID runID = UUID.nameUUIDFromBytes("testGetCompleteEvent".getBytes(StandardCharsets.UTF_8));
        RunEvent result = listener.getCompletedEvent(runID, TrinoEventData.queryCompleteEvent);

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
                .isEqualTo(runID);

        assertThat(result)
                .extracting(RunEvent::getJob)
                .extracting(Job::getNamespace)
                .isEqualTo("trino://testhost");
    }

    @Test
    void testGetStartEvent()
            throws IllegalAccessException
    {
        OpenLineageListener listener = (OpenLineageListener) createEventListener(Map.of(
                "openlineage-event-listener.transport.type", OpenLineageTransport.CONSOLE.toString(),
                "openlineage-event-listener.trino.uri", "http://testhost:8080"));

        UUID runID = UUID.nameUUIDFromBytes("testGetStartEvent".getBytes(StandardCharsets.UTF_8));
        RunEvent result = listener.getStartEvent(runID, TrinoEventData.queryCreatedEvent);

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
                .isEqualTo(runID);

        assertThat(result)
                .extracting(RunEvent::getJob)
                .extracting(Job::getNamespace)
                .isEqualTo("trino://testhost:8080");
    }

    private EventListener createEventListener(Map<String, String> config)
    {
        return factory.create(ImmutableMap.copyOf(config));
    }
}
