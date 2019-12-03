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
package io.prestosql.tracer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.prestosql.spi.eventlistener.TracerEvent;
import io.prestosql.spi.tracer.DefaultTracer;
import io.prestosql.spi.tracer.Tracer;
import io.prestosql.spi.tracer.TracerEventType;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static io.prestosql.spi.tracer.TracerEventType.PLAN_QUERY_END;
import static io.prestosql.spi.tracer.TracerEventType.PLAN_QUERY_START;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestTracer
{
    @Test
    public void testTracer()
    {
        final String queryId = "query";
        final String stageId = "stage";
        final String taskId = "task";
        final String nodeId = "node";
        final URI testUri = URI.create("http://test.com");
        final TracerEventType queryEventType = PLAN_QUERY_START;
        final TracerEventType taskEventType = PLAN_QUERY_END;
        final Map<String, Object> payload = ImmutableMap.of("key", "value");

        final JsonCodec<Map<String, Object>> jsonCodec = JsonCodec.mapJsonCodec(String.class, Object.class);

        ImmutableList.Builder<TracerEvent> tracerEvents = new ImmutableList.Builder();
        Tracer queryTracer = DefaultTracer.createBasicTracer(tracerEvents::add, jsonCodec::toJson, nodeId, testUri, queryId);
        queryTracer.emitEvent(queryEventType, null);

        Tracer taskTracer = queryTracer.withStageId(stageId).withTaskId(taskId);
        taskTracer.emitEvent(taskEventType, () -> payload);

        List<TracerEvent> events = tracerEvents.build();
        assertEquals(events.size(), 2);

        TracerEvent queryEvent = events.get(0);
        assertEquals(queryEvent.getEventType(), queryEventType.name());
        assertEquals(queryEvent.getNodeId(), nodeId);
        assertEquals(queryEvent.getUri(), testUri.getHost() + ":" + String.valueOf(testUri.getPort()));
        assertEquals(queryEvent.getQueryId(), queryId);
        assertFalse(queryEvent.getStageId().isPresent());
        assertFalse(queryEvent.getTaskId().isPresent());
        assertFalse(queryEvent.getPipelineId().isPresent());
        assertFalse(queryEvent.getSplitId().isPresent());
        assertFalse(queryEvent.getOperatorId().isPresent());
        assertFalse(queryEvent.getPayload().isPresent());

        TracerEvent taskEvent = events.get(1);
        assertEquals(taskEvent.getEventType(), taskEventType.name());
        assertEquals(taskEvent.getNodeId(), nodeId);
        assertEquals(taskEvent.getUri(), testUri.getHost() + ":" + String.valueOf(testUri.getPort()));
        assertEquals(taskEvent.getQueryId(), queryId);
        assertEquals(taskEvent.getStageId().get(), stageId);
        assertEquals(taskEvent.getTaskId().get(), taskId);
        assertFalse(taskEvent.getPipelineId().isPresent());
        assertFalse(taskEvent.getSplitId().isPresent());
        assertFalse(taskEvent.getOperatorId().isPresent());
        assertEquals(taskEvent.getPayload().get(), jsonCodec.toJson(payload));
    }
}
