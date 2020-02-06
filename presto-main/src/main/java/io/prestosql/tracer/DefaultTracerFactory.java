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

import io.airlift.http.server.HttpServerInfo;
import io.airlift.json.JsonCodec;
import io.airlift.node.NodeInfo;
import io.prestosql.Session;
import io.prestosql.eventlistener.EventListenerManager;
import io.prestosql.server.InternalCommunicationConfig;
import io.prestosql.spi.tracer.DefaultTracer;
import io.prestosql.spi.tracer.NoOpTracer;
import io.prestosql.spi.tracer.Tracer;

import javax.inject.Inject;

import java.net.URI;
import java.util.Map;

import static io.prestosql.SystemSessionProperties.isQueryDebuggingTracerEnabled;
import static java.util.Objects.requireNonNull;

public final class DefaultTracerFactory
        implements TracerFactory
{
    private final EventListenerManager eventListenerManager;
    private final NodeInfo nodeInfo;
    private final URI uri;
    private final JsonCodec<Map<String, Object>> jsonCodec = JsonCodec.mapJsonCodec(String.class, Object.class);

    @Inject
    public DefaultTracerFactory(EventListenerManager eventListenerManager, NodeInfo nodeInfo, HttpServerInfo httpServerInfo, InternalCommunicationConfig config)
    {
        this.eventListenerManager = requireNonNull(eventListenerManager, "eventListenerManger is null");
        this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
        this.uri = requireNonNull(config.isHttpsRequired() ? httpServerInfo.getHttpsUri() : httpServerInfo.getHttpUri(), "uri is null");
    }

    @Override
    public Tracer createTracer(String queryId, Session session)
    {
        if (isQueryDebuggingTracerEnabled(session)) {
            return DefaultTracer.createBasicTracer(
                    eventListenerManager::tracerEventOccurred,
                    jsonCodec::toJson,
                    nodeInfo.getNodeId(),
                    uri,
                    queryId);
        }
        return new NoOpTracer();
    }
}
