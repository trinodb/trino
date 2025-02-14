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
package io.trino.server.tracing;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.InternalNodeManager.NodesSnapshot;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static java.util.Objects.requireNonNull;

public class CoordinatorDynamicTracingController
        extends WorkerDynamicTracingController
{
    private static final Logger log = Logger.get(CoordinatorDynamicTracingController.class);

    private final HttpClient httpClient;
    private final InternalNodeManager nodeManager;
    private final JsonCodec<TracingStatus> tracingStatusJsonCodec;

    @Inject
    public CoordinatorDynamicTracingController(
            SdkTracerProvider tracerProvider,
            DynamicTracingConfig config,
            @ForDynamicTracing HttpClient httpClient,
            InternalNodeManager nodeManager,
            JsonCodec<TracingStatus> tracingStatusJsonCodec)
    {
        super(tracerProvider, config);
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.tracingStatusJsonCodec = requireNonNull(tracingStatusJsonCodec, "tracingStatusJsonCodec is null");
    }

    @Override
    public TracingStatus enableExport()
    {
        ImmutableList.Builder<String> errorsBuilder = ImmutableList.builder();
        NodesSnapshot activeNodesSnapshot = nodeManager.getActiveNodesSnapshot();
        for (InternalNode node : activeNodesSnapshot.getAllNodes()) {
            if (node.equals(nodeManager.getCurrentNode())) {
                continue;
            }
            try {
                TracingStatus status = sendRequest(node, "v1/tracing");
                if (!status.enabled()) {
                    sendRequest(node, "v1/tracing/enable");
                    log.info("Enabled exporting OpenTelemetry traces on node %s with status %s", node.getNodeIdentifier(), status);
                }
            }
            catch (Exception e) {
                errorsBuilder.add("Node " + node.getNodeIdentifier() + " failed to enable export: " + e.getMessage());
            }
        }

        List<String> errors = errorsBuilder.build();
        TracingStatus status = super.enableExport();

        if (!errors.isEmpty()) {
            return new TracingStatus(
                    status.enabled(),
                    status.since(),
                    Optional.of("Encountered errors enabling export on worker nodes: " + Joiner.on(", ").join(errors)));
        }

        return status;
    }

    @Override
    public TracingStatus disableExport()
    {
        ImmutableList.Builder<String> errorsBuilder = ImmutableList.builder();
        NodesSnapshot activeNodesSnapshot = nodeManager.getActiveNodesSnapshot();
        for (InternalNode node : activeNodesSnapshot.getAllNodes()) {
            if (node.equals(nodeManager.getCurrentNode())) {
                continue;
            }
            try {
                TracingStatus status = sendRequest(node, "v1/tracing");
                if (status.enabled()) {
                    sendRequest(node, "v1/tracing/disable");
                    log.info("Disabled exporting OpenTelemetry traces on node %s with status %s", node.getNodeIdentifier(), status);
                }
            }
            catch (Exception e) {
                errorsBuilder.add("Node " + node.getNodeIdentifier() + " failed to disable export: " + e.getMessage());
            }
        }

        TracingStatus status = super.disableExport();
        List<String> errors = errorsBuilder.build();
        if (!errors.isEmpty()) {
            return new TracingStatus(
                    status.enabled(),
                    status.since(),
                    Optional.of("Encountered errors disabling export on worker nodes: " + Joiner.on(", ").join(errors)));
        }

        return status;
    }

    @Override
    public TracingStatus getStatus()
    {
        return super.getStatus();
    }

    public NodesTracingStatus getNodesStatus()
    {
        NodesSnapshot activeNodesSnapshot = nodeManager.getActiveNodesSnapshot();
        ImmutableMap.Builder<String, TracingStatus> nodes = ImmutableMap.builder();
        for (InternalNode node : activeNodesSnapshot.getAllNodes()) {
            if (node.equals(nodeManager.getCurrentNode())) {
                nodes.put(node.getNodeIdentifier(), getStatus());
                continue;
            }
            try {
                nodes.put(node.getNodeIdentifier(), sendRequest(node, "v1/tracing"));
            }
            catch (Exception e) {
                nodes.put(node.getNodeIdentifier(), new TracingStatus(false, null, Optional.of(e.getMessage())));
            }
        }
        return new NodesTracingStatus(nodes.buildOrThrow());
    }

    public record NodesTracingStatus(Map<String, TracingStatus> nodes)
    {
        public NodesTracingStatus {
            nodes = ImmutableMap.copyOf(nodes);
        }
    }

    private TracingStatus sendRequest(InternalNode node, String endpoint)
    {
        Request request = Request.builder()
                .setMethod("GET")
                .setUri(HttpUriBuilder.uriBuilderFrom(node.getInternalUri())
                        .appendPath(endpoint)
                        .build())
                .build();
        JsonResponse<TracingStatus> execute = httpClient.execute(request, createFullJsonResponseHandler(tracingStatusJsonCodec));
        if (execute.getStatusCode() != 200) {
            throw new IllegalStateException("Expected status code 200 but got " + execute.getStatusCode());
        }
        return execute.getValue();
    }
}
