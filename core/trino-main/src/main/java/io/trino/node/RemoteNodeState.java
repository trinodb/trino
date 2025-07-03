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
package io.trino.node;

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.client.NodeVersion;
import io.trino.server.ServerInfo;

import java.net.ConnectException;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.node.NodeState.ACTIVE;
import static io.trino.node.NodeState.GONE;
import static io.trino.node.NodeState.INACTIVE;
import static io.trino.node.NodeState.INVALID;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class RemoteNodeState
{
    private static final Logger log = Logger.get(RemoteNodeState.class);
    private static final JsonCodec<ServerInfo> SERVER_INFO_CODEC = jsonCodec(ServerInfo.class);

    private final URI serverUri;
    private final String expectedNodeEnvironment;
    private final NodeVersion expectedNodeVersion;
    private final HttpClient httpClient;
    private final URI infoUri;
    private final Ticker ticker;
    private final AtomicReference<InternalNode> internalNode = new AtomicReference<>();
    private final AtomicReference<NodeState> nodeState = new AtomicReference<>(INACTIVE);
    private final AtomicBoolean hasBeenActive = new AtomicBoolean(false);
    private final AtomicReference<ListenableFuture<Boolean>> future = new AtomicReference<>();
    private final AtomicLong lastSeenNanos;
    private final AtomicLong lastUpdateNanos;
    private final AtomicLong lastWarningLogged;

    public RemoteNodeState(URI serverUri, String expectedNodeEnvironment, NodeVersion expectedNodeVersion, HttpClient httpClient, Ticker ticker)
    {
        this.serverUri = requireNonNull(serverUri, "serverUri is null");
        this.expectedNodeEnvironment = requireNonNull(expectedNodeEnvironment, "expectedNodeEnvironment is null");
        this.expectedNodeVersion = requireNonNull(expectedNodeVersion, "expectedNodeVersion is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.infoUri = uriBuilderFrom(serverUri).appendPath("/v1/info").build();
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.lastSeenNanos = new AtomicLong(ticker.read());
        // Initialize to 30 seconds in the past to force immediate refresh
        this.lastUpdateNanos = new AtomicLong(ticker.read() - SECONDS.toNanos(30));
        this.lastWarningLogged = new AtomicLong(ticker.read() - SECONDS.toNanos(30));
    }

    public NodeState getState()
    {
        return nodeState.get();
    }

    public Optional<InternalNode> getInternalNode()
    {
        return Optional.ofNullable(internalNode.get());
    }

    public boolean hasBeenActive()
    {
        return hasBeenActive.get();
    }

    public void setSeen()
    {
        lastSeenNanos.set(ticker.read());
    }

    public boolean isMissing()
    {
        // If the node has not been seen for 30 seconds, it is considered missing.
        return lastSeenNanos.get() + SECONDS.toNanos(30) <= ticker.read();
    }

    public synchronized ListenableFuture<Boolean> asyncRefresh(boolean forceAndWait)
    {
        long millisSinceUpdate = millisSince(lastUpdateNanos.get());
        if (millisSinceUpdate > 10_000 && future.get() != null) {
            logWarning("Node state update request to %s has not returned in %sms", infoUri, millisSinceUpdate);
        }
        if (!forceAndWait && (millisSinceUpdate < 1_000 || future.get() != null)) {
            return requireNonNullElseGet(future.get(), () -> Futures.immediateFuture(true));
        }
        Request request = prepareGet()
                .setUri(infoUri)
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();
        HttpResponseFuture<JsonResponse<ServerInfo>> responseFuture = httpClient.executeAsync(request, createFullJsonResponseHandler(SERVER_INFO_CODEC));
        ListenableFuture<Boolean> transformSuccess = Futures.transform(
                responseFuture,
                result ->
                {
                    try {
                        // If the result is null, mark the node as INACTIVE to prevent work from being scheduled on it
                        NodeState nodeState;
                        if (result == null || !result.hasValue()) {
                            nodeState = INACTIVE;
                        }
                        else {
                            ServerInfo serverInfo = result.getValue();

                            // Set state to INVALID if the node is not in the expected environment or version
                            // This prevents the node from being visible outside the node manager
                            if (!serverInfo.environment().equals(expectedNodeEnvironment)) {
                                logWarning("Node environment mismatch: expected %s, got %s", expectedNodeEnvironment, serverInfo.environment());
                                nodeState = INVALID;
                            }
                            else if (!serverInfo.nodeVersion().equals(expectedNodeVersion)) {
                                logWarning("Node version mismatch: expected %s, got %s", expectedNodeVersion, serverInfo.nodeVersion());
                                nodeState = INVALID;
                            }
                            else {
                                nodeState = serverInfo.state();
                            }

                            internalNode.set(new InternalNode(
                                    serverInfo.nodeId(),
                                    serverUri,
                                    serverInfo.nodeVersion(),
                                    serverInfo.coordinator()));
                        }

                        RemoteNodeState.this.nodeState.set(nodeState);
                        if (nodeState == ACTIVE) {
                            hasBeenActive.set(true);
                        }
                        if (result == null || result.getStatusCode() != OK.code()) {
                            logWarning("Error fetching node state from %s", infoUri);
                        }
                        return nodeState == ACTIVE;
                    }
                    catch (Throwable e) {
                        // Any failure results in the node being marked as INACTIVE to prevent work from being scheduled on it
                        nodeState.set(INACTIVE);
                        logWarning("Error processing node state from %s: %s", infoUri, e.getMessage());
                        throw e;
                    }
                }, directExecutor());
        ListenableFuture<Boolean> catching = Futures.catching(
                transformSuccess,
                Throwable.class, t ->
                {
                    // Any failure results in the node being marked a GONE or INACTIVE to prevent work from being scheduled on it
                    nodeState.set(t instanceof ConnectException ? GONE : INACTIVE);
                    logWarning("Error fetching node state from %s: %s", infoUri, t.getMessage());
                    return false;
                }, directExecutor());

        future.compareAndSet(null, catching);
        addSuccessCallback(catching, _ -> {
            future.compareAndSet(catching, null);
            lastUpdateNanos.set(ticker.read());
        });

        return catching;
    }

    private long millisSince(long startNanos)
    {
        return (ticker.read() - startNanos) / MILLISECONDS.toNanos(1);
    }

    @FormatMethod
    private void logWarning(String format, Object... args)
    {
        // log at most once per second per node
        if (ticker.read() - lastWarningLogged.get() >= SECONDS.toNanos(1)) {
            log.warn(format, args);
            lastWarningLogged.set(ticker.read());
        }
    }
}
