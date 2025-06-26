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
package io.trino.metadata;

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
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
import jakarta.annotation.Nullable;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.metadata.NodeState.ACTIVE;
import static io.trino.metadata.NodeState.INACTIVE;
import static io.trino.metadata.NodeState.INVALID;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static java.util.Objects.requireNonNull;
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
    private final AtomicReference<Future<?>> future = new AtomicReference<>();
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
        // Initialize to 30 in the past to force immediate refresh
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

    public synchronized void asyncRefresh()
    {
        long millisSinceUpdate = millisSince(lastUpdateNanos.get());
        if (millisSinceUpdate > 10_000 && future.get() != null) {
            logWarning("Node state update request to %s has not returned in %sms", infoUri, millisSinceUpdate);
        }
        if (millisSinceUpdate >= 1_000 && future.get() == null) {
            Request request = prepareGet()
                    .setUri(infoUri)
                    .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                    .build();
            HttpResponseFuture<JsonResponse<ServerInfo>> responseFuture = httpClient.executeAsync(request, createFullJsonResponseHandler(SERVER_INFO_CODEC));
            future.compareAndSet(null, responseFuture);

            Futures.addCallback(responseFuture, new FutureCallback<>()
            {
                @Override
                public void onSuccess(@Nullable JsonResponse<ServerInfo> result)
                {
                    try {
                        // If the result is null, mark the node as INACTIVE to prevent work from being scheduled on it
                        NodeState nodeState;
                        if (result == null || !result.hasValue()) {
                            nodeState = ACTIVE;
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
                    }
                    catch (Throwable e) {
                        // Any failure results in the node being marked as INACTIVE to prevent work from being scheduled on it
                        nodeState.set(INACTIVE);
                        logWarning("Error processing node state from %s: %s", infoUri, e.getMessage());
                        throw e;
                    }
                    finally {
                        lastUpdateNanos.set(ticker.read());
                        future.compareAndSet(responseFuture, null);
                    }
                }

                @Override
                public void onFailure(Throwable t)
                {
                    // Any failure results in the node being marked as INACTIVE to prevent work from being scheduled on it
                    nodeState.set(INACTIVE);
                    logWarning("Error fetching node state from %s: %s", infoUri, t.getMessage());
                    lastUpdateNanos.set(ticker.read());
                    future.compareAndSet(responseFuture, null);
                }
            }, directExecutor());
        }
    }

    private long millisSince(long startNanos)
    {
        return (ticker.read() - startNanos) / 1_000_000;
    }

    @FormatMethod
    private void logWarning(String format, Object... args)
    {
        // log at most once per second per node
        if (ticker.read() - lastWarningLogged.get() >= 1_000_000_000) {
            log.warn(format, args);
            lastWarningLogged.set(ticker.read());
        }
    }
}
