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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.FormatMethod;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.log.Logger;
import jakarta.annotation.PreDestroy;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Predicate.not;

public class AnnounceNodeAnnouncer
        implements Announcer
{
    private static final Logger log = Logger.get(AnnounceNodeAnnouncer.class);

    private final HttpClient httpClient;
    private final List<URI> announceUris;
    private final StaticBodyGenerator currentHostAnnouncement;

    private final ScheduledExecutorService announceExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("node-state-poller"));
    private final AtomicLong lastWarningLogged = new AtomicLong(0);

    private final AtomicBoolean started = new AtomicBoolean();
    private final boolean coordinator;

    @Inject
    public AnnounceNodeAnnouncer(InternalNode currentNode, AnnounceNodeAnnouncerConfig config, @ForAnnouncer HttpClient httpClient)
    {
        this(currentNode.getInternalUri(), config.getCoordinatorUris(), currentNode.isCoordinator(), httpClient);
    }

    @VisibleForTesting
    public AnnounceNodeAnnouncer(URI internalUri, Collection<URI> coordinatorUris, boolean coordinator, HttpClient httpClient)
    {
        URI currentUri = internalUri;
        this.announceUris = coordinatorUris.stream()
                .filter(not(currentUri::equals))
                .map(uri -> uriBuilderFrom(uri).appendPath("/v1/announce").build())
                .distinct()
                .toList();
        this.coordinator = coordinator;
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.currentHostAnnouncement = createStaticBodyGenerator(currentUri.toString(), UTF_8);
    }

    @Override
    public void start()
    {
        if (announceUris.isEmpty()) {
            if (!coordinator) {
                log.warn("No coordinator URIs configured, skipping node state announcements");
            }
            return;
        }

        checkState(!announceExecutor.isShutdown(), "Announcer has been destroyed");
        if (!started.compareAndSet(false, true)) {
            return;
        }

        announceExecutor.scheduleWithFixedDelay(
                () -> {
                    try {
                        forceAnnounce();
                    }
                    catch (RuntimeException e) {
                        // this should not happen, but if it does we do not want to stop announcing
                        log.error(e, "Error announcing node to coordinators");
                    }
                }, 5, 5, SECONDS);
        forceAnnounce();
    }

    @Override
    public ListenableFuture<?> forceAnnounce()
    {
        return Futures.allAsList(announceUris.stream()
                .map(this::announce)
                .toList());
    }

    private ListenableFuture<?> announce(URI announceUri)
    {
        Request request = preparePost()
                .setUri(announceUri)
                .setBodyGenerator(currentHostAnnouncement)
                .setHeader(HttpHeaders.CONTENT_TYPE, TEXT_PLAIN)
                .build();
        ListenableFuture<StatusResponse> responseFuture = httpClient.executeAsync(request, createStatusResponseHandler());

        addSuccessCallback(responseFuture, response -> {
            int statusCode = response.getStatusCode();
            if (statusCode < 200 || statusCode >= 300) {
                logWarning("Failed to announce node state to %s: %s", announceUri, statusCode);
            }
        });
        addExceptionCallback(responseFuture, t -> logWarning("Error announcing node state to %s: %s", announceUri, t.getMessage()));
        return Futures.nonCancellationPropagating(responseFuture);
    }

    @PreDestroy
    @Override
    public void stop()
    {
        announceExecutor.shutdownNow();
        try {
            announceExecutor.awaitTermination(30, SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @FormatMethod
    private void logWarning(String format, Object... args)
    {
        // log at most once per second per node
        long now = System.nanoTime();
        if (now - lastWarningLogged.get() >= SECONDS.toNanos(1)) {
            log.warn(format, args);
            lastWarningLogged.set(now);
        }
    }
}
