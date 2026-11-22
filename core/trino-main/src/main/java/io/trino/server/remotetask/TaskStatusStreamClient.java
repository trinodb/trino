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
package io.trino.server.remotetask;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StreamingResponse;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStatus;
import jakarta.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.http.client.HeaderNames.ACCEPT;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.trino.server.InternalHeaders.TRINO_MAX_WAIT_HEADER;
import static io.trino.server.InternalHeaders.TRINO_NODE_INSTANCE_ID_HEADER;
import static jakarta.ws.rs.core.MediaType.SERVER_SENT_EVENTS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class TaskStatusStreamClient
{
    private static final Logger log = Logger.get(TaskStatusStreamClient.class);
    private static final int MAX_EVENT_SIZE = 16 * 1024 * 1024;

    private final URI uri;
    private final String nodeInstanceId;
    private final HttpClient httpClient;
    private final Duration refreshInterval;
    private final Duration infoInterval;
    private final JsonCodec<TaskStatus> codec;
    private final RemoteTaskStats stats;
    private final Map<TaskId, ContinuousTaskStatusFetcher> members = new HashMap<>();

    @Nullable
    private Thread reader;
    @Nullable
    private String streamId;
    private boolean ready;
    private boolean closed;
    private long lastFailure;

    public TaskStatusStreamClient(
            URI uri,
            String nodeInstanceId,
            HttpClient httpClient,
            Duration refreshInterval,
            Duration infoInterval,
            JsonCodec<TaskStatus> codec,
            RemoteTaskStats stats)
    {
        this.uri = uri;
        this.nodeInstanceId = nodeInstanceId;
        this.httpClient = httpClient;
        this.refreshInterval = refreshInterval;
        this.infoInterval = infoInterval;
        this.codec = codec;
        this.stats = stats;
    }

    synchronized boolean subscribe(TaskId id, ContinuousTaskStatusFetcher fetcher)
    {
        if (ready && !closed && !fetcher.getTaskStatus().state().isDone()) {
            members.put(id, fetcher);
            return true;
        }
        if (!closed && reader == null && (lastFailure == 0 || System.nanoTime() - lastFailure > SECONDS.toNanos(10))) {
            open();
        }
        return false;
    }

    // One blocking I/O reader per worker. The query scheduling and callback pools stay available.
    private void open()
    {
        String id = UUID.randomUUID().toString();
        streamId = id;
        Request get = prepareGet()
                .setUri(uriBuilderFrom(uri.resolve(id)).addParameter("infoIntervalMillis", Long.toString(infoInterval.toMillis())).build())
                .setHeader(ACCEPT, SERVER_SENT_EVENTS)
                .setHeader(TRINO_NODE_INSTANCE_ID_HEADER, nodeInstanceId)
                .setHeader(TRINO_MAX_WAIT_HEADER, refreshInterval.toString())
                .setIdleTimeout(new Duration(2 * refreshInterval.toMillis() + 5000, MILLISECONDS))
                .setRequestTimeout(new Duration(0, SECONDS))
                .build();
        reader = Thread.ofVirtual().name("task-status-stream-" + nodeInstanceId).start(() -> {
            try (StreamingResponse response = httpClient.executeStreaming(get)) {
                checkState(response.getStatusCode() == 200 && response.getHeader(CONTENT_TYPE).orElse("").split(";", 2)[0].equals(SERVER_SENT_EVENTS),
                        "Unexpected task status stream response: %s",
                        response.getStatusCode());
                readEvents(response.getInputStream());
            }
            catch (IOException | RuntimeException e) {
                log.debug(e, "Task status stream closed for %s", uri);
            }
            finally {
                disconnected();
            }
        });
    }

    synchronized void unsubscribe(TaskId task)
    {
        members.remove(task);
    }

    synchronized Optional<String> tokenFor(TaskId task)
    {
        return members.containsKey(task) ? Optional.of(streamId) : Optional.empty();
    }

    private void disconnected()
    {
        List<ContinuousTaskStatusFetcher> fallback;
        synchronized (this) {
            ready = false;
            reader = null;
            streamId = null;
            lastFailure = System.nanoTime();
            fallback = List.copyOf(members.values());
            members.clear();
        }
        for (ContinuousTaskStatusFetcher member : fallback) {
            stats.recordStreamFallbackTask();
            member.startAfterStreamClosed();
        }
    }

    public synchronized void close()
    {
        closed = true;
        members.clear();
        if (reader != null) {
            reader.interrupt();
        }
    }

    // The worker sends one compact JSON data line per event. A network read may split
    // a UTF-8 character or contain many events. Bound memory independently of stream lifetime.
    private void readEvents(InputStream input)
            throws IOException
    {
        byte[] buffer = new byte[8192];
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        TaskStatus pending = null;
        for (int length; (length = input.read(buffer)) != -1; ) {
            int start = 0;
            for (int index = 0; index < length; index++) {
                if (buffer[index] != '\n') {
                    continue;
                }
                append(line, buffer, start, index);
                String value = line.toString(UTF_8).stripTrailing();
                line.reset();
                start = index + 1;
                if (value.startsWith(":")) {
                    synchronized (this) {
                        if (!closed && !ready) {
                            ready = true;
                            stats.recordStreamConnection();
                        }
                    }
                }
                else if (value.startsWith("data: ")) {
                    pending = codec.fromJson(value.substring(6));
                }
                else if (value.isEmpty()) {
                    if (pending != null) {
                        ContinuousTaskStatusFetcher member;
                        synchronized (this) {
                            member = members.get(pending.taskId());
                        }
                        if (member != null) {
                            stats.recordStreamEvent();
                            member.updateTaskStatus(pending);
                        }
                        pending = null;
                    }
                }
                else {
                    throw new IOException("Invalid task status event");
                }
            }
            append(line, buffer, start, length);
        }
    }

    private static void append(ByteArrayOutputStream line, byte[] buffer, int start, int end)
    {
        checkState(line.size() + end - start <= MAX_EVENT_SIZE, "Task status event too large");
        line.write(buffer, start, end - start);
    }
}
