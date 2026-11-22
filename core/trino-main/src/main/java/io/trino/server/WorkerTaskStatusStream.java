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
package io.trino.server;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.execution.SqlTask;
import io.trino.execution.TaskStatus;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getDone;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

final class WorkerTaskStatusStream
{
    private static final byte[] DATA = "data: ".getBytes(UTF_8);
    private static final byte[] END = "\n\n".getBytes(UTF_8);
    private static final byte[] HEARTBEAT = ":\n\n".getBytes(UTF_8);

    private final JsonCodec<TaskStatus> codec;
    private final long refreshNanos;
    private final Map<SqlTask, ListenableFuture<TaskStatus>> watches = new HashMap<>();
    private final Set<SqlTask> ready = new HashSet<>();
    private boolean closed;

    WorkerTaskStatusStream(JsonCodec<TaskStatus> codec, Duration refreshInterval)
    {
        this.codec = codec;
        this.refreshNanos = refreshInterval.roundTo(NANOSECONDS);
    }

    public synchronized void watch(SqlTask task, long version)
    {
        if (!closed && !watches.containsKey(task)) {
            arm(task, version);
        }
    }

    private void arm(SqlTask task, long version)
    {
        ListenableFuture<TaskStatus> future = task.getTaskStatus(version);
        watches.put(task, future);
        future.addListener(() -> signal(task), directExecutor());
    }

    private synchronized void signal(SqlTask task)
    {
        if (!closed && ready.add(task)) {
            notifyAll();
        }
    }

    public void writeTo(OutputStream output)
            throws IOException
    {
        output.write(HEARTBEAT);
        output.flush();
        long nextRefresh = System.nanoTime() + refreshNanos;
        long lastActive = System.nanoTime();
        while (true) {
            List<SqlTask> batch;
            synchronized (this) {
                while (!closed && ready.isEmpty() && System.nanoTime() - nextRefresh < 0) {
                    try {
                        NANOSECONDS.timedWait(this, nextRefresh - System.nanoTime());
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new InterruptedIOException();
                    }
                }
                if (closed) {
                    return;
                }
                if (System.nanoTime() - nextRefresh >= 0) {
                    // Some counters change without a task status version change.
                    ready.addAll(watches.keySet());
                    nextRefresh = System.nanoTime() + refreshNanos;
                }
                batch = new ArrayList<>(ready);
                ready.clear();
            }
            if (batch.isEmpty()) {
                if (System.nanoTime() - lastActive >= SECONDS.toNanos(60)) {
                    return;
                }
                output.write(HEARTBEAT);
            }
            else {
                lastActive = System.nanoTime();
                for (SqlTask task : batch) {
                    TaskStatus status;
                    synchronized (this) {
                        ListenableFuture<TaskStatus> future = watches.get(task);
                        if (future == null) {
                            continue;
                        }
                        if (future.isDone()) {
                            status = getDone(future);
                            if (status.state().isDone()) {
                                watches.remove(task);
                            }
                            else {
                                arm(task, status.version());
                            }
                        }
                        else {
                            status = task.getTaskStatus();
                        }
                    }
                    output.write(DATA);
                    output.write(codec.toJsonBytes(status));
                    output.write(END);
                }
            }
            output.flush();
        }
    }

    public synchronized void close()
    {
        closed = true;
        notifyAll();
        watches.values().forEach(future -> future.cancel(false));
        watches.clear();
        ready.clear();
    }
}
