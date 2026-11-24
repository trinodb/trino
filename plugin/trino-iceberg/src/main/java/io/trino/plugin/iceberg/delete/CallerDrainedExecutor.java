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
package io.trino.plugin.iceberg.delete;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import jakarta.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

/**
 * An {@link Executor} that queues tasks until the owner runs them with {@link #runPending()}, on the owner's thread.
 * <p>
 * Each split owns one instance. Delete file opens complete on the shared loading pool and queue their decode here,
 * so the thread reading the split runs the decode, which accounts its CPU to the split, and the split reports
 * itself blocked through {@link #taskAvailable()} while nothing is queued.
 * <p>
 * Closing drops queued and later tasks without completing their futures, since loads a closed split gave up on can
 * still complete on the pool. The owner cancels those futures before closing.
 */
@ThreadSafe
public final class CallerDrainedExecutor
        implements Executor
{
    @GuardedBy("this")
    private final Queue<Runnable> tasks = new ArrayDeque<>();
    // present only while the owner waits for a task
    @GuardedBy("this")
    @Nullable
    private SettableFuture<Void> taskAvailable;
    @GuardedBy("this")
    private boolean closed;

    @Override
    public void execute(Runnable task)
    {
        requireNonNull(task, "task is null");
        SettableFuture<Void> taskAvailable;
        synchronized (this) {
            if (closed) {
                return;
            }
            tasks.add(task);
            taskAvailable = this.taskAvailable;
            this.taskAvailable = null;
        }

        // notify the owner outside of lock since this may result in a callback
        if (taskAvailable != null) {
            taskAvailable.set(null);
        }
    }

    /**
     * Runs the queued tasks on the calling thread, including tasks queued while they run.
     * A task that throws leaves the tasks after it queued.
     */
    public void runPending()
    {
        while (true) {
            Runnable task;
            synchronized (this) {
                task = tasks.poll();
            }
            if (task == null) {
                return;
            }
            task.run();
        }
    }

    /**
     * Returns a future that completes once a task is queued. The future is already done if a task is queued or the executor is closed.
     */
    public synchronized ListenableFuture<Void> taskAvailable()
    {
        if (closed || !tasks.isEmpty()) {
            return immediateVoidFuture();
        }
        if (taskAvailable == null) {
            taskAvailable = SettableFuture.create();
        }
        return taskAvailable;
    }

    public void close()
    {
        SettableFuture<Void> taskAvailable;
        synchronized (this) {
            closed = true;
            tasks.clear();
            taskAvailable = this.taskAvailable;
            this.taskAvailable = null;
        }

        // notify the owner outside of lock since this may result in a callback
        if (taskAvailable != null) {
            taskAvailable.set(null);
        }
    }
}
