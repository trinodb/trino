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
package io.trino.execution.scheduler;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getDone;
import static java.util.Objects.requireNonNull;

/**
 * Runs one scheduling pass at a time. A pass returns empty on completion, or a future
 * indicating when the next pass can run. Only the worker executes scheduling logic.
 */
public final class QuerySchedulingTask
{
    private final Executor executor;
    private final Supplier<Optional<ListenableFuture<Void>>> process;
    private final Consumer<Optional<Throwable>> finish;
    private final SettableFuture<Void> completion = SettableFuture.create();

    private QuerySchedulingTask(Executor executor, Supplier<Optional<ListenableFuture<Void>>> process, Consumer<Optional<Throwable>> finish)
    {
        this.executor = requireNonNull(executor, "executor is null");
        this.process = requireNonNull(process, "process is null");
        this.finish = requireNonNull(finish, "finish is null");
    }

    /**
     * The returned future signals that the finish callback has completed, including after a
     * failed pass. Query retries use it to wait for cleanup of the previous attempt.
     */
    public static ListenableFuture<Void> submit(Executor executor, Supplier<Optional<ListenableFuture<Void>>> process, Consumer<Optional<Throwable>> finish)
    {
        QuerySchedulingTask task = new QuerySchedulingTask(executor, process, finish);
        task.enqueue(immediateVoidFuture());
        return task.completion;
    }

    private void enqueue(ListenableFuture<Void> ready)
    {
        try {
            executor.execute(() -> run(ready));
        }
        catch (Throwable t) {
            finish(Optional.of(t));
        }
    }

    private void run(ListenableFuture<Void> ready)
    {
        try {
            getDone(ready);
            Optional<ListenableFuture<Void>> next = process.get();
            if (next.isPresent()) {
                ListenableFuture<Void> wakeup = next.orElseThrow();
                wakeup.addListener(() -> enqueue(wakeup), directExecutor());
                return;
            }
        }
        catch (Throwable t) {
            finish(Optional.of(t));
            return;
        }
        finish(Optional.empty());
    }

    private void finish(Optional<Throwable> failure)
    {
        try {
            finish.accept(failure);
            completion.set(null);
        }
        catch (Throwable t) {
            completion.setException(t);
        }
    }
}
