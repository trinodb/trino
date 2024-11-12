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
package io.trino.plugin.base.util;

import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.opentelemetry.context.Context;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public final class ExecutorUtil
{
    private ExecutorUtil() {}

    /**
     * Process tasks in executors and additionally in calling thread.
     * Upon task execution failure, other tasks are canceled and interrupted, but not waited
     * for.
     * <p>
     * This method propagates {@link Context#current()} into tasks it starts within the executor.
     * <p>
     * Note: using this method allows simple parallelization of tasks within executor, when sub-tasks
     * are also scheduled in that executor, without risking starvation when pool is saturated.
     *
     * @throws ExecutionException if any task fails; exception cause is the first task failure
     */
    public static <T> List<T> processWithAdditionalThreads(Collection<Callable<T>> tasks, Executor executor)
            throws ExecutionException
    {
        List<Task<T>> wrapped = tasks.stream()
                .map(Task::new)
                .collect(toImmutableList());
        CompletionService<TaskResult<T>> completionService = new ExecutorCompletionService<>(executor);
        List<Future<?>> futures = new ArrayList<>(wrapped.size());
        Context tracingContext = Context.current();

        try {
            // schedule in the executor
            for (int i = 0; i < wrapped.size(); i++) {
                int index = i;
                Task<T> task = wrapped.get(i);
                futures.add(completionService.submit(() -> {
                    if (!task.take()) {
                        return null; // will be ignored
                    }
                    try (var _ = tracingContext.makeCurrent()) {
                        return new TaskResult<>(index, task.callable.call());
                    }
                }));
            }

            List<T> results = new ArrayList<>(nCopies(wrapped.size(), null));
            int pending = wrapped.size();
            // process in the calling thread (in reverse order, as an optimization)
            for (int i = wrapped.size() - 1; i >= 0; i--) {
                // process ready results to fail fast on exceptions
                for (Future<TaskResult<T>> ready = completionService.poll(); ready != null; ready = completionService.poll()) {
                    TaskResult<T> taskResult = ready.get();
                    // Null result means task was processed by the calling thread
                    if (taskResult != null) {
                        results.set(taskResult.taskIndex(), taskResult.result());
                        pending--;
                    }
                }
                Task<T> task = wrapped.get(i);
                if (!task.take()) {
                    continue;
                }
                try {
                    results.set(i, task.callable.call());
                    pending--;
                }
                catch (Exception e) {
                    throw new ExecutionException(e);
                }
            }

            while (pending > 0) {
                TaskResult<T> taskResult = completionService.take().get();
                // Null result means task was processed by the calling thread
                if (taskResult != null) {
                    results.set(taskResult.taskIndex(), taskResult.result());
                    pending--;
                }
            }

            return results;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted", e);
        }
        finally {
            futures.forEach(future -> future.cancel(true));
        }
    }

    @ThreadSafe
    private static final class Task<T>
    {
        private final Callable<T> callable;
        @GuardedBy("this")
        private boolean taken;

        public Task(Callable<T> callable)
        {
            this.callable = requireNonNull(callable, "callable is null");
        }

        public synchronized boolean take()
        {
            if (taken) {
                return false;
            }
            taken = true;
            return true;
        }
    }

    private record TaskResult<T>(int taskIndex, T result) {}
}
