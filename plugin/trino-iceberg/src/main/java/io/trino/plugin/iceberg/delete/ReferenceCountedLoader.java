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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

/**
 * Runs a load at most once per key, sharing it with everyone that asks for the same key.
 * Reference counts the loads such that a cancellation by one caller only cancels the load if the reference
 * count drops to 0. Each caller gets its own future, so cancelling one never affects the others.
 * A load that has started always runs to completion, even once every caller has given up, so a loader is
 * never left half applied.
 */
@ThreadSafe
final class ReferenceCountedLoader
{
    private final Executor executor;
    private final Map<String, Load> loads = new ConcurrentHashMap<>();

    public ReferenceCountedLoader(Executor executor)
    {
        this.executor = requireNonNull(executor, "executor is null");
    }

    /**
     * Loads {@code key} on the executor, deduping concurrent loads.
     * Cancelling the future only cancels the load if the reference count drops to 0 and the load has not yet started.
     *
     * @throws RejectedExecutionException if the load could not be scheduled
     */
    public ListenableFuture<?> load(String key, Runnable loader)
    {
        while (true) {
            Load newLoad = new Load(key, loader);
            Load load = loads.computeIfAbsent(key, _ -> newLoad);
            if (load == newLoad) {
                return load.start();
            }
            Optional<ListenableFuture<?>> reference = load.tryAcquire();
            if (reference.isPresent()) {
                return reference.get();
            }
            // The load was abandoned between computeIfAbsent and tryAcquire.
            loads.remove(key, load);
        }
    }

    private final class Load
    {
        private final String key;
        private final ListenableFutureTask<?> task;

        @GuardedBy("this")
        private int referenceCount;
        @GuardedBy("this")
        private boolean abandoned;
        @GuardedBy("this")
        private boolean started;

        public Load(String key, Runnable loader)
        {
            this.key = requireNonNull(key, "key is null");
            this.task = ListenableFutureTask.create(() -> {
                if (tryStart()) {
                    loader.run();
                }
            }, null);
        }

        /**
         * Marks the load as started, or returns false if every caller gave up before the executor got to it.
         */
        private synchronized boolean tryStart()
        {
            if (abandoned) {
                return false;
            }
            started = true;
            return true;
        }

        /**
         * Submits the load, holding the first reference to it.
         */
        public ListenableFuture<?> start()
        {
            ListenableFuture<?> reference = tryAcquire().orElseThrow();
            try {
                executor.execute(task);
            }
            catch (RejectedExecutionException e) {
                // The load will never run, so abandon it instead of leaving a later caller waiting on it forever.
                synchronized (this) {
                    abandoned = true;
                }
                loads.remove(key, this);
                task.cancel(false);
                throw e;
            }
            return reference;
        }

        /**
         * Takes one more reference to the load, or returns empty if the load has already been abandoned.
         */
        public Optional<ListenableFuture<?>> tryAcquire()
        {
            ListenableFuture<?> reference;
            synchronized (this) {
                if (abandoned) {
                    return Optional.empty();
                }
                referenceCount++;
                // do not propagate cancellation, as the load is shared with the other callers
                reference = Futures.nonCancellationPropagating(task);
            }
            reference.addListener(this::release, directExecutor());
            return Optional.of(reference);
        }

        /**
         * Invoked when a reference completes, either because the load finished or because its caller canceled it.
         */
        private void release()
        {
            synchronized (this) {
                if (abandoned || task.isDone()) {
                    return;
                }
                referenceCount--;
                // Let the load continue loading if:
                // 1. Another caller is still waiting for this load.
                // 2. Load is already started, cancelling a started load can leave behind partially applied state.
                if (referenceCount > 0 || started) {
                    return;
                }
                abandoned = true;
            }

            // The load never started, so drop it and let a later caller schedule a fresh one.
            loads.remove(key, this);
            task.cancel(false);
        }
    }
}
