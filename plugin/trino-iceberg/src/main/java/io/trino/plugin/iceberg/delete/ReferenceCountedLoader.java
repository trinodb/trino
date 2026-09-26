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

import com.google.common.annotations.VisibleForTesting;
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
     * If the executor rejects the load, which only happens once the pool is shut down at close, the returned future fails.
     */
    public ListenableFuture<?> load(String key, Runnable loader)
    {
        while (true) {
            Load newLoad = new Load(key, loader);
            Load load = loads.computeIfAbsent(key, _ -> newLoad);
            Optional<ListenableFuture<?>> reference = (load == newLoad) ? load.start() : load.tryAcquire();
            if (reference.isPresent()) {
                return reference.get();
            }
            // The load was concurrently abandoned, must retry
            loads.remove(key, load);
        }
    }

    @VisibleForTesting
    final class Load
    {
        private final String key;
        private final ListenableFutureTask<?> task;

        @GuardedBy("this")
        private int referenceCount;
        @GuardedBy("this")
        private State state = State.PENDING;

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
            if (state == State.ABANDONED) {
                return false;
            }
            state = State.STARTED;
            return true;
        }

        /**
         * Submits the load or returns empty if the load was concurrently abandoned.
         */
        public Optional<ListenableFuture<?>> start()
        {
            Optional<ListenableFuture<?>> reference = tryAcquire();
            if (reference.isEmpty()) {
                return Optional.empty();
            }
            try {
                executor.execute(task);
            }
            catch (RejectedExecutionException e) {
                // Only abandon a pending load, if a load has already started let it finish
                boolean abandoned;
                synchronized (this) {
                    abandoned = state == State.PENDING;
                    if (abandoned) {
                        state = State.ABANDONED;
                    }
                }
                if (abandoned) {
                    loads.remove(key, this);
                    task.cancel(false);
                }
                return reference;
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
                if (state == State.ABANDONED) {
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
                if (state == State.ABANDONED) {
                    return;
                }
                referenceCount--;
                // Let the load continue loading if:
                // 1. Another caller is still waiting for this load.
                // 2. Load is already started, cancelling a started load can leave behind partially applied state.
                if (referenceCount > 0 || state == State.STARTED) {
                    return;
                }
                state = State.ABANDONED;
            }

            // The load never started, so drop it and let a later caller schedule a fresh one.
            loads.remove(key, this);
            task.cancel(false);
        }

        private enum State
        {
            /**
             * Not yet running. The only state from which the load can still be abandoned.
             */
            PENDING,
            /**
             * Running or finished. Always runs to completion so a loader is never left partially applied.
             */
            STARTED,
            /**
             * Every caller gave up before the load started. The load will never run.
             */
            ABANDONED,
        }
    }
}
