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
import io.airlift.log.Logger;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getDone;
import static java.util.Objects.requireNonNull;

/**
 * Runs a load at most once per path, sharing it with everyone that asks for the same path.
 * A load transitions through {@link Load.State} described below.
 * Notably OPENING and DECODING are separate states to allow for separation of IO/CPU bound work.
 * Reference counts the loads such that a cancellation by one caller only cancels the load if the reference
 * count drops to 0. Each caller gets its own future, so cancelling one never affects the others.
 * Queued loads are canceled when their last caller cancels.
 * Started opens can be canceled but started decodes always run to completion since decoding can result in side effects.
 */
@ThreadSafe
final class ReferenceCountedLoader<R extends Closeable>
{
    private static final Logger log = Logger.get(ReferenceCountedLoader.class);

    private final Map<String, Load> loads = new ConcurrentHashMap<>();

    /**
     * Loads {@code path} on the given executors, deduping concurrent loads.
     * Cancelling the future only cancels the load if the reference count drops to 0 and the decode has not yet started.
     * If the {@code openExecutor} rejects the load, which only happens once the pool is shut down at close, the returned future is canceled.
     */
    public ListenableFuture<?> load(String path, Executor openExecutor, Callable<R> open, Executor decodeExecutor, Consumer<R> decode)
    {
        requireNonNull(path, "path is null");
        requireNonNull(openExecutor, "openExecutor is null");
        requireNonNull(open, "open is null");
        requireNonNull(decodeExecutor, "decodeExecutor is null");
        requireNonNull(decode, "decode is null");
        while (true) {
            Load newLoad = new Load(path, openExecutor, open, decode);
            Load load = loads.computeIfAbsent(path, _ -> newLoad);
            Optional<ListenableFuture<?>> reference = (load == newLoad) ? load.start(decodeExecutor) : load.tryAcquire(decodeExecutor);
            if (reference.isPresent()) {
                return reference.get();
            }
            // The load was concurrently abandoned, must retry
            loads.remove(path, load);
        }
    }

    @VisibleForTesting
    final class Load
    {
        private final String path;
        private final Executor openExecutor;
        private final ListenableFutureTask<Void> openTask;
        // ListenableFutureTask guarantees the decodeTask is only run once
        private final ListenableFutureTask<Void> decodeTask;

        @GuardedBy("this")
        private int referenceCount;
        @GuardedBy("this")
        private State state = State.PENDING;
        @GuardedBy("this")
        private R opened;

        public Load(String path, Executor openExecutor, Callable<R> open, Consumer<R> decode)
        {
            this.path = path;
            this.openExecutor = openExecutor;
            this.openTask = ListenableFutureTask.create(() -> {
                if (tryStartOpen()) {
                    finishOpen(requireNonNull(open.call(), "open returned null"));
                }
                return null;
            });
            this.decodeTask = ListenableFutureTask.create(() -> {
                getDone(openTask);
                Optional<R> toDecode = tryStartDecode();
                checkState(toDecode.isPresent(), "Decode of an abandoned load of %s", path);
                try (R closing = toDecode.get()) {
                    decode.accept(closing);
                }
                return null;
            });
        }

        /**
         * Marks the open as started or returns false if every caller gave up before the executor got to it.
         */
        private synchronized boolean tryStartOpen()
        {
            if (state == State.ABANDONED) {
                return false;
            }
            state = State.OPENING;
            return true;
        }

        private void finishOpen(R opened)
        {
            synchronized (this) {
                if (state != State.ABANDONED) {
                    state = State.OPENED;
                    this.opened = opened;
                    return;
                }
            }
            // every caller gave up while the open was running, and it was not interrupted in time
            closeAbandoned(opened);
        }

        /**
         * Marks the decode as started or returns empty if every caller gave up before a decode executor got to it.
         */
        private synchronized Optional<R> tryStartDecode()
        {
            if (state == State.ABANDONED) {
                return Optional.empty();
            }
            checkState(state == State.OPENED, "Decode started in state %s", state);
            state = State.DECODING;
            R toDecode = opened;
            opened = null;
            return Optional.of(toDecode);
        }

        /**
         * Takes the first reference and submits the open or returns empty if the load was concurrently abandoned.
         */
        public Optional<ListenableFuture<?>> start(Executor decodeExecutor)
        {
            Optional<ListenableFuture<?>> reference = tryAcquire(decodeExecutor);
            if (reference.isEmpty()) {
                return Optional.empty();
            }
            try {
                openExecutor.execute(openTask);
            }
            catch (RejectedExecutionException e) {
                // the open never ran and this caller still holds its reference, so the load is still pending
                synchronized (this) {
                    checkState(state == State.PENDING, "Rejected load is in state %s", state);
                    state = State.ABANDONED;
                }
                loads.remove(path, this);
                decodeTask.cancel(false);
                openTask.cancel(false);
            }
            return reference;
        }

        /**
         * Takes one more reference to the load, or returns empty if the load has already been abandoned.
         */
        public Optional<ListenableFuture<?>> tryAcquire(Executor decodeExecutor)
        {
            ListenableFuture<?> reference;
            synchronized (this) {
                if (state == State.ABANDONED) {
                    return Optional.empty();
                }
                referenceCount++;
                // do not propagate cancellation, as the load is shared with the other callers
                reference = Futures.nonCancellationPropagating(decodeTask);
            }
            reference.addListener(this::release, directExecutor());
            openTask.addListener(() -> decodeExecutor.execute(decodeTask), directExecutor());
            return Optional.of(reference);
        }

        /**
         * Invoked when a reference completes, either because the load finished or because its caller canceled it.
         */
        private void release()
        {
            R toClose = null;
            synchronized (this) {
                if (state == State.ABANDONED) {
                    return;
                }
                referenceCount--;
                // Let the load continue if:
                // 1. Another caller is still waiting for this load.
                // 2. The decode started, see the class comment.
                // 3. The load completed, which includes a failed open, so that the failure is shared like any other result.
                if (referenceCount > 0 || state == State.DECODING || decodeTask.isDone()) {
                    return;
                }
                switch (state) {
                    // a running open is interrupted below, and closes what it opened in finishOpen if it completes anyway
                    case PENDING, OPENING -> {}
                    case OPENED -> {
                        toClose = opened;
                        opened = null;
                    }
                    case DECODING, ABANDONED -> throw new IllegalStateException("Unexpected state " + state);
                }
                state = State.ABANDONED;
            }

            // The load never decoded, so drop it and let a later caller schedule a fresh one.
            loads.remove(path, this);
            decodeTask.cancel(false);
            if (toClose != null) {
                closeAbandoned(toClose);
            }
            else {
                // opening writes no shared state, so it can be interrupted
                openTask.cancel(true);
            }
        }

        private void closeAbandoned(R toClose)
        {
            try {
                toClose.close();
            }
            catch (Exception e) {
                log.warn(e, "Failed to close abandoned load of %s", path);
            }
        }

        @Override
        public synchronized String toString()
        {
            return "Load[path=%s, state=%s, referenceCount=%s]".formatted(path, state, referenceCount);
        }

        private enum State
        {
            /**
             * The open is not yet running. Abandoning the load skips the open.
             */
            PENDING,
            /**
             * The open is running. Abandoning the load interrupts it, and closes what it opened if it completes anyway.
             */
            OPENING,
            /**
             * The open completed and what it opened waits to be decoded. Abandoning the load closes it.
             */
            OPENED,
            /**
             * The decode is running or finished. Always runs to completion, see the class comment.
             */
            DECODING,
            /**
             * Every caller gave up before the decode started. The load will never complete.
             */
            ABANDONED,
        }
    }
}
