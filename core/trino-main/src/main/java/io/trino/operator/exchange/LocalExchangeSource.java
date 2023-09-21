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
package io.trino.operator.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.spi.Page;
import jakarta.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class LocalExchangeSource
{
    private static final ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();

    private final LocalExchangeMemoryManager memoryManager;
    private final Consumer<LocalExchangeSource> onFinish;

    @GuardedBy("this")
    private final Queue<Page> buffer = new ArrayDeque<>();

    private final AtomicLong bufferedBytes = new AtomicLong();
    private final AtomicInteger bufferedPages = new AtomicInteger();

    @Nullable
    @GuardedBy("this")
    private SettableFuture<Void> notEmptyFuture; // null indicates no callback has been registered

    private volatile boolean finishing;

    public LocalExchangeSource(LocalExchangeMemoryManager memoryManager, Consumer<LocalExchangeSource> onFinish)
    {
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.onFinish = requireNonNull(onFinish, "onFinish is null");
    }

    public LocalExchangeBufferInfo getBufferInfo()
    {
        // This must be lock free to assure task info creation is fast
        // Note: the stats my be internally inconsistent
        return new LocalExchangeBufferInfo(bufferedBytes.get(), bufferedPages.get());
    }

    void addPage(Page page)
    {
        assertNotHoldsLock();

        boolean added = false;
        SettableFuture<Void> notEmptyFuture = null;
        long retainedSizeInBytes = page.getRetainedSizeInBytes();
        synchronized (this) {
            // ignore pages after finish
            if (!finishing) {
                // buffered bytes must be updated before adding to the buffer to assure
                // the count does not go negative
                bufferedBytes.addAndGet(retainedSizeInBytes);
                bufferedPages.incrementAndGet();
                buffer.add(page);
                added = true;
            }

            // we just added a page (or we are finishing) so we are not empty
            if (this.notEmptyFuture != null) {
                notEmptyFuture = this.notEmptyFuture;
                this.notEmptyFuture = null;
            }
        }

        if (!added) {
            memoryManager.updateMemoryUsage(-retainedSizeInBytes);
        }

        // notify readers outside of lock since this may result in a callback
        if (notEmptyFuture != null) {
            notEmptyFuture.set(null);
        }
    }

    public WorkProcessor<Page> pages()
    {
        return WorkProcessor.create(() -> {
            Page page = removePage();
            if (page == null) {
                if (isFinished()) {
                    return ProcessState.finished();
                }

                ListenableFuture<Void> blocked = waitForReading();
                if (!blocked.isDone()) {
                    return ProcessState.blocked(blocked);
                }

                return ProcessState.yielded();
            }

            return ProcessState.ofResult(page);
        });
    }

    public Page removePage()
    {
        assertNotHoldsLock();

        // NOTE: there is no need to acquire a lock here. The buffer is concurrent
        // and buffered bytes is not expected to be consistent with the buffer (only
        // best effort).
        Page page;
        synchronized (this) {
            page = buffer.poll();
        }
        if (page == null) {
            return null;
        }

        // dereference the page outside of lock, since may trigger a callback
        long retainedSizeInBytes = page.getRetainedSizeInBytes();
        memoryManager.updateMemoryUsage(-retainedSizeInBytes);
        bufferedBytes.addAndGet(-retainedSizeInBytes);
        bufferedPages.decrementAndGet();

        checkFinished();

        return page;
    }

    public ListenableFuture<Void> waitForReading()
    {
        assertNotHoldsLock();
        // Fast path, definitely not blocked
        if (finishing || bufferedPages.get() > 0) {
            return NOT_BLOCKED;
        }

        synchronized (this) {
            // re-check after synchronizing
            if (finishing || bufferedPages.get() > 0) {
                return NOT_BLOCKED;
            }
            // if we need to block readers, and the current future is complete, create a new one
            if (notEmptyFuture == null) {
                notEmptyFuture = SettableFuture.create();
            }
            return notEmptyFuture;
        }
    }

    public boolean isFinished()
    {
        // Common case fast-path without synchronizing
        if (!finishing) {
            return false;
        }
        synchronized (this) {
            // Synchronize to ensure effects of an in-flight close() or finish() are observed
            return finishing && bufferedPages.get() == 0;
        }
    }

    public void finish()
    {
        assertNotHoldsLock();

        SettableFuture<Void> notEmptyFuture;
        synchronized (this) {
            if (finishing) {
                return;
            }
            finishing = true;

            // Unblock any waiters
            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = null;
        }

        // notify readers outside of lock since this may result in a callback
        if (notEmptyFuture != null) {
            notEmptyFuture.set(null);
        }

        checkFinished();
    }

    public void close()
    {
        assertNotHoldsLock();

        int remainingPagesCount = 0;
        long remainingPagesRetainedSizeInBytes = 0;
        SettableFuture<Void> notEmptyFuture;
        synchronized (this) {
            finishing = true;

            for (Page page : buffer) {
                remainingPagesCount++;
                remainingPagesRetainedSizeInBytes += page.getRetainedSizeInBytes();
            }
            buffer.clear();
            bufferedBytes.addAndGet(-remainingPagesRetainedSizeInBytes);
            bufferedPages.addAndGet(-remainingPagesCount);

            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = null;
        }

        // free all the remaining pages
        memoryManager.updateMemoryUsage(-remainingPagesRetainedSizeInBytes);

        // notify readers outside of lock since this may result in a callback
        if (notEmptyFuture != null) {
            notEmptyFuture.set(null);
        }

        // this will always fire the finished event
        checkState(isFinished(), "Expected buffer to be finished");
        checkFinished();
    }

    private void checkFinished()
    {
        assertNotHoldsLock();

        if (isFinished()) {
            // notify finish listener outside of lock, since it may make a callback
            // NOTE: due the race in this method, the onFinish may be called multiple times
            // it is expected that the implementer handles this (which is why this source
            // is passed to the function)
            onFinish.accept(this);
        }
    }

    @SuppressWarnings("checkstyle:IllegalToken")
    private void assertNotHoldsLock()
    {
        assert !Thread.holdsLock(this) : "Cannot execute this method while holding the lock";
    }
}
