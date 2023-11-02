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
package io.trino.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.util.LockUtils.CloseableLock;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.util.LockUtils.closeable;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Methods setWithDelay and reset should be used in pairs;
 * usually follow the following idiom:
 * <pre> {@code
 * DriverYieldSignal signal = ...;
 * signal.setWithDelay(duration, executor);
 * try {
 *     // block
 * } finally {
 *     signal.reset();
 * }
 * }</pre>
 */
@ThreadSafe
public class DriverYieldSignal
{
    private final ReentrantLock lock = new ReentrantLock();

    @GuardedBy("lock")
    private long runningSequence;

    @GuardedBy("lock")
    private boolean terminationStarted;

    @GuardedBy("lock")
    private ScheduledFuture<?> yieldFuture;

    private final AtomicBoolean yield = new AtomicBoolean();

    public void setWithDelay(long maxRunNanos, ScheduledExecutorService executor)
    {
        try (CloseableLock<ReentrantLock> ignored = closeable(lock)) {
            checkState(yieldFuture == null, "there is an ongoing yield");
            checkState(!isSet(), "yield while driver was not running");
            if (terminationStarted) {
                return;
            }

            this.runningSequence++;
            long expectedRunningSequence = this.runningSequence;
            yieldFuture = executor.schedule(() -> {
                try (CloseableLock<ReentrantLock> ignored2 = closeable(lock)) {
                    if (expectedRunningSequence == runningSequence && yieldFuture != null) {
                        yield.set(true);
                    }
                }
            }, maxRunNanos, NANOSECONDS);
        }
    }

    public void reset()
    {
        try (CloseableLock<ReentrantLock> ignored = closeable(lock)) {
            if (terminationStarted) {
                return;
            }
            checkState(yieldFuture != null, "there is no ongoing yield");
            yield.set(false);
            yieldFuture.cancel(true);
            yieldFuture = null;
        }
    }

    public boolean isSet()
    {
        return yield.get();
    }

    /**
     * Signals an immediate yield to the driver to improve responsiveness to termination commands that may arrive while drivers are
     * still running. After calling this method, the driver should not attempt to start another interval of running and attempting
     * to call {@link DriverYieldSignal#setWithDelay(long, ScheduledExecutorService)} will fail.
     */
    public void yieldImmediatelyForTermination()
    {
        try (CloseableLock<ReentrantLock> ignored = closeable(lock)) {
            terminationStarted = true;
            yield.set(true);
            if (yieldFuture != null) {
                yieldFuture.cancel(true);
                yieldFuture = null;
            }
        }
    }

    @Override
    public String toString()
    {
        try (CloseableLock<ReentrantLock> ignored = closeable(lock)) {
            return toStringHelper(this)
                    .add("yieldScheduled", yieldFuture != null)
                    .add("yield", yield.get())
                    .toString();
        }
    }

    @VisibleForTesting
    public void forceYieldForTesting()
    {
        try (CloseableLock<ReentrantLock> ignored = closeable(lock)) {
            yield.set(true);
        }
    }

    @VisibleForTesting
    public void resetYieldForTesting()
    {
        try (CloseableLock<ReentrantLock> ignored = closeable(lock)) {
            yield.set(false);
        }
    }
}
