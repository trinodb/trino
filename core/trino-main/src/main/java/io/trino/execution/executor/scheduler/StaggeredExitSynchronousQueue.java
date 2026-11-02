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
package io.trino.execution.executor.scheduler;

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/// Hand-off queue for a [java.util.concurrent.ThreadPoolExecutor] that lets idle threads exit one at a time.
/// The JVM starts and exits threads under a global lock with work proportional to the thread count,
/// so many threads exiting together stall every thread start in the process.
/// A thread whose keep-alive expires waits for its own exit slot and still takes tasks until then.
final class StaggeredExitSynchronousQueue
        extends SynchronousQueue<Runnable>
{
    private final long exitIntervalNanos;
    private final AtomicLong nextExitNanos = new AtomicLong(System.nanoTime());

    public StaggeredExitSynchronousQueue(long exitInterval, TimeUnit unit)
    {
        checkArgument(exitInterval > 0, "exitInterval must be positive");
        this.exitIntervalNanos = unit.toNanos(exitInterval);
    }

    @Override
    public Runnable poll(long timeout, TimeUnit unit)
            throws InterruptedException
    {
        Runnable task = super.poll(timeout, unit);
        if (task != null) {
            return task;
        }
        return super.poll(reserveExitDelay(System.nanoTime()), NANOSECONDS);
    }

    /// Reserves the next free exit slot and returns the nanoseconds until it begins.
    @VisibleForTesting
    long reserveExitDelay(long now)
    {
        long slot = later(nextExitNanos.getAndUpdate(next -> later(next, now) + exitIntervalNanos), now);
        return slot - now;
    }

    private static long later(long first, long second)
    {
        if (first - second > 0) {
            return first;
        }
        return second;
    }
}
