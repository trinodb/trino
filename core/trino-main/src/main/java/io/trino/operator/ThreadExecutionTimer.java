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

import io.airlift.vthreadtime.VirtualThreadTime;
import jakarta.annotation.Nullable;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;

public final class ThreadExecutionTimer
{
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
    private static final ThreadLocal<ThreadExecutionTimer> ACTIVE_VIRTUAL_THREAD_TIMER = new ThreadLocal<>();

    @Nullable
    private final VirtualThreadTime virtualThreadTime;
    private final long startNanos;

    private boolean closed;

    private ThreadExecutionTimer(@Nullable VirtualThreadTime virtualThreadTime)
    {
        this.virtualThreadTime = virtualThreadTime;
        this.startNanos = currentTimeNanos();
    }

    /// Runs the task with a timer for the CPU time of the current platform thread or the mounted time of the current virtual thread.
    public static void run(Consumer<ThreadExecutionTimer> task)
    {
        if (!Thread.currentThread().isVirtual()) {
            ThreadExecutionTimer timer = new ThreadExecutionTimer(null);
            try {
                task.accept(timer);
            }
            finally {
                timer.closed = true;
            }
            return;
        }

        checkState(VirtualThreadTime.isSupported(), "Virtual thread execution timing is not supported");
        checkState(ACTIVE_VIRTUAL_THREAD_TIMER.get() == null, "Virtual thread execution timing is already active");
        try (VirtualThreadTime virtualThreadTime = VirtualThreadTime.register()) {
            ThreadExecutionTimer timer = new ThreadExecutionTimer(virtualThreadTime);
            ACTIVE_VIRTUAL_THREAD_TIMER.set(timer);
            try {
                task.accept(timer);
            }
            finally {
                timer.closed = true;
                ACTIVE_VIRTUAL_THREAD_TIMER.remove();
            }
        }
    }

    public long elapsedNanos()
    {
        return max(0, currentTimeNanos() - startNanos);
    }

    long currentTimeNanos()
    {
        checkState(!closed, "Timer is closed");
        if (virtualThreadTime == null) {
            return currentPlatformThreadTimeNanos();
        }
        return virtualThreadTime.mountedTimeNanos();
    }

    @Nullable
    static ThreadExecutionTimer activeVirtualThreadTimer()
    {
        return ACTIVE_VIRTUAL_THREAD_TIMER.get();
    }

    static long currentPlatformThreadTimeNanos()
    {
        return THREAD_MX_BEAN.getCurrentThreadCpuTime();
    }
}
