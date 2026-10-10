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

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;

public final class ThreadExecutionTimer
        implements AutoCloseable
{
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
    private static final ThreadLocal<VirtualThreadTime> VIRTUAL_THREAD_TIME = new ThreadLocal<>();

    private final boolean virtualThread;
    private final long startNanos;
    @Nullable
    private final VirtualThreadTime virtualThreadTime;

    private boolean closed;

    private ThreadExecutionTimer(boolean virtualThread, long startNanos, @Nullable VirtualThreadTime virtualThreadTime)
    {
        this.virtualThread = virtualThread;
        this.startNanos = startNanos;
        this.virtualThreadTime = virtualThreadTime;
    }

    public static ThreadExecutionTimer start()
    {
        if (!Thread.currentThread().isVirtual()) {
            return new ThreadExecutionTimer(false, currentPlatformThreadTimeNanos(), null);
        }

        if (!VirtualThreadTime.isSupported()) {
            return new ThreadExecutionTimer(true, 0, null);
        }

        checkState(VIRTUAL_THREAD_TIME.get() == null, "Virtual thread execution timing is already active");
        VirtualThreadTime virtualThreadTime = VirtualThreadTime.register();
        VIRTUAL_THREAD_TIME.set(virtualThreadTime);
        return new ThreadExecutionTimer(true, virtualThreadTime.mountedTimeNanos(), virtualThreadTime);
    }

    public long elapsedNanos()
    {
        checkState(!closed, "Timer is closed");
        return max(0, currentTimeNanos() - startNanos);
    }

    @Nullable
    static VirtualThreadTime currentVirtualThreadTime()
    {
        return VIRTUAL_THREAD_TIME.get();
    }

    static long currentPlatformThreadTimeNanos()
    {
        return THREAD_MX_BEAN.getCurrentThreadCpuTime();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        if (virtualThreadTime != null) {
            checkState(VIRTUAL_THREAD_TIME.get() == virtualThreadTime, "Virtual thread execution timer is not active");
            VIRTUAL_THREAD_TIME.remove();
            virtualThreadTime.close();
        }
    }

    private long currentTimeNanos()
    {
        if (!virtualThread) {
            return currentPlatformThreadTimeNanos();
        }
        if (virtualThreadTime == null) {
            return 0;
        }
        return virtualThreadTime.mountedTimeNanos();
    }
}
