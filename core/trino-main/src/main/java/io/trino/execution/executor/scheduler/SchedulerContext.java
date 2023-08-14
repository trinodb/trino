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

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.annotation.NotThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;

@NotThreadSafe
public final class SchedulerContext
{
    private final FairScheduler scheduler;
    private final TaskControl handle;

    public SchedulerContext(FairScheduler scheduler, TaskControl handle)
    {
        this.scheduler = scheduler;
        this.handle = handle;
    }

    /**
     * Attempt to relinquish control to let other tasks run.
     *
     * @return false if the task was interrupted or cancelled while yielding,
     * for example if the Java thread was interrupted, the scheduler was shutdown,
     * or the scheduling group was removed. The caller is expected to clean up and finish.
     */
    public boolean maybeYield()
    {
        checkArgument(handle.getState() == TaskControl.State.RUNNING, "Task is not running");

        return scheduler.yield(handle);
    }

    /**
     * Indicate that the current task is blocked. The method returns when the future
     * completes of it the task is interrupted.
     *
     * @return false if the task was interrupted or cancelled while blocked,
     * for example if the Java thread was interrupted, the scheduler was shutdown,
     * or the scheduling group was removed. The caller is expected to clean up and finish.
     */
    public boolean block(ListenableFuture<?> future)
    {
        checkArgument(handle.getState() == TaskControl.State.RUNNING, "Task is not running");

        return scheduler.block(handle, future);
    }

    public long getStartNanos()
    {
        return scheduler.getStartNanos(handle);
    }

    public long getWaitNanos()
    {
        return scheduler.getWaitNanos(handle);
    }

    public long getScheduledNanos()
    {
        return scheduler.getScheduledNanos(handle);
    }

    public long getBlockedNanos()
    {
        return scheduler.getBlockedNanos(handle);
    }
}
