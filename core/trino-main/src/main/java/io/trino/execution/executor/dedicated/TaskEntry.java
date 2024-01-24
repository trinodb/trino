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
package io.trino.execution.executor.dedicated;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.execution.SplitRunner;
import io.trino.execution.TaskId;
import io.trino.execution.executor.TaskHandle;
import io.trino.execution.executor.scheduler.Group;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

class TaskEntry
        implements TaskHandle
{
    private final TaskId taskId;
    private final Group group;
    private final AtomicInteger nextSplitId = new AtomicInteger();
    private volatile boolean destroyed;

    @GuardedBy("this")
    private final Set<SplitRunner> splits = new HashSet<>();

    public TaskEntry(TaskId taskId, Group group)
    {
        this.taskId = taskId;
        this.group = group;
    }

    public TaskId taskId()
    {
        return taskId;
    }

    public Group group()
    {
        return group;
    }

    public synchronized void destroy()
    {
        destroyed = true;

        for (SplitRunner split : splits) {
            split.close();
        }

        splits.clear();
    }

    public synchronized void addSplit(SplitRunner split)
    {
        checkArgument(!destroyed, "Task already destroyed: %s", taskId);
        splits.add(split);
    }

    public synchronized void removeSplit(SplitRunner split)
    {
        splits.remove(split);
    }

    public int nextSplitId()
    {
        return nextSplitId.incrementAndGet();
    }

    @Override
    public boolean isDestroyed()
    {
        return destroyed;
    }
}
