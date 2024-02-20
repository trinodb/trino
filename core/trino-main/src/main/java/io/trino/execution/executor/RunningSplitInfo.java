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
package io.trino.execution.executor;

import com.google.common.collect.ComparisonChain;
import io.trino.execution.TaskId;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * A class representing a split that is running on the TaskRunner.
 * It has a Thread object that gets assigned while assigning the split
 * to the taskRunner. However, when the TaskRunner moves to a different split,
 * the thread stored here will not remain assigned to this split anymore.
 */
public class RunningSplitInfo
        implements Comparable<RunningSplitInfo>
{
    private final long startTime;
    private final String threadId;
    private final Thread thread;
    private boolean printed;
    private final TaskId taskId;
    private final Supplier<String> splitInfo;

    public RunningSplitInfo(long startTime, String threadId, Thread thread, TaskId taskId, Supplier<String> splitInfo)
    {
        this.startTime = startTime;
        this.threadId = requireNonNull(threadId, "threadId is null");
        this.thread = requireNonNull(thread, "thread is null");
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.splitInfo = requireNonNull(splitInfo, "split is null");
        this.printed = false;
    }

    public long getStartTime()
    {
        return startTime;
    }

    public String getThreadId()
    {
        return threadId;
    }

    public Thread getThread()
    {
        return thread;
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    /**
     * {@link PrioritizedSplitRunner#getInfo()} provides runtime statistics for the split (such as total cpu utilization so far).
     * A value returned from this method changes over time and cannot be cached as a field of {@link RunningSplitInfo}.
     *
     * @return Formatted string containing runtime statistics for the split.
     */
    public String getSplitInfo()
    {
        return splitInfo.get();
    }

    public boolean isPrinted()
    {
        return printed;
    }

    public void setPrinted()
    {
        printed = true;
    }

    @Override
    public int compareTo(RunningSplitInfo o)
    {
        return ComparisonChain.start()
                .compare(startTime, o.getStartTime())
                .compare(threadId, o.getThreadId())
                .result();
    }
}
