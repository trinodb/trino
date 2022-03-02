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
package io.trino.testing;

import io.airlift.stats.GcMonitor;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStateMachine;
import io.trino.memory.MemoryPool;
import io.trino.memory.QueryContext;
import io.trino.operator.TaskContext;
import io.trino.spi.QueryId;
import io.trino.spiller.SpillSpaceTracker;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public final class TestingTaskContext
{
    // Don't start this monitor
    private static final GcMonitor GC_MONITOR = new TestingGcMonitor();

    private TestingTaskContext() {}

    public static TaskContext createTaskContext(Executor notificationExecutor, ScheduledExecutorService yieldExecutor, Session session)
    {
        return builder(notificationExecutor, yieldExecutor, session).build();
    }

    public static TaskContext createTaskContext(Executor notificationExecutor, ScheduledExecutorService yieldExecutor, Session session, DataSize maxMemory)
    {
        return builder(notificationExecutor, yieldExecutor, session)
                .setQueryMaxMemory(maxMemory)
                .build();
    }

    public static TaskContext createTaskContext(Executor notificationExecutor, ScheduledExecutorService yieldExecutor, Session session, TaskStateMachine taskStateMachine)
    {
        return builder(notificationExecutor, yieldExecutor, session)
                .setTaskStateMachine(taskStateMachine)
                .build();
    }

    public static TaskContext createTaskContext(QueryContext queryContext, Executor executor, Session session)
    {
        return createTaskContext(queryContext, session, new TaskStateMachine(new TaskId(new StageId("query", 0), 0, 0), executor));
    }

    private static TaskContext createTaskContext(QueryContext queryContext, Session session, TaskStateMachine taskStateMachine)
    {
        return queryContext.addTaskContext(
                taskStateMachine,
                session,
                () -> {},
                true,
                true);
    }

    public static Builder builder(Executor notificationExecutor, ScheduledExecutorService yieldExecutor, Session session)
    {
        return new Builder(notificationExecutor, yieldExecutor, session);
    }

    public static class Builder
    {
        private final Executor notificationExecutor;
        private final ScheduledExecutorService yieldExecutor;
        private final Session session;
        private QueryId queryId = new QueryId("test_query");
        private TaskStateMachine taskStateMachine;
        private DataSize queryMaxMemory = DataSize.of(256, MEGABYTE);
        private DataSize memoryPoolSize = DataSize.of(1, GIGABYTE);
        private DataSize maxSpillSize = DataSize.of(1, GIGABYTE);
        private DataSize queryMaxSpillSize = DataSize.of(1, GIGABYTE);

        private Builder(Executor notificationExecutor, ScheduledExecutorService yieldExecutor, Session session)
        {
            this.notificationExecutor = notificationExecutor;
            this.yieldExecutor = yieldExecutor;
            this.session = session;
            this.taskStateMachine = new TaskStateMachine(new TaskId(new StageId("query", 0), 0, 0), notificationExecutor);
        }

        public Builder setTaskStateMachine(TaskStateMachine taskStateMachine)
        {
            this.taskStateMachine = taskStateMachine;
            return this;
        }

        public Builder setQueryMaxMemory(DataSize queryMaxMemory)
        {
            this.queryMaxMemory = queryMaxMemory;
            return this;
        }

        public Builder setMemoryPoolSize(DataSize memoryPoolSize)
        {
            this.memoryPoolSize = memoryPoolSize;
            return this;
        }

        public Builder setMaxSpillSize(DataSize maxSpillSize)
        {
            this.maxSpillSize = maxSpillSize;
            return this;
        }

        public Builder setQueryMaxSpillSize(DataSize queryMaxSpillSize)
        {
            this.queryMaxSpillSize = queryMaxSpillSize;
            return this;
        }

        public Builder setQueryId(QueryId queryId)
        {
            this.queryId = queryId;
            return this;
        }

        public TaskContext build()
        {
            MemoryPool memoryPool = new MemoryPool(memoryPoolSize);
            SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(maxSpillSize);
            QueryContext queryContext = new QueryContext(
                    queryId,
                    queryMaxMemory,
                    memoryPool,
                    0L,
                    GC_MONITOR,
                    notificationExecutor,
                    yieldExecutor,
                    queryMaxSpillSize,
                    spillSpaceTracker);

            return createTaskContext(queryContext, session, taskStateMachine);
        }
    }
}
