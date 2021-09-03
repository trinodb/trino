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
package io.trino.execution;

import io.airlift.concurrent.SetThreadName;
import io.trino.Session;
import io.trino.event.SplitMonitor;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.executor.TaskExecutor;
import io.trino.memory.QueryContext;
import io.trino.operator.TaskContext;
import io.trino.sql.planner.LocalExecutionPlanner;
import io.trino.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.TypeProvider;

import java.util.List;
import java.util.concurrent.Executor;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.execution.SqlTaskExecution.createSqlTaskExecution;
import static java.util.Objects.requireNonNull;

public class SqlTaskExecutionFactory
{
    private final Executor taskNotificationExecutor;

    private final TaskExecutor taskExecutor;

    private final LocalExecutionPlanner planner;
    private final SplitMonitor splitMonitor;
    private final boolean perOperatorCpuTimerEnabled;
    private final boolean cpuTimerEnabled;

    public SqlTaskExecutionFactory(
            Executor taskNotificationExecutor,
            TaskExecutor taskExecutor,
            LocalExecutionPlanner planner,
            SplitMonitor splitMonitor,
            TaskManagerConfig config)
    {
        this.taskNotificationExecutor = requireNonNull(taskNotificationExecutor, "taskNotificationExecutor is null");
        this.taskExecutor = requireNonNull(taskExecutor, "taskExecutor is null");
        this.planner = requireNonNull(planner, "planner is null");
        this.splitMonitor = requireNonNull(splitMonitor, "splitMonitor is null");
        requireNonNull(config, "config is null");
        this.perOperatorCpuTimerEnabled = config.isPerOperatorCpuTimerEnabled();
        this.cpuTimerEnabled = config.isTaskCpuTimerEnabled();
    }

    public SqlTaskExecution create(
            Session session,
            QueryContext queryContext,
            TaskStateMachine taskStateMachine,
            OutputBuffer outputBuffer,
            PlanFragment fragment,
            List<TaskSource> sources,
            Runnable notifyStatusChanged)
    {
        TaskContext taskContext = queryContext.addTaskContext(
                taskStateMachine,
                session,
                notifyStatusChanged,
                perOperatorCpuTimerEnabled,
                cpuTimerEnabled);

        LocalExecutionPlan localExecutionPlan;
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskStateMachine.getTaskId())) {
            try {
                localExecutionPlan = planner.plan(
                        taskContext,
                        fragment.getRoot(),
                        TypeProvider.copyOf(fragment.getSymbols()),
                        fragment.getPartitioningScheme(),
                        fragment.getStageExecutionDescriptor(),
                        fragment.getPartitionedSources(),
                        outputBuffer);
            }
            catch (Throwable e) {
                // planning failed
                taskStateMachine.failed(e);
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }
        return createSqlTaskExecution(
                taskStateMachine,
                taskContext,
                outputBuffer,
                sources,
                localExecutionPlan,
                taskExecutor,
                taskNotificationExecutor,
                splitMonitor);
    }
}
