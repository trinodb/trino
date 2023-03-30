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
package io.trino.execution.scheduler;

import io.airlift.log.Logger;
import io.airlift.stats.DistributionStat;
import io.airlift.stats.TimeStat;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskState;
import io.trino.operator.TaskStats;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Optional;

import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.util.Failures.toFailure;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TaskExecutionStats
{
    private static final Logger log = Logger.get(TaskExecutionStats.class);

    private final ExecutionStats finishedTasks = new ExecutionStats();
    private final ExecutionStats abortedTasks = new ExecutionStats();
    private final FailedTasksStats failedTasks = new FailedTasksStats();

    public void update(TaskInfo info)
    {
        TaskState state = info.getTaskStatus().getState();
        switch (state) {
            case FINISHED:
                finishedTasks.update(info.getStats());
                break;
            case FAILED:
                failedTasks.update(info);
                break;
            case CANCELED:
            case ABORTED:
                abortedTasks.update(info.getStats());
                break;
            case PLANNED:
            case RUNNING:
            case FLUSHING:
            default:
                log.error("Unexpected task state: %s", state);
        }
    }

    @Managed
    @Nested
    public ExecutionStats getFinishedTasks()
    {
        return finishedTasks;
    }

    @Managed
    @Nested
    public ExecutionStats getAbortedTasks()
    {
        return abortedTasks;
    }

    @Managed
    @Nested
    public FailedTasksStats getFailedTasks()
    {
        return failedTasks;
    }

    public static class ExecutionStats
    {
        private final TimeStat elapsedTime = new TimeStat(MILLISECONDS);
        private final TimeStat scheduledTime = new TimeStat(MILLISECONDS);
        private final TimeStat cpuTime = new TimeStat(MILLISECONDS);
        private final TimeStat inputBlockedTime = new TimeStat(MILLISECONDS);
        private final TimeStat outputBlockedTime = new TimeStat(MILLISECONDS);
        private final DistributionStat peakMemoryReservationInBytes = new DistributionStat();

        public void update(TaskStats stats)
        {
            elapsedTime.add(stats.getElapsedTime());
            scheduledTime.add(stats.getTotalScheduledTime());
            cpuTime.add(stats.getTotalCpuTime());
            inputBlockedTime.add(stats.getInputBlockedTime());
            outputBlockedTime.add(stats.getOutputBlockedTime());
            peakMemoryReservationInBytes.add(stats.getPeakUserMemoryReservation().toBytes());
        }

        @Managed
        @Nested
        public TimeStat getElapsedTime()
        {
            return elapsedTime;
        }

        @Managed
        @Nested
        public TimeStat getScheduledTime()
        {
            return scheduledTime;
        }

        @Managed
        @Nested
        public TimeStat getCpuTime()
        {
            return cpuTime;
        }

        @Managed
        @Nested
        public TimeStat getInputBlockedTime()
        {
            return inputBlockedTime;
        }

        @Managed
        @Nested
        public TimeStat getOutputBlockedTime()
        {
            return outputBlockedTime;
        }

        @Managed
        @Nested
        public DistributionStat getPeakMemoryReservationInBytes()
        {
            return peakMemoryReservationInBytes;
        }
    }

    public static class FailedTasksStats
    {
        private final ExecutionStats userError = new ExecutionStats();
        private final ExecutionStats internalError = new ExecutionStats();
        private final ExecutionStats externalError = new ExecutionStats();
        private final ExecutionStats insufficientResources = new ExecutionStats();

        public void update(TaskInfo info)
        {
            ExecutionFailureInfo failureInfo = info.getTaskStatus().getFailures().stream()
                    .findFirst()
                    .orElseGet(() -> toFailure(new TrinoException(GENERIC_INTERNAL_ERROR, "A task failed for an unknown reason")));
            ErrorType errorType = Optional.ofNullable(failureInfo.getErrorCode()).map(ErrorCode::getType).orElse(INTERNAL_ERROR);
            TaskStats stats = info.getStats();
            switch (errorType) {
                case USER_ERROR:
                    userError.update(stats);
                    break;
                case INTERNAL_ERROR:
                    internalError.update(stats);
                    break;
                case EXTERNAL:
                    externalError.update(stats);
                    break;
                case INSUFFICIENT_RESOURCES:
                    insufficientResources.update(stats);
                    break;
                default:
                    log.error("Unexpected error type: %s", errorType);
            }
        }

        @Managed
        @Nested
        public ExecutionStats getUserError()
        {
            return userError;
        }

        @Managed
        @Nested
        public ExecutionStats getInternalError()
        {
            return internalError;
        }

        @Managed
        @Nested
        public ExecutionStats getExternalError()
        {
            return externalError;
        }

        @Managed
        @Nested
        public ExecutionStats getInsufficientResources()
        {
            return insufficientResources;
        }
    }
}
