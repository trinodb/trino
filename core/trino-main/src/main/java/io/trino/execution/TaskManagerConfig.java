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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;
import io.airlift.units.ThreadCount;
import io.trino.util.PowerOfTwo;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

import static com.google.common.math.IntMath.ceilingPowerOfTwo;

@DefunctConfig({
        "experimental.big-query-max-task-memory",
        "task.max-memory",
        "task.http-notification-threads",
        "task.info-refresh-max-wait",
        "task.operator-pre-allocated-memory",
        "sink.new-implementation",
        "task.legacy-scheduling-behavior",
        "task.level-absolute-priority"})
public class TaskManagerConfig
{
    private boolean perOperatorCpuTimerEnabled = true;
    private boolean taskCpuTimerEnabled = true;
    private boolean statisticsCpuTimerEnabled = true;
    private DataSize maxPartialAggregationMemoryUsage = DataSize.of(16, Unit.MEGABYTE);
    private DataSize maxPartialTopNMemory = DataSize.of(16, Unit.MEGABYTE);
    private DataSize maxLocalExchangeBufferSize = DataSize.of(128, Unit.MEGABYTE);
    private DataSize maxIndexMemoryUsage = DataSize.of(64, Unit.MEGABYTE);
    private boolean shareIndexLoading;
    private ThreadCount maxWorkerThreads = ThreadCount.valueOf("2C");
    private Integer minDrivers;
    private Integer initialSplitsPerNode;
    private int minDriversPerTask = 3;
    private int maxDriversPerTask = Integer.MAX_VALUE;
    private Duration splitConcurrencyAdjustmentInterval = new Duration(100, TimeUnit.MILLISECONDS);

    private DataSize sinkMaxBufferSize = DataSize.of(32, Unit.MEGABYTE);
    private DataSize sinkMaxBroadcastBufferSize = DataSize.of(200, Unit.MEGABYTE);
    private DataSize maxPagePartitioningBufferSize = DataSize.of(32, Unit.MEGABYTE);
    private ThreadCount pagePartitioningBufferPoolSize = ThreadCount.exactValueOf(8);

    private Duration clientTimeout = new Duration(2, TimeUnit.MINUTES);
    private Duration infoMaxAge = new Duration(15, TimeUnit.MINUTES);

    private Duration statusRefreshMaxWait = new Duration(1, TimeUnit.SECONDS);
    private Duration infoUpdateInterval = new Duration(3, TimeUnit.SECONDS);
    private Duration taskTerminationTimeout = new Duration(1, TimeUnit.MINUTES);

    private boolean interruptStuckSplitTasksEnabled = true;
    private Duration interruptStuckSplitTasksWarningThreshold = new Duration(10, TimeUnit.MINUTES);
    private Duration interruptStuckSplitTasksTimeout = new Duration(15, TimeUnit.MINUTES);
    private Duration interruptStuckSplitTasksDetectionInterval = new Duration(2, TimeUnit.MINUTES);

    private boolean scaleWritersEnabled = true;
    // Set the value of default max writer count to 2 * max(the number of processors, 32). We can do this
    // because preferred write partitioning is always enabled for local exchange thus partitioned inserts will never
    // use this property. Hence, there is no risk in terms of more numbers of physical writers which can cause high
    // resource utilization.

    private ThreadCount scaleWritersMaxWriterCount = ThreadCount.boundedValueOf(nextPowerOfTwo(), "2", "64");

    private int writerCount = 1;
    // Default value of partitioned task writer count should be above 1, otherwise it can create a plan
    // with a single gather exchange node on the coordinator due to a single available processor. Whereas,
    // on the worker nodes due to more available processors, the default value could be above 1. Therefore,
    // it can cause error due to config mismatch during execution. Additionally, cap it to 32 in order to
    // avoid small pages produced by local partitioning exchanges.
    private ThreadCount partitionedWriterCount = ThreadCount.boundedValueOf(nextPowerOfTwo(), "2", "32");

    // Default value of task concurrency should be above 1, otherwise it can create a plan with a single gather
    // exchange node on the coordinator due to a single available processor. Whereas, on the worker nodes due to
    // more available processors, the default value could be above 1. Therefore, it can cause error due to config
    // mismatch during execution. Additionally, cap it to 32 in order to avoid small pages produced by local
    // partitioning exchanges.
    /**
     * default value is overwritten for fault tolerant execution in {@link #applyFaultTolerantExecutionDefaults()}}
     */
    private ThreadCount taskConcurrency = ThreadCount.boundedValueOf(nextPowerOfTwo(), "2", "32");
    private ThreadCount httpResponseThreads = ThreadCount.exactValueOf(100);
    private ThreadCount httpTimeoutThreads = ThreadCount.exactValueOf(3);

    private ThreadCount taskNotificationThreads = ThreadCount.exactValueOf(5);
    private ThreadCount taskYieldThreads = ThreadCount.exactValueOf(3);

    private BigDecimal levelTimeMultiplier = new BigDecimal(2.0);

    @MinDuration("1ms")
    @MaxDuration("10s")
    @NotNull
    public Duration getStatusRefreshMaxWait()
    {
        return statusRefreshMaxWait;
    }

    @Config("task.status-refresh-max-wait")
    public TaskManagerConfig setStatusRefreshMaxWait(Duration statusRefreshMaxWait)
    {
        this.statusRefreshMaxWait = statusRefreshMaxWait;
        return this;
    }

    @MinDuration("1ms")
    @MaxDuration("10s")
    @NotNull
    public Duration getInfoUpdateInterval()
    {
        return infoUpdateInterval;
    }

    @Config("task.info-update-interval")
    @ConfigDescription("Interval between updating task data")
    public TaskManagerConfig setInfoUpdateInterval(Duration infoUpdateInterval)
    {
        this.infoUpdateInterval = infoUpdateInterval;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getTaskTerminationTimeout()
    {
        return taskTerminationTimeout;
    }

    @Config("task.termination-timeout")
    @ConfigDescription("Maximum duration to wait for a task to complete termination before failing the task on the coordinator")
    public TaskManagerConfig setTaskTerminationTimeout(Duration taskTerminationTimeout)
    {
        this.taskTerminationTimeout = taskTerminationTimeout;
        return this;
    }

    public boolean isPerOperatorCpuTimerEnabled()
    {
        return perOperatorCpuTimerEnabled;
    }

    @LegacyConfig("task.verbose-stats")
    @Config("task.per-operator-cpu-timer-enabled")
    public TaskManagerConfig setPerOperatorCpuTimerEnabled(boolean perOperatorCpuTimerEnabled)
    {
        this.perOperatorCpuTimerEnabled = perOperatorCpuTimerEnabled;
        return this;
    }

    public boolean isTaskCpuTimerEnabled()
    {
        return taskCpuTimerEnabled;
    }

    @Config("task.cpu-timer-enabled")
    public TaskManagerConfig setTaskCpuTimerEnabled(boolean taskCpuTimerEnabled)
    {
        this.taskCpuTimerEnabled = taskCpuTimerEnabled;
        return this;
    }

    public boolean isStatisticsCpuTimerEnabled()
    {
        return statisticsCpuTimerEnabled;
    }

    @Config("task.statistics-cpu-timer-enabled")
    public TaskManagerConfig setStatisticsCpuTimerEnabled(boolean statisticsCpuTimerEnabled)
    {
        this.statisticsCpuTimerEnabled = statisticsCpuTimerEnabled;
        return this;
    }

    @NotNull
    public DataSize getMaxPartialAggregationMemoryUsage()
    {
        return maxPartialAggregationMemoryUsage;
    }

    @Config("task.max-partial-aggregation-memory")
    public TaskManagerConfig setMaxPartialAggregationMemoryUsage(DataSize maxPartialAggregationMemoryUsage)
    {
        this.maxPartialAggregationMemoryUsage = maxPartialAggregationMemoryUsage;
        return this;
    }

    @NotNull
    public DataSize getMaxPartialTopNMemory()
    {
        return maxPartialTopNMemory;
    }

    @Config("task.max-partial-top-n-memory")
    public TaskManagerConfig setMaxPartialTopNMemory(DataSize maxPartialTopNMemory)
    {
        this.maxPartialTopNMemory = maxPartialTopNMemory;
        return this;
    }

    @NotNull
    public DataSize getMaxLocalExchangeBufferSize()
    {
        return maxLocalExchangeBufferSize;
    }

    @Config("task.max-local-exchange-buffer-size")
    public TaskManagerConfig setMaxLocalExchangeBufferSize(DataSize size)
    {
        this.maxLocalExchangeBufferSize = size;
        return this;
    }

    @NotNull
    public DataSize getMaxIndexMemoryUsage()
    {
        return maxIndexMemoryUsage;
    }

    @Config("task.max-index-memory")
    public TaskManagerConfig setMaxIndexMemoryUsage(DataSize maxIndexMemoryUsage)
    {
        this.maxIndexMemoryUsage = maxIndexMemoryUsage;
        return this;
    }

    @NotNull
    public boolean isShareIndexLoading()
    {
        return shareIndexLoading;
    }

    @Config("task.share-index-loading")
    public TaskManagerConfig setShareIndexLoading(boolean shareIndexLoading)
    {
        this.shareIndexLoading = shareIndexLoading;
        return this;
    }

    @Min(0)
    public BigDecimal getLevelTimeMultiplier()
    {
        return levelTimeMultiplier;
    }

    @Config("task.level-time-multiplier")
    @ConfigDescription("Factor that determines the target scheduled time for a level relative to the next")
    public TaskManagerConfig setLevelTimeMultiplier(BigDecimal levelTimeMultiplier)
    {
        this.levelTimeMultiplier = levelTimeMultiplier;
        return this;
    }

    @Min(1)
    public int getMaxWorkerThreads()
    {
        return maxWorkerThreads.getThreadCount();
    }

    @LegacyConfig("task.shard.max-threads")
    @Config("task.max-worker-threads")
    public TaskManagerConfig setMaxWorkerThreads(String maxWorkerThreads)
    {
        this.maxWorkerThreads = ThreadCount.valueOf(maxWorkerThreads);
        return this;
    }

    @Min(1)
    public int getInitialSplitsPerNode()
    {
        if (initialSplitsPerNode == null) {
            return maxWorkerThreads.getThreadCount();
        }
        return initialSplitsPerNode;
    }

    @Config("task.initial-splits-per-node")
    public TaskManagerConfig setInitialSplitsPerNode(int initialSplitsPerNode)
    {
        this.initialSplitsPerNode = initialSplitsPerNode;
        return this;
    }

    @MinDuration("1ms")
    public Duration getSplitConcurrencyAdjustmentInterval()
    {
        return splitConcurrencyAdjustmentInterval;
    }

    @Config("task.split-concurrency-adjustment-interval")
    public TaskManagerConfig setSplitConcurrencyAdjustmentInterval(Duration splitConcurrencyAdjustmentInterval)
    {
        this.splitConcurrencyAdjustmentInterval = splitConcurrencyAdjustmentInterval;
        return this;
    }

    @Min(1)
    public int getMinDrivers()
    {
        if (minDrivers == null) {
            return 2 * maxWorkerThreads.getThreadCount();
        }
        return minDrivers;
    }

    @Config("task.min-drivers")
    public TaskManagerConfig setMinDrivers(int minDrivers)
    {
        this.minDrivers = minDrivers;
        return this;
    }

    @Min(1)
    public int getMaxDriversPerTask()
    {
        return maxDriversPerTask;
    }

    @Config("task.max-drivers-per-task")
    @ConfigDescription("Maximum number of drivers a task can run")
    public TaskManagerConfig setMaxDriversPerTask(int maxDriversPerTask)
    {
        this.maxDriversPerTask = maxDriversPerTask;
        return this;
    }

    @Min(1)
    public int getMinDriversPerTask()
    {
        return minDriversPerTask;
    }

    @Config("task.min-drivers-per-task")
    @ConfigDescription("Minimum number of drivers guaranteed to run per task given there is sufficient work to do")
    public TaskManagerConfig setMinDriversPerTask(int minDriversPerTask)
    {
        this.minDriversPerTask = minDriversPerTask;
        return this;
    }

    @NotNull
    public DataSize getSinkMaxBufferSize()
    {
        return sinkMaxBufferSize;
    }

    @Config("sink.max-buffer-size")
    public TaskManagerConfig setSinkMaxBufferSize(DataSize sinkMaxBufferSize)
    {
        this.sinkMaxBufferSize = sinkMaxBufferSize;
        return this;
    }

    public DataSize getSinkMaxBroadcastBufferSize()
    {
        return sinkMaxBroadcastBufferSize;
    }

    @Config("sink.max-broadcast-buffer-size")
    public TaskManagerConfig setSinkMaxBroadcastBufferSize(DataSize sinkMaxBroadcastBufferSize)
    {
        this.sinkMaxBroadcastBufferSize = sinkMaxBroadcastBufferSize;
        return this;
    }

    @NotNull
    public DataSize getMaxPagePartitioningBufferSize()
    {
        return maxPagePartitioningBufferSize;
    }

    @Config("driver.max-page-partitioning-buffer-size")
    public TaskManagerConfig setMaxPagePartitioningBufferSize(DataSize size)
    {
        this.maxPagePartitioningBufferSize = size;
        return this;
    }

    @Min(0)
    public int getPagePartitioningBufferPoolSize()
    {
        return pagePartitioningBufferPoolSize.getThreadCount();
    }

    @Config("driver.page-partitioning-buffer-pool-size")
    @ConfigDescription("Maximum number of free buffers in the per task partitioned page buffer pool. Setting this to zero effectively disables the pool")
    public TaskManagerConfig setPagePartitioningBufferPoolSize(String pagePartitioningBufferPoolSize)
    {
        this.pagePartitioningBufferPoolSize = ThreadCount.valueOf(pagePartitioningBufferPoolSize);
        return this;
    }

    @MinDuration("5s")
    @NotNull
    public Duration getClientTimeout()
    {
        return clientTimeout;
    }

    @Config("task.client.timeout")
    public TaskManagerConfig setClientTimeout(Duration clientTimeout)
    {
        this.clientTimeout = clientTimeout;
        return this;
    }

    @NotNull
    public Duration getInfoMaxAge()
    {
        return infoMaxAge;
    }

    @Config("task.info.max-age")
    public TaskManagerConfig setInfoMaxAge(Duration infoMaxAge)
    {
        this.infoMaxAge = infoMaxAge;
        return this;
    }

    public boolean isScaleWritersEnabled()
    {
        return scaleWritersEnabled;
    }

    @Config("task.scale-writers.enabled")
    @ConfigDescription("Scale the number of concurrent table writers per task based on throughput")
    public TaskManagerConfig setScaleWritersEnabled(boolean scaleWritersEnabled)
    {
        this.scaleWritersEnabled = scaleWritersEnabled;
        return this;
    }

    @Min(1)
    public int getScaleWritersMaxWriterCount()
    {
        return scaleWritersMaxWriterCount.getThreadCount();
    }

    @Config("task.scale-writers.max-writer-count")
    @ConfigDescription("Maximum number of writers per task up to which scaling will happen if task.scale-writers.enabled is set")
    public TaskManagerConfig setScaleWritersMaxWriterCount(String scaleWritersMaxWriterCount)
    {
        this.scaleWritersMaxWriterCount = ThreadCount.valueOf(scaleWritersMaxWriterCount);
        return this;
    }

    @Min(1)
    public int getWriterCount()
    {
        return writerCount;
    }

    @Config("task.writer-count")
    @ConfigDescription("Number of local parallel table writers per task when prefer partitioning and task writer scaling are not used")
    public TaskManagerConfig setWriterCount(int writerCount)
    {
        this.writerCount = writerCount;
        return this;
    }

    @Min(1)
    @PowerOfTwo
    public int getPartitionedWriterCount()
    {
        return partitionedWriterCount.getThreadCount();
    }

    @Config("task.partitioned-writer-count")
    @ConfigDescription("Number of local parallel table writers per task when prefer partitioning is used")
    public TaskManagerConfig setPartitionedWriterCount(String partitionedWriterCount)
    {
        this.partitionedWriterCount = ThreadCount.valueOf(partitionedWriterCount);
        return this;
    }

    @Min(1)
    @PowerOfTwo
    public int getTaskConcurrency()
    {
        return taskConcurrency.getThreadCount();
    }

    @Config("task.concurrency")
    @ConfigDescription("Default number of local parallel jobs per worker")
    public TaskManagerConfig setTaskConcurrency(String taskConcurrency)
    {
        this.taskConcurrency = ThreadCount.valueOf(taskConcurrency);
        return this;
    }

    @Min(1)
    public int getHttpResponseThreads()
    {
        return httpResponseThreads.getThreadCount();
    }

    @Config("task.http-response-threads")
    public TaskManagerConfig setHttpResponseThreads(String httpResponseThreads)
    {
        this.httpResponseThreads = ThreadCount.valueOf(httpResponseThreads);
        return this;
    }

    @Min(1)
    public int getHttpTimeoutThreads()
    {
        return httpTimeoutThreads.getThreadCount();
    }

    @Config("task.http-timeout-threads")
    public TaskManagerConfig setHttpTimeoutThreads(String httpTimeoutThreads)
    {
        this.httpTimeoutThreads = ThreadCount.valueOf(httpTimeoutThreads);
        return this;
    }

    @Min(1)
    public int getTaskNotificationThreads()
    {
        return taskNotificationThreads.getThreadCount();
    }

    @Config("task.task-notification-threads")
    @ConfigDescription("Number of threads used for internal task event notifications")
    public TaskManagerConfig setTaskNotificationThreads(String taskNotificationThreads)
    {
        this.taskNotificationThreads = ThreadCount.valueOf(taskNotificationThreads);
        return this;
    }

    @Min(1)
    public int getTaskYieldThreads()
    {
        return taskYieldThreads.getThreadCount();
    }

    @Config("task.task-yield-threads")
    @ConfigDescription("Number of threads used for setting yield signals")
    public TaskManagerConfig setTaskYieldThreads(String taskYieldThreads)
    {
        this.taskYieldThreads = ThreadCount.valueOf(taskYieldThreads);
        return this;
    }

    public boolean isInterruptStuckSplitTasksEnabled()
    {
        return interruptStuckSplitTasksEnabled;
    }

    @Config("task.interrupt-stuck-split-tasks-enabled")
    public TaskManagerConfig setInterruptStuckSplitTasksEnabled(boolean interruptStuckSplitTasksEnabled)
    {
        this.interruptStuckSplitTasksEnabled = interruptStuckSplitTasksEnabled;
        return this;
    }

    @MinDuration("1m")
    public Duration getInterruptStuckSplitTasksWarningThreshold()
    {
        return interruptStuckSplitTasksWarningThreshold;
    }

    @Config("task.interrupt-stuck-split-tasks-warning-threshold")
    @ConfigDescription("Print out call stacks and generate JMX metrics for splits running longer than the threshold")
    public TaskManagerConfig setInterruptStuckSplitTasksWarningThreshold(Duration interruptStuckSplitTasksWarningThreshold)
    {
        this.interruptStuckSplitTasksWarningThreshold = interruptStuckSplitTasksWarningThreshold;
        return this;
    }

    @MinDuration("3m")
    public Duration getInterruptStuckSplitTasksTimeout()
    {
        return interruptStuckSplitTasksTimeout;
    }

    @Config("task.interrupt-stuck-split-tasks-timeout")
    @ConfigDescription("Interrupt task processing thread after this timeout if the thread is stuck in certain external libraries used by Trino functions")
    public TaskManagerConfig setInterruptStuckSplitTasksTimeout(Duration interruptStuckSplitTasksTimeout)
    {
        this.interruptStuckSplitTasksTimeout = interruptStuckSplitTasksTimeout;
        return this;
    }

    @MinDuration("1m")
    public Duration getInterruptStuckSplitTasksDetectionInterval()
    {
        return interruptStuckSplitTasksDetectionInterval;
    }

    @Config("task.interrupt-stuck-split-tasks-detection-interval")
    @ConfigDescription("Interval between detecting stuck split")
    public TaskManagerConfig setInterruptStuckSplitTasksDetectionInterval(Duration interruptStuckSplitTasksDetectionInterval)
    {
        this.interruptStuckSplitTasksDetectionInterval = interruptStuckSplitTasksDetectionInterval;
        return this;
    }

    public void applyFaultTolerantExecutionDefaults()
    {
        taskConcurrency = ThreadCount.exactValueOf(8);
    }

    private static String nextPowerOfTwo()
    {
        return String.valueOf(ceilingPowerOfTwo(ThreadCount.valueOf("1C").getThreadCount()));
    }
}
