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
package io.trino.plugin.varada.configuration;

import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;

import java.time.Duration;

import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.LOCAL_DATA_STORAGE_PREFIX;
import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX;

public class WarmupDemoterConfiguration
{
    private double maxUsageThresholdPercentage = 90;
    private double cleanupUsageThresholdPercentage = 85;
    private int batchSize = 100;
    private int defaultRulePriority;
    private int warmingPriorityAllowThreshold = 2;
    private long maxElementsToDemoteInIteration = 100;
    private double epsilon = 1;
    private Duration delayAcquireThread = Duration.ofMillis(100);
    private Duration maxDurationAcquireThread = Duration.ofSeconds(200);
    private int maxRetriesAcquireThread = 300;
    private int tasksExecutorQueueSize = 2_000_000;
    private int prioritizeExecutorPollSize = 1000;
    private int cloudExecutorPollSize = 200;
    private long tasksExecutorKeepAliveTtl = 1000;
    private boolean enableDemote = true;

    public double getMaxUsageThresholdPercentage()
    {
        return maxUsageThresholdPercentage;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "warmup-demoter.max-usage-threshold-percentage")
    @Config(WARP_SPEED_PREFIX + "warmup-demoter.max-usage-threshold-percentage")
    public void setMaxUsageThresholdPercentage(double maxUsageThresholdPercentage)
    {
        this.maxUsageThresholdPercentage = maxUsageThresholdPercentage;
    }

    public double getCleanupUsageThresholdPercentage()
    {
        return cleanupUsageThresholdPercentage;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "warmup-demoter.max-cleanup-threshold-percentage")
    @Config(WARP_SPEED_PREFIX + "warmup-demoter.max-cleanup-threshold-percentage")
    public void setCleanupUsageThresholdPercentage(double cleanupUsageThresholdPercentage)
    {
        this.cleanupUsageThresholdPercentage = cleanupUsageThresholdPercentage;
    }

    public int getBatchSize()
    {
        return batchSize;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "warmup-demoter.batch-size")
    @Config(WARP_SPEED_PREFIX + "warmup-demoter.batch-size")
    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
    }

    public int getWarmingPriorityAllowThreshold()
    {
        return warmingPriorityAllowThreshold;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "warmup-demoter.priority-allowed-threshold")
    @Config(WARP_SPEED_PREFIX + "warmup-demoter.priority-allowed-threshold")
    public void setWarmingPriorityAllowThreshold(int warmingPriorityAllowThreshold)
    {
        this.warmingPriorityAllowThreshold = warmingPriorityAllowThreshold;
    }

    public int getDefaultRulePriority()
    {
        return defaultRulePriority;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "warmup-demoter.default-rule-priority")
    @Config(WARP_SPEED_PREFIX + "warmup-demoter.default-rule-priority")
    public void setDefaultRulePriority(int defaultRulePriority)
    {
        this.defaultRulePriority = defaultRulePriority;
    }

    public long getMaxElementsToDemoteInIteration()
    {
        return maxElementsToDemoteInIteration;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "warmup-demoter.max-elements-to-demote-in-iteration")
    @Config(WARP_SPEED_PREFIX + "warmup-demoter.max-elements-to-demote-in-iteration")
    public void setMaxElementsToDemoteInIteration(long maxElementsToDemoteInIteration)
    {
        this.maxElementsToDemoteInIteration = maxElementsToDemoteInIteration;
    }

    public double getEpsilon()
    {
        return epsilon;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "warmup-demoter.epsilon")
    @Config(WARP_SPEED_PREFIX + "warmup-demoter.epsilon")
    public void setEpsilon(double epsilon)
    {
        this.epsilon = epsilon;
    }

    public Duration getDelayAcquireThread()
    {
        return delayAcquireThread;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "warmup-demoter.delay-duration-acquire-thread")
    @Config(WARP_SPEED_PREFIX + "warmup-demoter.delay-duration-acquire-thread")
    public void setDelayAcquireThread(io.airlift.units.Duration delayAcquireThread)
    {
        this.delayAcquireThread = delayAcquireThread.toJavaTime();
    }

    public Duration getMaxDurationAcquireThread()
    {
        return maxDurationAcquireThread;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "warmup-demoter.max-duration-acquire-thread")
    @Config(WARP_SPEED_PREFIX + "warmup-demoter.max-duration-acquire-thread")
    public void setMaxDurationAcquireThread(io.airlift.units.Duration maxDurationAcquireThread)
    {
        this.maxDurationAcquireThread = maxDurationAcquireThread.toJavaTime();
    }

    public int getMaxRetriesAcquireThread()
    {
        return maxRetriesAcquireThread;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "warmup-demoter.max-retries-acquire-thread")
    @Config(WARP_SPEED_PREFIX + "warmup-demoter.max-retries-acquire-thread")
    public void setMaxRetriesAcquireThread(int maxRetriesAcquireThread)
    {
        this.maxRetriesAcquireThread = maxRetriesAcquireThread;
    }

    public int getTasksExecutorQueueSize()
    {
        return tasksExecutorQueueSize;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.task.executor-queue-size")
    @Config(WARP_SPEED_PREFIX + "config.task.executor-queue-size")
    public void setTasksExecutorQueueSize(int tasksExecutorQueueSize)
    {
        this.tasksExecutorQueueSize = tasksExecutorQueueSize;
    }

    public int getPrioritizeExecutorPollSize()
    {
        return prioritizeExecutorPollSize;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.task.prioritize-executor-pool-size")
    @Config(WARP_SPEED_PREFIX + "config.task.prioritize-executor-pool-size")
    public void setPrioritizeExecutorPollSize(int prioritizeExecutorPollSize)
    {
        this.prioritizeExecutorPollSize = prioritizeExecutorPollSize;
    }

    public int getCloudExecutorPollSize()
    {
        return cloudExecutorPollSize;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.task.cloud-executor-pool-size")
    @Config(WARP_SPEED_PREFIX + "config.task.cloud-executor-pool-size")
    public void setCloudExecutorPollSize(int cloudExecutorPollSize)
    {
        this.cloudExecutorPollSize = cloudExecutorPollSize;
    }

    public long getTasksExecutorKeepAliveTtl()
    {
        return tasksExecutorKeepAliveTtl;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.task.executor-keep-alive-ttl")
    @Config(WARP_SPEED_PREFIX + "config.task.executor-keep-alive-ttl")
    public void setTasksExecutorKeepAliveTtl(long tasksExecutorKeepAliveTtl)
    {
        this.tasksExecutorKeepAliveTtl = tasksExecutorKeepAliveTtl;
    }

    public boolean isEnableDemote()
    {
        return enableDemote;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "enable.demote")
    @Config(WARP_SPEED_PREFIX + "enable.demote")
    public void setEnableDemote(boolean enableDemote)
    {
        this.enableDemote = enableDemote;
    }

    @Override
    public String toString()
    {
        return "WarmupDemoterConfiguration{" +
                "maxUsageThresholdPercentage=" + maxUsageThresholdPercentage +
                ", cleanupUsageThresholdPercentage=" + cleanupUsageThresholdPercentage +
                ", batchSize=" + batchSize +
                ", defaultRulePriority=" + defaultRulePriority +
                ", warmupHysteresis=" + warmingPriorityAllowThreshold +
                ", maxElementsToDemoteInIteration=" + maxElementsToDemoteInIteration +
                ", epsilon=" + epsilon +
                ", tasksExecutorQueueSize=" + tasksExecutorQueueSize +
                ", tasksExecutorKeepAliveTtl=" + tasksExecutorKeepAliveTtl +
                ", enableDemote=" + enableDemote +
                ", cloudExecutorPollSize=" + cloudExecutorPollSize +
                ", prioritizeExecutorPollSize=" + prioritizeExecutorPollSize +
                '}';
    }
}
