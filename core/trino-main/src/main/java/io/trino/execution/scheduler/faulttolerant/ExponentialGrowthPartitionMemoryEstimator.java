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
package io.trino.execution.scheduler.faulttolerant;

import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.stats.TDigest;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.memory.ClusterMemoryManager;
import io.trino.memory.MemoryInfo;
import io.trino.memory.MemoryManagerConfig;
import io.trino.spi.ErrorCode;
import io.trino.spi.memory.MemoryPoolInfo;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.assertj.core.util.VisibleForTesting;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.SystemSessionProperties.getFaultTolerantExecutionDefaultCoordinatorTaskMemory;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionDefaultTaskMemory;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionTaskMemoryEstimationQuantile;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionTaskMemoryGrowthFactor;
import static io.trino.execution.scheduler.ErrorCodes.isOutOfMemoryError;
import static io.trino.execution.scheduler.ErrorCodes.isWorkerCrashAssociatedError;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class ExponentialGrowthPartitionMemoryEstimator
        implements PartitionMemoryEstimator
{
    public static class Factory
            implements PartitionMemoryEstimatorFactory
    {
        private static final Logger log = Logger.get(Factory.class);

        private final Supplier<Map<String, Optional<MemoryInfo>>> workerMemoryInfoSupplier;
        private final boolean memoryRequirementIncreaseOnWorkerCrashEnabled;
        private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
        private final AtomicReference<Optional<DataSize>> maxNodePoolSize = new AtomicReference<>(Optional.empty());

        @Inject
        public Factory(
                ClusterMemoryManager clusterMemoryManager,
                MemoryManagerConfig memoryManagerConfig)
        {
            this(
                    clusterMemoryManager::getWorkerMemoryInfo,
                    memoryManagerConfig.isFaultTolerantExecutionMemoryRequirementIncreaseOnWorkerCrashEnabled());
        }

        @VisibleForTesting
        Factory(
                Supplier<Map<String, Optional<MemoryInfo>>> workerMemoryInfoSupplier,
                boolean memoryRequirementIncreaseOnWorkerCrashEnabled)
        {
            this.workerMemoryInfoSupplier = requireNonNull(workerMemoryInfoSupplier, "workerMemoryInfoSupplier is null");
            this.memoryRequirementIncreaseOnWorkerCrashEnabled = memoryRequirementIncreaseOnWorkerCrashEnabled;
        }

        @PostConstruct
        public void start()
        {
            refreshNodePoolMemoryInfos();
            executor.scheduleWithFixedDelay(() -> {
                try {
                    refreshNodePoolMemoryInfos();
                }
                catch (Throwable e) {
                    // ignore to avoid getting unscheduled
                    log.error(e, "Unexpected error while refreshing node pool memory infos");
                }
            }, 1, 1, TimeUnit.SECONDS);
        }

        @PreDestroy
        public void stop()
        {
            executor.shutdownNow();
        }

        @VisibleForTesting
        void refreshNodePoolMemoryInfos()
        {
            Map<String, Optional<MemoryInfo>> workerMemoryInfos = workerMemoryInfoSupplier.get();
            long maxNodePoolSizeBytes = -1;
            for (Map.Entry<String, Optional<MemoryInfo>> entry : workerMemoryInfos.entrySet()) {
                if (entry.getValue().isEmpty()) {
                    continue;
                }
                MemoryPoolInfo poolInfo = entry.getValue().get().getPool();
                maxNodePoolSizeBytes = Math.max(poolInfo.getMaxBytes(), maxNodePoolSizeBytes);
            }
            maxNodePoolSize.set(maxNodePoolSizeBytes == -1 ? Optional.empty() : Optional.of(DataSize.ofBytes(maxNodePoolSizeBytes)));
        }

        @Override
        public PartitionMemoryEstimator createPartitionMemoryEstimator(
                Session session,
                PlanFragment planFragment,
                Function<PlanFragmentId, PlanFragment> sourceFragmentLookup)
        {
            DataSize defaultInitialMemoryLimit = planFragment.getPartitioning().equals(COORDINATOR_DISTRIBUTION) ?
                    getFaultTolerantExecutionDefaultCoordinatorTaskMemory(session) :
                    getFaultTolerantExecutionDefaultTaskMemory(session);

            return new ExponentialGrowthPartitionMemoryEstimator(
                    defaultInitialMemoryLimit,
                    memoryRequirementIncreaseOnWorkerCrashEnabled,
                    getFaultTolerantExecutionTaskMemoryGrowthFactor(session),
                    getFaultTolerantExecutionTaskMemoryEstimationQuantile(session),
                    maxNodePoolSize::get);
        }
    }

    private final DataSize defaultInitialMemoryLimit;
    private final boolean memoryRequirementIncreaseOnWorkerCrashEnabled;
    private final double growthFactor;
    private final double estimationQuantile;

    private final Supplier<Optional<DataSize>> maxNodePoolSizeSupplier;
    private final TDigest memoryUsageDistribution = new TDigest();

    private ExponentialGrowthPartitionMemoryEstimator(
            DataSize defaultInitialMemoryLimit,
            boolean memoryRequirementIncreaseOnWorkerCrashEnabled,
            double growthFactor,
            double estimationQuantile,
            Supplier<Optional<DataSize>> maxNodePoolSizeSupplier)
    {
        this.defaultInitialMemoryLimit = requireNonNull(defaultInitialMemoryLimit, "defaultInitialMemoryLimit is null");
        this.memoryRequirementIncreaseOnWorkerCrashEnabled = memoryRequirementIncreaseOnWorkerCrashEnabled;
        this.growthFactor = growthFactor;
        this.estimationQuantile = estimationQuantile;
        this.maxNodePoolSizeSupplier = requireNonNull(maxNodePoolSizeSupplier, "maxNodePoolSizeSupplier is null");
    }

    @Override
    public MemoryRequirements getInitialMemoryRequirements()
    {
        DataSize memory = Ordering.natural().max(defaultInitialMemoryLimit, getEstimatedMemoryUsage());
        memory = capMemoryToMaxNodeSize(memory);
        return new MemoryRequirements(memory);
    }

    @Override
    public MemoryRequirements getNextRetryMemoryRequirements(MemoryRequirements previousMemoryRequirements, DataSize peakMemoryUsage, ErrorCode errorCode)
    {
        DataSize previousMemory = previousMemoryRequirements.getRequiredMemory();

        // start with the maximum of previously used memory and actual usage
        DataSize newMemory = Ordering.natural().max(peakMemoryUsage, previousMemory);
        if (shouldIncreaseMemoryRequirement(errorCode)) {
            // multiply if we hit an oom error

            newMemory = DataSize.of((long) (newMemory.toBytes() * growthFactor), DataSize.Unit.BYTE);
        }

        // if we are still below current estimate for new partition let's bump further
        newMemory = Ordering.natural().max(newMemory, getEstimatedMemoryUsage());

        newMemory = capMemoryToMaxNodeSize(newMemory);
        return new MemoryRequirements(newMemory);
    }

    private DataSize capMemoryToMaxNodeSize(DataSize memory)
    {
        Optional<DataSize> currentMaxNodePoolSize = maxNodePoolSizeSupplier.get();
        if (currentMaxNodePoolSize.isEmpty()) {
            return memory;
        }
        return Ordering.natural().min(memory, currentMaxNodePoolSize.get());
    }

    @Override
    public synchronized void registerPartitionFinished(MemoryRequirements previousMemoryRequirements, DataSize peakMemoryUsage, boolean success, Optional<ErrorCode> errorCode)
    {
        if (success) {
            memoryUsageDistribution.add(peakMemoryUsage.toBytes());
        }
        if (!success && errorCode.isPresent() && shouldIncreaseMemoryRequirement(errorCode.get())) {
            // take previousRequiredBytes into account when registering failure on oom. It is conservative hence safer (and in-line with getNextRetryMemoryRequirements)
            long previousRequiredBytes = previousMemoryRequirements.getRequiredMemory().toBytes();
            long previousPeakBytes = peakMemoryUsage.toBytes();
            memoryUsageDistribution.add(Math.max(previousRequiredBytes, previousPeakBytes) * growthFactor);
        }
    }

    private synchronized DataSize getEstimatedMemoryUsage()
    {
        double estimation = memoryUsageDistribution.valueAt(estimationQuantile);
        if (Double.isNaN(estimation)) {
            return DataSize.ofBytes(0);
        }
        return DataSize.ofBytes((long) estimation);
    }

    private String memoryUsageDistributionInfo()
    {
        double[] quantiles = new double[] {0.01, 0.05, 0.1, 0.2, 0.5, 0.8, 0.9, 0.95, 0.99};
        double[] values;
        synchronized (this) {
            values = memoryUsageDistribution.valuesAt(quantiles);
        }

        return IntStream.range(0, quantiles.length)
                .mapToObj(i -> "" + quantiles[i] + "=" + values[i])
                .collect(Collectors.joining(", ", "[", "]"));
    }

    @Override
    public String toString()
    {
        return "memoryUsageDistribution=" + memoryUsageDistributionInfo();
    }

    private boolean shouldIncreaseMemoryRequirement(ErrorCode errorCode)
    {
        return isOutOfMemoryError(errorCode) || (memoryRequirementIncreaseOnWorkerCrashEnabled && isWorkerCrashAssociatedError(errorCode));
    }
}
