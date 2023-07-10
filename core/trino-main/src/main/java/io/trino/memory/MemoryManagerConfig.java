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
package io.trino.memory;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import jakarta.validation.constraints.NotNull;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig({
        "experimental.cluster-memory-manager-enabled",
        "query.low-memory-killer.enabled",
        "resources.reserved-system-memory"})
public class MemoryManagerConfig
{
    // enforced against user memory allocations
    private DataSize maxQueryMemory = DataSize.of(20, GIGABYTE);
    // enforced against user + system memory allocations (default is maxQueryMemory * 2)
    private DataSize maxQueryTotalMemory;
    private DataSize faultTolerantExecutionCoordinatorTaskMemory = DataSize.of(2, GIGABYTE);
    private DataSize faultTolerantExecutionTaskMemory = DataSize.of(5, GIGABYTE);
    private double faultTolerantExecutionTaskMemoryGrowthFactor = 3.0;
    private double faultTolerantExecutionTaskMemoryEstimationQuantile = 0.9;
    private DataSize faultTolerantExecutionTaskRuntimeMemoryEstimationOverhead = DataSize.of(1, GIGABYTE);
    private LowMemoryQueryKillerPolicy lowMemoryQueryKillerPolicy = LowMemoryQueryKillerPolicy.TOTAL_RESERVATION_ON_BLOCKED_NODES;
    private LowMemoryTaskKillerPolicy lowMemoryTaskKillerPolicy = LowMemoryTaskKillerPolicy.TOTAL_RESERVATION_ON_BLOCKED_NODES;
    private boolean faultTolerantExecutionMemoryRequirementIncreaseOnWorkerCrashEnabled = true;

    /**
     * default value is overwritten for fault tolerant execution in {@link #applyFaultTolerantExecutionDefaults()}}
     */
    private Duration killOnOutOfMemoryDelay = new Duration(5, MINUTES);

    public LowMemoryQueryKillerPolicy getLowMemoryQueryKillerPolicy()
    {
        return lowMemoryQueryKillerPolicy;
    }

    @Config("query.low-memory-killer.policy")
    public MemoryManagerConfig setLowMemoryQueryKillerPolicy(LowMemoryQueryKillerPolicy lowMemoryQueryKillerPolicy)
    {
        this.lowMemoryQueryKillerPolicy = lowMemoryQueryKillerPolicy;
        return this;
    }

    public LowMemoryTaskKillerPolicy getLowMemoryTaskKillerPolicy()
    {
        return lowMemoryTaskKillerPolicy;
    }

    @Config("task.low-memory-killer.policy")
    public MemoryManagerConfig setLowMemoryTaskKillerPolicy(LowMemoryTaskKillerPolicy lowMemoryTaskKillerPolicy)
    {
        this.lowMemoryTaskKillerPolicy = lowMemoryTaskKillerPolicy;
        return this;
    }

    @NotNull
    public Duration getKillOnOutOfMemoryDelay()
    {
        return killOnOutOfMemoryDelay;
    }

    @Config("query.low-memory-killer.delay")
    @ConfigDescription("Delay between cluster running low on memory and invoking killer")
    public MemoryManagerConfig setKillOnOutOfMemoryDelay(Duration killOnOutOfMemoryDelay)
    {
        this.killOnOutOfMemoryDelay = killOnOutOfMemoryDelay;
        return this;
    }

    @NotNull
    public DataSize getMaxQueryMemory()
    {
        return maxQueryMemory;
    }

    @Config("query.max-memory")
    public MemoryManagerConfig setMaxQueryMemory(DataSize maxQueryMemory)
    {
        this.maxQueryMemory = maxQueryMemory;
        return this;
    }

    @NotNull
    public DataSize getMaxQueryTotalMemory()
    {
        if (maxQueryTotalMemory == null) {
            return succinctBytes(maxQueryMemory.toBytes() * 2);
        }
        return maxQueryTotalMemory;
    }

    @Config("query.max-total-memory")
    public MemoryManagerConfig setMaxQueryTotalMemory(DataSize maxQueryTotalMemory)
    {
        this.maxQueryTotalMemory = maxQueryTotalMemory;
        return this;
    }

    @NotNull
    public DataSize getFaultTolerantExecutionCoordinatorTaskMemory()
    {
        return faultTolerantExecutionCoordinatorTaskMemory;
    }

    @Config("fault-tolerant-execution-coordinator-task-memory")
    @ConfigDescription("Estimated amount of memory a single coordinator task will use when task level retries are used; value is used when allocating nodes for tasks execution")
    public MemoryManagerConfig setFaultTolerantExecutionCoordinatorTaskMemory(DataSize faultTolerantExecutionCoordinatorTaskMemory)
    {
        this.faultTolerantExecutionCoordinatorTaskMemory = faultTolerantExecutionCoordinatorTaskMemory;
        return this;
    }

    @NotNull
    public DataSize getFaultTolerantExecutionTaskMemory()
    {
        return faultTolerantExecutionTaskMemory;
    }

    @Config("fault-tolerant-execution-task-memory")
    @ConfigDescription("Estimated amount of memory a single task will use when task level retries are used; value is used when allocating nodes for tasks execution")
    public MemoryManagerConfig setFaultTolerantExecutionTaskMemory(DataSize faultTolerantExecutionTaskMemory)
    {
        this.faultTolerantExecutionTaskMemory = faultTolerantExecutionTaskMemory;
        return this;
    }

    @NotNull
    public DataSize getFaultTolerantExecutionTaskRuntimeMemoryEstimationOverhead()
    {
        return faultTolerantExecutionTaskRuntimeMemoryEstimationOverhead;
    }

    @Config("fault-tolerant-execution-task-runtime-memory-estimation-overhead")
    @ConfigDescription("Extra memory to account for when estimating actual task runtime memory consumption")
    public MemoryManagerConfig setFaultTolerantExecutionTaskRuntimeMemoryEstimationOverhead(DataSize faultTolerantExecutionTaskRuntimeMemoryEstimationOverhead)
    {
        this.faultTolerantExecutionTaskRuntimeMemoryEstimationOverhead = faultTolerantExecutionTaskRuntimeMemoryEstimationOverhead;
        return this;
    }

    @NotNull
    public double getFaultTolerantExecutionTaskMemoryGrowthFactor()
    {
        return faultTolerantExecutionTaskMemoryGrowthFactor;
    }

    @Config("fault-tolerant-execution-task-memory-growth-factor")
    @ConfigDescription("Factor by which estimated task memory is increased if task execution runs out of memory; value is used allocating nodes for tasks execution")
    public MemoryManagerConfig setFaultTolerantExecutionTaskMemoryGrowthFactor(double faultTolerantExecutionTaskMemoryGrowthFactor)
    {
        checkArgument(faultTolerantExecutionTaskMemoryGrowthFactor >= 1.0, "faultTolerantExecutionTaskMemoryGrowthFactor must not be less than 1.0");
        this.faultTolerantExecutionTaskMemoryGrowthFactor = faultTolerantExecutionTaskMemoryGrowthFactor;
        return this;
    }

    @NotNull
    public double getFaultTolerantExecutionTaskMemoryEstimationQuantile()
    {
        return faultTolerantExecutionTaskMemoryEstimationQuantile;
    }

    @Config("fault-tolerant-execution-task-memory-estimation-quantile")
    @ConfigDescription("What quantile of memory usage of completed tasks to look at when estimating memory usage for upcoming tasks")
    public MemoryManagerConfig setFaultTolerantExecutionTaskMemoryEstimationQuantile(double faultTolerantExecutionTaskMemoryEstimationQuantile)
    {
        checkArgument(faultTolerantExecutionTaskMemoryEstimationQuantile >= 0.0 && faultTolerantExecutionTaskMemoryEstimationQuantile <= 1.0,
                "fault-tolerant-execution-task-memory-estimation-quantile must not be in [0.0, 1.0] range");
        this.faultTolerantExecutionTaskMemoryEstimationQuantile = faultTolerantExecutionTaskMemoryEstimationQuantile;
        return this;
    }

    public boolean isFaultTolerantExecutionMemoryRequirementIncreaseOnWorkerCrashEnabled()
    {
        return faultTolerantExecutionMemoryRequirementIncreaseOnWorkerCrashEnabled;
    }

    @Config("fault-tolerant-execution.memory-requirement-increase-on-worker-crash-enabled")
    @ConfigDescription("Increase memory requirement for tasks failed due to a suspected worker crash")
    public MemoryManagerConfig setFaultTolerantExecutionMemoryRequirementIncreaseOnWorkerCrashEnabled(boolean faultTolerantExecutionMemoryRequirementIncreaseOnWorkerCrashEnabled)
    {
        this.faultTolerantExecutionMemoryRequirementIncreaseOnWorkerCrashEnabled = faultTolerantExecutionMemoryRequirementIncreaseOnWorkerCrashEnabled;
        return this;
    }

    public void applyFaultTolerantExecutionDefaults()
    {
        killOnOutOfMemoryDelay = new Duration(0, MINUTES);
    }

    public enum LowMemoryQueryKillerPolicy
    {
        NONE,
        TOTAL_RESERVATION,
        TOTAL_RESERVATION_ON_BLOCKED_NODES,
        /**/;

        public static LowMemoryQueryKillerPolicy fromString(String value)
        {
            switch (value.toLowerCase(ENGLISH)) {
                case "none":
                    return NONE;
                case "total-reservation":
                    return TOTAL_RESERVATION;
                case "total-reservation-on-blocked-nodes":
                    return TOTAL_RESERVATION_ON_BLOCKED_NODES;
            }

            throw new IllegalArgumentException(format("Unrecognized value: '%s'", value));
        }
    }

    public enum LowMemoryTaskKillerPolicy
    {
        NONE,
        TOTAL_RESERVATION_ON_BLOCKED_NODES,
        LEAST_WASTE,
        /**/;

        public static LowMemoryTaskKillerPolicy fromString(String value)
        {
            switch (value.toLowerCase(ENGLISH)) {
                case "none":
                    return NONE;
                case "total-reservation-on-blocked-nodes":
                    return TOTAL_RESERVATION_ON_BLOCKED_NODES;
                case "least-waste":
                    return LEAST_WASTE;
            }

            throw new IllegalArgumentException(format("Unrecognized value: '%s'", value));
        }
    }
}
