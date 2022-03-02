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
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig({
        "experimental.cluster-memory-manager-enabled",
        "query.low-memory-killer.enabled",
        "resources.reserved-system-memory"})
public class MemoryManagerConfig
{
    public static final String FAULT_TOLERANT_TASK_MEMORY_CONFIG = "fault-tolerant-task-memory";

    // enforced against user memory allocations
    private DataSize maxQueryMemory = DataSize.of(20, GIGABYTE);
    // enforced against user + system memory allocations (default is maxQueryMemory * 2)
    private DataSize maxQueryTotalMemory;
    private DataSize faultTolerantTaskMemory = DataSize.of(1, GIGABYTE);
    private LowMemoryKillerPolicy lowMemoryKillerPolicy = LowMemoryKillerPolicy.TOTAL_RESERVATION_ON_BLOCKED_NODES;
    private Duration killOnOutOfMemoryDelay = new Duration(5, MINUTES);

    public LowMemoryKillerPolicy getLowMemoryKillerPolicy()
    {
        return lowMemoryKillerPolicy;
    }

    @Config("query.low-memory-killer.policy")
    public MemoryManagerConfig setLowMemoryKillerPolicy(LowMemoryKillerPolicy lowMemoryKillerPolicy)
    {
        this.lowMemoryKillerPolicy = lowMemoryKillerPolicy;
        return this;
    }

    @NotNull
    @MinDuration("5s")
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
    public DataSize getFaultTolerantTaskMemory()
    {
        return faultTolerantTaskMemory;
    }

    @Config(FAULT_TOLERANT_TASK_MEMORY_CONFIG)
    @ConfigDescription("Estimated amount of memory a single task will use when task level retries are used; value is used allocating nodes for tasks execution")
    public MemoryManagerConfig setFaultTolerantTaskMemory(DataSize faultTolerantTaskMemory)
    {
        this.faultTolerantTaskMemory = faultTolerantTaskMemory;
        return this;
    }

    public enum LowMemoryKillerPolicy
    {
        NONE,
        TOTAL_RESERVATION,
        TOTAL_RESERVATION_ON_BLOCKED_NODES,
        /**/;

        public static LowMemoryKillerPolicy fromString(String value)
        {
            switch (requireNonNull(value, "value is null").toLowerCase(ENGLISH)) {
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
}
