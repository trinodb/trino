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
package io.trino.execution.admission;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Cluster-wide configuration for resource-aware query admission.
 * <p>
 * When enabled, a query is held in {@code WAITING_FOR_RESOURCES} until the cluster has enough
 * observed free memory and vCPU to run it. This admission gate is independent of, and additional
 * to, the existing minimum-worker-count requirement ({@code query-manager.required-workers}); the
 * worker-count gate is unchanged.
 * <p>
 * The feature is off by default ({@code enabled=false}). When disabled, admission behaves exactly
 * as it does without this gate.
 */
public class ResourceAwareAdmissionConfig
{
    private boolean enabled;
    private DataSize requiredFreeMemory = DataSize.of(0, BYTE);
    private int requiredFreeVcpu;
    private Duration maxWait = new Duration(5, MINUTES);
    private Duration pollInterval = new Duration(1, SECONDS);
    private int memoryWindowSize = 50;
    private double requiredMemoryMultiplier = 1.0;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("query-admission.enabled")
    @ConfigDescription("Hold queries in WAITING_FOR_RESOURCES until the cluster has enough free capacity")
    public ResourceAwareAdmissionConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @NotNull
    public DataSize getRequiredFreeMemory()
    {
        return requiredFreeMemory;
    }

    @Config("query-admission.required-free-memory")
    @ConfigDescription("Minimum cluster-wide free memory required before a query is admitted (cold-start default before history accumulates)")
    public ResourceAwareAdmissionConfig setRequiredFreeMemory(DataSize requiredFreeMemory)
    {
        this.requiredFreeMemory = requiredFreeMemory;
        return this;
    }

    @Min(0)
    public int getRequiredFreeVcpu()
    {
        return requiredFreeVcpu;
    }

    @Config("query-admission.required-free-vcpu")
    @ConfigDescription("Minimum cluster-wide free vCPU required before a query is admitted")
    public ResourceAwareAdmissionConfig setRequiredFreeVcpu(int requiredFreeVcpu)
    {
        this.requiredFreeVcpu = requiredFreeVcpu;
        return this;
    }

    @NotNull
    @MinDuration("0s")
    public Duration getMaxWait()
    {
        return maxWait;
    }

    @Config("query-admission.max-wait")
    @ConfigDescription("Maximum time a query is held waiting for capacity before failing")
    public ResourceAwareAdmissionConfig setMaxWait(Duration maxWait)
    {
        this.maxWait = maxWait;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getPollInterval()
    {
        return pollInterval;
    }

    @Config("query-admission.poll-interval")
    @ConfigDescription("How often held queries are re-evaluated against observed cluster capacity")
    public ResourceAwareAdmissionConfig setPollInterval(Duration pollInterval)
    {
        this.pollInterval = pollInterval;
        return this;
    }

    @Min(1)
    public int getMemoryWindowSize()
    {
        return memoryWindowSize;
    }

    @Config("query-admission.memory-window-size")
    @ConfigDescription("Number of most-recent completed queries averaged to estimate a query's required memory")
    public ResourceAwareAdmissionConfig setMemoryWindowSize(int memoryWindowSize)
    {
        this.memoryWindowSize = memoryWindowSize;
        return this;
    }

    @DecimalMin("0")
    public double getRequiredMemoryMultiplier()
    {
        return requiredMemoryMultiplier;
    }

    @Config("query-admission.required-memory-multiplier")
    @ConfigDescription("Headroom multiplier applied to the rolling-average required memory before admitting a query")
    public ResourceAwareAdmissionConfig setRequiredMemoryMultiplier(double requiredMemoryMultiplier)
    {
        this.requiredMemoryMultiplier = requiredMemoryMultiplier;
        return this;
    }
}
