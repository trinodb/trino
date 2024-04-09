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
package io.trino.cost;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Objects;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;

/**
 * Represents inherent cost of some plan node, not including cost of its sources.
 */
public class LocalCostEstimate
{
    private final double cpuCost;
    private final double maxMemory;
    private final double networkCost;

    public static LocalCostEstimate unknown()
    {
        return of(NaN, NaN, NaN);
    }

    public static LocalCostEstimate zero()
    {
        return of(0, 0, 0);
    }

    public static LocalCostEstimate ofCpu(double cpuCost)
    {
        return of(cpuCost, 0, 0);
    }

    public static LocalCostEstimate ofNetwork(double networkCost)
    {
        return of(0, 0, networkCost);
    }

    public static LocalCostEstimate of(double cpuCost, double maxMemory, double networkCost)
    {
        return new LocalCostEstimate(cpuCost, maxMemory, networkCost);
    }

    @JsonCreator
    public LocalCostEstimate(
            @JsonProperty("cpuCost") double cpuCost,
            @JsonProperty("maxMemory") double maxMemory,
            @JsonProperty("networkCost") double networkCost)
    {
        checkArgument(isNaN(cpuCost) || cpuCost >= 0, "cpuCost cannot be negative: %s", cpuCost);
        checkArgument(isNaN(maxMemory) || maxMemory >= 0, "maxMemory cannot be negative: %s", maxMemory);
        checkArgument(isNaN(networkCost) || networkCost >= 0, "networkCost cannot be negative: %s", networkCost);
        this.cpuCost = cpuCost;
        this.maxMemory = maxMemory;
        this.networkCost = networkCost;
    }

    @JsonProperty
    public double getCpuCost()
    {
        return cpuCost;
    }

    @JsonProperty
    public double getMaxMemory()
    {
        return maxMemory;
    }

    @JsonProperty
    public double getNetworkCost()
    {
        return networkCost;
    }

    /**
     * @deprecated This class represents individual cost of a part of a plan (usually of a single {@link PlanNode}). Use {@link CostProvider} instead.
     */
    @Deprecated
    public PlanCostEstimate toPlanCost()
    {
        return new PlanCostEstimate(cpuCost, maxMemory, maxMemory, networkCost, this);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("cpuCost", cpuCost)
                .add("maxMemory", maxMemory)
                .add("networkCost", networkCost)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LocalCostEstimate that = (LocalCostEstimate) o;
        return Double.compare(that.cpuCost, cpuCost) == 0 &&
                Double.compare(that.maxMemory, maxMemory) == 0 &&
                Double.compare(that.networkCost, networkCost) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cpuCost, maxMemory, networkCost);
    }

    /**
     * Sums partial cost estimates of some (single) plan node.
     */
    public static LocalCostEstimate addPartialComponents(LocalCostEstimate one, LocalCostEstimate two, LocalCostEstimate... more)
    {
        return Stream.concat(Stream.of(one, two), Stream.of(more))
                .reduce(zero(), (a, b) -> new LocalCostEstimate(
                        a.cpuCost + b.cpuCost,
                        a.maxMemory + b.maxMemory,
                        a.networkCost + b.networkCost));
    }
}
