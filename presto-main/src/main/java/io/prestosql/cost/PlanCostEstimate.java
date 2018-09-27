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
package io.prestosql.cost;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;

public final class PlanCostEstimate
{
    public static PlanCostEstimate zero()
    {
        return new PlanCostEstimate(0, 0, 0, 0);
    }

    public static PlanCostEstimate unknown()
    {
        return new PlanCostEstimate(NaN, NaN, NaN, NaN);
    }

    private final double cpuCost;
    private final double maxMemory;
    private final double maxMemoryWhenOutputting;
    private final double networkCost;

    public PlanCostEstimate(double cpuCost, double maxMemory, double maxMemoryWhenOutputting, double networkCost)
    {
        checkArgument(!(cpuCost < 0), "cpuCost cannot be negative: %s", cpuCost);
        checkArgument(!(maxMemory < 0), "maxMemory cannot be negative: %s", maxMemory);
        checkArgument(!(maxMemoryWhenOutputting < 0), "maxMemoryWhenOutputting cannot be negative: %s", maxMemoryWhenOutputting);
        checkArgument(!(maxMemoryWhenOutputting > maxMemory), "maxMemoryWhenOutputting cannot be greater than maxMemory: %s > %s", maxMemoryWhenOutputting, maxMemory);
        checkArgument(!(networkCost < 0), "networkCost cannot be negative: %s", networkCost);
        this.cpuCost = cpuCost;
        this.maxMemory = maxMemory;
        this.maxMemoryWhenOutputting = maxMemoryWhenOutputting;
        this.networkCost = networkCost;
    }

    public double getCpuCost()
    {
        return cpuCost;
    }

    public double getMaxMemory()
    {
        return maxMemory;
    }

    public double getMaxMemoryWhenOutputting()
    {
        return maxMemoryWhenOutputting;
    }

    public double getNetworkCost()
    {
        return networkCost;
    }

    public PlanNodeCostEstimate toPlanNodeCostEstimate()
    {
        return new PlanNodeCostEstimate(cpuCost, maxMemory, networkCost);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("cpu", cpuCost)
                .add("memory", maxMemory)
                .add("network", networkCost)
                .toString();
    }
}
