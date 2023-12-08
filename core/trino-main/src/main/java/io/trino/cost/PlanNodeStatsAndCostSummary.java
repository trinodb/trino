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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.isNaN;

public class PlanNodeStatsAndCostSummary
{
    private final double outputRowCount;
    private final double outputSizeInBytes;
    private final double cpuCost;
    private final double memoryCost;
    private final double networkCost;

    @JsonCreator
    public PlanNodeStatsAndCostSummary(
            @JsonProperty("outputRowCount") double outputRowCount,
            @JsonProperty("outputSizeInBytes") double outputSizeInBytes,
            @JsonProperty("cpuCost") double cpuCost,
            @JsonProperty("memoryCost") double memoryCost,
            @JsonProperty("networkCost") double networkCost)
    {
        checkArgument(isNaN(outputRowCount) || outputRowCount >= 0, "outputRowCount cannot be negative: %s", outputRowCount);
        checkArgument(isNaN(outputSizeInBytes) || outputSizeInBytes >= 0, "outputSizeInBytes cannot be negative: %s", outputSizeInBytes);
        checkArgument(isNaN(cpuCost) || cpuCost >= 0, "cpuCost cannot be negative: %s", cpuCost);
        checkArgument(isNaN(memoryCost) || memoryCost >= 0, "memoryCost cannot be negative: %s", memoryCost);
        checkArgument(isNaN(networkCost) || networkCost >= 0, "networkCost cannot be negative: %s", networkCost);
        this.outputRowCount = outputRowCount;
        this.outputSizeInBytes = outputSizeInBytes;
        this.cpuCost = cpuCost;
        this.memoryCost = memoryCost;
        this.networkCost = networkCost;
    }

    @JsonProperty("outputRowCount")
    public double getOutputRowCount()
    {
        return outputRowCount;
    }

    @JsonProperty("outputSizeInBytes")
    public double getOutputSizeInBytes()
    {
        return outputSizeInBytes;
    }

    @JsonProperty("cpuCost")
    public double getCpuCost()
    {
        return cpuCost;
    }

    @JsonProperty("memoryCost")
    public double getMemoryCost()
    {
        return memoryCost;
    }

    @JsonProperty("networkCost")
    public double getNetworkCost()
    {
        return networkCost;
    }
}
