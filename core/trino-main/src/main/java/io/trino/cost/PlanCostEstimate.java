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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.cost.EstimateConfidence.HIGH;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

public final class PlanCostEstimate
{
    private static final PlanCostEstimate INFINITE = new PlanCostEstimate(POSITIVE_INFINITY, POSITIVE_INFINITY, POSITIVE_INFINITY, POSITIVE_INFINITY);
    private static final PlanCostEstimate UNKNOWN = new PlanCostEstimate(NaN, NaN, NaN, NaN);
    private static final PlanCostEstimate ZERO = new PlanCostEstimate(0, 0, 0, 0);

    private final double cpuCost;
    private final double maxMemory;
    private final double maxMemoryWhenOutputting;
    private final double networkCost;
    private final LocalCostEstimate rootNodeLocalCostEstimate;
    private final EstimateConfidence confidence;

    public static PlanCostEstimate infinite()
    {
        return INFINITE;
    }

    public static PlanCostEstimate unknown()
    {
        return UNKNOWN;
    }

    public static PlanCostEstimate zero()
    {
        return ZERO;
    }

    public PlanCostEstimate(
            double cpuCost,
            double maxMemory,
            double maxMemoryWhenOutputting,
            double networkCost)
    {
        this(cpuCost, maxMemory, maxMemoryWhenOutputting, networkCost, LocalCostEstimate.unknown());
    }

    public PlanCostEstimate(
            double cpuCost,
            double maxMemory,
            double maxMemoryWhenOutputting,
            double networkCost,
            LocalCostEstimate rootNodeLocalCostEstimate)
    {
        this(cpuCost, maxMemory, maxMemoryWhenOutputting, networkCost, rootNodeLocalCostEstimate, HIGH);
    }

    @JsonCreator
    public PlanCostEstimate(
            @JsonProperty("cpuCost") double cpuCost,
            @JsonProperty("maxMemory") double maxMemory,
            @JsonProperty("maxMemoryWhenOutputting") double maxMemoryWhenOutputting,
            @JsonProperty("networkCost") double networkCost,
            @JsonProperty("rootNodeLocalCostEstimate") LocalCostEstimate rootNodeLocalCostEstimate,
            @JsonProperty("confidence") EstimateConfidence confidence)
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
        this.rootNodeLocalCostEstimate = requireNonNull(rootNodeLocalCostEstimate, "rootNodeLocalCostEstimate is null");
        // costs serialized before confidence was tracked carry no value for it
        this.confidence = confidence == null ? HIGH : confidence;
    }

    /**
     * Returns CPU component of the cost.
     * <p>
     * Unknown value is represented by {@link Double#NaN}
     */
    @JsonProperty
    public double getCpuCost()
    {
        return cpuCost;
    }

    /**
     * Returns maximal memory usage of a query plan (or subplan).
     * <p>
     * Unknown value is represented by {@link Double#NaN}
     */
    @JsonProperty
    public double getMaxMemory()
    {
        return maxMemory;
    }

    /**
     * Returns maximal memory usage of a query plan (or subplan) after a first output row was produced.
     * When this cost represents a cost of a subplan, this information can be used to determine maximum memory
     * usage (and maximum memory usage after a first output row was produced) for plan nodes higher up in the plan tree.
     * <p>
     * Unknown value is represented by {@link Double#NaN}.
     */
    @JsonProperty
    public double getMaxMemoryWhenOutputting()
    {
        return maxMemoryWhenOutputting;
    }

    /**
     * Returns network component of the cost.
     * <p>
     * Unknown value is represented by {@link Double#NaN}
     */
    @JsonProperty
    public double getNetworkCost()
    {
        return networkCost;
    }

    /**
     * Returns local cost estimate for the root plan node.
     */
    @JsonProperty
    public LocalCostEstimate getRootNodeLocalCostEstimate()
    {
        return rootNodeLocalCostEstimate;
    }

    /**
     * Returns how much the engine trusts the statistics this cost was derived from.
     */
    @JsonProperty
    public EstimateConfidence getConfidence()
    {
        return confidence;
    }

    public PlanCostEstimate withConfidence(EstimateConfidence confidence)
    {
        if (this.confidence == confidence) {
            return this;
        }
        return new PlanCostEstimate(cpuCost, maxMemory, maxMemoryWhenOutputting, networkCost, rootNodeLocalCostEstimate, confidence);
    }

    /**
     * Returns true if this cost has unknown components.
     */
    public boolean hasUnknownComponents()
    {
        return isNaN(cpuCost) || isNaN(maxMemory) || isNaN(maxMemoryWhenOutputting) || isNaN(networkCost);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("cpu", cpuCost)
                .add("memory", maxMemory)
                // maxMemoryWhenOutputting is not that useful in toString
                .add("network", networkCost)
                .add("rootNodeLocalCostEstimate", rootNodeLocalCostEstimate)
                .add("confidence", confidence)
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
        PlanCostEstimate that = (PlanCostEstimate) o;
        return Double.compare(that.cpuCost, cpuCost) == 0 &&
                Double.compare(that.maxMemory, maxMemory) == 0 &&
                Double.compare(that.maxMemoryWhenOutputting, maxMemoryWhenOutputting) == 0 &&
                Double.compare(that.networkCost, networkCost) == 0 &&
                Objects.equals(rootNodeLocalCostEstimate, that.rootNodeLocalCostEstimate) &&
                confidence == that.confidence;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cpuCost, maxMemory, maxMemoryWhenOutputting, networkCost, rootNodeLocalCostEstimate, confidence);
    }
}
