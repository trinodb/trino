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
package io.trino.plugin.resourcegroups;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.spi.resourcegroups.SchedulingPolicy;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ResourceGroupSpec
{
    private static final Pattern PERCENT_PATTERN = Pattern.compile("(\\d{1,3}(:?\\.\\d+)?)%");

    private final ResourceGroupNameTemplate name;
    private final Optional<DataSize> softMemoryLimit;
    private final OptionalDouble softMemoryLimitFraction;
    private final int maxQueued;
    private final Optional<Integer> softConcurrencyLimit;
    private final int hardConcurrencyLimit;
    private final Optional<SchedulingPolicy> schedulingPolicy;
    private final Optional<Integer> schedulingWeight;
    private final List<ResourceGroupSpec> subGroups;
    private final Optional<Boolean> jmxExport;
    private final Optional<Duration> softCpuLimit;
    private final Optional<Duration> hardCpuLimit;
    private final Optional<DataSize> hardPhysicalDataScanLimit;
    private final Optional<DataSize> perQueryMemoryLimit;
    private final Optional<Duration> perQueryCpuLimit;
    private final Optional<DataSize> perQueryScanLimit;
    private final Optional<Integer> hardTotalDriverLimit;
    private final Optional<Integer> hardPlanningConcurrencyLimit;

    @JsonCreator
    public ResourceGroupSpec(
            @JsonProperty("name") ResourceGroupNameTemplate name,
            @JsonProperty("softMemoryLimit") Optional<String> softMemoryLimit,
            @JsonProperty("maxQueued") int maxQueued,
            @JsonProperty("softConcurrencyLimit") Optional<Integer> softConcurrencyLimit,
            @JsonProperty("hardConcurrencyLimit") Optional<Integer> hardConcurrencyLimit,
            @JsonProperty("maxRunning") Optional<Integer> maxRunning,
            @JsonProperty("schedulingPolicy") Optional<String> schedulingPolicy,
            @JsonProperty("schedulingWeight") Optional<Integer> schedulingWeight,
            @JsonProperty("subGroups") Optional<List<ResourceGroupSpec>> subGroups,
            @JsonProperty("jmxExport") Optional<Boolean> jmxExport,
            @JsonProperty("softCpuLimit") Optional<Duration> softCpuLimit,
            @JsonProperty("hardCpuLimit") Optional<Duration> hardCpuLimit,
            @JsonProperty("hardPhysicalDataScanLimit") Optional<DataSize> hardPhysicalDataScanLimit,
            @JsonProperty("perQueryMemoryLimit") Optional<DataSize> perQueryMemoryLimit,
            @JsonProperty("perQueryCpuLimit") Optional<Duration> perQueryCpuLimit,
            @JsonProperty("perQueryScanLimit") Optional<DataSize> perQueryScanLimit,
            @JsonProperty("hardTotalDriverLimit") Optional<Integer> hardTotalDriverLimit,
            @JsonProperty("hardPlanningConcurrencyLimit") Optional<Integer> hardPlanningConcurrencyLimit)
    {
        this.softCpuLimit = requireNonNull(softCpuLimit, "softCpuLimit is null");
        this.hardCpuLimit = requireNonNull(hardCpuLimit, "hardCpuLimit is null");
        this.jmxExport = requireNonNull(jmxExport, "jmxExport is null");
        this.name = requireNonNull(name, "name is null");
        checkArgument(maxQueued >= 0, "maxQueued is negative");
        this.maxQueued = maxQueued;
        this.softConcurrencyLimit = softConcurrencyLimit;
        this.hardPhysicalDataScanLimit = requireNonNull(hardPhysicalDataScanLimit, "hardPhysicalDataScanLimit is null");
        this.perQueryMemoryLimit = requireNonNull(perQueryMemoryLimit, "perQueryMemoryLimit is null");
        this.perQueryCpuLimit = requireNonNull(perQueryCpuLimit, "perQueryCpuLimit is null");
        this.perQueryScanLimit = requireNonNull(perQueryScanLimit, "perQueryScanLimit is null");
        this.hardTotalDriverLimit = requireNonNull(hardTotalDriverLimit, "hardTotalDriverLimit is null");
        this.hardPlanningConcurrencyLimit = requireNonNull(hardPlanningConcurrencyLimit, "hardPlanningConcurrencyLimit is null");
        hardTotalDriverLimit.ifPresent(limit -> checkArgument(limit > 0, "hardTotalDriverLimit must be positive"));
        hardPlanningConcurrencyLimit.ifPresent(limit -> checkArgument(limit > 0, "hardPlanningConcurrencyLimit must be positive"));

        checkArgument(hardConcurrencyLimit.isPresent() || maxRunning.isPresent(), "Missing required property: hardConcurrencyLimit");
        this.hardConcurrencyLimit = hardConcurrencyLimit.orElseGet(maxRunning::get);
        checkArgument(this.hardConcurrencyLimit >= 0, "hardConcurrencyLimit is negative");

        softConcurrencyLimit.ifPresent(soft -> checkArgument(soft >= 0, "softConcurrencyLimit is negative"));
        softConcurrencyLimit.ifPresent(soft -> checkArgument(this.hardConcurrencyLimit >= soft, "hardConcurrencyLimit must be greater than or equal to softConcurrencyLimit"));
        this.schedulingPolicy = schedulingPolicy.map(value -> SchedulingPolicy.valueOf(value.toUpperCase(ENGLISH)));
        this.schedulingWeight = requireNonNull(schedulingWeight, "schedulingWeight is null");

        requireNonNull(softMemoryLimit, "softMemoryLimit is null");
        if (softMemoryLimit.isEmpty()) {
            this.softMemoryLimit = Optional.empty();
            this.softMemoryLimitFraction = OptionalDouble.of(1.0);
        }
        else {
            Matcher matcher = PERCENT_PATTERN.matcher(softMemoryLimit.get());
            if (matcher.matches()) {
                this.softMemoryLimit = Optional.empty();
                this.softMemoryLimitFraction = OptionalDouble.of(Double.parseDouble(matcher.group(1)) / 100.0);
                checkArgument(softMemoryLimitFraction.getAsDouble() <= 1.0, "softMemoryLimit percentage is over 100%");
            }
            else {
                this.softMemoryLimit = Optional.of(DataSize.valueOf(softMemoryLimit.get()));
                this.softMemoryLimitFraction = OptionalDouble.empty();
            }
        }

        this.subGroups = ImmutableList.copyOf(subGroups.orElse(ImmutableList.of()));
        Set<ResourceGroupNameTemplate> names = new HashSet<>();
        for (ResourceGroupSpec subGroup : this.subGroups) {
            checkArgument(names.add(subGroup.getName()), "Duplicated sub group: %s", subGroup.getName());
        }
    }

    public Optional<DataSize> getSoftMemoryLimit()
    {
        return softMemoryLimit;
    }

    public OptionalDouble getSoftMemoryLimitFraction()
    {
        return softMemoryLimitFraction;
    }

    public int getMaxQueued()
    {
        return maxQueued;
    }

    public Optional<Integer> getSoftConcurrencyLimit()
    {
        return softConcurrencyLimit;
    }

    public int getHardConcurrencyLimit()
    {
        return hardConcurrencyLimit;
    }

    public Optional<SchedulingPolicy> getSchedulingPolicy()
    {
        return schedulingPolicy;
    }

    public Optional<Integer> getSchedulingWeight()
    {
        return schedulingWeight;
    }

    public ResourceGroupNameTemplate getName()
    {
        return name;
    }

    public List<ResourceGroupSpec> getSubGroups()
    {
        return subGroups;
    }

    public Optional<Boolean> getJmxExport()
    {
        return jmxExport;
    }

    public Optional<Duration> getSoftCpuLimit()
    {
        return softCpuLimit;
    }

    public Optional<Duration> getHardCpuLimit()
    {
        return hardCpuLimit;
    }

    public Optional<DataSize> getHardPhysicalDataScanLimit()
    {
        return hardPhysicalDataScanLimit;
    }

    public Optional<DataSize> getPerQueryMemoryLimit()
    {
        return perQueryMemoryLimit;
    }

    public Optional<Duration> getPerQueryCpuLimit()
    {
        return perQueryCpuLimit;
    }

    public Optional<DataSize> getPerQueryScanLimit()
    {
        return perQueryScanLimit;
    }

    public Optional<Integer> getHardTotalDriverLimit()
    {
        return hardTotalDriverLimit;
    }

    public Optional<Integer> getHardPlanningConcurrencyLimit()
    {
        return hardPlanningConcurrencyLimit;
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof ResourceGroupSpec that)) {
            return false;
        }
        return (name.equals(that.name) &&
                softMemoryLimit.equals(that.softMemoryLimit) &&
                softMemoryLimitFraction.equals(that.softMemoryLimitFraction) &&
                maxQueued == that.maxQueued &&
                softConcurrencyLimit.equals(that.softConcurrencyLimit) &&
                hardConcurrencyLimit == that.hardConcurrencyLimit &&
                schedulingPolicy.equals(that.schedulingPolicy) &&
                schedulingWeight.equals(that.schedulingWeight) &&
                subGroups.equals(that.subGroups) &&
                jmxExport.equals(that.jmxExport) &&
                softCpuLimit.equals(that.softCpuLimit) &&
                hardCpuLimit.equals(that.hardCpuLimit) &&
                hardPhysicalDataScanLimit.equals(that.hardPhysicalDataScanLimit) &&
                perQueryMemoryLimit.equals(that.perQueryMemoryLimit) &&
                perQueryCpuLimit.equals(that.perQueryCpuLimit) &&
                perQueryScanLimit.equals(that.perQueryScanLimit) &&
                hardTotalDriverLimit.equals(that.hardTotalDriverLimit) &&
                hardPlanningConcurrencyLimit.equals(that.hardPlanningConcurrencyLimit));
    }

    // Subgroups not included, used to determine whether a group needs to be reconfigured
    public boolean sameConfig(ResourceGroupSpec other)
    {
        if (other == null) {
            return false;
        }
        return (name.equals(other.name) &&
                softMemoryLimit.equals(other.softMemoryLimit) &&
                maxQueued == other.maxQueued &&
                softConcurrencyLimit.equals(other.softConcurrencyLimit) &&
                hardConcurrencyLimit == other.hardConcurrencyLimit &&
                schedulingPolicy.equals(other.schedulingPolicy) &&
                schedulingWeight.equals(other.schedulingWeight) &&
                jmxExport.equals(other.jmxExport) &&
                softCpuLimit.equals(other.softCpuLimit) &&
                hardCpuLimit.equals(other.hardCpuLimit) &&
                hardPhysicalDataScanLimit.equals(other.hardPhysicalDataScanLimit) &&
                perQueryMemoryLimit.equals(other.perQueryMemoryLimit) &&
                perQueryCpuLimit.equals(other.perQueryCpuLimit) &&
                perQueryScanLimit.equals(other.perQueryScanLimit) &&
                hardTotalDriverLimit.equals(other.hardTotalDriverLimit) &&
                hardPlanningConcurrencyLimit.equals(other.hardPlanningConcurrencyLimit));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                name,
                softMemoryLimit,
                maxQueued,
                softConcurrencyLimit,
                hardConcurrencyLimit,
                schedulingPolicy,
                schedulingWeight,
                subGroups,
                jmxExport,
                softCpuLimit,
                hardCpuLimit,
                hardPhysicalDataScanLimit,
                perQueryMemoryLimit,
                perQueryCpuLimit,
                perQueryScanLimit,
                hardTotalDriverLimit,
                hardPlanningConcurrencyLimit);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("softMemoryLimit", softMemoryLimit)
                .add("maxQueued", maxQueued)
                .add("softConcurrencyLimit", softConcurrencyLimit)
                .add("hardConcurrencyLimit", hardConcurrencyLimit)
                .add("schedulingPolicy", schedulingPolicy)
                .add("schedulingWeight", schedulingWeight)
                .add("jmxExport", jmxExport)
                .add("softCpuLimit", softCpuLimit)
                .add("hardCpuLimit", hardCpuLimit)
                .add("hardPhysicalDataScanLimit", hardPhysicalDataScanLimit)
                .add("perQueryMemoryLimit", perQueryMemoryLimit)
                .add("perQueryCpuLimit", perQueryCpuLimit)
                .add("perQueryScanLimit", perQueryScanLimit)
                .add("hardTotalDriverLimit", hardTotalDriverLimit)
                .add("hardPlanningConcurrencyLimit", hardPlanningConcurrencyLimit)
                .toString();
    }
}
