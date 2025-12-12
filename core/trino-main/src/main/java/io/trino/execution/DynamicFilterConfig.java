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
package io.trino.execution;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

@DefunctConfig({
        "dynamic-filtering-max-per-driver-row-count",
        "dynamic-filtering-max-per-driver-size",
        "dynamic-filtering-range-row-limit-per-driver",
        "dynamic-filtering.service-thread-count",
        "experimental.dynamic-filtering-max-per-driver-row-count",
        "experimental.dynamic-filtering-max-per-driver-size",
        "experimental.dynamic-filtering-refresh-interval",
        "experimental.enable-dynamic-filtering",
        "enable-coordinator-dynamic-filters-distribution",
        "dynamic-filtering.small.max-distinct-values-per-driver",
        "dynamic-filtering.small.max-size-per-driver",
        "dynamic-filtering.small.range-row-limit-per-driver",
        "dynamic-filtering.small.max-size-per-operator",
        "dynamic-filtering.small.max-size-per-filter",
        "dynamic-filtering.small-broadcast.max-distinct-values-per-driver",
        "dynamic-filtering.small-broadcast.max-size-per-driver",
        "dynamic-filtering.small-broadcast.range-row-limit-per-driver",
        "dynamic-filtering.small-broadcast.max-size-per-operator",
        "dynamic-filtering.small-partitioned.max-distinct-values-per-driver",
        "dynamic-filtering.small-partitioned.max-size-per-driver",
        "dynamic-filtering.small-partitioned.range-row-limit-per-driver",
        "dynamic-filtering.small-partitioned.max-size-per-operator",
        "enable-large-dynamic-filters",
        "dynamic-filtering.large-broadcast.max-distinct-values-per-driver",
        "dynamic-filtering.large-broadcast.max-size-per-driver",
        "dynamic-filtering.large-broadcast.range-row-limit-per-driver",
        "dynamic-filtering.large-broadcast.max-size-per-operator",
})
public class DynamicFilterConfig
{
    private boolean enableDynamicFiltering = true;
    private boolean enableDynamicRowFiltering = true;
    private double dynamicRowFilterSelectivityThreshold = 0.7;

    /*
     * dynamic-filtering.* limits are applied when
     * collected over a not pre-partitioned source (when join distribution type is
     * REPLICATED or when FTE is enabled).
     *
     * dynamic-filtering.partitioned.*
     * limits are applied when collected over a pre-partitioned source (when join
     * distribution type is PARTITIONED and FTE is disabled).
     *
     * When FTE is enabled dynamic filters are always collected over non partitioned data,
     * hence the dynamic-filtering.* limits applied.
     */
    private int maxDistinctValuesPerDriver = 50_000;
    private DataSize maxSizePerDriver = DataSize.of(4, MEGABYTE);
    private int rangeRowLimitPerDriver = 100_000;
    private DataSize maxSizePerOperator = DataSize.of(5, MEGABYTE);
    private int partitionedMaxDistinctValuesPerDriver = 20_000;
    private DataSize partitionedMaxSizePerDriver = DataSize.of(200, KILOBYTE);
    private int partitionedRangeRowLimitPerDriver = 30_000;
    private DataSize partitionedMaxSizePerOperator = DataSize.of(5, MEGABYTE);
    private DataSize maxSizePerFilter = DataSize.of(10, MEGABYTE);

    public boolean isEnableDynamicFiltering()
    {
        return enableDynamicFiltering;
    }

    @Config("enable-dynamic-filtering")
    public DynamicFilterConfig setEnableDynamicFiltering(boolean enableDynamicFiltering)
    {
        this.enableDynamicFiltering = enableDynamicFiltering;
        return this;
    }

    public boolean isEnableDynamicRowFiltering()
    {
        return enableDynamicRowFiltering;
    }

    @Config("enable-dynamic-row-filtering")
    @ConfigDescription("Enable fine-grained filtering of rows in the scan operator using dynamic filters")
    public DynamicFilterConfig setEnableDynamicRowFiltering(boolean enableDynamicRowFiltering)
    {
        this.enableDynamicRowFiltering = enableDynamicRowFiltering;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getDynamicRowFilterSelectivityThreshold()
    {
        return dynamicRowFilterSelectivityThreshold;
    }

    @Config("dynamic-row-filtering.selectivity-threshold")
    @ConfigDescription("Avoid using dynamic row filters when fraction of rows selected is above threshold")
    public DynamicFilterConfig setDynamicRowFilterSelectivityThreshold(double dynamicRowFilterSelectivityThreshold)
    {
        this.dynamicRowFilterSelectivityThreshold = dynamicRowFilterSelectivityThreshold;
        return this;
    }

    @Min(0)
    public int getMaxDistinctValuesPerDriver()
    {
        return maxDistinctValuesPerDriver;
    }

    @LegacyConfig("dynamic-filtering.large.max-distinct-values-per-driver")
    @Config("dynamic-filtering.max-distinct-values-per-driver")
    public DynamicFilterConfig setMaxDistinctValuesPerDriver(int maxDistinctValuesPerDriver)
    {
        this.maxDistinctValuesPerDriver = maxDistinctValuesPerDriver;
        return this;
    }

    public DataSize getMaxSizePerDriver()
    {
        return maxSizePerDriver;
    }

    @LegacyConfig("dynamic-filtering.large.max-size-per-driver")
    @Config("dynamic-filtering.max-size-per-driver")
    public DynamicFilterConfig setMaxSizePerDriver(DataSize maxSizePerDriver)
    {
        this.maxSizePerDriver = maxSizePerDriver;
        return this;
    }

    @Min(0)
    public int getRangeRowLimitPerDriver()
    {
        return rangeRowLimitPerDriver;
    }

    @LegacyConfig("dynamic-filtering.large.range-row-limit-per-driver")
    @Config("dynamic-filtering.range-row-limit-per-driver")
    public DynamicFilterConfig setRangeRowLimitPerDriver(int rangeRowLimitPerDriver)
    {
        this.rangeRowLimitPerDriver = rangeRowLimitPerDriver;
        return this;
    }

    @MaxDataSize("100MB")
    public DataSize getMaxSizePerOperator()
    {
        return maxSizePerOperator;
    }

    @LegacyConfig("dynamic-filtering.large.max-size-per-operator")
    @Config("dynamic-filtering.max-size-per-operator")
    public DynamicFilterConfig setMaxSizePerOperator(DataSize maxSizePerOperator)
    {
        this.maxSizePerOperator = maxSizePerOperator;
        return this;
    }

    @Min(0)
    public int getPartitionedMaxDistinctValuesPerDriver()
    {
        return partitionedMaxDistinctValuesPerDriver;
    }

    @LegacyConfig("dynamic-filtering.large-partitioned.max-distinct-values-per-driver")
    @Config("dynamic-filtering.partitioned.max-distinct-values-per-driver")
    public DynamicFilterConfig setPartitionedMaxDistinctValuesPerDriver(int partitionedMaxDistinctValuesPerDriver)
    {
        this.partitionedMaxDistinctValuesPerDriver = partitionedMaxDistinctValuesPerDriver;
        return this;
    }

    public DataSize getPartitionedMaxSizePerDriver()
    {
        return partitionedMaxSizePerDriver;
    }

    @LegacyConfig("dynamic-filtering.large-partitioned.max-size-per-driver")
    @Config("dynamic-filtering.partitioned.max-size-per-driver")
    public DynamicFilterConfig setPartitionedMaxSizePerDriver(DataSize partitionedMaxSizePerDriver)
    {
        this.partitionedMaxSizePerDriver = partitionedMaxSizePerDriver;
        return this;
    }

    @Min(0)
    public int getPartitionedRangeRowLimitPerDriver()
    {
        return partitionedRangeRowLimitPerDriver;
    }

    @LegacyConfig("dynamic-filtering.large-partitioned.range-row-limit-per-driver")
    @Config("dynamic-filtering.partitioned.range-row-limit-per-driver")
    public DynamicFilterConfig setPartitionedRangeRowLimitPerDriver(int partitionedRangeRowLimitPerDriver)
    {
        this.partitionedRangeRowLimitPerDriver = partitionedRangeRowLimitPerDriver;
        return this;
    }

    @MaxDataSize("50MB")
    public DataSize getPartitionedMaxSizePerOperator()
    {
        return partitionedMaxSizePerOperator;
    }

    @LegacyConfig("dynamic-filtering.large-partitioned.max-size-per-operator")
    @Config("dynamic-filtering.partitioned.max-size-per-operator")
    public DynamicFilterConfig setPartitionedMaxSizePerOperator(DataSize partitionedMaxSizePerOperator)
    {
        this.partitionedMaxSizePerOperator = partitionedMaxSizePerOperator;
        return this;
    }

    @NotNull
    @MaxDataSize("10MB")
    public DataSize getMaxSizePerFilter()
    {
        return maxSizePerFilter;
    }

    @LegacyConfig("dynamic-filtering.large.max-size-per-filter")
    @Config("dynamic-filtering.max-size-per-filter")
    public DynamicFilterConfig setMaxSizePerFilter(DataSize maxSizePerFilter)
    {
        this.maxSizePerFilter = maxSizePerFilter;
        return this;
    }
}
