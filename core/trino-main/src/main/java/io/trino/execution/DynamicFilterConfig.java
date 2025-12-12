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
})
public class DynamicFilterConfig
{
    private boolean enableDynamicFiltering = true;
    private boolean enableDynamicRowFiltering = true;
    private double dynamicRowFilterSelectivityThreshold = 0.7;

    /*
     * dynamic-filtering.large.* limits are applied when
     * collected over a not pre-partitioned source (when join distribution type is
     * REPLICATED or when FTE is enabled).
     *
     * dynamic-filtering.large-partitioned.*
     * limits are applied when collected over a pre-partitioned source (when join
     * distribution type is PARTITIONED and FTE is disabled).
     *
     * When FTE is enabled dynamic filters are always collected over non partitioned data,
     * hence the dynamic-filtering.large.* limits applied.
     */
    private int largeMaxDistinctValuesPerDriver = 50_000;
    private DataSize largeMaxSizePerDriver = DataSize.of(4, MEGABYTE);
    private int largeRangeRowLimitPerDriver = 100_000;
    private DataSize largeMaxSizePerOperator = DataSize.of(5, MEGABYTE);
    private int largePartitionedMaxDistinctValuesPerDriver = 20_000;
    private DataSize largePartitionedMaxSizePerDriver = DataSize.of(200, KILOBYTE);
    private int largePartitionedRangeRowLimitPerDriver = 30_000;
    private DataSize largePartitionedMaxSizePerOperator = DataSize.of(5, MEGABYTE);
    private DataSize largeMaxSizePerFilter = DataSize.of(10, MEGABYTE);

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
    public int getLargeMaxDistinctValuesPerDriver()
    {
        return largeMaxDistinctValuesPerDriver;
    }

    @LegacyConfig("dynamic-filtering.large-broadcast.max-distinct-values-per-driver")
    @Config("dynamic-filtering.large.max-distinct-values-per-driver")
    public DynamicFilterConfig setLargeMaxDistinctValuesPerDriver(int largeMaxDistinctValuesPerDriver)
    {
        this.largeMaxDistinctValuesPerDriver = largeMaxDistinctValuesPerDriver;
        return this;
    }

    public DataSize getLargeMaxSizePerDriver()
    {
        return largeMaxSizePerDriver;
    }

    @LegacyConfig("dynamic-filtering.large-broadcast.max-size-per-driver")
    @Config("dynamic-filtering.large.max-size-per-driver")
    public DynamicFilterConfig setLargeMaxSizePerDriver(DataSize largeMaxSizePerDriver)
    {
        this.largeMaxSizePerDriver = largeMaxSizePerDriver;
        return this;
    }

    @Min(0)
    public int getLargeRangeRowLimitPerDriver()
    {
        return largeRangeRowLimitPerDriver;
    }

    @LegacyConfig("dynamic-filtering.large-broadcast.range-row-limit-per-driver")
    @Config("dynamic-filtering.large.range-row-limit-per-driver")
    public DynamicFilterConfig setLargeRangeRowLimitPerDriver(int largeRangeRowLimitPerDriver)
    {
        this.largeRangeRowLimitPerDriver = largeRangeRowLimitPerDriver;
        return this;
    }

    @MaxDataSize("100MB")
    public DataSize getLargeMaxSizePerOperator()
    {
        return largeMaxSizePerOperator;
    }

    @LegacyConfig("dynamic-filtering.large-broadcast.max-size-per-operator")
    @Config("dynamic-filtering.large.max-size-per-operator")
    public DynamicFilterConfig setLargeMaxSizePerOperator(DataSize largeMaxSizePerOperator)
    {
        this.largeMaxSizePerOperator = largeMaxSizePerOperator;
        return this;
    }

    @Min(0)
    public int getLargePartitionedMaxDistinctValuesPerDriver()
    {
        return largePartitionedMaxDistinctValuesPerDriver;
    }

    @Config("dynamic-filtering.large-partitioned.max-distinct-values-per-driver")
    public DynamicFilterConfig setLargePartitionedMaxDistinctValuesPerDriver(int largePartitionedMaxDistinctValuesPerDriver)
    {
        this.largePartitionedMaxDistinctValuesPerDriver = largePartitionedMaxDistinctValuesPerDriver;
        return this;
    }

    public DataSize getLargePartitionedMaxSizePerDriver()
    {
        return largePartitionedMaxSizePerDriver;
    }

    @Config("dynamic-filtering.large-partitioned.max-size-per-driver")
    public DynamicFilterConfig setLargePartitionedMaxSizePerDriver(DataSize largePartitionedMaxSizePerDriver)
    {
        this.largePartitionedMaxSizePerDriver = largePartitionedMaxSizePerDriver;
        return this;
    }

    @Min(0)
    public int getLargePartitionedRangeRowLimitPerDriver()
    {
        return largePartitionedRangeRowLimitPerDriver;
    }

    @Config("dynamic-filtering.large-partitioned.range-row-limit-per-driver")
    public DynamicFilterConfig setLargePartitionedRangeRowLimitPerDriver(int largePartitionedRangeRowLimitPerDriver)
    {
        this.largePartitionedRangeRowLimitPerDriver = largePartitionedRangeRowLimitPerDriver;
        return this;
    }

    @MaxDataSize("50MB")
    public DataSize getLargePartitionedMaxSizePerOperator()
    {
        return largePartitionedMaxSizePerOperator;
    }

    @Config("dynamic-filtering.large-partitioned.max-size-per-operator")
    public DynamicFilterConfig setLargePartitionedMaxSizePerOperator(DataSize largePartitionedMaxSizePerOperator)
    {
        this.largePartitionedMaxSizePerOperator = largePartitionedMaxSizePerOperator;
        return this;
    }

    @NotNull
    @MaxDataSize("10MB")
    public DataSize getLargeMaxSizePerFilter()
    {
        return largeMaxSizePerFilter;
    }

    @Config("dynamic-filtering.large.max-size-per-filter")
    public DynamicFilterConfig setLargeMaxSizePerFilter(DataSize largeMaxSizePerFilter)
    {
        this.largeMaxSizePerFilter = largeMaxSizePerFilter;
        return this;
    }
}
