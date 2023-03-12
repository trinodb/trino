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

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

@DefunctConfig({
        "dynamic-filtering-max-per-driver-row-count",
        "experimental.dynamic-filtering-max-per-driver-row-count",
        "dynamic-filtering-max-per-driver-size",
        "experimental.dynamic-filtering-max-per-driver-size",
        "dynamic-filtering-range-row-limit-per-driver",
        "experimental.dynamic-filtering-refresh-interval",
        "dynamic-filtering.service-thread-count"
})
public class DynamicFilterConfig
{
    private boolean enableDynamicFiltering = true;
    private boolean enableCoordinatorDynamicFiltersDistribution = true;
    private boolean enableLargeDynamicFilters;

    private int smallBroadcastMaxDistinctValuesPerDriver = 200;
    private DataSize smallBroadcastMaxSizePerDriver = DataSize.of(20, KILOBYTE);
    private int smallBroadcastRangeRowLimitPerDriver = 400;
    private DataSize smallBroadcastMaxSizePerOperator = DataSize.of(200, KILOBYTE);
    private int smallPartitionedMaxDistinctValuesPerDriver = 20;
    private DataSize smallPartitionedMaxSizePerDriver = DataSize.of(10, KILOBYTE);
    private int smallPartitionedRangeRowLimitPerDriver = 100;
    private DataSize smallPartitionedMaxSizePerOperator = DataSize.of(100, KILOBYTE);
    private DataSize smallMaxSizePerFilter = DataSize.of(1, MEGABYTE);

    private int largeBroadcastMaxDistinctValuesPerDriver = 5_000;
    private DataSize largeBroadcastMaxSizePerDriver = DataSize.of(500, KILOBYTE);
    private int largeBroadcastRangeRowLimitPerDriver = 10_000;
    private DataSize largeBroadcastMaxSizePerOperator = DataSize.of(5, MEGABYTE);
    private int largePartitionedMaxDistinctValuesPerDriver = 500;
    private DataSize largePartitionedMaxSizePerDriver = DataSize.of(50, KILOBYTE);
    private int largePartitionedRangeRowLimitPerDriver = 1_000;
    private DataSize largePartitionedMaxSizePerOperator = DataSize.of(500, KILOBYTE);
    private DataSize largeMaxSizePerFilter = DataSize.of(5, MEGABYTE);

    public boolean isEnableDynamicFiltering()
    {
        return enableDynamicFiltering;
    }

    @Config("enable-dynamic-filtering")
    @LegacyConfig("experimental.enable-dynamic-filtering")
    public DynamicFilterConfig setEnableDynamicFiltering(boolean enableDynamicFiltering)
    {
        this.enableDynamicFiltering = enableDynamicFiltering;
        return this;
    }

    public boolean isEnableCoordinatorDynamicFiltersDistribution()
    {
        return enableCoordinatorDynamicFiltersDistribution;
    }

    @Config("enable-coordinator-dynamic-filters-distribution")
    @ConfigDescription("Enable distribution of dynamic filters from coordinator to all workers")
    public DynamicFilterConfig setEnableCoordinatorDynamicFiltersDistribution(boolean enableCoordinatorDynamicFiltersDistribution)
    {
        this.enableCoordinatorDynamicFiltersDistribution = enableCoordinatorDynamicFiltersDistribution;
        return this;
    }

    public boolean isEnableLargeDynamicFilters()
    {
        return enableLargeDynamicFilters;
    }

    @Config("enable-large-dynamic-filters")
    public DynamicFilterConfig setEnableLargeDynamicFilters(boolean enableLargeDynamicFilters)
    {
        this.enableLargeDynamicFilters = enableLargeDynamicFilters;
        return this;
    }

    @Min(0)
    public int getSmallBroadcastMaxDistinctValuesPerDriver()
    {
        return smallBroadcastMaxDistinctValuesPerDriver;
    }

    @Config("dynamic-filtering.small-broadcast.max-distinct-values-per-driver")
    public DynamicFilterConfig setSmallBroadcastMaxDistinctValuesPerDriver(int smallBroadcastMaxDistinctValuesPerDriver)
    {
        this.smallBroadcastMaxDistinctValuesPerDriver = smallBroadcastMaxDistinctValuesPerDriver;
        return this;
    }

    @MaxDataSize("1MB")
    public DataSize getSmallBroadcastMaxSizePerDriver()
    {
        return smallBroadcastMaxSizePerDriver;
    }

    @Config("dynamic-filtering.small-broadcast.max-size-per-driver")
    public DynamicFilterConfig setSmallBroadcastMaxSizePerDriver(DataSize smallBroadcastMaxSizePerDriver)
    {
        this.smallBroadcastMaxSizePerDriver = smallBroadcastMaxSizePerDriver;
        return this;
    }

    @Min(0)
    public int getSmallBroadcastRangeRowLimitPerDriver()
    {
        return smallBroadcastRangeRowLimitPerDriver;
    }

    @Config("dynamic-filtering.small-broadcast.range-row-limit-per-driver")
    public DynamicFilterConfig setSmallBroadcastRangeRowLimitPerDriver(int smallBroadcastRangeRowLimitPerDriver)
    {
        this.smallBroadcastRangeRowLimitPerDriver = smallBroadcastRangeRowLimitPerDriver;
        return this;
    }

    @MaxDataSize("10MB")
    public DataSize getSmallBroadcastMaxSizePerOperator()
    {
        return smallBroadcastMaxSizePerOperator;
    }

    @Config("dynamic-filtering.small-broadcast.max-size-per-operator")
    public DynamicFilterConfig setSmallBroadcastMaxSizePerOperator(DataSize smallBroadcastMaxSizePerOperator)
    {
        this.smallBroadcastMaxSizePerOperator = smallBroadcastMaxSizePerOperator;
        return this;
    }

    @Min(0)
    public int getSmallPartitionedMaxDistinctValuesPerDriver()
    {
        return smallPartitionedMaxDistinctValuesPerDriver;
    }

    @Config("dynamic-filtering.small-partitioned.max-distinct-values-per-driver")
    public DynamicFilterConfig setSmallPartitionedMaxDistinctValuesPerDriver(int smallPartitionedMaxDistinctValuesPerDriver)
    {
        this.smallPartitionedMaxDistinctValuesPerDriver = smallPartitionedMaxDistinctValuesPerDriver;
        return this;
    }

    @MaxDataSize("1MB")
    public DataSize getSmallPartitionedMaxSizePerDriver()
    {
        return smallPartitionedMaxSizePerDriver;
    }

    @Config("dynamic-filtering.small-partitioned.max-size-per-driver")
    public DynamicFilterConfig setSmallPartitionedMaxSizePerDriver(DataSize smallPartitionedMaxSizePerDriver)
    {
        this.smallPartitionedMaxSizePerDriver = smallPartitionedMaxSizePerDriver;
        return this;
    }

    @Min(0)
    public int getSmallPartitionedRangeRowLimitPerDriver()
    {
        return smallPartitionedRangeRowLimitPerDriver;
    }

    @Config("dynamic-filtering.small-partitioned.range-row-limit-per-driver")
    public DynamicFilterConfig setSmallPartitionedRangeRowLimitPerDriver(int smallPartitionedRangeRowLimitPerDriver)
    {
        this.smallPartitionedRangeRowLimitPerDriver = smallPartitionedRangeRowLimitPerDriver;
        return this;
    }

    @MaxDataSize("10MB")
    public DataSize getSmallPartitionedMaxSizePerOperator()
    {
        return smallPartitionedMaxSizePerOperator;
    }

    @Config("dynamic-filtering.small-partitioned.max-size-per-operator")
    public DynamicFilterConfig setSmallPartitionedMaxSizePerOperator(DataSize smallPartitionedMaxSizePerOperator)
    {
        this.smallPartitionedMaxSizePerOperator = smallPartitionedMaxSizePerOperator;
        return this;
    }

    @NotNull
    @MaxDataSize("10MB")
    public DataSize getSmallMaxSizePerFilter()
    {
        return smallMaxSizePerFilter;
    }

    @Config("dynamic-filtering.small.max-size-per-filter")
    public DynamicFilterConfig setSmallMaxSizePerFilter(DataSize smallMaxSizePerFilter)
    {
        this.smallMaxSizePerFilter = smallMaxSizePerFilter;
        return this;
    }

    @Min(0)
    public int getLargeBroadcastMaxDistinctValuesPerDriver()
    {
        return largeBroadcastMaxDistinctValuesPerDriver;
    }

    @Config("dynamic-filtering.large-broadcast.max-distinct-values-per-driver")
    public DynamicFilterConfig setLargeBroadcastMaxDistinctValuesPerDriver(int largeBroadcastMaxDistinctValuesPerDriver)
    {
        this.largeBroadcastMaxDistinctValuesPerDriver = largeBroadcastMaxDistinctValuesPerDriver;
        return this;
    }

    public DataSize getLargeBroadcastMaxSizePerDriver()
    {
        return largeBroadcastMaxSizePerDriver;
    }

    @Config("dynamic-filtering.large-broadcast.max-size-per-driver")
    public DynamicFilterConfig setLargeBroadcastMaxSizePerDriver(DataSize largeBroadcastMaxSizePerDriver)
    {
        this.largeBroadcastMaxSizePerDriver = largeBroadcastMaxSizePerDriver;
        return this;
    }

    @Min(0)
    public int getLargeBroadcastRangeRowLimitPerDriver()
    {
        return largeBroadcastRangeRowLimitPerDriver;
    }

    @Config("dynamic-filtering.large-broadcast.range-row-limit-per-driver")
    public DynamicFilterConfig setLargeBroadcastRangeRowLimitPerDriver(int largeBroadcastRangeRowLimitPerDriver)
    {
        this.largeBroadcastRangeRowLimitPerDriver = largeBroadcastRangeRowLimitPerDriver;
        return this;
    }

    @MaxDataSize("100MB")
    public DataSize getLargeBroadcastMaxSizePerOperator()
    {
        return largeBroadcastMaxSizePerOperator;
    }

    @Config("dynamic-filtering.large-broadcast.max-size-per-operator")
    public DynamicFilterConfig setLargeBroadcastMaxSizePerOperator(DataSize largeBroadcastMaxSizePerOperator)
    {
        this.largeBroadcastMaxSizePerOperator = largeBroadcastMaxSizePerOperator;
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
