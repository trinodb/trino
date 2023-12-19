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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestDynamicFilterConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DynamicFilterConfig.class)
                .setEnableDynamicFiltering(true)
                .setEnableCoordinatorDynamicFiltersDistribution(true)
                .setEnableLargeDynamicFilters(true)
                .setSmallMaxDistinctValuesPerDriver(1_000)
                .setSmallMaxSizePerDriver(DataSize.of(100, KILOBYTE))
                .setSmallRangeRowLimitPerDriver(2_000)
                .setSmallPartitionedMaxDistinctValuesPerDriver(100)
                .setSmallMaxSizePerOperator(DataSize.of(1, MEGABYTE))
                .setSmallPartitionedMaxSizePerDriver(DataSize.of(50, KILOBYTE))
                .setSmallPartitionedRangeRowLimitPerDriver(500)
                .setSmallPartitionedMaxSizePerOperator(DataSize.of(500, KILOBYTE))
                .setSmallMaxSizePerFilter(DataSize.of(5, MEGABYTE))
                .setSmallDynamicFilterWaitTimeout(new Duration(20, TimeUnit.SECONDS))
                .setSmallDynamicFilterMaxRowCount(100_000)
                .setSmallDynamicFilterMaxNdvCount(500)
                .setLargeMaxDistinctValuesPerDriver(50_000)
                .setLargeMaxSizePerDriver(DataSize.of(4, MEGABYTE))
                .setLargeRangeRowLimitPerDriver(100_000)
                .setLargeMaxSizePerOperator(DataSize.of(5, MEGABYTE))
                .setLargePartitionedMaxDistinctValuesPerDriver(20_000)
                .setLargePartitionedMaxSizePerDriver(DataSize.of(200, KILOBYTE))
                .setLargePartitionedRangeRowLimitPerDriver(30_000)
                .setLargePartitionedMaxSizePerOperator(DataSize.of(5, MEGABYTE))
                .setLargeMaxSizePerFilter(DataSize.of(10, MEGABYTE))
                .setDynamicRowFilteringEnabled(true)
                .setDynamicRowFilterSelectivityThreshold(0.7)
                .setDynamicRowFilteringWaitTimeout(new Duration(0, SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("enable-dynamic-filtering", "false")
                .put("enable-coordinator-dynamic-filters-distribution", "false")
                .put("enable-large-dynamic-filters", "false")
                .put("small-dynamic-filter.wait-timeout", "50s")
                .put("small-dynamic-filter.max-row-count", "500000")
                .put("small-dynamic-filter.max-ndv-count", "2000")
                .put("dynamic-filtering.small.max-distinct-values-per-driver", "256")
                .put("dynamic-filtering.small.max-size-per-driver", "64kB")
                .put("dynamic-filtering.small.range-row-limit-per-driver", "20000")
                .put("dynamic-filtering.small.max-size-per-operator", "640kB")
                .put("dynamic-filtering.small-partitioned.max-distinct-values-per-driver", "256")
                .put("dynamic-filtering.small-partitioned.max-size-per-driver", "64kB")
                .put("dynamic-filtering.small-partitioned.range-row-limit-per-driver", "20000")
                .put("dynamic-filtering.small-partitioned.max-size-per-operator", "641kB")
                .put("dynamic-filtering.small.max-size-per-filter", "341kB")
                .put("dynamic-filtering.large.max-distinct-values-per-driver", "256")
                .put("dynamic-filtering.large.max-size-per-driver", "64kB")
                .put("dynamic-filtering.large.range-row-limit-per-driver", "200000")
                .put("dynamic-filtering.large.max-size-per-operator", "642kB")
                .put("dynamic-filtering.large-partitioned.max-distinct-values-per-driver", "256")
                .put("dynamic-filtering.large-partitioned.max-size-per-driver", "64kB")
                .put("dynamic-filtering.large-partitioned.range-row-limit-per-driver", "200000")
                .put("dynamic-filtering.large-partitioned.max-size-per-operator", "643kB")
                .put("dynamic-filtering.large.max-size-per-filter", "3411kB")
                .put("dynamic-row-filtering.enabled", "false")
                .put("dynamic-row-filtering.selectivity-threshold", "0.9")
                .put("dynamic-row-filtering.wait-timeout", "10s")
                .buildOrThrow();

        DynamicFilterConfig expected = new DynamicFilterConfig()
                .setEnableDynamicFiltering(false)
                .setEnableCoordinatorDynamicFiltersDistribution(false)
                .setEnableLargeDynamicFilters(false)
                .setSmallDynamicFilterMaxRowCount(500_000)
                .setSmallDynamicFilterMaxNdvCount(2000)
                .setSmallDynamicFilterWaitTimeout(new Duration(50, TimeUnit.SECONDS))
                .setSmallMaxDistinctValuesPerDriver(256)
                .setSmallMaxSizePerDriver(DataSize.of(64, KILOBYTE))
                .setSmallRangeRowLimitPerDriver(20000)
                .setSmallMaxSizePerOperator(DataSize.of(640, KILOBYTE))
                .setSmallPartitionedMaxDistinctValuesPerDriver(256)
                .setSmallPartitionedMaxSizePerDriver(DataSize.of(64, KILOBYTE))
                .setSmallPartitionedRangeRowLimitPerDriver(20000)
                .setSmallPartitionedMaxSizePerOperator(DataSize.of(641, KILOBYTE))
                .setSmallMaxSizePerFilter(DataSize.of(341, KILOBYTE))
                .setLargeMaxDistinctValuesPerDriver(256)
                .setLargeMaxSizePerDriver(DataSize.of(64, KILOBYTE))
                .setLargeRangeRowLimitPerDriver(200_000)
                .setLargeMaxSizePerOperator(DataSize.of(642, KILOBYTE))
                .setLargePartitionedMaxDistinctValuesPerDriver(256)
                .setLargePartitionedMaxSizePerDriver(DataSize.of(64, KILOBYTE))
                .setLargePartitionedRangeRowLimitPerDriver(200000)
                .setLargePartitionedMaxSizePerOperator(DataSize.of(643, KILOBYTE))
                .setLargeMaxSizePerFilter(DataSize.of(3411, KILOBYTE))
                .setDynamicRowFilteringEnabled(false)
                .setDynamicRowFilterSelectivityThreshold(0.9)
                .setDynamicRowFilteringWaitTimeout(new Duration(10, SECONDS));

        assertFullMapping(properties, expected);
    }
}
