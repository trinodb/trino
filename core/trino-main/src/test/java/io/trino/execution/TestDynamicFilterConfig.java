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
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.KILOBYTE;

public class TestDynamicFilterConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DynamicFilterConfig.class)
                .setEnableDynamicFiltering(true)
                .setEnableCoordinatorDynamicFiltersDistribution(true)
                .setEnableLargeDynamicFilters(false)
                .setServiceThreadCount(2)
                .setSmallBroadcastMaxDistinctValuesPerDriver(200)
                .setSmallBroadcastMaxSizePerDriver(DataSize.of(20, KILOBYTE))
                .setSmallBroadcastRangeRowLimitPerDriver(400)
                .setSmallPartitionedMaxDistinctValuesPerDriver(20)
                .setSmallPartitionedMaxSizePerDriver(DataSize.of(10, KILOBYTE))
                .setSmallPartitionedRangeRowLimitPerDriver(100)
                .setLargeBroadcastMaxDistinctValuesPerDriver(5000)
                .setLargeBroadcastMaxSizePerDriver(DataSize.of(500, KILOBYTE))
                .setLargeBroadcastRangeRowLimitPerDriver(10_000)
                .setLargePartitionedMaxDistinctValuesPerDriver(500)
                .setLargePartitionedMaxSizePerDriver(DataSize.of(50, KILOBYTE))
                .setLargePartitionedRangeRowLimitPerDriver(1_000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("enable-dynamic-filtering", "false")
                .put("enable-coordinator-dynamic-filters-distribution", "false")
                .put("enable-large-dynamic-filters", "true")
                .put("dynamic-filtering.service-thread-count", "4")
                .put("dynamic-filtering.small-broadcast.max-distinct-values-per-driver", "256")
                .put("dynamic-filtering.small-broadcast.max-size-per-driver", "64kB")
                .put("dynamic-filtering.small-broadcast.range-row-limit-per-driver", "10000")
                .put("dynamic-filtering.small-partitioned.max-distinct-values-per-driver", "256")
                .put("dynamic-filtering.small-partitioned.max-size-per-driver", "64kB")
                .put("dynamic-filtering.small-partitioned.range-row-limit-per-driver", "10000")
                .put("dynamic-filtering.large-broadcast.max-distinct-values-per-driver", "256")
                .put("dynamic-filtering.large-broadcast.max-size-per-driver", "64kB")
                .put("dynamic-filtering.large-broadcast.range-row-limit-per-driver", "100000")
                .put("dynamic-filtering.large-partitioned.max-distinct-values-per-driver", "256")
                .put("dynamic-filtering.large-partitioned.max-size-per-driver", "64kB")
                .put("dynamic-filtering.large-partitioned.range-row-limit-per-driver", "100000")
                .build();

        DynamicFilterConfig expected = new DynamicFilterConfig()
                .setEnableDynamicFiltering(false)
                .setEnableCoordinatorDynamicFiltersDistribution(false)
                .setEnableLargeDynamicFilters(true)
                .setServiceThreadCount(4)
                .setSmallBroadcastMaxDistinctValuesPerDriver(256)
                .setSmallBroadcastMaxSizePerDriver(DataSize.of(64, KILOBYTE))
                .setSmallBroadcastRangeRowLimitPerDriver(10000)
                .setSmallPartitionedMaxDistinctValuesPerDriver(256)
                .setSmallPartitionedMaxSizePerDriver(DataSize.of(64, KILOBYTE))
                .setSmallPartitionedRangeRowLimitPerDriver(10000)
                .setLargeBroadcastMaxDistinctValuesPerDriver(256)
                .setLargeBroadcastMaxSizePerDriver(DataSize.of(64, KILOBYTE))
                .setLargeBroadcastRangeRowLimitPerDriver(100000)
                .setLargePartitionedMaxDistinctValuesPerDriver(256)
                .setLargePartitionedMaxSizePerDriver(DataSize.of(64, KILOBYTE))
                .setLargePartitionedRangeRowLimitPerDriver(100000);

        assertFullMapping(properties, expected);
    }
}
