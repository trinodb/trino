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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestDynamicFilterConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DynamicFilterConfig.class)
                .setEnableDynamicFiltering(true)
                .setEnableLargeDynamicFilters(true)
                .setEnableDynamicRowFiltering(true)
                .setDynamicRowFilterSelectivityThreshold(0.7)
                .setSmallMaxDistinctValuesPerDriver(1_000)
                .setSmallMaxSizePerDriver(DataSize.of(100, KILOBYTE))
                .setSmallPartitionedMaxDistinctValuesPerDriver(100)
                .setSmallMaxSizePerOperator(DataSize.of(1, MEGABYTE))
                .setSmallPartitionedMaxSizePerDriver(DataSize.of(50, KILOBYTE))
                .setSmallPartitionedMaxSizePerOperator(DataSize.of(500, KILOBYTE))
                .setSmallMaxSizePerFilter(DataSize.of(5, MEGABYTE))
                .setLargeMaxDistinctValuesPerDriver(50_000)
                .setLargeMaxSizePerDriver(DataSize.of(4, MEGABYTE))
                .setLargeMaxSizePerOperator(DataSize.of(5, MEGABYTE))
                .setLargePartitionedMaxDistinctValuesPerDriver(20_000)
                .setLargePartitionedMaxSizePerDriver(DataSize.of(200, KILOBYTE))
                .setLargePartitionedMaxSizePerOperator(DataSize.of(5, MEGABYTE))
                .setLargeMaxSizePerFilter(DataSize.of(10, MEGABYTE))
                .setBloomFilterMaxDistinctValuesPerDriver(100_000)
                .setPartitionedBloomFilterMaxDistinctValuesPerDriver(25_000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("enable-dynamic-filtering", "false")
                .put("enable-large-dynamic-filters", "false")
                .put("enable-dynamic-row-filtering", "false")
                .put("dynamic-row-filtering.selectivity-threshold", "0.8")
                .put("dynamic-filtering.small.max-distinct-values-per-driver", "256")
                .put("dynamic-filtering.small.max-size-per-driver", "64kB")
                .put("dynamic-filtering.small.max-size-per-operator", "640kB")
                .put("dynamic-filtering.small-partitioned.max-distinct-values-per-driver", "256")
                .put("dynamic-filtering.small-partitioned.max-size-per-driver", "64kB")
                .put("dynamic-filtering.small-partitioned.max-size-per-operator", "641kB")
                .put("dynamic-filtering.small.max-size-per-filter", "341kB")
                .put("dynamic-filtering.large.max-distinct-values-per-driver", "256")
                .put("dynamic-filtering.large.max-size-per-driver", "64kB")
                .put("dynamic-filtering.large.max-size-per-operator", "642kB")
                .put("dynamic-filtering.large-partitioned.max-distinct-values-per-driver", "256")
                .put("dynamic-filtering.large-partitioned.max-size-per-driver", "64kB")
                .put("dynamic-filtering.large-partitioned.max-size-per-operator", "643kB")
                .put("dynamic-filtering.large.max-size-per-filter", "3411kB")
                .put("dynamic-filtering.bloom-filter.max-distinct-values-per-driver", "15000")
                .put("dynamic-filtering.partitioned-bloom-filter.max-distinct-values-per-driver", "5000")
                .buildOrThrow();

        DynamicFilterConfig expected = new DynamicFilterConfig()
                .setEnableDynamicFiltering(false)
                .setEnableLargeDynamicFilters(false)
                .setEnableDynamicRowFiltering(false)
                .setDynamicRowFilterSelectivityThreshold(0.8)
                .setSmallMaxDistinctValuesPerDriver(256)
                .setSmallMaxSizePerDriver(DataSize.of(64, KILOBYTE))
                .setSmallMaxSizePerOperator(DataSize.of(640, KILOBYTE))
                .setSmallPartitionedMaxDistinctValuesPerDriver(256)
                .setSmallPartitionedMaxSizePerDriver(DataSize.of(64, KILOBYTE))
                .setSmallPartitionedMaxSizePerOperator(DataSize.of(641, KILOBYTE))
                .setSmallMaxSizePerFilter(DataSize.of(341, KILOBYTE))
                .setLargeMaxDistinctValuesPerDriver(256)
                .setLargeMaxSizePerDriver(DataSize.of(64, KILOBYTE))
                .setLargeMaxSizePerOperator(DataSize.of(642, KILOBYTE))
                .setLargePartitionedMaxDistinctValuesPerDriver(256)
                .setLargePartitionedMaxSizePerDriver(DataSize.of(64, KILOBYTE))
                .setLargePartitionedMaxSizePerOperator(DataSize.of(643, KILOBYTE))
                .setLargeMaxSizePerFilter(DataSize.of(3411, KILOBYTE))
                .setBloomFilterMaxDistinctValuesPerDriver(15000)
                .setPartitionedBloomFilterMaxDistinctValuesPerDriver(5000);

        assertFullMapping(properties, expected);
    }
}
