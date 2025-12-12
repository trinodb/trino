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
                .setEnableDynamicRowFiltering(true)
                .setDynamicRowFilterSelectivityThreshold(0.7)
                .setMaxDistinctValuesPerDriver(50_000)
                .setMaxSizePerDriver(DataSize.of(4, MEGABYTE))
                .setRangeRowLimitPerDriver(100_000)
                .setMaxSizePerOperator(DataSize.of(5, MEGABYTE))
                .setPartitionedMaxDistinctValuesPerDriver(20_000)
                .setPartitionedMaxSizePerDriver(DataSize.of(200, KILOBYTE))
                .setPartitionedRangeRowLimitPerDriver(30_000)
                .setPartitionedMaxSizePerOperator(DataSize.of(5, MEGABYTE))
                .setMaxSizePerFilter(DataSize.of(10, MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("enable-dynamic-filtering", "false")
                .put("enable-dynamic-row-filtering", "false")
                .put("dynamic-row-filtering.selectivity-threshold", "0.8")
                .put("dynamic-filtering.max-distinct-values-per-driver", "256")
                .put("dynamic-filtering.max-size-per-driver", "64kB")
                .put("dynamic-filtering.range-row-limit-per-driver", "200000")
                .put("dynamic-filtering.max-size-per-operator", "642kB")
                .put("dynamic-filtering.partitioned.max-distinct-values-per-driver", "256")
                .put("dynamic-filtering.partitioned.max-size-per-driver", "64kB")
                .put("dynamic-filtering.partitioned.range-row-limit-per-driver", "200000")
                .put("dynamic-filtering.partitioned.max-size-per-operator", "643kB")
                .put("dynamic-filtering.max-size-per-filter", "3411kB")
                .buildOrThrow();

        DynamicFilterConfig expected = new DynamicFilterConfig()
                .setEnableDynamicFiltering(false)
                .setEnableDynamicRowFiltering(false)
                .setDynamicRowFilterSelectivityThreshold(0.8)
                .setMaxDistinctValuesPerDriver(256)
                .setMaxSizePerDriver(DataSize.of(64, KILOBYTE))
                .setRangeRowLimitPerDriver(200000)
                .setMaxSizePerOperator(DataSize.of(642, KILOBYTE))
                .setPartitionedMaxDistinctValuesPerDriver(256)
                .setPartitionedMaxSizePerDriver(DataSize.of(64, KILOBYTE))
                .setPartitionedRangeRowLimitPerDriver(200000)
                .setPartitionedMaxSizePerOperator(DataSize.of(643, KILOBYTE))
                .setMaxSizePerFilter(DataSize.of(3411, KILOBYTE));

        assertFullMapping(properties, expected);
    }
}
