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
package io.trino.plugin.hive.orc;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestOrcReaderConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OrcReaderConfig.class)
                .setUseColumnNames(false)
                .setBloomFiltersEnabled(false)
                .setMaxMergeDistance(DataSize.of(1, Unit.MEGABYTE))
                .setMaxBufferSize(DataSize.of(8, Unit.MEGABYTE))
                .setStreamBufferSize(DataSize.of(8, Unit.MEGABYTE))
                .setTinyStripeThreshold(DataSize.of(8, Unit.MEGABYTE))
                .setMaxBlockSize(DataSize.of(16, Unit.MEGABYTE))
                .setLazyReadSmallRanges(true)
                .setNestedLazy(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.orc.use-column-names", "true")
                .put("hive.orc.bloom-filters.enabled", "true")
                .put("hive.orc.max-merge-distance", "22kB")
                .put("hive.orc.max-buffer-size", "44kB")
                .put("hive.orc.stream-buffer-size", "55kB")
                .put("hive.orc.tiny-stripe-threshold", "61kB")
                .put("hive.orc.max-read-block-size", "66kB")
                .put("hive.orc.lazy-read-small-ranges", "false")
                .put("hive.orc.nested-lazy", "false")
                .buildOrThrow();

        OrcReaderConfig expected = new OrcReaderConfig()
                .setUseColumnNames(true)
                .setBloomFiltersEnabled(true)
                .setMaxMergeDistance(DataSize.of(22, Unit.KILOBYTE))
                .setMaxBufferSize(DataSize.of(44, Unit.KILOBYTE))
                .setStreamBufferSize(DataSize.of(55, Unit.KILOBYTE))
                .setTinyStripeThreshold(DataSize.of(61, Unit.KILOBYTE))
                .setMaxBlockSize(DataSize.of(66, Unit.KILOBYTE))
                .setLazyReadSmallRanges(false)
                .setNestedLazy(false);

        assertFullMapping(properties, expected);
    }
}
