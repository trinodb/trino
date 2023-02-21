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
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestOrcMetadataCacheConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OrcMetadataCacheConfig.class)
                .setFileTailCacheEnabled(true)
                .setFileTailCacheSize(DataSize.of(256, MEGABYTE))
                .setFileTailCacheTtlSinceLastAccess(new Duration(4, HOURS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.orc.file-tail-cache-enabled", "false")
                .put("hive.orc.file-tail-cache-size", "5MB")
                .put("hive.orc.file-tail-cache-ttl-since-last-access", "10m")
                .buildOrThrow();

        OrcMetadataCacheConfig expected = new OrcMetadataCacheConfig()
                .setFileTailCacheEnabled(false)
                .setFileTailCacheSize(DataSize.of(5, MEGABYTE))
                .setFileTailCacheTtlSinceLastAccess(new Duration(10, MINUTES));

        assertFullMapping(properties, expected);
    }
}
