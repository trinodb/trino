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
package io.trino.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;

public class TestCacheManagerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(CacheManagerConfig.class)
                .setCacheManagerConfigFiles(ImmutableList.of()));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path config1 = Files.createTempFile(null, null);
        Path config2 = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.of(
                "cache-manager.config-files", config1 + "," + config2);

        CacheManagerConfig expected = new CacheManagerConfig()
                .setCacheManagerConfigFiles(ImmutableList.of(config1.toFile().getPath(), config2.toFile().getPath()));

        assertFullMapping(properties, expected);
    }
}
