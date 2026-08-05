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
package io.trino.server;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestServerPluginsProviderConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ServerPluginsProviderConfig.class)
                .setInstalledPluginsDirs(List.of(Path.of("plugin"))));
    }

    @Test
    public void testExplicitPropertyMappings(@TempDir Path tempDir)
    {
        Map<String, String> properties = ImmutableMap.of("plugin.dir", tempDir.toString());

        ServerPluginsProviderConfig expected = new ServerPluginsProviderConfig()
                .setInstalledPluginsDirs(List.of(tempDir));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testExplicitPropertyMappingMultiDir(@TempDir Path tempDir1, @TempDir Path tempDir2)
    {
        Map<String, String> properties = ImmutableMap.of("plugin.dir", tempDir1.toString() + "," + tempDir2.toString());

        ServerPluginsProviderConfig expected = new ServerPluginsProviderConfig()
                .setInstalledPluginsDirs(List.of(tempDir1, tempDir2));

        assertFullMapping(properties, expected);
    }
}
