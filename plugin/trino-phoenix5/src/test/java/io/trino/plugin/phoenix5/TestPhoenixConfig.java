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
package io.trino.plugin.phoenix5;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestPhoenixConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(PhoenixConfig.class)
                .setConnectionUrl(null)
                .setResourceConfigFiles("")
                .setMaxScansPerSplit(20));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path configFile = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("phoenix.connection-url", "jdbc:phoenix:localhost:2181:/hbase")
                .put("phoenix.config.resources", configFile.toString())
                .put("phoenix.max-scans-per-split", "1")
                .buildOrThrow();

        PhoenixConfig expected = new PhoenixConfig()
                .setConnectionUrl("jdbc:phoenix:localhost:2181:/hbase")
                .setResourceConfigFiles(configFile.toString())
                .setMaxScansPerSplit(1);

        assertFullMapping(properties, expected);
    }
}
