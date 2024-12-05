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
package io.trino.plugin.ranger;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

final class TestRangerConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RangerConfig.class)
                .setServiceName(null)
                .setPluginConfigResource(List.of())
                .setHadoopConfigResource(List.of()));
    }

    @Test
    void testExplicitPropertyMappings()
            throws Exception
    {
        Path pluginConfigResourceFile = Files.createTempFile(null, null);
        Path hadoopConfigResourceFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("ranger.service.name", "trino")
                .put("ranger.plugin.config.resource", pluginConfigResourceFile.toString())
                .put("ranger.hadoop.config.resource", hadoopConfigResourceFile.toString())
                .buildOrThrow();

        RangerConfig expected = new RangerConfig()
                .setServiceName("trino")
                .setPluginConfigResource(List.of(pluginConfigResourceFile.toFile()))
                .setHadoopConfigResource(List.of(hadoopConfigResourceFile.toFile()));

        assertFullMapping(properties, expected);
    }
}
