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
package io.trino.plugin.hive.cos;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.nio.file.Files.createTempFile;

public class TestHiveCosServiceConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HiveCosServiceConfig.class)
                .setServiceConfig(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path serviceConfig = createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.of("hive.cos.service-config", serviceConfig.toString());

        HiveCosServiceConfig expected = new HiveCosServiceConfig()
                .setServiceConfig(serviceConfig.toFile());

        assertFullMapping(properties, expected);
    }
}
