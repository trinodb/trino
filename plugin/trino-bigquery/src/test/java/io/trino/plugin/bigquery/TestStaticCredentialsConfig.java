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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import io.airlift.configuration.ConfigurationFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestStaticCredentialsConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StaticCredentialsConfig.class)
                .setCredentialsKey(null)
                .setCredentialsFile(null));
    }

    @Test
    public void testExplicitPropertyMappingsWithCredentialsKey()
    {
        Map<String, String> properties = ImmutableMap.of("bigquery.credentials-key", "key");

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        StaticCredentialsConfig config = configurationFactory.build(StaticCredentialsConfig.class);

        assertEquals(config.getCredentialsKey(), Optional.of("key"));
        assertEquals(config.getCredentialsFile(), Optional.empty());
    }

    @Test
    public void testExplicitPropertyMappingsWithCredentialsFile()
    {
        try {
            Path file = Files.createTempFile("config", ".json");

            Map<String, String> properties = ImmutableMap.of("bigquery.credentials-file", file.toString());

            ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
            StaticCredentialsConfig config = configurationFactory.build(StaticCredentialsConfig.class);

            assertEquals(config.getCredentialsKey(), Optional.empty());
            assertEquals(config.getCredentialsFile(), Optional.of(file.toString()));
        }
        catch (IOException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testExplicitPropertyMappingsValidation()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bigquery.credentials-key", "key")
                .put("bigquery.credentials-file", "file")
                .buildOrThrow();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        assertThatThrownBy(() -> configurationFactory.build(StaticCredentialsConfig.class))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Exactly one of 'bigquery.credentials-key' or 'bigquery.credentials-file' must be specified");
    }
}
