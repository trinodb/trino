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
package io.trino.plugin.base.security;

import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_REFRESH_PERIOD;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFileBasedAccessControlConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FileBasedAccessControlConfig.class)
                .setConfigFile(null)
                .setRefreshPeriod(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path securityConfigFile = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put(SECURITY_CONFIG_FILE, securityConfigFile.toString())
                .put(SECURITY_REFRESH_PERIOD, "1s")
                .buildOrThrow();

        FileBasedAccessControlConfig expected = new FileBasedAccessControlConfig()
                .setConfigFile(securityConfigFile.toFile())
                .setRefreshPeriod(new Duration(1, TimeUnit.SECONDS));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
            throws IOException
    {
        Path securityConfigFile = Files.createTempFile(null, null);

        assertThatThrownBy(() -> newInstance(ImmutableMap.of(SECURITY_REFRESH_PERIOD, "1ms")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("security.config-file: may not be null ");

        assertThatThrownBy(() -> newInstance(ImmutableMap.of(
                SECURITY_CONFIG_FILE, securityConfigFile.toString(),
                SECURITY_REFRESH_PERIOD, "1us")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Invalid configuration property security.refresh-period");

        newInstance(ImmutableMap.of(SECURITY_CONFIG_FILE, securityConfigFile.toString()));
    }

    private static FileBasedAccessControlConfig newInstance(Map<String, String> properties)
    {
        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        return configurationFactory.build(FileBasedAccessControlConfig.class);
    }
}
