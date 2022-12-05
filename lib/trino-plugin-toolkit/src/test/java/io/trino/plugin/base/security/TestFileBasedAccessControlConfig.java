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
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_REFRESH_PERIOD;

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

        Map<String, String> properties = ImmutableMap.<String, String>builder()
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
        File securityConfigFile = Files.createTempFile(null, null).toFile();

        assertFailsValidation(
                new FileBasedAccessControlConfig()
                    .setRefreshPeriod(Duration.valueOf("1ms")),
                "configFile",
                "may not be null",
                NotNull.class);

        assertFailsValidation(
                new FileBasedAccessControlConfig()
                    .setRefreshPeriod(Duration.valueOf("1ms"))
                    .setConfigFile(new File("not_existing_file")),
                "configFile",
                "file does not exist: not_existing_file",
                FileExists.class);

        assertFailsValidation(
                new FileBasedAccessControlConfig()
                    .setRefreshPeriod(Duration.valueOf("0ms"))
                    .setConfigFile(securityConfigFile),
                "refreshPeriod",
                "must be greater than or equal to 1ms",
                MinDuration.class);

        assertValidates(
                new FileBasedAccessControlConfig()
                    .setConfigFile(securityConfigFile));

        assertValidates(
                new FileBasedAccessControlConfig()
                    .setConfigFile(securityConfigFile)
                    .setRefreshPeriod(Duration.valueOf("1ms")));
    }
}
