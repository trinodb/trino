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
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.testng.annotations.Test;

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
import static org.testng.Assert.assertEquals;

public class TestFileBasedAccessControlConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FileBasedAccessControlConfig.class)
                .setConfigFile(null)
                .setJsonPointer("")
                .setRefreshPeriod(null));
    }

    @Test
    public void testExplicitPropertyMappingsWithLocalFile()
            throws IOException
    {
        Path securityConfigFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put(SECURITY_CONFIG_FILE, securityConfigFile.toString())
                .put("security.json-pointer", "/a/b")
                .put(SECURITY_REFRESH_PERIOD, "1s")
                .buildOrThrow();

        FileBasedAccessControlConfig expected = new FileBasedAccessControlConfig()
                .setConfigFile(securityConfigFile.toString())
                .setJsonPointer("/a/b")
                .setRefreshPeriod(new Duration(1, TimeUnit.SECONDS));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testExplicitPropertyMappingsWithUrl()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put(SECURITY_CONFIG_FILE, "http://test:1234/example")
                .put("security.json-pointer", "/data")
                .put(SECURITY_REFRESH_PERIOD, "1s")
                .buildOrThrow();

        FileBasedAccessControlConfig expected = new FileBasedAccessControlConfig()
                .setConfigFile("http://test:1234/example")
                .setJsonPointer("/data")
                .setRefreshPeriod(new Duration(1, TimeUnit.SECONDS));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidationWithLocalFile()
            throws IOException
    {
        String securityConfigFile = Files.createTempFile(null, null).toString();

        assertFailsValidation(
                new FileBasedAccessControlConfig()
                    .setRefreshPeriod(Duration.valueOf("1ms")),
                "configFile",
                "must not be null",
                NotNull.class);

        assertFailsValidation(
                new FileBasedAccessControlConfig()
                    .setRefreshPeriod(Duration.valueOf("0ms"))
                    .setConfigFile(securityConfigFile),
                "refreshPeriod",
                "must be greater than or equal to 1ms",
                MinDuration.class);

        assertFailsValidation(
                new FileBasedAccessControlConfig()
                    .setRefreshPeriod(Duration.valueOf("1ms"))
                    .setConfigFile("not_existing_file"),
                "configFileValid",
                "Config file does not exist.",
                AssertTrue.class);

        assertValidates(
                new FileBasedAccessControlConfig()
                    .setConfigFile(securityConfigFile));

        assertValidates(
                new FileBasedAccessControlConfig()
                    .setConfigFile(securityConfigFile)
                    .setRefreshPeriod(Duration.valueOf("1ms")));
    }

    @Test
    public void testValidationWithUrl()
    {
        String securityConfigUrl = "http://test:1234/example";

        assertFailsValidation(
                new FileBasedAccessControlConfig()
                        .setRefreshPeriod(Duration.valueOf("1ms")),
                "configFile",
                "must not be null",
                NotNull.class);

        assertFailsValidation(
                new FileBasedAccessControlConfig()
                        .setRefreshPeriod(Duration.valueOf("0ms"))
                        .setConfigFile(securityConfigUrl),
                "refreshPeriod",
                "must be greater than or equal to 1ms",
                MinDuration.class);

        assertFailsValidation(
                new FileBasedAccessControlConfig()
                        .setConfigFile(securityConfigUrl)
                        .setJsonPointer(null),
                "jsonPointer",
                "must not be null",
                NotNull.class);

        assertValidates(
                new FileBasedAccessControlConfig()
                        .setConfigFile(securityConfigUrl));

        assertValidates(
                new FileBasedAccessControlConfig()
                        .setConfigFile(securityConfigUrl)
                        .setRefreshPeriod(Duration.valueOf("1ms")));

        assertValidates(
                new FileBasedAccessControlConfig()
                        .setConfigFile(securityConfigUrl)
                        .setRefreshPeriod(Duration.valueOf("1ms"))
                        .setJsonPointer("/data"));
    }

    @Test
    public void testConfigSource()
    {
        FileBasedAccessControlConfig fileBasedAccessControlConfig = new FileBasedAccessControlConfig()
                .setConfigFile("/etc/access-control");
        assertEquals(fileBasedAccessControlConfig.isHttp(), false);

        fileBasedAccessControlConfig = new FileBasedAccessControlConfig()
                .setConfigFile("http://trino.example/config");
        assertEquals(fileBasedAccessControlConfig.isHttp(), true);

        fileBasedAccessControlConfig = new FileBasedAccessControlConfig()
                .setConfigFile("https://trino.example/config");
        assertEquals(fileBasedAccessControlConfig.isHttp(), true);
    }
}
