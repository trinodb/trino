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
package io.trino.plugin.google.sheets;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.AssertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertDeprecatedEquivalence;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestSheetsConfig
{
    private static final String BASE_64_ENCODED_TEST_KEY = Base64.getEncoder()
            .encodeToString("blah blah blah".getBytes(UTF_8));

    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SheetsConfig.class)
                .setCredentialsFilePath(null)
                .setCredentialsKey(null)
                .setMetadataSheetId(null)
                .setSheetsDataMaxCacheSize(1000)
                .setSheetsDataExpireAfterWrite(new Duration(5, TimeUnit.MINUTES))
                .setReadTimeout(new Duration(20, TimeUnit.SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappingsCredentialsPath()
            throws IOException
    {
        Path credentialsFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("gsheets.credentials-path", credentialsFile.toString())
                .put("gsheets.metadata-sheet-id", "foo_bar_sheet_id#Sheet1")
                .put("gsheets.max-data-cache-size", "2000")
                .put("gsheets.data-cache-ttl", "10m")
                .put("gsheets.read-timeout", "1m")
                .buildOrThrow();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        SheetsConfig config = configurationFactory.build(SheetsConfig.class);

        assertEquals(config.getCredentialsKey(), Optional.empty());
        assertEquals(config.getCredentialsFilePath(), Optional.of(credentialsFile.toString()));
        assertEquals(config.getMetadataSheetId(), Optional.of("foo_bar_sheet_id#Sheet1"));
        assertEquals(config.getSheetsDataMaxCacheSize(), 2000);
        assertEquals(config.getSheetsDataExpireAfterWrite(), Duration.valueOf("10m"));
        assertEquals(config.getReadTimeout(), Duration.valueOf("1m"));
    }

    @Test
    public void testExplicitPropertyMappingsCredentialsKey()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("gsheets.credentials-key", BASE_64_ENCODED_TEST_KEY)
                .put("gsheets.metadata-sheet-id", "foo_bar_sheet_id#Sheet1")
                .put("gsheets.max-data-cache-size", "2000")
                .put("gsheets.data-cache-ttl", "10m")
                .put("gsheets.read-timeout", "1m")
                .buildOrThrow();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        SheetsConfig config = configurationFactory.build(SheetsConfig.class);

        assertEquals(config.getCredentialsKey(), Optional.of(BASE_64_ENCODED_TEST_KEY));
        assertEquals(config.getCredentialsFilePath(), Optional.empty());
        assertEquals(config.getMetadataSheetId(), Optional.of("foo_bar_sheet_id#Sheet1"));
        assertEquals(config.getSheetsDataMaxCacheSize(), 2000);
        assertEquals(config.getSheetsDataExpireAfterWrite(), Duration.valueOf("10m"));
        assertEquals(config.getReadTimeout(), Duration.valueOf("1m"));
    }

    @Test
    public void testLegacyPropertyMappings()
            throws IOException
    {
        Path credentialsFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("gsheets.credentials-path", credentialsFile.toString())
                .put("gsheets.metadata-sheet-id", "foo_bar_sheet_id#Sheet1")
                .put("gsheets.max-data-cache-size", "2000")
                .put("gsheets.data-cache-ttl", "10m")
                .buildOrThrow();

        Map<String, String> oldProperties = ImmutableMap.<String, String>builder()
                .put("credentials-path", credentialsFile.toString())
                .put("metadata-sheet-id", "foo_bar_sheet_id#Sheet1")
                .put("sheets-data-max-cache-size", "2000")
                .put("sheets-data-expire-after-write", "10m")
                .buildOrThrow();

        assertDeprecatedEquivalence(SheetsConfig.class, properties, oldProperties);
    }

    @Test
    public void testCredentialValidation()
            throws IOException
    {
        Path credentialsFile = Files.createTempFile(null, null);
        assertValidates(
                new SheetsConfig().setMetadataSheetId("foo_bar_sheet_id#Sheet1")
                        .setCredentialsFilePath(credentialsFile.toString()));

        assertValidates(
                new SheetsConfig().setMetadataSheetId("foo_bar_sheet_id#Sheet1")
                        .setCredentialsKey(BASE_64_ENCODED_TEST_KEY));
    }

    @Test
    public void testCredentialValidationFailure()
            throws IOException
    {
        Path credentialsFile = Files.createTempFile(null, null);
        assertFailsCredentialsValidation(
                new SheetsConfig().setMetadataSheetId("foo_bar_sheet_id#Sheet1")
                        .setCredentialsFilePath(credentialsFile.toString())
                        .setCredentialsKey(BASE_64_ENCODED_TEST_KEY));

        assertFailsCredentialsValidation(
                new SheetsConfig().setMetadataSheetId("foo_bar_sheet_id#Sheet1"));
    }

    private static void assertFailsCredentialsValidation(SheetsConfig config)
    {
        assertFailsValidation(
                config,
                "credentialsConfigurationValid",
                "Exactly one of 'gsheets.credentials-key' or 'gsheets.credentials-path' must be specified",
                AssertTrue.class);
    }
}
