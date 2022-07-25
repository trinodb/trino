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
package io.trino.plugin.hive.azure;

import org.testng.annotations.Test;

import java.util.Set;
import java.util.function.BiConsumer;

import static com.google.common.collect.Sets.difference;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoAzureConfigurationInitializer
{
    @Test
    public void testAdl()
    {
        testPropertyGroup(
                "If any of ADL client ID, credential, and refresh URL are set, all must be set",
                HiveAzureConfig::setAdlClientId,
                HiveAzureConfig::setAdlCredential,
                HiveAzureConfig::setAdlRefreshUrl);
    }

    @Test
    public void testWasb()
    {
        testPropertyGroup(
                "If WASB storage account or access key is set, both must be set",
                HiveAzureConfig::setWasbAccessKey,
                HiveAzureConfig::setWasbStorageAccount);
    }

    @Test
    public void testAbfsAccessKey()
    {
        testPropertyGroup(
                "If ABFS storage account or access key is set, both must be set",
                HiveAzureConfig::setAbfsAccessKey,
                HiveAzureConfig::setAbfsStorageAccount);
    }

    @Test
    public void testAbfsOAuth()
    {
        testPropertyGroup(
                "If any of ABFS OAuth2 Client endpoint, ID, and secret are set, all must be set.",
                HiveAzureConfig::setAbfsOAuthClientEndpoint,
                HiveAzureConfig::setAbfsOAuthClientId,
                HiveAzureConfig::setAbfsOAuthClientSecret);
    }

    @Test
    public void testExclusiveProperties()
    {
        assertThatThrownBy(() -> testProperties(
                HiveAzureConfig::setAbfsAccessKey,
                HiveAzureConfig::setAbfsStorageAccount,
                HiveAzureConfig::setAbfsOAuthClientEndpoint,
                HiveAzureConfig::setAbfsOAuthClientId,
                HiveAzureConfig::setAbfsOAuthClientSecret))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Multiple ABFS authentication methods configured: access key and OAuth2");
    }

    @SafeVarargs
    private static void testPropertyGroup(String expectedErrorMessage, BiConsumer<HiveAzureConfig, String>... setters)
    {
        testPropertyGroup(expectedErrorMessage, Set.of(setters));
    }

    private static void testPropertyGroup(String expectedErrorMessage, Set<BiConsumer<HiveAzureConfig, String>> setters)
    {
        // All properties work together
        testProperties(setters);

        // Dropping any one property fails
        for (BiConsumer<HiveAzureConfig, String> setter : setters) {
            assertThatThrownBy(() -> testProperties(difference(setters, Set.of(setter))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(expectedErrorMessage);
        }
    }

    @SafeVarargs
    private static void testProperties(BiConsumer<HiveAzureConfig, String>... setters)
    {
        testProperties(Set.of(setters));
    }

    private static void testProperties(Set<BiConsumer<HiveAzureConfig, String>> setters)
    {
        HiveAzureConfig config = new HiveAzureConfig();
        for (BiConsumer<HiveAzureConfig, String> setter : setters) {
            setter.accept(config, "test value");
        }
        new TrinoAzureConfigurationInitializer(config);
    }
}
