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
package io.prestosql.plugin.hive.azure;

import org.testng.annotations.Test;

import java.util.Set;
import java.util.function.BiConsumer;

import static com.google.common.collect.Sets.difference;
import static org.testng.Assert.assertThrows;

public class TestPrestoAzureConfigurationInitializer
{
    @Test
    public void testAdl()
    {
        testPropertyGroup(
                HiveAzureConfig::setAdlClientId,
                HiveAzureConfig::setAdlCredential,
                HiveAzureConfig::setAdlRefreshUrl);
    }

    @Test
    public void testWasb()
    {
        testPropertyGroup(
                HiveAzureConfig::setWasbAccessKey,
                HiveAzureConfig::setWasbStorageAccount);
    }

    @Test
    public void testAbfsAccessKey()
    {
        testPropertyGroup(
                HiveAzureConfig::setAbfsAccessKey,
                HiveAzureConfig::setAbfsStorageAccount);
    }

    @Test
    public void testAbfsOAuth()
    {
        testPropertyGroup(
                HiveAzureConfig::setAbfsOAuthClientEndpoint,
                HiveAzureConfig::setAbfsOAuthClientId,
                HiveAzureConfig::setAbfsOAuthClientSecret);
    }

    @Test
    public void testExclusiveProperties()
    {
        assertThrows(() -> testProperties(
                HiveAzureConfig::setAbfsAccessKey,
                HiveAzureConfig::setAbfsStorageAccount,
                HiveAzureConfig::setAbfsOAuthClientEndpoint,
                HiveAzureConfig::setAbfsOAuthClientId,
                HiveAzureConfig::setAbfsOAuthClientSecret));
    }

    @SafeVarargs
    private static void testPropertyGroup(BiConsumer<HiveAzureConfig, String>... setters)
    {
        testPropertyGroup(Set.of(setters));
    }

    private static void testPropertyGroup(Set<BiConsumer<HiveAzureConfig, String>> setters)
    {
        // All properties work together
        testProperties(setters);

        // Dropping any one property fails
        for (var setter : setters) {
            assertThrows(() -> testProperties(difference(setters, Set.of(setter))));
        }
    }

    @SafeVarargs
    private static void testProperties(BiConsumer<HiveAzureConfig, String>... setters)
    {
        testProperties(Set.of(setters));
    }

    private static void testProperties(Set<BiConsumer<HiveAzureConfig, String>> setters)
    {
        var config = new HiveAzureConfig();
        for (var setter : setters) {
            setter.accept(config, "test value");
        }
        new PrestoAzureConfigurationInitializer(config);
    }
}
