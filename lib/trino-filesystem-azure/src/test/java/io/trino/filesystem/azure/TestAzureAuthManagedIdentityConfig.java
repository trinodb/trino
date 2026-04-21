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
package io.trino.filesystem.azure;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static org.assertj.core.api.Assertions.assertThat;

final class TestAzureAuthManagedIdentityConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AzureAuthManagedIdentityConfig.class)
                .setClientId(null)
                .setResourceId(null));
    }

    @Test
    void testClientIdPropertyMapping()
    {
        AzureAuthManagedIdentityConfig actual = buildManagedConfig(ImmutableMap.<String, String>builder()
                .put("azure.user-assigned-managed-identity.client-id", "clientId")
                .buildOrThrow());

        assertThat(actual.getClientId()).hasValue("clientId");
        assertThat(actual.getResourceId()).isEmpty();
    }

    @Test
    void testResourceIdPropertyMapping()
    {
        AzureAuthManagedIdentityConfig actual = buildManagedConfig(ImmutableMap.<String, String>builder()
                .put("azure.user-assigned-managed-identity.resource-id", "resourceId")
                .buildOrThrow());

        assertThat(actual.getClientId()).isEmpty();
        assertThat(actual.getResourceId()).hasValue("resourceId");
    }

    @Test
    void testValidation()
    {
        assertValidates(new AzureAuthManagedIdentityConfig());

        assertFailsValidation(
                new AzureAuthManagedIdentityConfig()
                        .setClientId("clientId")
                        .setResourceId("resourceId"),
                "configValid",
                "Both azure.user-assigned-managed-identity.client-id and azure.user-assigned-managed-identity.resource-id cannot be set",
                AssertTrue.class);
    }

    private static AzureAuthManagedIdentityConfig buildManagedConfig(Map<String, String> properties)
    {
        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        return configurationFactory.build(AzureAuthManagedIdentityConfig.class);
    }
}
