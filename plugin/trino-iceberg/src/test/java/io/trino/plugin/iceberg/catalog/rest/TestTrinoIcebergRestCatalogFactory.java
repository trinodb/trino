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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.CatalogProperties;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.spi.security.ExtraCredentials.authenticatedExtraCredentialName;
import static org.apache.iceberg.rest.auth.OAuth2Properties.CREDENTIAL;
import static org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoIcebergRestCatalogFactory
{
    @Test
    public void testUserSessionOAuth2AddsSessionCredentialsToInitialCatalogProperties()
    {
        Map<String, String> sessionCredentials = OAuth2SessionCredentials.fromIdentity(
                ConnectorIdentity.forUser("alice")
                        .withExtraCredentials(ImmutableMap.of(
                                TOKEN, "client-supplied-token",
                                authenticatedExtraCredentialName(TOKEN), "alice-token",
                                CREDENTIAL, "client-supplied-credential",
                                authenticatedExtraCredentialName(CREDENTIAL), "alice-credential",
                                "unrelated", "secret"))
                        .build());

        Map<String, String> properties = OAuth2SessionCredentials.catalogPropertiesWithSessionCredentials(
                ImmutableMap.of(CatalogProperties.URI, "https://catalog.example.com"),
                sessionCredentials);

        assertThat(properties)
                .containsEntry(CatalogProperties.URI, "https://catalog.example.com")
                .containsEntry(TOKEN, "alice-token")
                .containsEntry(CREDENTIAL, "alice-credential")
                .doesNotContainKey("unrelated");
    }

    @Test
    public void testStaticCatalogCredentialsTakePrecedenceForInitialCatalogProperties()
    {
        Map<String, String> sessionCredentials = OAuth2SessionCredentials.fromIdentity(
                ConnectorIdentity.forUser("alice")
                        .withExtraCredentials(ImmutableMap.of(
                                authenticatedExtraCredentialName(TOKEN), "alice-token",
                                authenticatedExtraCredentialName(CREDENTIAL), "alice-credential"))
                        .build());

        Map<String, String> properties = OAuth2SessionCredentials.catalogPropertiesWithSessionCredentials(
                ImmutableMap.of(
                        CatalogProperties.URI, "https://catalog.example.com",
                        TOKEN, "catalog-token",
                        CREDENTIAL, "catalog-credential"),
                sessionCredentials);

        assertThat(properties)
                .containsEntry(TOKEN, "catalog-token")
                .containsEntry(CREDENTIAL, "catalog-credential");
    }

    @Test
    public void testClientSuppliedOAuth2SessionCredentialsAreIgnored()
    {
        Map<String, String> sessionCredentials = OAuth2SessionCredentials.fromIdentity(
                ConnectorIdentity.forUser("alice")
                        .withExtraCredentials(ImmutableMap.of(
                                TOKEN, "client-supplied-token",
                                CREDENTIAL, "client-supplied-credential"))
                        .build());

        Map<String, String> properties = OAuth2SessionCredentials.catalogPropertiesWithSessionCredentials(
                ImmutableMap.of(CatalogProperties.URI, "https://catalog.example.com"),
                sessionCredentials);

        assertThat(properties)
                .doesNotContainKey(TOKEN)
                .doesNotContainKey(CREDENTIAL);
    }

    @Test
    public void testUserSessionOAuth2CacheKeyUsesOnlyAuthenticatedCredentials()
    {
        String cacheKey = OAuth2SessionCredentials.cacheKey(
                        ConnectorIdentity.forUser("alice")
                                .withExtraCredentials(ImmutableMap.of(
                                        TOKEN, "client-supplied-token",
                                        authenticatedExtraCredentialName(TOKEN), "alice-token",
                                        "unrelated", "secret"))
                                .build())
                .orElseThrow();

        assertThat(cacheKey)
                .isNotBlank()
                .matches("[0-9a-f]+");

        assertThat(OAuth2SessionCredentials.cacheKey(
                ConnectorIdentity.forUser("alice")
                        .withExtraCredentials(ImmutableMap.of(
                                TOKEN, "different-client-supplied-token",
                                authenticatedExtraCredentialName(TOKEN), "alice-token",
                                "unrelated", "different-secret"))
                        .build()))
                .contains(cacheKey);

        assertThat(OAuth2SessionCredentials.cacheKey(
                ConnectorIdentity.forUser("alice")
                        .withExtraCredentials(ImmutableMap.of(authenticatedExtraCredentialName(TOKEN), "bob-token"))
                        .build()))
                .isPresent()
                .hasValueSatisfying(differentCacheKey -> assertThat(differentCacheKey).isNotEqualTo(cacheKey));
    }
}
