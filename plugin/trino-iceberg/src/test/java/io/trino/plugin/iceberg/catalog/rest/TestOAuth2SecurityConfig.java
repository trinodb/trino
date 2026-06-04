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
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOAuth2SecurityConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OAuth2SecurityConfig.class)
                .setCredential(null)
                .setToken(null)
                .setScope(null)
                .setServerUri(null)
                .setTokenRefreshEnabled(OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT)
                .setTokenExchangeEnabled(OAuth2Properties.TOKEN_EXCHANGE_ENABLED_DEFAULT));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.rest-catalog.oauth2.token", "token")
                .put("iceberg.rest-catalog.oauth2.credential", "credential")
                .put("iceberg.rest-catalog.oauth2.scope", "scope")
                .put("iceberg.rest-catalog.oauth2.server-uri", "http://localhost:8080/realms/iceberg/protocol/openid-connect/token")
                .put("iceberg.rest-catalog.oauth2.token-refresh-enabled", "false")
                .put("iceberg.rest-catalog.oauth2.token-exchange-enabled", "false")
                .buildOrThrow();

        OAuth2SecurityConfig expected = new OAuth2SecurityConfig()
                .setCredential("credential")
                .setToken("token")
                .setScope("scope")
                .setServerUri(URI.create("http://localhost:8080/realms/iceberg/protocol/openid-connect/token"))
                .setTokenRefreshEnabled(false)
                .setTokenExchangeEnabled(false);
        assertThat(expected.scopePresentOnlyWithCredential()).isFalse();
        assertFullMapping(properties, expected);
    }

    @Test
    public void testUserSessionAllowsSessionOAuth2Credentials()
    {
        OAuth2SecurityProperties securityProperties = new OAuth2SecurityProperties(
                new OAuth2SecurityConfig()
                        .setServerUri(URI.create("https://auth.example.com/oauth2/token"))
                        .setTokenExchangeEnabled(true));

        assertThat(securityProperties.get())
                .containsEntry(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_OAUTH2)
                .containsEntry(OAuth2Properties.OAUTH2_SERVER_URI, "https://auth.example.com/oauth2/token")
                .containsEntry(OAuth2Properties.TOKEN_EXCHANGE_ENABLED, "true")
                .doesNotContainKey(OAuth2Properties.TOKEN)
                .doesNotContainKey(OAuth2Properties.CREDENTIAL);
    }
}
