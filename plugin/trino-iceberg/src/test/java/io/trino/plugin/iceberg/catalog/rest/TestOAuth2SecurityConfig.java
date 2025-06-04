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
                .setTokenRefreshEnabled(OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT));
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
                .buildOrThrow();

        OAuth2SecurityConfig expected = new OAuth2SecurityConfig()
                .setCredential("credential")
                .setToken("token")
                .setScope("scope")
                .setServerUri(URI.create("http://localhost:8080/realms/iceberg/protocol/openid-connect/token"))
                .setTokenRefreshEnabled(false);
        assertThat(expected.credentialOrTokenPresent()).isTrue();
        assertThat(expected.scopePresentOnlyWithCredential()).isFalse();
        assertFullMapping(properties, expected);
    }
}
