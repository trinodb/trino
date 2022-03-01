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
package io.trino.server.security.oauth2;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestOAuth2Config
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OAuth2Config.class)
                .setStateKey(null)
                .setIssuer(null)
                .setAccessTokenIssuer(null)
                .setAuthUrl(null)
                .setTokenUrl(null)
                .setRefreshTokenUrl(null)
                .setJwksUrl(null)
                .setUserinfoUrl(null)
                .setClientId(null)
                .setClientSecret(null)
                .setScopes("openid")
                .setChallengeTimeout(new Duration(15, MINUTES))
                .setPrincipalField("sub")
                .setGroupsField(null)
                .setAdditionalAudiences(Collections.emptyList())
                .setUserMappingPattern(null)
                .setUserMappingFile(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path userMappingFile = Files.createTempFile(null, null);
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("http-server.authentication.oauth2.state-key", "key-secret")
                .put("http-server.authentication.oauth2.issuer", "http://127.0.0.1:9000/oauth2")
                .put("http-server.authentication.oauth2.access-token-issuer", "http://127.0.0.1:9000/oauth2/access-token")
                .put("http-server.authentication.oauth2.auth-url", "http://127.0.0.1:9000/oauth2/auth")
                .put("http-server.authentication.oauth2.token-url", "http://127.0.0.1:9000/oauth2/token")
                .put("http-server.authentication.oauth2.refresh-token-url", "http://127.0.0.1:9000/oauth2/refresh")
                .put("http-server.authentication.oauth2.jwks-url", "http://127.0.0.1:9000/.well-known/jwks.json")
                .put("http-server.authentication.oauth2.userinfo-url", "http://127.0.0.1:9000/oauth2/userinfo")
                .put("http-server.authentication.oauth2.client-id", "another-consumer")
                .put("http-server.authentication.oauth2.client-secret", "consumer-secret")
                .put("http-server.authentication.oauth2.scopes", "email,offline")
                .put("http-server.authentication.oauth2.principal-field", "some-field")
                .put("http-server.authentication.oauth2.groups-field", "groups")
                .put("http-server.authentication.oauth2.additional-audiences", "test-aud1,test-aud2")
                .put("http-server.authentication.oauth2.challenge-timeout", "90s")
                .put("http-server.authentication.oauth2.user-mapping.pattern", "(.*)@something")
                .put("http-server.authentication.oauth2.user-mapping.file", userMappingFile.toString())
                .buildOrThrow();

        OAuth2Config expected = new OAuth2Config()
                .setStateKey("key-secret")
                .setIssuer("http://127.0.0.1:9000/oauth2")
                .setAccessTokenIssuer("http://127.0.0.1:9000/oauth2/access-token")
                .setAuthUrl("http://127.0.0.1:9000/oauth2/auth")
                .setTokenUrl("http://127.0.0.1:9000/oauth2/token")
                .setRefreshTokenUrl("http://127.0.0.1:9000/oauth2/refresh")
                .setJwksUrl("http://127.0.0.1:9000/.well-known/jwks.json")
                .setUserinfoUrl("http://127.0.0.1:9000/oauth2/userinfo")
                .setClientId("another-consumer")
                .setClientSecret("consumer-secret")
                .setScopes("email, offline")
                .setPrincipalField("some-field")
                .setGroupsField("groups")
                .setAdditionalAudiences(List.of("test-aud1", "test-aud2"))
                .setChallengeTimeout(new Duration(90, SECONDS))
                .setUserMappingPattern("(.*)@something")
                .setUserMappingFile(userMappingFile.toFile());

        assertFullMapping(properties, expected);
    }
}
