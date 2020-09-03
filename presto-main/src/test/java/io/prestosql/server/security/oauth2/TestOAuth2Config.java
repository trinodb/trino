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
package io.prestosql.server.security.oauth2;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestOAuth2Config
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OAuth2Config.class)
                .setAuthUrl(null)
                .setTokenUrl(null)
                .setClientId(null)
                .setClientSecret(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("http-server.authentication.oauth2.auth-url", "http://127.0.0.1:9000/oauth2/auth")
                .put("http-server.authentication.oauth2.token-url", "http://127.0.0.1:9000/oauth2/token")
                .put("http-server.authentication.oauth2.client-id", "another-consumer")
                .put("http-server.authentication.oauth2.client-secret", "consumer-secret")
                .build();

        OAuth2Config expected = new OAuth2Config()
                .setAuthUrl("http://127.0.0.1:9000/oauth2/auth")
                .setTokenUrl("http://127.0.0.1:9000/oauth2/token")
                .setClientId("another-consumer")
                .setClientSecret("consumer-secret");

        assertFullMapping(properties, expected);
    }
}
