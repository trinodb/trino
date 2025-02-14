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
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.jsonwebtoken.impl.DefaultClaims;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static jakarta.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class TestOAuth2WebUiAuthenticationFilterWithOpaque
        extends BaseOAuth2WebUiAuthenticationFilterTest
{
    @Override
    protected Map<String, String> getOAuth2Config(String idpUrl)
    {
        return ImmutableMap.<String, String>builder()
                .put("web-ui.enabled", "true")
                .put("web-ui.authentication.type", "oauth2")
                .put("http-server.https.enabled", "true")
                .put("http-server.https.keystore.path", Resources.getResource("cert/localhost.pem").getPath())
                .put("http-server.https.keystore.key", "")
                .put("http-server.authentication.oauth2.issuer", "https://localhost:4444/")
                .put("http-server.authentication.oauth2.auth-url", idpUrl + "/oauth2/auth")
                .put("http-server.authentication.oauth2.token-url", idpUrl + "/oauth2/token")
                .put("http-server.authentication.oauth2.end-session-url", idpUrl + "/oauth2/sessions/logout")
                .put("http-server.authentication.oauth2.jwks-url", idpUrl + "/.well-known/jwks.json")
                .put("http-server.authentication.oauth2.userinfo-url", idpUrl + "/userinfo")
                .put("http-server.authentication.oauth2.client-id", TRINO_CLIENT_ID)
                .put("http-server.authentication.oauth2.client-secret", TRINO_CLIENT_SECRET)
                // This is necessary as Hydra does not return `sub` from `/userinfo` for client credential grants.
                .put("http-server.authentication.oauth2.principal-field", "iss")
                .put("http-server.authentication.oauth2.additional-audiences", TRUSTED_CLIENT_ID)
                .put("http-server.authentication.oauth2.max-clock-skew", "0s")
                .put("http-server.authentication.oauth2.user-mapping.pattern", "(.*)(@.*)?")
                .put("http-server.authentication.oauth2.oidc.discovery", "false")
                .put("oauth2-jwk.http-client.trust-store-path", Resources.getResource("cert/localhost.pem").getPath())
                .buildOrThrow();
    }

    @Override
    protected TestingHydraIdentityProvider getHydraIdp()
            throws Exception
    {
        TestingHydraIdentityProvider hydraIdP = new TestingHydraIdentityProvider(TTL_ACCESS_TOKEN_IN_SECONDS, false, false);
        hydraIdP.start();

        return hydraIdP;
    }

    @Override
    protected void validateAccessToken(String cookieValue)
    {
        Request request = new Request.Builder().url("https://localhost:" + hydraIdP.getAuthPort() + "/userinfo").addHeader(AUTHORIZATION, "Bearer " + cookieValue).build();
        try (Response response = httpClient.newCall(request).execute()) {
            assertThat(response.body()).isNotNull();
            DefaultClaims claims = new DefaultClaims(JsonCodec.mapJsonCodec(String.class, Object.class).fromJson(response.body().byteStream()));
            assertThat(claims.getSubject()).isEqualTo("foo@bar.com");
            assertThat(claims.get("aud")).isEqualTo(Set.of(TRINO_CLIENT_ID));
        }
        catch (IOException e) {
            fail("Exception while calling /userinfo", e);
        }
    }
}
