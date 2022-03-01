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

import static io.trino.server.security.oauth2.OAuth2WebUiAccessTokenUtil.validateJwtAccessToken;
import static io.trino.server.security.oauth2.TestingHydraIdentityProvider.createHydraIdp;

public class TestOAuth2WebUiAuthenticationFilterWithJwt
        extends BaseOAuth2WebUiAuthenticationFilterTest
{
    @Override
    protected ImmutableMap<String, String> getOAuth2Config(String idpUrl)
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
                .put("http-server.authentication.oauth2.jwks-url", idpUrl + "/.well-known/jwks.json")
                .put("http-server.authentication.oauth2.scopes", "openid")
                .put("http-server.authentication.oauth2.client-id", TRINO_CLIENT_ID)
                .put("http-server.authentication.oauth2.client-secret", TRINO_CLIENT_SECRET)
                .put("http-server.authentication.oauth2.additional-audiences", TRUSTED_CLIENT_ID)
                .put("http-server.authentication.oauth2.user-mapping.pattern", "(.*)(@.*)?")
                .put("oauth2-jwk.http-client.trust-store-path", Resources.getResource("cert/localhost.pem").getPath())
                .buildOrThrow();
    }

    @Override
    protected TestingHydraIdentityProvider getHydraIdp()
            throws Exception
    {
        return createHydraIdp(TTL_ACCESS_TOKEN_IN_SECONDS, true);
    }

    @Override
    protected void validateAccessToken(String cookieValue)
    {
        validateJwtAccessToken(cookieValue, TRINO_CLIENT_ID, hydraIdP);
    }
}
