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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Key;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.server.HttpServerInfo;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.trino.server.security.jwt.JwkService;
import io.trino.server.security.jwt.JwkSigningKeyResolver;
import io.trino.testing.TestingAccessControlManager;
import okhttp3.Request;
import okhttp3.Response;
import org.testng.annotations.Test;

import javax.ws.rs.core.HttpHeaders;

import java.net.URI;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.trino.server.security.oauth2.TokenEndpointAuthMethod.CLIENT_SECRET_BASIC;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.IMPERSONATE_USER;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static java.util.Locale.ENGLISH;
import static javax.ws.rs.core.Response.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestOAuth2WebUiAuthenticationFilterWithJwt
        extends BaseOAuth2WebUiAuthenticationFilterTest
{
    @Override
    protected ImmutableMap<String, String> getOAuth2Config(String idpUrl)
    {
        return ImmutableMap.<String, String>builder()
                .put("web-ui.enabled", "true")
                .put("web-ui.authentication.type", "oauth2")
                .put("http-server.authentication.type", "oauth2")
                .put("http-server.https.enabled", "true")
                .put("http-server.https.keystore.path", Resources.getResource("cert/localhost.pem").getPath())
                .put("http-server.https.keystore.key", "")
                .put("http-server.authentication.oauth2.issuer", "https://localhost:4444/")
                .put("http-server.authentication.oauth2.auth-url", idpUrl + "/oauth2/auth")
                .put("http-server.authentication.oauth2.token-url", idpUrl + "/oauth2/token")
                .put("http-server.authentication.oauth2.jwks-url", idpUrl + "/.well-known/jwks.json")
                .put("http-server.authentication.oauth2.client-id", TRINO_CLIENT_ID)
                .put("http-server.authentication.oauth2.client-secret", TRINO_CLIENT_SECRET)
                .put("http-server.authentication.oauth2.additional-audiences", TRUSTED_CLIENT_ID)
                .put("http-server.authentication.oauth2.user-mapping.file", Resources.getResource("user-mapping.json").getPath())
                .put("oauth2-jwk.http-client.trust-store-path", Resources.getResource("cert/localhost.pem").getPath())
                .build();
    }

    @Override
    protected TestingHydraIdentityProvider getHydraIdp()
            throws Exception
    {
        TestingHydraIdentityProvider hydraIdP = new TestingHydraIdentityProvider(TTL_ACCESS_TOKEN_IN_SECONDS, true, false);
        hydraIdP.start();

        return hydraIdP;
    }

    @Override
    protected void validateAccessToken(String cookieValue)
    {
        assertThat(cookieValue).isNotBlank();
        Jws<Claims> jwt = Jwts.parserBuilder()
                .setSigningKeyResolver(new JwkSigningKeyResolver(new JwkService(
                        URI.create("https://localhost:" + hydraIdP.getAuthPort() + "/.well-known/jwks.json"),
                        new JettyHttpClient(new HttpClientConfig()
                                .setTrustStorePath(Resources.getResource("cert/localhost.pem").getPath())))))
                .build()
                .parseClaimsJws(cookieValue);
        Claims claims = jwt.getBody();
        assertThat(claims.getSubject()).isEqualTo("foo@bar.com");
        assertThat(claims.get("client_id")).isEqualTo(TRINO_CLIENT_ID);
        assertThat(claims.getIssuer()).isEqualTo("https://localhost:4444/");
    }

    @Test
    public void testLowercaseTrinoUserWillNotTriggerImpersonationWithAppropriateUserMapping()
            throws Exception
    {
        String usernameUppercase = TRINO_CLIENT_ID.toUpperCase(ENGLISH);
        String usernameLowerCase = TRINO_CLIENT_ID.toLowerCase(ENGLISH);
        assertThat(usernameUppercase).isNotEqualTo(TRINO_CLIENT_ID);
        assertThat(usernameLowerCase).isNotEqualTo(TRINO_CLIENT_ID);

        hydraIdP.createClient(
                usernameUppercase,
                TRINO_CLIENT_SECRET,
                CLIENT_SECRET_BASIC,
                ImmutableList.of(TRINO_AUDIENCE),
                server.getHttpsBaseUrl() + "/oauth2/callback");

        HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
        TestingAccessControlManager accessControl = server.getAccessControl();

        String token = hydraIdP.getToken(usernameUppercase, TRINO_CLIENT_SECRET, ImmutableList.of(TRINO_AUDIENCE));
        try {
            accessControl.deny(privilege(usernameLowerCase, IMPERSONATE_USER));
            try (Response response = httpClient.newCall(new Request.Builder()
                            .url(uriBuilderFrom(httpServerInfo.getHttpsUri())
                                    .replacePath("/username")
                                    .toString())
                            .addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                            .addHeader("X-Trino-User", usernameLowerCase)
                            .build())
                    .execute()) {
                assertThat(response.code()).isEqualTo(OK.getStatusCode());
                // We expect user-mapping rule '([^@]+)(@.*)?' to lowercase Principal name and skip impersonation as it matches Trino user.
                assertEquals(response.header("user"), usernameLowerCase);
            }
        }
        finally {
            accessControl.reset();
        }
    }
}
