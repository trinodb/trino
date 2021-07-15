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

import com.github.scribejava.core.model.OAuth2AccessToken;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.units.Duration;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SigningKeyResolver;
import io.trino.server.security.jwt.JwkService;
import io.trino.server.security.jwt.JwkSigningKeyResolver;
import io.trino.server.security.oauth2.ScribeJavaOAuth2Client.DynamicCallbackOAuth2Service;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.io.Resources.getResource;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.server.security.oauth2.TokenEndpointAuthMethod.CLIENT_SECRET_BASIC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicCallbackOAuth2Service
{
    private static final String CLIENT_ID = "client";
    private static final String CLIENT_SECRET = "secret";

    private final TestingHydraIdentityProvider hydraIdP = new TestingHydraIdentityProvider();
    private final HttpClient httpClient = new JettyHttpClient(new HttpClientConfig()
            .setTrustStorePath(getResource("cert/localhost.pem").getPath()));
    private String hydraUrl;
    private SigningKeyResolver signingKeyResolver;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        hydraIdP.start();
        hydraUrl = "https://localhost:" + hydraIdP.getAuthPort();
        hydraIdP.createClient(
                CLIENT_ID,
                CLIENT_SECRET,
                CLIENT_SECRET_BASIC,
                ImmutableList.of("https://localhost:8080"),
                "https://localhost:8080/oauth2/callback");
        signingKeyResolver = new JwkSigningKeyResolver(new JwkService(
                URI.create(hydraUrl + "/.well-known/jwks.json"),
                httpClient,
                new Duration(5, TimeUnit.MINUTES)));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        closeAll(hydraIdP, httpClient);
    }

    @Test
    public void testMultipleScopes()
            throws Exception
    {
        DynamicCallbackOAuth2Service service = new DynamicCallbackOAuth2Service(
                new OAuth2Config()
                        .setAuthUrl(hydraUrl + "/oauth2/auth")
                        .setTokenUrl(hydraUrl + "/oauth2/token")
                        .setJwksUrl(hydraUrl + "/.well-known/jwks.json")
                        .setClientId(CLIENT_ID)
                        .setClientSecret(CLIENT_SECRET)
                        .setScopes("openid,offline"),
                httpClient);

        OAuth2AccessToken token = service.getAccessTokenClientCredentialsGrant();

        Claims claims = Jwts.parser()
                .setSigningKeyResolver(signingKeyResolver)
                .parseClaimsJws(token.getAccessToken())
                .getBody();
        assertThat(claims.get("scp", List.class)).containsExactlyInAnyOrder("openid", "offline");
    }
}
