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
import io.airlift.log.Level;
import io.airlift.log.Logging;
import io.trino.server.security.jwt.JwkService;
import io.trino.server.security.jwt.JwkSigningKeyResolver;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.server.ui.OAuth2WebUiAuthenticationFilter;
import io.trino.server.ui.WebUiModule;
import okhttp3.JavaNetCookieJar;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.CookieManager;
import java.net.CookieStore;
import java.net.HttpCookie;
import java.net.URI;
import java.time.Duration;

import static io.airlift.testing.Closeables.closeAll;
import static io.trino.client.OkHttpUtil.setupInsecureSsl;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static io.trino.server.security.oauth2.TokenEndpointAuthMethod.CLIENT_SECRET_BASIC;
import static io.trino.server.ui.OAuthIdTokenCookie.ID_TOKEN_COOKIE;
import static jakarta.servlet.http.HttpServletResponse.SC_OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestOAuth2WebUiAuthenticationFilterWithRefreshTokens
{
    protected static final Duration TTL_ACCESS_TOKEN_IN_SECONDS = Duration.ofSeconds(5);

    protected static final String TRINO_CLIENT_ID = "trino-client";
    protected static final String TRINO_CLIENT_SECRET = "trino-secret";
    private static final String TRINO_AUDIENCE = TRINO_CLIENT_ID;
    private static final String ADDITIONAL_AUDIENCE = "https://external-service.com";
    protected static final String TRUSTED_CLIENT_ID = "trusted-client";

    protected OkHttpClient httpClient;
    protected TestingHydraIdentityProvider hydraIdP;
    private TestingTrinoServer server;
    private URI serverUri;
    private URI uiUri;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.setLevel(OAuth2WebUiAuthenticationFilter.class.getName(), Level.DEBUG);
        logging.setLevel(OAuth2Service.class.getName(), Level.DEBUG);

        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();
        setupInsecureSsl(httpClientBuilder);
        httpClientBuilder.followRedirects(false);
        httpClient = httpClientBuilder.build();

        hydraIdP = new TestingHydraIdentityProvider(TTL_ACCESS_TOKEN_IN_SECONDS, true, false);
        hydraIdP.start();

        String idpUrl = "https://localhost:" + hydraIdP.getAuthPort();

        server = TestingTrinoServer.builder()
                .setCoordinator(true)
                .setAdditionalModule(new WebUiModule())
                .setProperties(ImmutableMap.<String, String>builder()
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
                        .put("http-server.authentication.oauth2.client-id", TRINO_CLIENT_ID)
                        .put("http-server.authentication.oauth2.client-secret", TRINO_CLIENT_SECRET)
                        .put("http-server.authentication.oauth2.additional-audiences", TRUSTED_CLIENT_ID)
                        .put("http-server.authentication.oauth2.max-clock-skew", "0s")
                        .put("http-server.authentication.oauth2.user-mapping.pattern", "(.*)(@.*)?")
                        .put("http-server.authentication.oauth2.oidc.discovery", "false")
                        .put("http-server.authentication.oauth2.scopes", "openid,offline")
                        .put("http-server.authentication.oauth2.refresh-tokens", "true")
                        .put("oauth2-jwk.http-client.trust-store-path", Resources.getResource("cert/localhost.pem").getPath())
                        .buildOrThrow())
                .build();
        server.getInstance(Key.get(OAuth2Client.class)).load();
        serverUri = server.getHttpsBaseUrl();
        uiUri = serverUri.resolve("/ui/");

        hydraIdP.createClient(
                TRINO_CLIENT_ID,
                TRINO_CLIENT_SECRET,
                CLIENT_SECRET_BASIC,
                ImmutableList.of(TRINO_AUDIENCE, ADDITIONAL_AUDIENCE),
                serverUri + "/oauth2/callback",
                serverUri + "/ui/logout/logout.html");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.clearLevel(OAuth2WebUiAuthenticationFilter.class.getName());
        logging.clearLevel(OAuth2Service.class.getName());

        closeAll(
                server,
                hydraIdP,
                () -> {
                    httpClient.dispatcher().executorService().shutdown();
                    httpClient.connectionPool().evictAll();
                });
        server = null;
        hydraIdP = null;
        httpClient = null;
    }

    @Test
    public void testSuccessfulFlowWithRefreshedAccessToken()
            throws Exception
    {
        // create a new HttpClient which follows redirects and give access to cookies
        CookieManager cookieManager = new CookieManager();
        CookieStore cookieStore = cookieManager.getCookieStore();
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();
        setupInsecureSsl(httpClientBuilder);
        OkHttpClient httpClient = httpClientBuilder
                .followRedirects(true)
                .cookieJar(new JavaNetCookieJar(cookieManager))
                .build();

        assertThat(cookieStore.get(uiUri)).isEmpty();

        accessUi(httpClient);

        HttpCookie idTokenCookie = cookieStore.get(uiUri)
                .stream()
                .filter(cookie -> cookie.getName().equals(ID_TOKEN_COOKIE))
                .findFirst()
                .orElseThrow();

        Thread.sleep(TTL_ACCESS_TOKEN_IN_SECONDS.plusSeconds(1).toMillis()); // wait for the token expiration = ttl of access token + 1 sec

        // Access the UI after timeout
        accessUi(httpClient);

        HttpCookie newIdTokenCookie = cookieStore.get(uiUri)
                .stream()
                .filter(cookie -> cookie.getName().equals(ID_TOKEN_COOKIE))
                .findFirst()
                .orElseThrow();

        // Post refresh of access token the id-token should remain same
        assertThat(newIdTokenCookie.getValue()).isEqualTo(idTokenCookie.getValue());

        // Check if the IDToken was expired
        assertTokenIsExpired(newIdTokenCookie.getValue());

        // Logout from the Trino
        try (Response response = httpClient.newCall(
                        new Request.Builder()
                                .url(uiUri.resolve("logout").toURL())
                                .get()
                                .build())
                .execute()) {
            assertEquals(response.code(), SC_OK);
            assertEquals(response.request().url().toString(), uiUri.resolve("logout/logout.html").toString());
        }
        assertThat(cookieStore.get(uiUri)).isEmpty();
    }

    protected void assertTokenIsExpired(String claimsJws)
    {
        assertThatThrownBy(() -> newJwtParserBuilder()
                .setSigningKeyResolver(new JwkSigningKeyResolver(new JwkService(
                        URI.create("https://localhost:" + hydraIdP.getAuthPort() + "/.well-known/jwks.json"),
                        new JettyHttpClient(new HttpClientConfig()
                                .setTrustStorePath(Resources.getResource("cert/localhost.pem").getPath())))))
                .build()
                .parseClaimsJws(claimsJws));
    }

    private void accessUi(OkHttpClient httpClient)
            throws Exception
    {
        // access UI and follow redirects in order to get OAuth2 and IDToken cookie
        try (Response response = httpClient.newCall(
                        new Request.Builder()
                                .url(uiUri.toURL())
                                .get()
                                .build())
                .execute()) {
            assertEquals(response.code(), SC_OK);
            assertEquals(response.request().url().toString(), uiUri.toString());
        }
    }
}
