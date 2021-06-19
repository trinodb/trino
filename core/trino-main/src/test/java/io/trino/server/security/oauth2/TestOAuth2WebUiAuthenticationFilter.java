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
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Level;
import io.airlift.log.Logging;
import io.airlift.testing.Closeables;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.DefaultClaims;
import io.trino.server.security.jwt.JwkService;
import io.trino.server.security.jwt.JwkSigningKeyResolver;
import io.trino.server.security.jwt.JwtAuthenticatorConfig;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.server.ui.OAuth2WebUiAuthenticationFilter;
import io.trino.server.ui.WebUiModule;
import okhttp3.Cookie;
import okhttp3.CookieJar;
import okhttp3.HttpUrl;
import okhttp3.JavaNetCookieJar;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.CookieManager;
import java.net.CookieStore;
import java.net.HttpCookie;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static io.trino.client.OkHttpUtil.setupInsecureSsl;
import static io.trino.server.security.oauth2.TokenEndpointAuthMethod.CLIENT_SECRET_BASIC;
import static io.trino.server.ui.OAuthWebUiCookie.OAUTH2_COOKIE;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.ws.rs.core.HttpHeaders.LOCATION;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.SEE_OTHER;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestOAuth2WebUiAuthenticationFilter
{
    private static final Duration TTL_ACCESS_TOKEN_IN_SECONDS = Duration.ofSeconds(5);

    private static final String TRINO_CLIENT_ID = "trino-client";
    private static final String TRINO_CLIENT_SECRET = "trino-secret";
    private static final String TRINO_AUDIENCE = "https://trino/ui";
    private static final String ADDITIONAL_AUDIENCE = "https://external-service.com";
    private static final String TRUSTED_CLIENT_ID = "trusted-client";
    private static final String TRUSTED_CLIENT_SECRET = "trusted-secret";
    private static final String UNTRUSTED_CLIENT_ID = "untrusted-client";
    private static final String UNTRUSTED_CLIENT_SECRET = "untrusted-secret";
    private static final String UNTRUSTED_CLIENT_AUDIENCE = "https://untrusted.com";

    private final Logging logging = Logging.initialize();
    private final OkHttpClient httpClient;
    private TestingHydraIdentityProvider hydraIdP;

    private TestingTrinoServer server;
    private URI serverUri;
    private URI uiUri;

    public TestOAuth2WebUiAuthenticationFilter()
    {
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();
        setupInsecureSsl(httpClientBuilder);
        httpClientBuilder.followRedirects(false);
        httpClient = httpClientBuilder.build();
    }

    @BeforeClass
    public void setup()
            throws Exception
    {
        logging.setLevel(OAuth2WebUiAuthenticationFilter.class.getName(), Level.DEBUG);

        hydraIdP = new TestingHydraIdentityProvider(TTL_ACCESS_TOKEN_IN_SECONDS);
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
                        .put("http-server.authentication.oauth2.auth-url", idpUrl + "/oauth2/auth")
                        .put("http-server.authentication.oauth2.token-url", idpUrl + "/oauth2/token")
                        .put("http-server.authentication.oauth2.jwks-url", idpUrl + "/.well-known/jwks.json")
                        .put("http-server.authentication.oauth2.client-id", TRINO_CLIENT_ID)
                        .put("http-server.authentication.oauth2.client-secret", TRINO_CLIENT_SECRET)
                        .put("http-server.authentication.oauth2.audience", TRINO_AUDIENCE)
                        .put("http-server.authentication.oauth2.user-mapping.pattern", "(.*)(@.*)?")
                        .put("oauth2-jwk.http-client.trust-store-path", Resources.getResource("cert/localhost.pem").getPath())
                        .build())
                .build();
        server.waitForNodeRefresh(Duration.ofSeconds(10));
        serverUri = server.getHttpsBaseUrl();
        uiUri = serverUri.resolve("/ui/");

        hydraIdP.createClient(
                TRINO_CLIENT_ID,
                TRINO_CLIENT_SECRET,
                CLIENT_SECRET_BASIC,
                ImmutableList.of(TRINO_AUDIENCE, ADDITIONAL_AUDIENCE),
                serverUri + "/oauth2/callback");
        hydraIdP.createClient(
                TRUSTED_CLIENT_ID,
                TRUSTED_CLIENT_SECRET,
                CLIENT_SECRET_BASIC,
                ImmutableList.of(TRINO_AUDIENCE),
                serverUri + "/oauth2/callback");
        hydraIdP.createClient(
                UNTRUSTED_CLIENT_ID,
                UNTRUSTED_CLIENT_SECRET,
                CLIENT_SECRET_BASIC,
                ImmutableList.of(UNTRUSTED_CLIENT_AUDIENCE),
                "https://untrusted.com/callback");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        logging.clearLevel(OAuth2WebUiAuthenticationFilter.class.getName());
        Closeables.closeAll(server, hydraIdP);
    }

    @Test
    public void testUnauthorizedApiCall()
            throws IOException
    {
        try (Response response = httpClient
                .newCall(apiCall().build())
                .execute()) {
            assertUnauthorizedResponse(response);
        }
    }

    @Test
    public void testUnauthorizedUICall()
            throws IOException
    {
        try (Response response = httpClient
                .newCall(uiCall().build())
                .execute()) {
            assertRedirectResponse(response);
        }
    }

    @Test
    public void testUnsignedToken()
            throws NoSuchAlgorithmException, IOException
    {
        KeyPairGenerator keyGenerator = KeyPairGenerator.getInstance("RSA");
        SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.RS256;
        keyGenerator.initialize(4096);
        long now = Instant.now().getEpochSecond();
        String token = Jwts.builder()
                .setHeaderParam("alg", "RS256")
                .setHeaderParam("kid", "public:f467aa08-1c1b-4cde-ba45-84b0ef5d2ba8")
                .setHeaderParam("typ", "JWT")
                .setClaims(
                        new DefaultClaims(
                                ImmutableMap.<String, Object>builder()
                                        .put("aud", ImmutableList.of())
                                        .put("client_id", TRINO_CLIENT_ID)
                                        .put("exp", now + 60L)
                                        .put("iat", now)
                                        .put("iss", "https://hydra:4444/")
                                        .put("jti", UUID.randomUUID())
                                        .put("nbf", now)
                                        .put("scp", ImmutableList.of("openid"))
                                        .put("sub", "foo@bar.com")
                                        .build()))
                .signWith(signatureAlgorithm, keyGenerator.generateKeyPair().getPrivate())
                .compact();
        try (Response response = httpClientWithOAuth2Cookie(token)
                .newCall(uiCall().build())
                .execute()) {
            assertRedirectResponse(response);
        }
    }

    @Test
    public void testTokenWithInvalidAudience()
            throws IOException
    {
        String token = hydraIdP.getToken(UNTRUSTED_CLIENT_ID, UNTRUSTED_CLIENT_SECRET, ImmutableList.of(UNTRUSTED_CLIENT_AUDIENCE));
        try (Response response = httpClientWithOAuth2Cookie(token)
                .newCall(uiCall().build())
                .execute()) {
            assertUnauthorizedResponse(response);
        }
    }

    @Test
    public void testTokenFromTrustedClient()
            throws IOException
    {
        String token = hydraIdP.getToken(TRUSTED_CLIENT_ID, TRUSTED_CLIENT_SECRET, ImmutableList.of(TRINO_AUDIENCE));
        assertUICallWithCookie(token);
    }

    @Test
    public void testTokenWithMultipleAudiences()
            throws IOException
    {
        String token = hydraIdP.getToken(TRINO_CLIENT_ID, TRINO_CLIENT_SECRET, ImmutableList.of(TRINO_AUDIENCE, ADDITIONAL_AUDIENCE));
        assertUICallWithCookie(token);
    }

    @Test
    public void testSuccessfulFlow()
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

        // access UI and follow redirects in order to get OAuth2 cookie
        Response response = httpClient.newCall(
                new Request.Builder()
                        .url(serverUri + "/ui/")
                        .get()
                        .build())
                .execute();

        assertEquals(response.code(), SC_OK);
        assertEquals(response.request().url().toString(), uiUri.toString());
        Optional<HttpCookie> oauth2Cookie = cookieStore.get(uiUri)
                .stream()
                .filter(cookie -> cookie.getName().equals(OAUTH2_COOKIE))
                .findFirst();
        assertThat(oauth2Cookie).isNotEmpty();
        assertTrinoCookie(oauth2Cookie.get());
        assertUICallWithCookie(oauth2Cookie.get().getValue());
    }

    @Test
    public void testExpiredAccessToken()
            throws Exception
    {
        String token = hydraIdP.getToken(TRINO_CLIENT_ID, TRINO_CLIENT_SECRET, ImmutableList.of(TRINO_AUDIENCE));
        assertUICallWithCookie(token);
        Thread.sleep(TTL_ACCESS_TOKEN_IN_SECONDS.plusSeconds(1).toMillis()); // wait for the token expiration
        try (Response response = httpClientWithOAuth2Cookie(token).newCall(uiCall().build()).execute()) {
            assertRedirectResponse(response);
        }
    }

    private Request.Builder uiCall()
    {
        return new Request.Builder()
                .url(serverUri.resolve("/ui/").toString())
                .get();
    }

    private Request.Builder apiCall()
    {
        return new Request.Builder()
                .url(serverUri.resolve("/ui/api/cluster").toString())
                .get();
    }

    private void assertTrinoCookie(HttpCookie cookie)
    {
        assertThat(cookie.getName()).isEqualTo(OAUTH2_COOKIE);
        assertThat(cookie.getDomain()).isIn("127.0.0.1", "::1");
        assertThat(cookie.getPath()).isEqualTo("/ui/");
        assertThat(cookie.getSecure()).isTrue();
        assertThat(cookie.isHttpOnly()).isTrue();
        assertThat(cookie.getValue()).isNotBlank();
        Jws<Claims> jwt = Jwts.parser()
                .setSigningKeyResolver(new JwkSigningKeyResolver(new JwkService(
                        new JwtAuthenticatorConfig().setKeyFile("https://localhost:" + hydraIdP.getAuthPort() + "/.well-known/jwks.json"),
                        new JettyHttpClient(new HttpClientConfig()
                                .setTrustStorePath(Resources.getResource("cert/localhost.pem").getPath())))))
                .parseClaimsJws(cookie.getValue());
        assertThat(cookie.getMaxAge()).isLessThanOrEqualTo(TTL_ACCESS_TOKEN_IN_SECONDS.getSeconds());
        assertAccessToken(jwt);
    }

    private void assertAccessToken(Jws<Claims> jwt)
    {
        assertThat(jwt.getBody().getSubject()).isEqualTo("foo@bar.com");
        assertThat(jwt.getBody().get("client_id")).isEqualTo(TRINO_CLIENT_ID);
        assertThat(jwt.getBody().getIssuer()).isEqualTo("https://localhost:4444/");
    }

    private void assertUICallWithCookie(String cookieValue)
            throws IOException
    {
        OkHttpClient httpClient = httpClientWithOAuth2Cookie(cookieValue);
        // pass access token in Trino UI cookie
        try (Response response = httpClient.newCall(uiCall().build())
                .execute()) {
            assertThat(response.code()).isEqualTo(OK.getStatusCode());
        }
    }

    @SuppressWarnings("NullableProblems")
    private OkHttpClient httpClientWithOAuth2Cookie(String cookieValue)
    {
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();
        setupInsecureSsl(httpClientBuilder);
        httpClientBuilder.followRedirects(false);
        httpClientBuilder.cookieJar(new CookieJar()
        {
            @Override
            public void saveFromResponse(HttpUrl url, List<Cookie> cookies)
            {
            }

            @Override
            public List<Cookie> loadForRequest(HttpUrl url)
            {
                if (url.encodedPath().equals("/ui/")) {
                    return ImmutableList.of(new Cookie.Builder()
                            .domain(serverUri.getHost())
                            .path("/ui/")
                            .name(OAUTH2_COOKIE)
                            .value(cookieValue)
                            .httpOnly()
                            .secure()
                            .build());
                }
                return ImmutableList.of();
            }
        });
        return httpClientBuilder.build();
    }

    private void assertRedirectResponse(Response response)
            throws MalformedURLException
    {
        assertThat(response.code()).isEqualTo(SEE_OTHER.getStatusCode());
        assertRedirectUrl(response.header(LOCATION));
    }

    private void assertUnauthorizedResponse(Response response)
            throws IOException
    {
        assertThat(response.code()).isEqualTo(UNAUTHORIZED.getStatusCode());
        assertThat(response.body()).isNotNull();
        assertThat(response.body().string()).isEqualTo("Unauthorized");
    }

    private void assertRedirectUrl(String redirectUrl)
            throws MalformedURLException
    {
        assertThat(redirectUrl).isNotNull();
        URL location = new URL(redirectUrl);
        HttpUrl url = HttpUrl.parse(redirectUrl);
        assertThat(url).isNotNull();
        assertThat(location.getProtocol()).isEqualTo("https");
        assertThat(location.getHost()).isEqualTo("localhost");
        assertThat(location.getPort()).isEqualTo(hydraIdP.getAuthPort());
        assertThat(location.getPath()).isEqualTo("/oauth2/auth");
        assertThat(url.queryParameterValues("response_type")).isEqualTo(ImmutableList.of("code"));
        assertThat(url.queryParameterValues("scope")).isEqualTo(ImmutableList.of("openid"));
        assertThat(url.queryParameterValues("redirect_uri")).isEqualTo(ImmutableList.of(serverUri + "/oauth2/callback"));
        assertThat(url.queryParameterValues("client_id")).isEqualTo(ImmutableList.of(TRINO_CLIENT_ID));
        assertThat(url.queryParameterValues("state")).isNotNull();
    }
}
