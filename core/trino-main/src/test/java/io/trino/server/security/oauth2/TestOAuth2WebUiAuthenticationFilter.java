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
import io.airlift.http.client.jetty.JettyHttpClient;
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
import io.trino.server.ui.WebUiModule;
import io.trino.testng.services.Flaky;
import okhttp3.CookieJar;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.openqa.selenium.By;
import org.openqa.selenium.Cookie;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.BrowserWebDriverContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static io.airlift.testing.Assertions.assertLessThan;
import static io.trino.client.OkHttpUtil.setupInsecureSsl;
import static io.trino.server.security.oauth2.TestingHydraService.TTL_ACCESS_TOKEN_IN_SECONDS;
import static io.trino.server.ui.OAuthWebUiCookie.OAUTH2_COOKIE;
import static java.lang.String.format;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static javax.ws.rs.core.HttpHeaders.LOCATION;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.SEE_OTHER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.openqa.selenium.support.ui.ExpectedConditions.elementToBeClickable;

@Test(singleThreaded = true)
public class TestOAuth2WebUiAuthenticationFilter
{
    private static final int HTTPS_PORT = findAvailablePort();

    private final TestingHydraService testingHydraService = new TestingHydraService();
    private final OkHttpClient httpClient;

    private TestingTrinoServer server;
    private URI serverUri;

    public TestOAuth2WebUiAuthenticationFilter()
    {
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();
        setupInsecureSsl(httpClientBuilder);
        httpClientBuilder.followRedirects(false);
        this.httpClient = httpClientBuilder.build();
    }

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();

        Testcontainers.exposeHostPorts(HTTPS_PORT);
        testingHydraService.start();
        testingHydraService.createConsumer(format("https://host.testcontainers.internal:%d/oauth2/callback", HTTPS_PORT));

        server = TestingTrinoServer.builder()
                .setCoordinator(true)
                .setAdditionalModule(new WebUiModule())
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("web-ui.enabled", "true")
                        .put("web-ui.authentication.type", "oauth2")
                        .put("http-server.https.port", Integer.toString(HTTPS_PORT))
                        .put("http-server.https.enabled", "true")
                        .put("http-server.https.keystore.path", Resources.getResource("cert/localhost.pem").getPath())
                        .put("http-server.https.keystore.key", "")
                        .put("http-server.authentication.oauth2.auth-url", "http://hydra:4444/oauth2/auth")
                        .put("http-server.authentication.oauth2.token-url", format("http://localhost:%s/oauth2/token", testingHydraService.getHydraPort()))
                        .put("http-server.authentication.oauth2.jwks-url", format("http://localhost:%s/.well-known/jwks.json", testingHydraService.getHydraPort()))
                        .put("http-server.authentication.oauth2.client-id", "another-consumer")
                        .put("http-server.authentication.oauth2.client-secret", "consumer-secret")
                        .put("http-server.authentication.oauth2.user-mapping.pattern", "(.*)@.*")
                        .build())
                .build();
        server.waitForNodeRefresh(Duration.ofSeconds(10));
        serverUri = server.getHttpsBaseUrl();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        Closeables.closeAll(server, testingHydraService);
    }

    @Test
    public void testUnauthorizedApiCall()
            throws IOException
    {
        try (Response response = httpClient
                .newCall(uiCall().build())
                .execute()) {
            assertUnauthorizedUICall(response);
        }
    }

    @Test
    public void testUnauthorizedUICall()
            throws IOException
    {
        try (Response response = httpClient
                .newCall(uiCall().build())
                .execute()) {
            assertUnauthorizedUICall(response);
        }
    }

    @Test
    public void testInvalidToken()
            throws NoSuchAlgorithmException, IOException
    {
        KeyPairGenerator keyGenerator = KeyPairGenerator.getInstance("RSA");
        SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.RS256;
        keyGenerator.initialize(4096);
        String token = Jwts.builder()
                .setHeaderParam("alg", "RS256")
                .setHeaderParam("kid", "public:f467aa08-1c1b-4cde-ba45-84b0ef5d2ba8")
                .setHeaderParam("typ", "JWT")
                .setClaims(
                        new DefaultClaims(
                                ImmutableMap.<String, Object>builder()
                                        .put("aud", ImmutableList.of())
                                        .put("client_id", "another-consumer")
                                        .put("exp", System.currentTimeMillis() / 1000L + 60L)
                                        .put("iat", System.currentTimeMillis())
                                        .put("iss", "http://hydra:4444/")
                                        .put("jti", UUID.randomUUID())
                                        .put("nbf", System.currentTimeMillis() - 60L)
                                        .put("scp", ImmutableList.of("openid"))
                                        .put("sub", "foo@bar.com")
                                        .build()))
                .signWith(signatureAlgorithm, keyGenerator.generateKeyPair().getPrivate())
                .compact();

        try (Response response = httpClient.newCall(
                uiCall()
                        .header(AUTHORIZATION, "Bearer " + token)
                        .build())
                .execute()) {
            assertUnauthorizedUICall(response);
        }
    }

    @Test
    public void testSuccessfulFlow()
            throws Exception
    {
        withSuccessfulAuthentication((driver, wait) -> {
            Cookie cookie = driver.manage().getCookieNamed(OAUTH2_COOKIE);
            assertTrinoCookie(cookie);
            assertUICallWithCookie(cookie);
        });
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/6223", match = OAUTH2_COOKIE + " is missing")
    public void testExpiredAccessToken()
            throws Exception
    {
        withSuccessfulAuthentication(((driver, wait) -> {
            Cookie cookie = driver.manage().getCookieNamed(OAUTH2_COOKIE);
            assertThat(cookie).withFailMessage(OAUTH2_COOKIE + " is missing").isNotNull();
            Thread.sleep((TTL_ACCESS_TOKEN_IN_SECONDS + 1) * 1000L); // wait for the token expiration
            try (Response response = httpClient.newCall(
                    uiCall()
                            .header(AUTHORIZATION, "Bearer " + cookie.getValue())
                            .build())
                    .execute()) {
                assertUnauthorizedUICall(response);
            }
        }));
    }

    private Request.Builder uiCall()
    {
        return new Request.Builder()
                .url(serverUri.resolve("/ui/").toString())
                .get();
    }

    private void withSuccessfulAuthentication(AuthenticationAssertion assertion)
            throws Exception
    {
        try (BrowserWebDriverContainer<?> browser = createChromeContainer()) {
            WebDriver driver = browser.getWebDriver();
            driver.get(format("https://host.testcontainers.internal:%d/ui/", HTTPS_PORT));
            WebDriverWait wait = new WebDriverWait(driver, 5);
            submitCredentials(driver, "foo@bar.com", "foobar", wait);
            giveConsent(driver, wait);
            wait.until(ExpectedConditions.urlMatches(format("https://host.testcontainers.internal:%d/ui/", HTTPS_PORT)));

            assertion.assertWith(driver, wait);
        }
    }

    private BrowserWebDriverContainer<?> createChromeContainer()
    {
        ChromeOptions options = new ChromeOptions();
        options.setAcceptInsecureCerts(true);
        BrowserWebDriverContainer<?> chromeContainer = new BrowserWebDriverContainer<>()
                .withNetwork(testingHydraService.getNetwork())
                .withCapabilities(options);
        chromeContainer.start();
        return chromeContainer;
    }

    private void submitCredentials(WebDriver driver, String email, String password, WebDriverWait wait)
    {
        By emailElementLocator = By.id("email");
        wait.until(elementToBeClickable(emailElementLocator));
        WebElement usernameElement = driver.findElement(emailElementLocator);
        usernameElement.sendKeys(email);
        By passwordElementLocator = By.id("password");
        wait.until(elementToBeClickable(passwordElementLocator));
        WebElement passwordElement = driver.findElement(passwordElementLocator);
        passwordElement.sendKeys(password + "\n");
    }

    private void giveConsent(WebDriver driver, WebDriverWait wait)
    {
        By openIdCheckboxLocator = By.id("openid");
        wait.until(elementToBeClickable(openIdCheckboxLocator));
        WebElement openIdCheckbox = driver.findElement(openIdCheckboxLocator);
        openIdCheckbox.click();
        By acceptButtonLocator = By.id("accept");
        wait.until(elementToBeClickable(acceptButtonLocator));
        WebElement acceptButton = driver.findElement(acceptButtonLocator);
        acceptButton.click();
    }

    private void assertTrinoCookie(Cookie cookie)
    {
        assertThat(cookie.getName()).isEqualTo(OAUTH2_COOKIE);
        assertThat(cookie.getDomain()).isEqualTo("host.testcontainers.internal");
        assertThat(cookie.getPath()).isEqualTo("/ui/");
        assertThat(cookie.isSecure()).isTrue();
        assertThat(cookie.isHttpOnly()).isTrue();
        assertThat(cookie.getValue()).isNotBlank();
        Jws<Claims> jwt = Jwts.parser()
                .setSigningKeyResolver(new JwkSigningKeyResolver(new JwkService(
                        new JwtAuthenticatorConfig().setKeyFile("http://localhost:" + testingHydraService.getHydraPort() + "/.well-known/jwks.json"),
                        new JettyHttpClient())))
                .parseClaimsJws(cookie.getValue());
        assertLessThan(Duration.between(cookie.getExpiry().toInstant(), jwt.getBody().getExpiration().toInstant()), Duration.ofSeconds(5));
        assertAccessToken(jwt);
    }

    private void assertAccessToken(Jws<Claims> jwt)
    {
        assertThat(jwt.getBody().getSubject()).isEqualTo("foo@bar.com");
        assertThat(jwt.getBody().get("client_id")).isEqualTo("another-consumer");
        assertThat(jwt.getBody().getIssuer()).isEqualTo("http://hydra:4444/");
    }

    private void assertUICallWithCookie(Cookie cookie)
            throws IOException
    {
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();
        setupInsecureSsl(httpClientBuilder);
        httpClientBuilder.followRedirects(false);
        httpClientBuilder.cookieJar(new CookieJar()
        {
            @Override
            public void saveFromResponse(HttpUrl url, List<okhttp3.Cookie> cookies)
            {
            }

            @Override
            public List<okhttp3.Cookie> loadForRequest(HttpUrl url)
            {
                return ImmutableList.of(new okhttp3.Cookie.Builder()
                        .domain("localhost")
                        .path("/ui/")
                        .name(OAUTH2_COOKIE)
                        .value(cookie.getValue())
                        .httpOnly()
                        .secure()
                        .build());
            }
        });

        // pass access token in Presto UI cookie
        assertThat(httpClientBuilder.build().newCall(uiCall().build())
                .execute()
                .code())
                .isEqualTo(OK.getStatusCode());
    }

    private static int findAvailablePort()
    {
        try (ServerSocket tempSocket = new ServerSocket(0)) {
            return tempSocket.getLocalPort();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void assertUnauthorizedUICall(Response response)
            throws MalformedURLException
    {
        assertThat(response.code()).isEqualTo(SEE_OTHER.getStatusCode());
        assertRedirectUrl(response.header(LOCATION));
    }

    private static void assertRedirectUrl(String redirectUrl)
            throws MalformedURLException
    {
        assertThat(redirectUrl).isNotNull();
        URL location = new URL(redirectUrl);
        HttpUrl url = HttpUrl.parse(redirectUrl);
        assertThat(url).isNotNull();
        assertThat(location.getProtocol()).isEqualTo("http");
        assertThat(location.getHost()).isEqualTo("hydra");
        assertThat(location.getPort()).isEqualTo(4444);
        assertThat(location.getPath()).isEqualTo("/oauth2/auth");
        assertThat(url.queryParameterValues("response_type")).isEqualTo(ImmutableList.of("code"));
        assertThat(url.queryParameterValues("scope")).isEqualTo(ImmutableList.of("openid"));
        assertThat(url.queryParameterValues("redirect_uri")).isEqualTo(ImmutableList.of(format("https://127.0.0.1:%s/oauth2/callback", HTTPS_PORT)));
        assertThat(url.queryParameterValues("client_id")).isEqualTo(ImmutableList.of("another-consumer"));
        assertThat(url.queryParameterValues("state")).isNotNull();
    }

    private interface AuthenticationAssertion
    {
        void assertWith(WebDriver driver, WebDriverWait wait)
                throws Exception;
    }
}
