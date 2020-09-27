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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.log.Logging;
import io.airlift.testing.Closeables;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.DefaultClaims;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.server.ui.WebUiModule;
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
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.UriBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.testing.Assertions.assertLessThan;
import static io.prestosql.client.OkHttpUtil.setupInsecureSsl;
import static io.prestosql.server.security.oauth2.TestingHydraService.TTL_ACCESS_TOKEN_IN_SECONDS;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static javax.ws.rs.core.HttpHeaders.LOCATION;
import static javax.ws.rs.core.HttpHeaders.WWW_AUTHENTICATE;
import static javax.ws.rs.core.Response.Status.FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.openqa.selenium.support.ui.ExpectedConditions.elementToBeClickable;

@Test(singleThreaded = true)
public class TestOAuth2Authenticator
{
    private static final int HTTPS_PORT = findAvailablePort();

    private final TestingHydraService testingHydraService = new TestingHydraService();
    private final OkHttpClient httpClient;

    private TestingPrestoServer server;
    private URI serverUri;

    public TestOAuth2Authenticator()
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

        createConsumer();

        server = TestingPrestoServer.builder()
                .setCoordinator(true)
                .setAdditionalModule(new WebUiModule())
                .setProperties(
                        ImmutableMap.<String, String>builder()
                                .put("web-ui.enabled", "true")
                                .put("web-ui.authentication.type", "oauth2")
                                .put("http-server.https.port", Integer.toString(HTTPS_PORT))
                                .put("http-server.https.enabled", "true")
                                .put("http-server.https.keystore.path", Resources.getResource("cert/localhost.pem").getPath())
                                .put("http-server.https.keystore.key", "")
                                .put("http-server.authentication.type", "oauth2")
                                .put("http-server.authentication.oauth2.server-url", "http://localhost:" + testingHydraService.getHydraPort())
                                .put("http-server.authentication.oauth2.auth-url", "http://hydra:4444/oauth2/auth")
                                .put("http-server.authentication.oauth2.token-url", format("http://localhost:%s/oauth2/token", testingHydraService.getHydraPort()))
                                .put("http-server.authentication.oauth2.client-id", "another-consumer")
                                .put("http-server.authentication.oauth2.client-secret", "consumer-secret")
                                .put("http-server.authentication.oauth2.user-mapping.pattern", "(.*)@.*")
                                .build())
                .build();
        waitForNodeRefresh(server);
        serverUri = server.getHttpsBaseUrl();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        Closeables.closeAll(server, testingHydraService);
    }

    @Test
    public void testUnauthorizedApiCall()
            throws IOException
    {
        try (Response response = httpClient
                .newCall(apiCall().build())
                .execute()) {
            assertUnauthorizedApiResponse(response);
        }
    }

    @Test
    public void testUnauthorizedUICall()
            throws IOException
    {
        try (Response response = httpClient
                .newCall(uiCall().build())
                .execute()) {
            assertThat(response.code()).isEqualTo(FOUND.getStatusCode());
            assertRedirectUrl(response.header(LOCATION));
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
                apiCall()
                        .header(AUTHORIZATION, "Bearer " + token)
                        .build())
                .execute()) {
            assertUnauthorizedApiResponse(response);
        }
    }

    @Test
    public void testSuccessfulFlow()
            throws Exception
    {
        withSuccessfulAuthentication((driver, wait) -> {
            assertPrestoCookie(driver.manage().getCookieNamed("Presto-OAuth2-Token"));
            String accessToken = driver.manage().getCookieNamed("Presto-OAuth2-Token").getValue();

            // pass access token in header
            assertThat(httpClient.newCall(
                    apiCall()
                            .header(AUTHORIZATION, "Bearer " + accessToken)
                            .build())
                    .execute()
                    .code())
                    .isEqualTo(OK.getStatusCode());

            // pass access token in query
            assertThat(httpClient.newCall(
                    apiCall()
                            .url(UriBuilder
                                    .fromUri(serverUri.resolve("/v1/query"))
                                    .queryParam("access_token", accessToken)
                                    .build()
                                    .toURL())
                            .build())
                    .execute()
                    .code())
                    .isEqualTo(OK.getStatusCode());
        });
    }

    @Test
    public void testExpiredAccessToken()
            throws Exception
    {
        withSuccessfulAuthentication(((driver, wait) -> {
            String accessToken = driver.manage().getCookieNamed("Presto-OAuth2-Token").getValue();
            Thread.sleep((TTL_ACCESS_TOKEN_IN_SECONDS + 1) * 1000L); // wait for the token expiration
            try (Response response = httpClient.newCall(
                    apiCall()
                            .header(AUTHORIZATION, "Bearer " + accessToken)
                            .build())
                    .execute()) {
                assertUnauthorizedApiResponse(response);
            }
        }));
    }

    private void createConsumer()
    {
        testingHydraService.createHydraContainer()
                .withCommand(format("clients create " +
                        "--endpoint http://hydra:4445 " +
                        "--id another-consumer " +
                        "--secret consumer-secret " +
                        "-g authorization_code,refresh_token " +
                        "-r token,code,id_token " +
                        "--scope openid,offline " +
                        "--callbacks https://host.testcontainers.internal:%d/oauth2/callback", HTTPS_PORT))
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(30)))
                .start();
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
                .url(serverUri.resolve("/v1/query/").toString())
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

    private void assertPrestoCookie(Cookie cookie)
    {
        assertThat(cookie.getName()).isEqualTo("Presto-OAuth2-Token");
        assertThat(cookie.getDomain()).isEqualTo("host.testcontainers.internal");
        assertThat(cookie.getPath()).isEqualTo("/ui/");
        assertThat(cookie.isSecure()).isTrue();
        assertThat(cookie.isHttpOnly()).isTrue();
        assertThat(cookie.getValue()).isNotBlank();
        Jws<Claims> jwt = Jwts.parser()
                .setSigningKeyResolver(new JWKSSigningKeyResolver("http://localhost:" + testingHydraService.getHydraPort()))
                .parseClaimsJws(cookie.getValue());
        // TODO: check source of the difference
        //        assertThat(cookie.getExpiry()).isEqualTo(jwt.getBody().getExpiration());
        assertAccessToken(jwt);
    }

    private void assertAccessToken(Jws<Claims> jwt)
    {
        assertThat(jwt.getBody().getSubject()).isEqualTo("foo@bar.com");
        assertThat(jwt.getBody().get("client_id")).isEqualTo("another-consumer");
        assertThat(jwt.getBody().getIssuer()).isEqualTo("http://hydra:4444/");
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

    private static void waitForNodeRefresh(TestingPrestoServer server)
            throws InterruptedException
    {
        Instant start = Instant.now();
        while (server.refreshNodes().getActiveNodes().size() < 1) {
            assertLessThan(Duration.between(start, Instant.now()), Duration.ofSeconds(10));
            MILLISECONDS.sleep(10);
        }
    }

    private static void assertUnauthorizedApiResponse(Response response)
            throws MalformedURLException
    {
        assertThat(response.code()).isEqualTo(UNAUTHORIZED.getStatusCode());
        String authenticateHeader = response.header(WWW_AUTHENTICATE);
        assertThat(authenticateHeader).isNotNull();
        Matcher authenticateHeaderMatcher = Pattern.compile("^Bearer realm=\"Presto\", authorizationUrl=\"(.*)\", status=\"STARTED\"$")
                .matcher(authenticateHeader);
        assertThat(authenticateHeaderMatcher.matches()).isTrue();
        assertRedirectUrl(authenticateHeaderMatcher.group(1));
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
