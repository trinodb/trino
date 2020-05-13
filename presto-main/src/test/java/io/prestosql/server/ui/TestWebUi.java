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
package io.prestosql.server.ui;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Key;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.node.NodeInfo;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.prestosql.server.security.PasswordAuthenticatorManager;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.BasicPrincipal;
import okhttp3.FormBody;
import okhttp3.JavaNetCookieJar;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.CookieManager;
import java.net.HttpCookie;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Principal;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.LOCATION;
import static com.google.common.net.HttpHeaders.X_FORWARDED_HOST;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PORT;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.testing.Closeables.closeQuietly;
import static io.prestosql.client.OkHttpUtil.setupSsl;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.servlet.http.HttpServletResponse.SC_SEE_OTHER;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

@Test
public class TestWebUi
{
    private static final String LOCALHOST_KEYSTORE = Resources.getResource("cert/localhost.pem").getPath();
    private static final ImmutableMap<String, String> SECURE_PROPERTIES = ImmutableMap.<String, String>builder()
            .put("http-server.https.enabled", "true")
            .put("http-server.https.keystore.path", LOCALHOST_KEYSTORE)
            .put("http-server.https.keystore.key", "")
            .put("http-server.process-forwarded", "true")
            .build();
    private static final String TEST_USER = "test-user";
    private static final String TEST_PASSWORD = "test-password";
    private static final String HMAC_KEY = Resources.getResource("hmac_key.txt").getPath();

    private TestingPrestoServer server;
    private OkHttpClient client;
    private URI insecureUrl;
    private URI secureUrl;

    @BeforeClass
    public void setup()
    {
        server = TestingPrestoServer.builder()
                .setProperties(SECURE_PROPERTIES)
                .build();
        server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticator(TestWebUi::authenticate);

        HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
        insecureUrl = httpServerInfo.getHttpUri();
        secureUrl = httpServerInfo.getHttpsUri();

        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
                .followRedirects(false);
        setupSsl(
                clientBuilder,
                Optional.empty(),
                Optional.empty(),
                Optional.of(LOCALHOST_KEYSTORE),
                Optional.empty());
        client = clientBuilder.build();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(server);
        server = null;
    }

    @Test
    public void testRootRedirect()
            throws Exception
    {
        testRootRedirect(insecureUrl);
        testRootRedirect(secureUrl);
    }

    private void testRootRedirect(URI baseUri)
            throws Exception
    {
        assertRedirect(client, uriBuilderFrom(baseUri).toString(), getUiLocation(baseUri));
    }

    @Test
    public void testLoggedOut()
            throws Exception
    {
        testLoggedOut(insecureUrl);
        testLoggedOut(secureUrl);
    }

    private void testLoggedOut(URI baseUri)
            throws Exception
    {
        assertRedirect(client, getUiLocation(baseUri), getLoginHtmlLocation(baseUri));

        assertRedirect(client, getLocation(baseUri, "/ui/query.html", "abc123"), getLocation(baseUri, "/ui/login.html", "/ui/query.html?abc123"), false);

        assertResponseCode(client, getValidApiLocation(baseUri), SC_UNAUTHORIZED);

        assertOk(client, getValidAssetsLocation(baseUri));

        assertOk(client, getValidVendorLocation(baseUri));
    }

    @Test
    public void testLogIn()
            throws Exception
    {
        testLogIn(insecureUrl);
        testLogIn(secureUrl);
    }

    private void testLogIn(URI baseUri)
            throws Exception
    {
        CookieManager cookieManager = new CookieManager();
        OkHttpClient client = this.client.newBuilder()
                .cookieJar(new JavaNetCookieJar(cookieManager))
                .build();

        Response response = assertOk(client, getLoginHtmlLocation(baseUri));
        String body = response.body().string();
        assertThat(body).contains("action=\"/ui/login\"");
        assertThat(body).contains("method=\"post\"");

        logIn(baseUri, client);
        HttpCookie cookie = getOnlyElement(cookieManager.getCookieStore().getCookies());
        assertEquals(cookie.getPath(), "/ui");
        assertEquals(cookie.getDomain(), baseUri.getHost());
        assertEquals(cookie.getMaxAge(), -1);
        assertTrue(cookie.isHttpOnly());

        assertOk(client, getUiLocation(baseUri));

        assertOk(client, getValidApiLocation(baseUri));

        assertResponseCode(client, getLocation(baseUri, "/ui/unknown"), SC_NOT_FOUND);

        assertResponseCode(client, getLocation(baseUri, "/ui/api/unknown"), SC_NOT_FOUND);
        assertRedirect(client, getLogoutLocation(baseUri), getLoginHtmlLocation(baseUri), false);
        assertThat(cookieManager.getCookieStore().getCookies()).isEmpty();
    }

    @Test
    public void testFailedLogin()
            throws Exception
    {
        testFailedLogin(insecureUrl, Optional.empty(), Optional.empty());
        testFailedLogin(insecureUrl, Optional.empty(), Optional.of(TEST_PASSWORD));
        testFailedLogin(insecureUrl, Optional.empty(), Optional.of("unknown"));

        testFailedLogin(secureUrl, Optional.empty(), Optional.empty());
        testFailedLogin(secureUrl, Optional.empty(), Optional.of(TEST_PASSWORD));
        testFailedLogin(secureUrl, Optional.empty(), Optional.of("unknown"));

        testFailedLogin(secureUrl, Optional.of(TEST_USER), Optional.of("unknown"));
        testFailedLogin(secureUrl, Optional.of("unknown"), Optional.of(TEST_PASSWORD));
        testFailedLogin(secureUrl, Optional.of(TEST_USER), Optional.empty());
        testFailedLogin(secureUrl, Optional.empty(), Optional.of(TEST_PASSWORD));
    }

    private void testFailedLogin(URI httpsUrl, Optional<String> username, Optional<String> password)
            throws IOException
    {
        CookieManager cookieManager = new CookieManager();
        OkHttpClient client = this.client.newBuilder()
                .cookieJar(new JavaNetCookieJar(cookieManager))
                .build();

        FormBody.Builder formData = new FormBody.Builder();
        username.ifPresent(value -> formData.add("username", value));
        password.ifPresent(value -> formData.add("password", value));
        Request request = new Request.Builder()
                .url(getLoginLocation(httpsUrl))
                .post(formData.build())
                .build();
        Response response = client.newCall(request).execute();
        assertEquals(response.code(), SC_SEE_OTHER);
        assertEquals(response.header(LOCATION), getLoginHtmlLocation(httpsUrl));
        assertTrue(cookieManager.getCookieStore().getCookies().isEmpty());
    }

    @Test
    public void testWorkerResource()
            throws Exception
    {
        testWorkerResource(insecureUrl);
        testWorkerResource(secureUrl);
    }

    private void testWorkerResource(URI baseUri)
            throws Exception
    {
        OkHttpClient client = this.client.newBuilder()
                .cookieJar(new JavaNetCookieJar(new CookieManager()))
                .build();
        logIn(baseUri, client);

        String nodeId = server.getInstance(Key.get(NodeInfo.class)).getNodeId();
        assertOk(client, getLocation(baseUri, "/ui/api/worker/" + nodeId + "/status"));

        assertOk(client, getLocation(baseUri, "/ui/api/worker/" + nodeId + "/thread"));
    }

    private static void logIn(URI baseUri, OkHttpClient client)
            throws IOException
    {
        FormBody.Builder formData = new FormBody.Builder()
                .add("username", TEST_USER);
        if (baseUri.getScheme().equals("https")) {
            formData.add("password", TEST_PASSWORD);
        }

        Request request = new Request.Builder()
                .url(getLoginLocation(baseUri))
                .post(formData.build())
                .build();
        Response response = client.newCall(request).execute();
        assertEquals(response.code(), SC_SEE_OTHER);
        assertEquals(response.header(LOCATION), getUiLocation(baseUri));
    }

    @Test
    public void testDisabled()
            throws Exception
    {
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("web-ui.enabled", "false")
                        .build())
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticator(TestWebUi::authenticate);

            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            testDisabled(httpServerInfo.getHttpUri());
            testDisabled(httpServerInfo.getHttpsUri());
        }
    }

    private void testDisabled(URI baseUri)
            throws Exception
    {
        assertRedirect(client, getUiLocation(baseUri), getDisabledLocation(baseUri));

        assertRedirect(client, getLocation(baseUri, "/ui/query.html", "abc123"), getDisabledLocation(baseUri));

        assertResponseCode(client, getValidApiLocation(baseUri), SC_UNAUTHORIZED);

        assertRedirect(client, getLoginLocation(baseUri), getDisabledLocation(baseUri));

        assertRedirect(client, getLogoutLocation(baseUri), getDisabledLocation(baseUri));

        assertOk(client, getValidAssetsLocation(baseUri));

        assertOk(client, getValidVendorLocation(baseUri));
    }

    @Test
    public void testNoPasswordAuthenticator()
            throws Exception
    {
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(SECURE_PROPERTIES)
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            testLogIn(httpServerInfo.getHttpUri());
            testNoPasswordAuthenticator(httpServerInfo.getHttpsUri());
        }
    }

    private void testNoPasswordAuthenticator(URI baseUri)
            throws Exception
    {
        assertRedirect(client, getUiLocation(baseUri), getDisabledLocation(baseUri), false);

        assertRedirect(client, getLocation(baseUri, "/ui/query.html", "abc123"), getDisabledLocation(baseUri), false);

        assertResponseCode(client, getValidApiLocation(baseUri), SC_UNAUTHORIZED);

        assertRedirect(client, getLoginLocation(baseUri), getDisabledLocation(baseUri), false);

        assertRedirect(client, getLogoutLocation(baseUri), getDisabledLocation(baseUri), false);

        assertOk(client, getValidAssetsLocation(baseUri));

        assertOk(client, getValidVendorLocation(baseUri));
    }

    @Test
    public void testFixedAuthenticator()
            throws Exception
    {
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("web-ui.authentication.type", "fixed")
                        .put("web-ui.user", "test-user")
                        .build())
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            testAlwaysAuthorized(httpServerInfo.getHttpUri(), client);
            testAlwaysAuthorized(httpServerInfo.getHttpsUri(), client);

            testFixedAuthenticator(httpServerInfo.getHttpUri());
            testFixedAuthenticator(httpServerInfo.getHttpsUri());
        }
    }

    private void testFixedAuthenticator(URI baseUri)
            throws Exception
    {
        assertOk(client, getUiLocation(baseUri));

        assertOk(client, getValidApiLocation(baseUri));

        assertResponseCode(client, getLocation(baseUri, "/ui/unknown"), SC_NOT_FOUND);

        assertResponseCode(client, getLocation(baseUri, "/ui/api/unknown"), SC_NOT_FOUND);
    }

    @Test
    public void testCertAuthenticator()
            throws Exception
    {
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("web-ui.authentication.type", "certificate")
                        .put("http-server.https.truststore.path", LOCALHOST_KEYSTORE)
                        .put("http-server.https.truststore.key", "")
                        .build())
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            testLogIn(httpServerInfo.getHttpUri());

            testNeverAuthorized(httpServerInfo.getHttpsUri(), client);

            OkHttpClient.Builder clientBuilder = client.newBuilder();
            setupSsl(
                    clientBuilder,
                    Optional.of(LOCALHOST_KEYSTORE),
                    Optional.empty(),
                    Optional.of(LOCALHOST_KEYSTORE),
                    Optional.empty());
            OkHttpClient clientWithCert = clientBuilder.build();
            testAlwaysAuthorized(httpServerInfo.getHttpsUri(), clientWithCert);
        }
    }

    @Test
    public void testJwtAuthenticator()
            throws Exception
    {
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("web-ui.authentication.type", "jwt")
                        .put("http-server.authentication.jwt.key-file", HMAC_KEY)
                        .build())
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            testLogIn(httpServerInfo.getHttpUri());

            testNeverAuthorized(httpServerInfo.getHttpsUri(), client);

            String hmac = new String(Files.readAllBytes(Paths.get(HMAC_KEY)), UTF_8);
            String token = Jwts.builder()
                    .signWith(SignatureAlgorithm.HS256, hmac)
                    .setSubject("test-user")
                    .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                    .compact();

            OkHttpClient clientWithJwt = client.newBuilder()
                    .authenticator((route, response) -> response.request().newBuilder()
                            .header(AUTHORIZATION, "Bearer " + token)
                            .build())
                    .build();
            testAlwaysAuthorized(httpServerInfo.getHttpsUri(), clientWithJwt);
        }
    }

    private static void testAlwaysAuthorized(URI baseUri, OkHttpClient authorizedClient)
            throws IOException
    {
        assertOk(authorizedClient, getUiLocation(baseUri));

        assertOk(authorizedClient, getValidApiLocation(baseUri));

        assertRedirect(authorizedClient, getLoginHtmlLocation(baseUri), getUiLocation(baseUri), false);

        assertRedirect(authorizedClient, getLoginLocation(baseUri), getUiLocation(baseUri), false);

        assertRedirect(authorizedClient, getLogoutLocation(baseUri), getUiLocation(baseUri), false);

        assertResponseCode(authorizedClient, getLocation(baseUri, "/ui/unknown"), SC_NOT_FOUND);

        assertResponseCode(authorizedClient, getLocation(baseUri, "/ui/api/unknown"), SC_NOT_FOUND);
    }

    private static void testNeverAuthorized(URI baseUri, OkHttpClient notAuthorizedClient)
            throws IOException
    {
        assertResponseCode(notAuthorizedClient, getUiLocation(baseUri), SC_UNAUTHORIZED);
        assertResponseCode(notAuthorizedClient, getValidApiLocation(baseUri), SC_UNAUTHORIZED);
        assertResponseCode(notAuthorizedClient, getLoginLocation(baseUri), SC_UNAUTHORIZED);
        assertResponseCode(notAuthorizedClient, getLogoutLocation(baseUri), SC_UNAUTHORIZED);
        assertResponseCode(notAuthorizedClient, getLocation(baseUri, "/ui/unknown"), SC_UNAUTHORIZED);
        assertResponseCode(notAuthorizedClient, getLocation(baseUri, "/ui/api/unknown"), SC_UNAUTHORIZED);
    }

    private static Response assertOk(OkHttpClient client, String url)
            throws IOException
    {
        return assertResponseCode(client, url, SC_OK);
    }

    private static void assertRedirect(OkHttpClient client, String url, String redirectLocation)
            throws IOException
    {
        assertRedirect(client, url, redirectLocation, true);
    }

    private static void assertRedirect(OkHttpClient client, String url, String redirectLocation, boolean testProxy)
            throws IOException
    {
        Request request = new Request.Builder()
                .url(url)
                .build();
        try (Response response = client.newCall(request).execute()) {
            assertEquals(response.code(), SC_SEE_OTHER);
            assertEquals(response.header(LOCATION), redirectLocation);
        }

        if (testProxy) {
            request = new Request.Builder()
                    .url(url)
                    .header(X_FORWARDED_PROTO, "test")
                    .header(X_FORWARDED_HOST, "my-load-balancer.local")
                    .header(X_FORWARDED_PORT, "123")
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), SC_SEE_OTHER);
                assertEquals(
                        response.header(LOCATION),
                        uriBuilderFrom(URI.create(redirectLocation))
                                .scheme("test")
                                .host("my-load-balancer.local")
                                .port(123)
                                .toString());
            }

            request = new Request.Builder()
                    .url(url)
                    .header(X_FORWARDED_PROTO, "test")
                    .header(X_FORWARDED_HOST, "my-load-balancer.local:123")
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), SC_SEE_OTHER);
                assertEquals(
                        response.header(LOCATION),
                        uriBuilderFrom(URI.create(redirectLocation))
                                .scheme("test")
                                .host("my-load-balancer.local")
                                .port(123)
                                .toString());
            }

            request = new Request.Builder()
                    .url(url)
                    .header(X_FORWARDED_PROTO, "test")
                    .header(X_FORWARDED_PORT, "123")
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), SC_SEE_OTHER);
                assertEquals(
                        response.header(LOCATION),
                        uriBuilderFrom(URI.create(redirectLocation))
                                .scheme("test")
                                .port(123)
                                .toString());
            }

            request = new Request.Builder()
                    .url(url)
                    .header(X_FORWARDED_PROTO, "test")
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), SC_SEE_OTHER);
                assertEquals(
                        response.header(LOCATION),
                        uriBuilderFrom(URI.create(redirectLocation))
                                .scheme("test")
                                .toString());
            }
            request = new Request.Builder()
                    .url(url)
                    .header(X_FORWARDED_HOST, "my-load-balancer.local")
                    .header(X_FORWARDED_PORT, "123")
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), SC_SEE_OTHER);
                assertEquals(
                        response.header(LOCATION),
                        uriBuilderFrom(URI.create(redirectLocation))
                                .host("my-load-balancer.local")
                                .port(123)
                                .toString());
            }

            request = new Request.Builder()
                    .url(url)
                    .header(X_FORWARDED_HOST, "my-load-balancer.local:123")
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), SC_SEE_OTHER);
                assertEquals(
                        response.header(LOCATION),
                        uriBuilderFrom(URI.create(redirectLocation))
                                .host("my-load-balancer.local")
                                .port(123)
                                .toString());
            }

            request = new Request.Builder()
                    .url(url)
                    .header(X_FORWARDED_HOST, "my-load-balancer.local")
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), SC_SEE_OTHER);
                assertEquals(
                        response.header(LOCATION),
                        uriBuilderFrom(URI.create(redirectLocation))
                                .host("my-load-balancer.local")
                                .defaultPort()
                                .toString());
            }
        }
    }

    private static Response assertResponseCode(OkHttpClient client, String url, int expectedCode)
            throws IOException
    {
        Request request = new Request.Builder()
                .url(url)
                .build();
        Response response = client.newCall(request).execute();
        assertEquals(response.code(), expectedCode);
        return response;
    }

    private static Principal authenticate(String user, String password)
    {
        if (TEST_USER.equals(user) && TEST_PASSWORD.equals(password)) {
            return new BasicPrincipal(user);
        }
        throw new AccessDeniedException("Invalid credentials");
    }

    private static String getUiLocation(URI baseUri)
    {
        return getLocation(baseUri, "/ui/");
    }

    private static String getLoginHtmlLocation(URI baseUri)
    {
        return getLocation(baseUri, "/ui/login.html");
    }

    private static String getLoginLocation(URI httpsUrl)
    {
        return getLocation(httpsUrl, "/ui/login");
    }

    private static String getLogoutLocation(URI baseUri)
    {
        return getLocation(baseUri, "/ui/logout");
    }

    private static String getDisabledLocation(URI baseUri)
    {
        return getLocation(baseUri, "/ui/disabled.html");
    }

    private static String getValidApiLocation(URI baseUri)
    {
        return getLocation(baseUri, "/ui/api/cluster");
    }

    private static String getValidAssetsLocation(URI baseUri)
    {
        return getLocation(baseUri, "/ui/assets/favicon.ico");
    }

    private static String getValidVendorLocation(URI baseUri)
    {
        return getLocation(baseUri, "/ui/vendor/bootstrap/css/bootstrap.css");
    }

    private static String getLocation(URI baseUri, String path)
    {
        return uriBuilderFrom(baseUri).replacePath(path).toString();
    }

    private static String getLocation(URI baseUri, String path, String query)
    {
        return uriBuilderFrom(baseUri).replacePath(path).replaceParameter(query).toString();
    }
}
