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
package io.trino.server.security;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import io.airlift.security.pem.PemReader;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.JwtBuilder;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.security.AccessControl;
import io.trino.security.AccessControlManager;
import io.trino.server.HttpRequestSessionContextFactory;
import io.trino.server.ProtocolConfig;
import io.trino.server.protocol.PreparedStatementEncoder;
import io.trino.server.security.oauth2.OAuth2Client;
import io.trino.server.security.oauth2.OAuthWebUiCookieTestUtils;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import okhttp3.Cookie;
import okhttp3.CookieJar;
import okhttp3.Credentials;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.JavaNetCookieJar;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import javax.inject.Inject;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;

import java.io.File;
import java.io.IOException;
import java.net.CookieManager;
import java.net.CookieStore;
import java.net.HttpCookie;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.security.PrivateKey;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.jsonwebtoken.security.Keys.hmacShaKeyFor;
import static io.trino.client.OkHttpUtil.setupSsl;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static io.trino.server.security.jwt.JwtUtil.newJwtBuilder;
import static io.trino.server.security.oauth2.OAuth2Service.NONCE;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOCATION;
import static io.trino.server.ui.OAuthRefreshWebUiCookie.OAUTH2_REFRESH_COOKIE;
import static io.trino.server.ui.OAuthWebUiCookie.OAUTH2_COOKIE;
import static io.trino.spi.security.AccessDeniedException.denyImpersonateUser;
import static io.trino.spi.security.AccessDeniedException.denyReadSystemInformationAccess;
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.servlet.http.HttpServletResponse.SC_SEE_OTHER;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
import static javax.ws.rs.core.HttpHeaders.LOCATION;
import static javax.ws.rs.core.HttpHeaders.SET_COOKIE;
import static javax.ws.rs.core.HttpHeaders.WWW_AUTHENTICATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestResourceSecurity
{
    private static final String LOCALHOST_KEYSTORE = Resources.getResource("cert/localhost.pem").getPath();
    private static final String ALLOWED_USER_MAPPING_PATTERN = "(.*)@allowed";
    private static final ImmutableMap<String, String> SECURE_PROPERTIES = ImmutableMap.<String, String>builder()
            .put("http-server.https.enabled", "true")
            .put("http-server.https.keystore.path", LOCALHOST_KEYSTORE)
            .put("http-server.https.keystore.key", "")
            .put("http-server.process-forwarded", "true")
            .put("http-server.authentication.insecure.user-mapping.pattern", ALLOWED_USER_MAPPING_PATTERN)
            .buildOrThrow();
    private static final String TEST_USER = "test-user";
    private static final String TEST_USER_LOGIN = TEST_USER + "@allowed";
    private static final String TEST_PASSWORD = "test-password";
    private static final String TEST_PASSWORD2 = "test-password-2";
    private static final String MANAGEMENT_USER = "management-user";
    private static final String MANAGEMENT_USER_LOGIN = MANAGEMENT_USER + "@allowed";
    private static final String MANAGEMENT_PASSWORD = "management-password";
    private static final String HMAC_KEY = Resources.getResource("hmac_key.txt").getPath();
    private static final String JWK_KEY_ID = "test-rsa";
    private static final String GROUPS_CLAIM = "groups";
    private static final PrivateKey JWK_PRIVATE_KEY;
    private static final ObjectMapper json = new ObjectMapper();

    static {
        try {
            JWK_PRIVATE_KEY = PemReader.loadPrivateKey(new File(Resources.getResource("jwk/jwk-rsa-private.pem").getPath()), Optional.empty());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private OkHttpClient client;
    private Path passwordConfigDummy;

    @BeforeClass
    public void setup()
            throws IOException
    {
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
                .followRedirects(false);
        setupSsl(
                clientBuilder,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(LOCALHOST_KEYSTORE),
                Optional.empty(),
                Optional.empty());
        client = clientBuilder.build();

        passwordConfigDummy = Files.createTempFile("passwordConfigDummy", "");
        passwordConfigDummy.toFile().deleteOnExit();
    }

    @Test
    public void testInsecureAuthenticatorHttp()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.authentication.insecure.user-mapping.pattern", ALLOWED_USER_MAPPING_PATTERN)
                        .buildOrThrow())
                .build()) {
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertInsecureAuthentication(httpServerInfo.getHttpUri());
        }
    }

    @Test
    public void testInsecureAuthenticatorHttps()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(SECURE_PROPERTIES)
                .build()) {
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertInsecureAuthentication(httpServerInfo.getHttpUri());
            assertInsecureAuthentication(httpServerInfo.getHttpsUri());
        }
    }

    @Test
    public void testInsecureAuthenticatorHttpsOnly()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.allow-insecure-over-http", "false")
                        .buildOrThrow())
                .build()) {
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertAuthenticationDisabled(httpServerInfo.getHttpUri());
            assertInsecureAuthentication(httpServerInfo.getHttpsUri());
        }
    }

    @Test
    public void testPasswordAuthenticator()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("password-authenticator.config-files", passwordConfigDummy.toString())
                        .put("http-server.authentication.type", "password")
                        .put("http-server.authentication.password.user-mapping.pattern", ALLOWED_USER_MAPPING_PATTERN)
                        .buildOrThrow())
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate);
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertAuthenticationDisabled(httpServerInfo.getHttpUri());
            assertPasswordAuthentication(httpServerInfo.getHttpsUri());
        }
    }

    @Test
    public void testMultiplePasswordAuthenticators()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("password-authenticator.config-files", passwordConfigDummy.toString())
                        .put("http-server.authentication.type", "password")
                        .put("http-server.authentication.password.user-mapping.pattern", ALLOWED_USER_MAPPING_PATTERN)
                        .buildOrThrow())
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate, TestResourceSecurity::authenticate2);
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertAuthenticationDisabled(httpServerInfo.getHttpUri());
            assertPasswordAuthentication(httpServerInfo.getHttpsUri(), TEST_PASSWORD, TEST_PASSWORD2);
        }
    }

    @Test
    public void testMultiplePasswordAuthenticatorsMessages()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("password-authenticator.config-files", passwordConfigDummy.toString())
                        .put("http-server.authentication.type", "password")
                        .put("http-server.authentication.password.user-mapping.pattern", ALLOWED_USER_MAPPING_PATTERN)
                        .buildOrThrow())
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate, TestResourceSecurity::authenticate2);
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            Request request = new Request.Builder()
                    .url(getAuthorizedUserLocation(httpServerInfo.getHttpsUri()))
                    .headers(Headers.of("Authorization", Credentials.basic(TEST_USER_LOGIN, "wrong_password")))
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertThat(response.message()).isEqualTo("Access Denied: Invalid credentials | Access Denied: Invalid credentials2");
            }
        }
    }

    @Test
    public void testPasswordAuthenticatorUserMapping()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("password-authenticator.config-files", passwordConfigDummy.toString())
                        .put("http-server.authentication.type", "password")
                        .put("http-server.authentication.password.user-mapping.pattern", ALLOWED_USER_MAPPING_PATTERN)
                        .buildOrThrow())
                .setAdditionalModule(binder -> jaxrsBinder(binder).bind(TestResource.class))
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate);
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            // Test sets basic auth user and X-Trino-User, and the authenticator is performing user mapping.
            // Normally this would result in an impersonation check to the X-Trino-User, but the password
            // authenticator has a hack to clear X-Trino-User in this case.
            Request request = new Request.Builder()
                    .url(getLocation(httpServerInfo.getHttpsUri(), "/protocol/identity"))
                    .addHeader("Authorization", Credentials.basic(TEST_USER_LOGIN, TEST_PASSWORD))
                    .addHeader("X-Trino-User", TEST_USER_LOGIN)
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), SC_OK);
                assertEquals(response.header("user"), TEST_USER);
                assertEquals(response.header("principal"), TEST_USER_LOGIN);
            }
        }
    }

    @Test
    public void testPasswordAuthenticatorWithInsecureHttp()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("password-authenticator.config-files", passwordConfigDummy.toString())
                        .put("http-server.authentication.type", "password")
                        .put("http-server.authentication.allow-insecure-over-http", "true")
                        .put("http-server.authentication.password.user-mapping.pattern", ALLOWED_USER_MAPPING_PATTERN)
                        .buildOrThrow())
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate);
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertInsecureAuthentication(httpServerInfo.getHttpUri());
            assertPasswordAuthentication(httpServerInfo.getHttpsUri());
        }
    }

    @Test
    public void testFixedManagerAuthenticatorHttpInsecureEnabledOnly()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("password-authenticator.config-files", passwordConfigDummy.toString())
                        .put("http-server.authentication.type", "password")
                        .put("http-server.authentication.allow-insecure-over-http", "true")
                        .put("http-server.authentication.password.user-mapping.pattern", ALLOWED_USER_MAPPING_PATTERN)
                        .put("management.user", MANAGEMENT_USER)
                        .buildOrThrow())
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate);
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION);

            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertFixedManagementUser(httpServerInfo.getHttpUri(), true);
            assertPasswordAuthentication(httpServerInfo.getHttpsUri());
        }
    }

    @Test
    public void testFixedManagerAuthenticatorHttpInsecureDisabledOnly()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("password-authenticator.config-files", passwordConfigDummy.toString())
                        .put("http-server.authentication.type", "password")
                        .put("http-server.authentication.allow-insecure-over-http", "false")
                        .put("http-server.authentication.password.user-mapping.pattern", ALLOWED_USER_MAPPING_PATTERN)
                        .put("management.user", MANAGEMENT_USER)
                        .buildOrThrow())
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate);
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION);

            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertResponseCode(client, getPublicLocation(httpServerInfo.getHttpUri()), SC_OK);
            assertResponseCode(client, getAuthorizedUserLocation(httpServerInfo.getHttpUri()), SC_FORBIDDEN, TEST_USER_LOGIN, null);
            assertResponseCode(client, getManagementLocation(httpServerInfo.getHttpUri()), SC_OK);
            assertResponseCode(client, getManagementLocation(httpServerInfo.getHttpUri()), SC_OK, "unknown", "something");

            assertPasswordAuthentication(httpServerInfo.getHttpsUri());
        }
    }

    @Test
    public void testFixedManagerAuthenticatorHttps()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("password-authenticator.config-files", passwordConfigDummy.toString())
                        .put("http-server.authentication.type", "password")
                        .put("http-server.authentication.allow-insecure-over-http", "true")
                        .put("management.user", MANAGEMENT_USER)
                        .put("management.user.https-enabled", "true")
                        .buildOrThrow())
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate);
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION);

            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertFixedManagementUser(httpServerInfo.getHttpUri(), true);
            assertFixedManagementUser(httpServerInfo.getHttpsUri(), false);
        }
    }

    @Test
    public void testCertAuthenticator()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "certificate")
                        .put("http-server.https.truststore.path", LOCALHOST_KEYSTORE)
                        .put("http-server.https.truststore.key", "")
                        .buildOrThrow())
                .build()) {
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.NO_IMPERSONATION);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            assertAuthenticationDisabled(httpServerInfo.getHttpUri());

            OkHttpClient.Builder clientBuilder = client.newBuilder();
            setupSsl(
                    clientBuilder,
                    Optional.of(LOCALHOST_KEYSTORE),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(LOCALHOST_KEYSTORE),
                    Optional.empty(),
                    Optional.empty());
            OkHttpClient clientWithCert = clientBuilder.build();
            assertAuthenticationAutomatic(httpServerInfo.getHttpsUri(), clientWithCert);
        }
    }

    @Test
    public void testJwtAuthenticator()
            throws Exception
    {
        verifyJwtAuthenticator(Optional.empty());
        verifyJwtAuthenticator(Optional.of("custom-principal"));
    }

    private void verifyJwtAuthenticator(Optional<String> principalField)
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "jwt")
                        .put("http-server.authentication.jwt.key-file", HMAC_KEY)
                        .put("http-server.authentication.jwt.principal-field", principalField.orElse("sub"))
                        .buildOrThrow())
                .build()) {
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.NO_IMPERSONATION);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            assertAuthenticationDisabled(httpServerInfo.getHttpUri());

            SecretKey hmac = hmacShaKeyFor(Base64.getDecoder().decode(Files.readString(Paths.get(HMAC_KEY)).trim()));
            JwtBuilder tokenBuilder = newJwtBuilder()
                    .signWith(hmac)
                    .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()));
            if (principalField.isPresent()) {
                tokenBuilder.claim(principalField.get(), "test-user");
            }
            else {
                tokenBuilder.setSubject("test-user");
            }
            String token = tokenBuilder.compact();

            OkHttpClient clientWithJwt = client.newBuilder()
                    .authenticator((route, response) -> response.request().newBuilder()
                            .header(AUTHORIZATION, "Bearer " + token)
                            .build())
                    .build();
            assertAuthenticationAutomatic(httpServerInfo.getHttpsUri(), clientWithJwt);
        }
    }

    @Test
    public void testJwtWithJwkAuthenticator()
            throws Exception
    {
        TestingHttpServer jwkServer = createTestingJwkServer();
        jwkServer.start();
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "jwt")
                        .put("http-server.authentication.jwt.key-file", jwkServer.getBaseUrl().toString())
                        .buildOrThrow())
                .build()) {
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.NO_IMPERSONATION);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            assertAuthenticationDisabled(httpServerInfo.getHttpUri());

            String token = newJwtBuilder()
                    .signWith(JWK_PRIVATE_KEY)
                    .setHeaderParam(JwsHeader.KEY_ID, JWK_KEY_ID)
                    .setSubject("test-user")
                    .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                    .compact();

            OkHttpClient clientWithJwt = client.newBuilder()
                    .authenticator((route, response) -> response.request().newBuilder()
                            .header(AUTHORIZATION, "Bearer " + token)
                            .build())
                    .build();
            assertAuthenticationAutomatic(httpServerInfo.getHttpsUri(), clientWithJwt);
        }
        finally {
            jwkServer.stop();
        }
    }

    @Test
    public void testOAuth2Authenticator()
            throws Exception
    {
        verifyOAuth2Authenticator(true, Optional.empty());
        verifyOAuth2Authenticator(false, Optional.empty());
        verifyOAuth2Authenticator(true, Optional.of("custom-principal"));
        verifyOAuth2Authenticator(false, Optional.of("custom-principal"));
    }

    private void verifyOAuth2Authenticator(boolean webUiEnabled, Optional<String> principalField)
            throws Exception
    {
        CookieManager cookieManager = new CookieManager();
        OkHttpClient client = this.client.newBuilder()
                .cookieJar(new JavaNetCookieJar(cookieManager))
                .build();

        Duration tokenExpiration = Duration.ofSeconds(20);
        try (TokenServer tokenServer = new TokenServer(principalField, Optional.of(tokenExpiration));
                TestingTrinoServer server = TestingTrinoServer.builder()
                        .setProperties(ImmutableMap.<String, String>builder()
                                .putAll(SECURE_PROPERTIES)
                                .put("web-ui.enabled", String.valueOf(webUiEnabled))
                                .put("http-server.authentication.type", "oauth2")
                                .putAll(getOAuth2Properties(tokenServer))
                                .put("http-server.authentication.oauth2.principal-field", principalField.orElse("sub"))
                                .buildOrThrow())
                        .setAdditionalModule(oauth2Module(tokenServer))
                        .build()) {
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.NO_IMPERSONATION);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            assertAuthenticationDisabled(httpServerInfo.getHttpUri());

            // not logged in
            URI baseUri = httpServerInfo.getHttpsUri();
            assertOk(client, getPublicLocation(baseUri));
            assertAuthenticateOAuth2Bearer(client, getManagementLocation(baseUri), "http://example.com/authorize");
            OAuthBearer bearer = assertAuthenticateOAuth2Bearer(client, getAuthorizedUserLocation(baseUri), "http://example.com/authorize");
            assertResponseCode(client, getInternalLocation(baseUri), SC_FORBIDDEN);

            // login with the callback endpoint
            assertOk(
                    client,
                    uriBuilderFrom(baseUri)
                            .replacePath("/oauth2/callback/")
                            .addParameter("code", "TEST_CODE")
                            .addParameter("state", bearer.getState())
                            .toString());
            assertEquals(getOauthToken(client, bearer.getTokenServer()), tokenServer.getAccessToken());

            // if Web UI is using oauth so we should get a cookie
            CookieStore cookieStore = cookieManager.getCookieStore();
            List<HttpCookie> storedCookies = cookieStore.getCookies();
            if (webUiEnabled) {
                OAuthWebUiCookieTestUtils.assertWebUiCookie(cookieStore, OAUTH2_COOKIE, Optional.of(tokenServer.getAccessToken()), Optional.of(MINUTES.toSeconds(5)));
                OAuthWebUiCookieTestUtils.assertWebUiCookie(cookieStore, OAUTH2_REFRESH_COOKIE, Optional.of(tokenServer.getRefreshToken()), Optional.empty());
                cookieStore.removeAll();
            }
            else {
                assertTrue(storedCookies.isEmpty(), "Expected no cookies when webUi is not enabled, but got: " + storedCookies);
            }

            assertAuthenticationAutomatic(
                    httpServerInfo.getHttpsUri(),
                    client.newBuilder()
                            .authenticator((route, response) -> response.request().newBuilder()
                                    .header(AUTHORIZATION, "Bearer " + tokenServer.getAccessToken())
                                    .build())
                            .build());

            // Wait for token to expire
            Thread.sleep(tokenExpiration.toMillis());

            // Fetch token refresh url
            Response tokenExpiredChallengeResponse = executeRequest(client, getAuthorizedUserLocation(baseUri), Headers.of(AUTHORIZATION, "Bearer " + tokenServer.getAccessToken()));
            String authenticateHeader = tokenExpiredChallengeResponse.header("WWW-Authenticate");
            assertThat(authenticateHeader).contains("x_token_refresh_server=\"");

            // Refresh Access Token
            String refreshTokenUrl = authenticateHeader.split("x_token_refresh_server=")[1].replace("\"", "");
            assertResponseCode(client, refreshTokenUrl + "?refresh_token=" + tokenServer.refreshToken, SC_OK);

            // Verify access using new access token
            assertAuthenticationAutomatic(
                    httpServerInfo.getHttpsUri(),
                    client.newBuilder()
                            .authenticator((route, response) -> response.request().newBuilder()
                                    .header(AUTHORIZATION, "Bearer " + tokenServer.getAccessToken())
                                    .build())
                            .build());
        }
    }

    private static OAuthBearer assertAuthenticateOAuth2Bearer(OkHttpClient client, String url, String expectedRedirect)
            throws IOException
    {
        Request request = new Request.Builder()
                .url(url)
                .build();
        String redirectTo;
        String tokenServer;
        try (Response response = client.newCall(request).execute()) {
            assertEquals(response.code(), SC_UNAUTHORIZED, url);
            String authenticateHeader = response.header(WWW_AUTHENTICATE);
            assertNotNull(authenticateHeader);
            Pattern oauth2BearerPattern = Pattern.compile("Bearer x_redirect_server=\"([^\"]+)\", x_token_server=\"([^\"]+)\"(, x_token_refresh_server=\"([^\"]+)\")?");
            Matcher matcher = oauth2BearerPattern.matcher(authenticateHeader);
            assertTrue(matcher.matches(), format("Invalid authentication header.\nExpected: %s\nPattern: %s", authenticateHeader, oauth2BearerPattern));
            redirectTo = matcher.group(1);
            tokenServer = matcher.group(2);
        }

        request = new Request.Builder()
                .url(redirectTo)
                .build();
        try (Response response = client.newCall(request).execute()) {
            assertEquals(response.code(), SC_SEE_OTHER);
            String locationHeader = response.header(LOCATION);
            assertNotNull(locationHeader);
            Pattern locationPattern = Pattern.compile(format("%s\\?(.+)", expectedRedirect));
            Matcher matcher = locationPattern.matcher(locationHeader);
            assertTrue(matcher.matches(), format("Invalid location header.\nExpected: %s\nPattern: %s", expectedRedirect, locationPattern));

            HttpCookie nonceCookie = HttpCookie.parse(requireNonNull(response.header(SET_COOKIE))).get(0);
            nonceCookie.setDomain(request.url().host());
            return new OAuthBearer(matcher.group(1), tokenServer, nonceCookie);
        }
    }

    private static class OAuthBearer
    {
        private final String state;
        private final String tokenServer;
        private final HttpCookie nonceCookie;

        public OAuthBearer(String state, String tokenServer, HttpCookie nonceCookie)
        {
            this.state = requireNonNull(state, "state is null");
            this.tokenServer = requireNonNull(tokenServer, "tokenServer is null");
            this.nonceCookie = requireNonNull(nonceCookie, "nonce is null");
        }

        public String getState()
        {
            return state;
        }

        public String getTokenServer()
        {
            return tokenServer;
        }

        public HttpCookie getNonceCookie()
        {
            return nonceCookie;
        }
    }

    @Test(dataProvider = "groups")
    public void testOAuth2Groups(Optional<Set<String>> groups)
            throws Exception
    {
        try (TokenServer tokenServer = new TokenServer(Optional.empty());
                TestingTrinoServer server = TestingTrinoServer.builder()
                        .setProperties(ImmutableMap.<String, String>builder()
                                .putAll(SECURE_PROPERTIES)
                                .put("web-ui.enabled", "true")
                                .put("http-server.authentication.type", "oauth2")
                                .putAll(getOAuth2Properties(tokenServer))
                                .put("http-server.authentication.oauth2.groups-field", GROUPS_CLAIM)
                                .buildOrThrow())
                        .setAdditionalModule(oauth2Module(tokenServer))
                        .build()) {
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.NO_IMPERSONATION);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            String accessToken = tokenServer.issueAccessToken(groups);
            OkHttpClient clientWithOAuthToken = client.newBuilder()
                    .authenticator((route, response) -> response.request().newBuilder()
                            .header(AUTHORIZATION, "Bearer " + accessToken)
                            .build())
                    .build();

            assertAuthenticationAutomatic(httpServerInfo.getHttpsUri(), clientWithOAuthToken);

            try (Response response = clientWithOAuthToken.newCall(new Request.Builder()
                            .url(getLocation(httpServerInfo.getHttpsUri(), "/protocol/identity"))
                            .build())
                    .execute()) {
                assertEquals(response.code(), SC_OK);
                assertEquals(response.header("user"), TEST_USER);
                assertEquals(response.header("principal"), TEST_USER);
                assertEquals(response.header("groups"), groups.map(TestResource::toHeader).orElse(""));
            }

            OkHttpClient clientWithOAuthCookie = client.newBuilder()
                    .cookieJar(new CookieJar()
                    {
                        @Override
                        public void saveFromResponse(HttpUrl url, List<Cookie> cookies)
                        {
                        }

                        @Override
                        public List<Cookie> loadForRequest(HttpUrl url)
                        {
                            return ImmutableList.of(new Cookie.Builder()
                                    .domain(httpServerInfo.getHttpsUri().getHost())
                                    .path(UI_LOCATION)
                                    .name(OAUTH2_COOKIE)
                                    .value(accessToken)
                                    .httpOnly()
                                    .secure()
                                    .build());
                        }
                    })
                    .build();
            try (Response response = clientWithOAuthCookie.newCall(new Request.Builder()
                            .url(getLocation(httpServerInfo.getHttpsUri(), "/ui/api/identity"))
                            .build())
                    .execute()) {
                assertEquals(response.code(), SC_OK);
                assertEquals(response.header("user"), TEST_USER);
                assertEquals(response.header("principal"), TEST_USER);
                assertEquals(response.header("groups"), groups.map(TestResource::toHeader).orElse(""));
            }
        }
    }

    @DataProvider(name = "groups")
    public static Object[][] groups()
    {
        return new Object[][] {
                {Optional.empty()},
                {Optional.of(ImmutableSet.of())},
                {Optional.of(ImmutableSet.of("admin", "public"))}
        };
    }

    @Test
    public void testJwtAndOAuth2AuthenticatorsSeparation()
            throws Exception
    {
        TestingHttpServer jwkServer = createTestingJwkServer();
        jwkServer.start();
        try (TokenServer tokenServer = new TokenServer(Optional.empty());
                TestingTrinoServer server = TestingTrinoServer.builder()
                        .setProperties(
                                ImmutableMap.<String, String>builder()
                                .putAll(SECURE_PROPERTIES)
                                .put("http-server.authentication.type", "jwt,oauth2")
                                .put("http-server.authentication.jwt.key-file", jwkServer.getBaseUrl().toString())
                                .putAll(getOAuth2Properties(tokenServer))
                                .put("web-ui.enabled", "true")
                                .buildOrThrow())
                        .setAdditionalModule(oauth2Module(tokenServer))
                        .build()) {
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(TestSystemAccessControl.NO_IMPERSONATION);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            assertAuthenticationDisabled(httpServerInfo.getHttpUri());

            OkHttpClient clientWithOAuthToken = client.newBuilder()
                    .authenticator((route, response) -> response.request().newBuilder()
                            .header(AUTHORIZATION, "Bearer " + tokenServer.getAccessToken())
                            .build())
                    .build();

            assertAuthenticationAutomatic(httpServerInfo.getHttpsUri(), clientWithOAuthToken);

            String token = newJwtBuilder()
                    .signWith(JWK_PRIVATE_KEY)
                    .setHeaderParam(JwsHeader.KEY_ID, JWK_KEY_ID)
                    .setSubject("test-user")
                    .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                    .compact();

            OkHttpClient clientWithJwt = client.newBuilder()
                    .authenticator((route, response) -> response.request().newBuilder()
                            .header(AUTHORIZATION, "Bearer " + token)
                            .build())
                    .build();
            assertAuthenticationAutomatic(httpServerInfo.getHttpsUri(), clientWithJwt);
        }
    }

    private static Module oauth2Module(TokenServer tokenServer)
    {
        return binder -> {
            jaxrsBinder(binder).bind(TestResource.class);
            newOptionalBinder(binder, OAuth2Client.class)
                    .setBinding()
                    .toInstance(tokenServer.getOAuth2Client());
        };
    }

    private static Map<String, String> getOAuth2Properties(TokenServer tokenServer)
    {
        return ImmutableMap.<String, String>builder()
                .put("http-server.authentication.oauth2.issuer", tokenServer.getIssuer())
                .put("http-server.authentication.oauth2.jwks-url", tokenServer.getJwksUrl())
                .put("http-server.authentication.oauth2.state-key", "test-state-key")
                .put("http-server.authentication.oauth2.auth-url", tokenServer.getIssuer())
                .put("http-server.authentication.oauth2.token-url", tokenServer.getIssuer())
                .put("http-server.authentication.oauth2.client-id", tokenServer.getClientId())
                .put("http-server.authentication.oauth2.client-secret", tokenServer.getClientSecret())
                .buildOrThrow();
    }

    private static String getOauthToken(OkHttpClient client, String url)
            throws IOException
    {
        Request request = new Request.Builder()
                .url(url)
                .build();
        try (Response response = client.newCall(request).execute()) {
            String body = requireNonNull(response.body()).string();
            return json.readValue(body, TokenDTO.class).token;
        }
    }

    private static class TokenServer
            implements AutoCloseable
    {
        private final String issuer = "http://example.com/";
        private final String clientId = "clientID";
        private final Duration tokenExpiration;
        private final Optional<String> principalField;
        private final TestingHttpServer jwkServer;
        private String accessToken;
        private String refreshToken;

        public TokenServer(Optional<String> principalField)
                throws Exception
        {
            this(principalField, Optional.empty());
        }

        public TokenServer(Optional<String> principalField, Optional<Duration> tokenExpiration)
                throws Exception
        {
            this.tokenExpiration = requireNonNull(tokenExpiration, "tokenExpiration is null").orElse(Duration.ofMinutes(5));
            this.principalField = requireNonNull(principalField, "principalField is null");
            jwkServer = createTestingJwkServer();
            jwkServer.start();
            accessToken = issueAccessToken(Optional.empty());
            refreshToken = issueRefreshToken();
        }

        @Override
        public void close()
                throws Exception
        {
            jwkServer.stop();
        }

        public OAuth2Client getOAuth2Client()
        {
            return new OAuth2Client()
            {
                private final AtomicReference<Optional<String>> nonceHash = new AtomicReference<>();

                @Override
                public URI getAuthorizationUri(String state, URI callbackUri, Optional<String> nonceHash)
                {
                    // Save the last nonce in order to add it to the next issued ID token
                    this.nonceHash.set(nonceHash);
                    return URI.create("http://example.com/authorize?" + state);
                }

                @Override
                public OAuth2Response getOAuth2Response(String code, URI callbackUri)
                {
                    if (!"TEST_CODE".equals(code)) {
                        throw new IllegalArgumentException("Expected TEST_CODE");
                    }
                    return new OAuth2Response(accessToken, Optional.of(refreshToken), Optional.of(now().plus(5, ChronoUnit.MINUTES)), Optional.of(issueIdToken(nonceHash.get())));
                }

                @Override
                public OAuth2Response getRefreshedOAuth2Response(String submittedRefreshToken)
                {
                    checkState(refreshToken.equals(submittedRefreshToken), "Invalid refresh token");
                    //Refresh tokens
                    accessToken = issueAccessToken(Optional.empty());
                    refreshToken = issueRefreshToken();
                    return new OAuth2Response(
                            accessToken,
                            Optional.of(refreshToken),
                            Optional.of(now().plus(5, ChronoUnit.MINUTES)),
                            Optional.of(issueIdToken(nonceHash.get())));
                }
            };
        }

        public String getIssuer()
        {
            return issuer;
        }

        public String getJwksUrl()
        {
            return jwkServer.getBaseUrl().toString();
        }

        public String getClientId()
        {
            return clientId;
        }

        public String getClientSecret()
        {
            return "clientSecret";
        }

        public String getAccessToken()
        {
            return accessToken;
        }

        public String getRefreshToken()
        {
            return refreshToken;
        }

        public String issueAccessToken(Optional<Set<String>> groups)
        {
            JwtBuilder accessToken = newJwtBuilder()
                    .signWith(JWK_PRIVATE_KEY)
                    .setHeaderParam(JwsHeader.KEY_ID, JWK_KEY_ID)
                    .setIssuer(issuer)
                    .setAudience(clientId)
                    .setExpiration(Date.from(ZonedDateTime.now().plus(tokenExpiration).toInstant()));
            if (principalField.isPresent()) {
                accessToken.claim(principalField.get(), TEST_USER);
            }
            else {
                accessToken.setSubject(TEST_USER);
            }
            groups.ifPresent(groupsClaim -> accessToken.claim(GROUPS_CLAIM, groupsClaim));
            return accessToken.compact();
        }

        private String issueIdToken(Optional<String> nonceHash)
        {
            JwtBuilder idToken = newJwtBuilder()
                    .signWith(JWK_PRIVATE_KEY)
                    .setHeaderParam(JwsHeader.KEY_ID, JWK_KEY_ID)
                    .setIssuer(issuer)
                    .setAudience(clientId)
                    .setExpiration(Date.from(ZonedDateTime.now().plus(tokenExpiration).toInstant()));
            if (principalField.isPresent()) {
                idToken.claim(principalField.get(), TEST_USER);
            }
            else {
                idToken.setSubject(TEST_USER);
            }
            nonceHash.ifPresent(nonce -> idToken.claim(NONCE, nonce));
            return idToken.compact();
        }

        private String issueRefreshToken()
        {
            return UUID.randomUUID().toString().replaceAll("-", "");
        }
    }

    @javax.ws.rs.Path("/")
    public static class TestResource
    {
        private final HttpRequestSessionContextFactory sessionContextFactory;

        @Inject
        public TestResource(AccessControl accessControl)
        {
            this.sessionContextFactory = new HttpRequestSessionContextFactory(
                    new PreparedStatementEncoder(new ProtocolConfig()),
                    createTestMetadataManager(),
                    user -> ImmutableSet.of(),
                    accessControl);
        }

        @ResourceSecurity(AUTHENTICATED_USER)
        @GET
        @javax.ws.rs.Path("/protocol/identity")
        public javax.ws.rs.core.Response protocolIdentity(@Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
        {
            return echoIdentity(servletRequest, httpHeaders);
        }

        @ResourceSecurity(WEB_UI)
        @GET
        @javax.ws.rs.Path("/ui/api/identity")
        public javax.ws.rs.core.Response webUiIdentity(@Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
        {
            return echoIdentity(servletRequest, httpHeaders);
        }

        public javax.ws.rs.core.Response echoIdentity(HttpServletRequest servletRequest, HttpHeaders httpHeaders)
        {
            Identity identity = sessionContextFactory.extractAuthorizedIdentity(servletRequest, httpHeaders, Optional.empty());
            return javax.ws.rs.core.Response.ok()
                    .header("user", identity.getUser())
                    .header("principal", identity.getPrincipal().map(Principal::getName).orElse(null))
                    .header("groups", toHeader(identity.getGroups()))
                    .build();
        }

        public static String toHeader(Set<String> groups)
        {
            return groups.stream().sorted().collect(Collectors.joining(","));
        }
    }

    private void assertInsecureAuthentication(URI baseUri)
            throws IOException
    {
        assertResponseCode(client, getManagementLocation(baseUri), SC_OK, MANAGEMENT_USER_LOGIN, null);
        // public
        assertOk(client, getPublicLocation(baseUri));
        // authorized user
        assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_UNAUTHORIZED);
        assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_OK, TEST_USER_LOGIN, null);
        assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_UNAUTHORIZED, TEST_USER_LOGIN, "something");
        assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_UNAUTHORIZED, "unknown", null);
        // management
        assertResponseCode(client, getManagementLocation(baseUri), SC_UNAUTHORIZED);
        assertResponseCode(client, getManagementLocation(baseUri), SC_FORBIDDEN, TEST_USER_LOGIN, null);
        assertResponseCode(client, getManagementLocation(baseUri), SC_UNAUTHORIZED, TEST_USER_LOGIN, "something");
        assertResponseCode(client, getManagementLocation(baseUri), SC_UNAUTHORIZED, "unknown", null);
        assertResponseCode(client, getManagementLocation(baseUri), SC_OK, MANAGEMENT_USER_LOGIN, null);
        assertResponseCode(client, getManagementLocation(baseUri), SC_UNAUTHORIZED, MANAGEMENT_USER_LOGIN, "something");
        assertResponseCode(client, getManagementLocation(baseUri), SC_UNAUTHORIZED, MANAGEMENT_USER_LOGIN, MANAGEMENT_PASSWORD);
        // internal
        assertResponseCode(client, getInternalLocation(baseUri), SC_FORBIDDEN);
        assertResponseCode(client, getInternalLocation(baseUri), SC_FORBIDDEN, TEST_USER_LOGIN, null);
        assertResponseCode(client, getInternalLocation(baseUri), SC_FORBIDDEN, MANAGEMENT_USER_LOGIN, null);
    }

    private void assertPasswordAuthentication(URI baseUri)
            throws IOException
    {
        assertPasswordAuthentication(baseUri, TEST_PASSWORD);
    }

    private void assertPasswordAuthentication(URI baseUri, String... allowedPasswords)
            throws IOException
    {
        // public
        assertOk(client, getPublicLocation(baseUri));
        // authorized user
        assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_UNAUTHORIZED);
        assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_UNAUTHORIZED, TEST_USER_LOGIN, null);
        assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_UNAUTHORIZED, TEST_USER_LOGIN, "invalid");
        for (String password : allowedPasswords) {
            assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_OK, TEST_USER_LOGIN, password);
        }
        // management
        assertResponseCode(client, getManagementLocation(baseUri), SC_UNAUTHORIZED);
        assertResponseCode(client, getManagementLocation(baseUri), SC_UNAUTHORIZED, TEST_USER_LOGIN, null);
        assertResponseCode(client, getManagementLocation(baseUri), SC_UNAUTHORIZED, TEST_USER_LOGIN, "invalid");
        for (String password : allowedPasswords) {
            assertResponseCode(client, getManagementLocation(baseUri), SC_FORBIDDEN, TEST_USER_LOGIN, password);
        }
        assertResponseCode(client, getManagementLocation(baseUri), SC_UNAUTHORIZED, MANAGEMENT_USER_LOGIN, null);
        assertResponseCode(client, getManagementLocation(baseUri), SC_UNAUTHORIZED, MANAGEMENT_USER_LOGIN, "invalid");
        assertResponseCode(client, getManagementLocation(baseUri), SC_OK, MANAGEMENT_USER_LOGIN, MANAGEMENT_PASSWORD);
        // internal
        assertResponseCode(client, getInternalLocation(baseUri), SC_FORBIDDEN);
        for (String password : allowedPasswords) {
            assertResponseCode(client, getInternalLocation(baseUri), SC_FORBIDDEN, TEST_USER_LOGIN, password);
        }
    }

    private static void assertAuthenticationAutomatic(URI baseUri, OkHttpClient authorizedClient)
            throws IOException
    {
        // public
        assertResponseCode(authorizedClient, getPublicLocation(baseUri), SC_OK);
        // authorized user
        assertResponseCode(authorizedClient, getAuthorizedUserLocation(baseUri), SC_OK);
        // management
        assertResponseCode(authorizedClient, getManagementLocation(baseUri), SC_FORBIDDEN);
        assertResponseCode(authorizedClient, getManagementLocation(baseUri), SC_OK, Headers.of(TRINO_HEADERS.requestUser(), MANAGEMENT_USER));
        // internal
        assertResponseCode(authorizedClient, getInternalLocation(baseUri), SC_FORBIDDEN);
    }

    private void assertAuthenticationDisabled(URI baseUri)
            throws IOException
    {
        // public
        assertOk(client, getPublicLocation(baseUri));
        // authorized user
        assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_FORBIDDEN);
        assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_FORBIDDEN, "unknown", null);
        assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_FORBIDDEN, "unknown", "something");
        assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_FORBIDDEN, TEST_USER_LOGIN, TEST_PASSWORD);
        // management
        assertResponseCode(client, getManagementLocation(baseUri), SC_FORBIDDEN);
        assertResponseCode(client, getManagementLocation(baseUri), SC_FORBIDDEN, "unknown", null);
        assertResponseCode(client, getManagementLocation(baseUri), SC_FORBIDDEN, "unknown", "something");
        assertResponseCode(client, getManagementLocation(baseUri), SC_FORBIDDEN, TEST_USER_LOGIN, TEST_PASSWORD);
        // internal
        assertResponseCode(client, getInternalLocation(baseUri), SC_FORBIDDEN);
        assertResponseCode(client, getInternalLocation(baseUri), SC_FORBIDDEN, "unknown", null);
        assertResponseCode(client, getInternalLocation(baseUri), SC_FORBIDDEN, "unknown", "something");
        assertResponseCode(client, getInternalLocation(baseUri), SC_FORBIDDEN, TEST_USER_LOGIN, TEST_PASSWORD);
    }

    private void assertFixedManagementUser(URI baseUri, boolean insecureAuthentication)
            throws IOException
    {
        assertResponseCode(client, getPublicLocation(baseUri), SC_OK);
        if (insecureAuthentication) {
            assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_OK, TEST_USER_LOGIN, null);
            assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_UNAUTHORIZED, "unknown", null);
        }
        else {
            assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_OK, TEST_USER_LOGIN, TEST_PASSWORD);
        }
        assertResponseCode(client, getManagementLocation(baseUri), SC_OK);
        assertResponseCode(client, getManagementLocation(baseUri), SC_OK, "unknown", "something");
    }

    private static void assertOk(OkHttpClient client, String url)
            throws IOException
    {
        assertResponseCode(client, url, SC_OK, null, null);
    }

    private static void assertResponseCode(OkHttpClient client, String url, int expectedCode)
            throws IOException
    {
        assertResponseCode(client, url, expectedCode, null, null);
    }

    private static void assertResponseCode(OkHttpClient client,
            String url,
            int expectedCode,
            String userName,
            String password)
            throws IOException
    {
        assertResponseCode(client, url, expectedCode, Headers.of("Authorization", Credentials.basic(firstNonNull(userName, ""), firstNonNull(password, ""))));
    }

    private static void assertResponseCode(OkHttpClient client,
            String url,
            int expectedCode,
            Headers headers)
            throws IOException
    {
        assertEquals(executeRequest(client, url, headers).code(), expectedCode, url);
    }

    private static Response executeRequest(OkHttpClient client, String url, Headers headers)
            throws IOException
    {
        Request request = new Request.Builder()
                .url(url)
                .headers(headers)
                .build();
        try (Response response = client.newCall(request).execute()) {
            return response;
        }
    }

    private static String getInternalLocation(URI baseUri)
    {
        return getLocation(baseUri, "/v1/task");
    }

    private static String getManagementLocation(URI baseUri)
    {
        return getLocation(baseUri, "/v1/node");
    }

    private static String getAuthorizedUserLocation(URI baseUri)
    {
        return getLocation(baseUri, "/v1/query");
    }

    private static String getPublicLocation(URI baseUri)
    {
        return getLocation(baseUri, "/v1/info");
    }

    private static String getLocation(URI baseUri, String path)
    {
        return uriBuilderFrom(baseUri).replacePath(path).toString();
    }

    private static Principal authenticate(String user, String password)
    {
        if ((TEST_USER_LOGIN.equals(user) && TEST_PASSWORD.equals(password)) || (MANAGEMENT_USER_LOGIN.equals(user) && MANAGEMENT_PASSWORD.equals(password))) {
            return new BasicPrincipal(user);
        }
        throw new AccessDeniedException("Invalid credentials");
    }

    private static Principal authenticate2(String user, String password)
    {
        if ((TEST_USER_LOGIN.equals(user) && TEST_PASSWORD2.equals(password)) || (MANAGEMENT_USER_LOGIN.equals(user) && MANAGEMENT_PASSWORD.equals(password))) {
            return new BasicPrincipal(user);
        }
        throw new AccessDeniedException("Invalid credentials2");
    }

    private static class TestSystemAccessControl
            extends AllowAllSystemAccessControl
    {
        public static final TestSystemAccessControl WITH_IMPERSONATION = new TestSystemAccessControl(false);
        public static final TestSystemAccessControl NO_IMPERSONATION = new TestSystemAccessControl(true);

        private final boolean allowImpersonation;

        private TestSystemAccessControl(boolean allowImpersonation)
        {
            this.allowImpersonation = allowImpersonation;
        }

        @Override
        public void checkCanImpersonateUser(SystemSecurityContext context, String userName)
        {
            if (!allowImpersonation) {
                denyImpersonateUser(context.getIdentity().getUser(), userName);
            }
        }

        @Override
        public void checkCanReadSystemInformation(SystemSecurityContext context)
        {
            if (!context.getIdentity().getUser().equals(MANAGEMENT_USER)) {
                denyReadSystemInformationAccess();
            }
        }
    }

    private static TestingHttpServer createTestingJwkServer()
            throws IOException
    {
        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig().setHttpPort(0);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, nodeInfo);

        return new TestingHttpServer(httpServerInfo, nodeInfo, config, new JwkServlet(), ImmutableMap.of());
    }

    private static class JwkServlet
            extends HttpServlet
    {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            String jwkKeys = Resources.toString(Resources.getResource("jwk/jwk-public.json"), UTF_8);
            response.getWriter().println(jwkKeys);
        }
    }

    private static class TokenDTO
    {
        @JsonProperty
        String token;

        @JsonProperty
        String refreshToken;
    }
}
