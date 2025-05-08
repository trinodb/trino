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
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import io.airlift.security.pem.PemReader;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.JwtParser;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.security.AccessControl;
import io.trino.server.HttpRequestSessionContextFactory;
import io.trino.server.ProtocolConfig;
import io.trino.server.protocol.PreparedStatementEncoder;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.security.oauth2.ChallengeFailedException;
import io.trino.server.security.oauth2.OAuth2Client;
import io.trino.server.security.oauth2.TokenPairSerializer;
import io.trino.server.security.oauth2.TokenPairSerializer.TokenPair;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import okhttp3.Cookie;
import okhttp3.CookieJar;
import okhttp3.Credentials;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.JavaNetCookieJar;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import javax.crypto.SecretKey;

import java.io.File;
import java.io.IOException;
import java.net.CookieManager;
import java.net.HttpCookie;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.hash.Hashing.sha256;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.jsonwebtoken.Claims.AUDIENCE;
import static io.jsonwebtoken.security.Keys.hmacShaKeyFor;
import static io.trino.client.OkHttpUtil.setupSsl;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.metadata.TestMetadataManager.createTestMetadataManager;
import static io.trino.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static io.trino.server.security.jwt.JwtUtil.newJwtBuilder;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static io.trino.server.security.oauth2.OAuth2Service.NONCE;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOCATION;
import static io.trino.server.ui.OAuthIdTokenCookie.ID_TOKEN_COOKIE;
import static io.trino.server.ui.OAuthWebUiCookie.OAUTH2_COOKIE;
import static io.trino.spi.security.AccessDeniedException.denyImpersonateUser;
import static io.trino.spi.security.AccessDeniedException.denyReadSystemInformationAccess;
import static jakarta.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static jakarta.servlet.http.HttpServletResponse.SC_OK;
import static jakarta.servlet.http.HttpServletResponse.SC_SEE_OTHER;
import static jakarta.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
import static jakarta.ws.rs.core.HttpHeaders.LOCATION;
import static jakarta.ws.rs.core.HttpHeaders.SET_COOKIE;
import static jakarta.ws.rs.core.HttpHeaders.WWW_AUTHENTICATE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestResourceSecurity
{
    private static final String LOCALHOST_KEYSTORE = Resources.getResource("cert/localhost.pem").getPath();
    private static final String ALLOWED_USER_MAPPING_PATTERN = "(.*)@allowed";
    private static final Map<String, String> SECURE_PROPERTIES = ImmutableMap.<String, String>builder()
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
    private static final String TRINO_AUDIENCE = "trino-client";
    private static final String ADDITIONAL_AUDIENCE = "https://external-service.com";
    private static final String UNTRUSTED_CLIENT_AUDIENCE = "https://untrusted.com";
    private static final PrivateKey JWK_PRIVATE_KEY;
    private static final PublicKey JWK_PUBLIC_KEY;
    private static final ObjectMapper json = new ObjectMapper();

    static {
        try {
            JWK_PRIVATE_KEY = PemReader.loadPrivateKey(new File(Resources.getResource("jwk/jwk-rsa-private.pem").toURI()), Optional.empty());
            JWK_PUBLIC_KEY = PemReader.loadPublicKey(new File(Resources.getResource("jwk/jwk-rsa-public.pem").getPath()));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private OkHttpClient client;
    private Path passwordConfigDummy;

    @BeforeAll
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
                false,
                Optional.of(LOCALHOST_KEYSTORE),
                Optional.empty(),
                Optional.empty(),
                false);
        client = clientBuilder.build();

        passwordConfigDummy = Files.createTempFile("passwordConfigDummy", "");
        passwordConfigDummy.toFile().deleteOnExit();
    }

    @Test
    public void testInsecureAuthenticatorHttp()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.of("http-server.authentication.insecure.user-mapping.pattern", ALLOWED_USER_MAPPING_PATTERN))
                .setSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION)
                .build()) {
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
                .setSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION)
                .build()) {
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
                .setSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION)
                .build()) {
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
                .setSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION)
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate);
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
                .setSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION)
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate, TestResourceSecurity::authenticate2);
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
                .setSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION)
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate, TestResourceSecurity::authenticate2);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            Request request = new Request.Builder()
                    .url(getAuthorizedUserLocation(httpServerInfo.getHttpsUri()))
                    .headers(Headers.of("Authorization", Credentials.basic(TEST_USER_LOGIN, "wrong_password")))
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertThat(requireNonNull(response.body()).string())
                        .isEqualTo("Access Denied: Invalid credentials | Access Denied: Invalid credentials2");
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
                .setSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION)
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate);
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
                assertThat(response.code()).isEqualTo(SC_OK);
                assertThat(response.header("user")).isEqualTo(TEST_USER);
                assertThat(response.header("principal")).isEqualTo(TEST_USER_LOGIN);
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
                .setSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION)
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate);
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
                .setSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION)
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate);

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
                .setSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION)
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate);

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
                .setSystemAccessControl(TestSystemAccessControl.WITH_IMPERSONATION)
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate);

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
                .setSystemAccessControl(TestSystemAccessControl.NO_IMPERSONATION)
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            assertAuthenticationDisabled(httpServerInfo.getHttpUri());

            OkHttpClient.Builder clientBuilder = client.newBuilder();
            setupSsl(
                    clientBuilder,
                    Optional.of(LOCALHOST_KEYSTORE),
                    Optional.empty(),
                    Optional.empty(),
                    false,
                    Optional.of(LOCALHOST_KEYSTORE),
                    Optional.empty(),
                    Optional.empty(),
                    false);
            OkHttpClient clientWithCert = clientBuilder.build();
            assertAuthenticationAutomatic(httpServerInfo.getHttpsUri(), clientWithCert);
        }
    }

    @Test
    public void testJwtAuthenticator()
            throws Exception
    {
        verifyJwtAuthenticator(Optional.empty(), Optional.empty());
        verifyJwtAuthenticator(Optional.of("custom-principal"), Optional.empty());
        verifyJwtAuthenticator(Optional.empty(), Optional.of(TRINO_AUDIENCE));
        verifyJwtAuthenticator(Optional.empty(), Optional.of(ImmutableList.of(TRINO_AUDIENCE, ADDITIONAL_AUDIENCE)));
    }

    private void verifyJwtAuthenticator(Optional<String> principalField, Optional<Object> audience)
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "jwt")
                        .put("http-server.authentication.jwt.key-file", HMAC_KEY)
                        .put("http-server.authentication.jwt.principal-field", principalField.orElse("sub"))
                        .put("http-server.authentication.jwt.required-audience", TRINO_AUDIENCE)
                        .buildOrThrow())
                .setSystemAccessControl(TestSystemAccessControl.NO_IMPERSONATION)
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            assertAuthenticationDisabled(httpServerInfo.getHttpUri());

            SecretKey hmac = hmacShaKeyFor(Base64.getDecoder().decode(Files.readString(Paths.get(HMAC_KEY)).trim()));
            JwtBuilder tokenBuilder = newJwtBuilder()
                    .signWith(hmac)
                    .expiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()));
            if (principalField.isPresent()) {
                tokenBuilder.claim(principalField.get(), TEST_USER);
            }
            else {
                tokenBuilder.subject(TEST_USER);
            }

            if (audience.isPresent()) {
                tokenBuilder.claim(AUDIENCE, audience.get());
            }
            else {
                tokenBuilder.claim(AUDIENCE, TRINO_AUDIENCE);
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
                .setSystemAccessControl(TestSystemAccessControl.NO_IMPERSONATION)
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            assertAuthenticationDisabled(httpServerInfo.getHttpUri());

            String token = newJwtBuilder()
                    .signWith(JWK_PRIVATE_KEY)
                    .header().keyId(JWK_KEY_ID).and()
                    .subject("test-user")
                    .expiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
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
    public void testJwtAuthenticatorWithInvalidAudience()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "jwt")
                        .put("http-server.authentication.jwt.key-file", HMAC_KEY)
                        .put("http-server.authentication.jwt.required-audience", TRINO_AUDIENCE)
                        .buildOrThrow())
                .setSystemAccessControl(TestSystemAccessControl.NO_IMPERSONATION)
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            SecretKey hmac = hmacShaKeyFor(Base64.getDecoder().decode(Files.readString(Paths.get(HMAC_KEY)).trim()));
            JwtBuilder tokenBuilder = newJwtBuilder()
                    .signWith(hmac)
                    .expiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                    .claim(AUDIENCE, ImmutableList.of(ADDITIONAL_AUDIENCE, UNTRUSTED_CLIENT_AUDIENCE));
            String token = tokenBuilder.compact();

            OkHttpClient clientWithJwt = this.client.newBuilder()
                    .build();
            assertResponseCode(clientWithJwt, getAuthorizedUserLocation(httpServerInfo.getHttpsUri()), SC_UNAUTHORIZED, Headers.of(AUTHORIZATION, "Bearer " + token));
        }
    }

    @Test
    public void testJwtAuthenticatorWithNoRequiredAudience()
            throws Exception
    {
        verifyJwtAuthenticatorWithoutRequiredAudience(Optional.empty());
        verifyJwtAuthenticatorWithoutRequiredAudience(Optional.of(ImmutableList.of(TRINO_AUDIENCE, ADDITIONAL_AUDIENCE)));
    }

    private void verifyJwtAuthenticatorWithoutRequiredAudience(Optional<Object> audience)
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "jwt")
                        .put("http-server.authentication.jwt.key-file", HMAC_KEY)
                        .buildOrThrow())
                .setSystemAccessControl(TestSystemAccessControl.NO_IMPERSONATION)
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            assertAuthenticationDisabled(httpServerInfo.getHttpUri());

            SecretKey hmac = hmacShaKeyFor(Base64.getDecoder().decode(Files.readString(Paths.get(HMAC_KEY)).trim()));
            JwtBuilder tokenBuilder = newJwtBuilder()
                    .signWith(hmac)
                    .expiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                    .subject(TEST_USER);

            if (audience.isPresent()) {
                tokenBuilder.claim(AUDIENCE, audience.get());
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
    public void testOAuth2Authenticator()
            throws Exception
    {
        verifyOAuth2Authenticator(true, false, Optional.empty());
        verifyOAuth2Authenticator(false, false, Optional.empty());
        verifyOAuth2Authenticator(true, false, Optional.of("custom-principal"));
        verifyOAuth2Authenticator(false, false, Optional.of("custom-principal"));
        verifyOAuth2Authenticator(false, true, Optional.empty());
    }

    private void verifyOAuth2Authenticator(boolean webUiEnabled, boolean refreshTokensEnabled, Optional<String> principalField)
            throws Exception
    {
        CookieManager cookieManager = new CookieManager();
        OkHttpClient client = this.client.newBuilder()
                .cookieJar(new JavaNetCookieJar(cookieManager))
                .build();

        try (TokenServer tokenServer = new TokenServer(principalField);
                TestingTrinoServer server = TestingTrinoServer.builder()
                        .setProperties(ImmutableMap.<String, String>builder()
                                .putAll(SECURE_PROPERTIES)
                                .put("web-ui.enabled", String.valueOf(webUiEnabled))
                                .put("http-server.authentication.type", "oauth2")
                                .putAll(getOAuth2Properties(tokenServer))
                                .put("http-server.authentication.oauth2.principal-field", principalField.orElse("sub"))
                                .put("http-server.authentication.oauth2.refresh-tokens", String.valueOf(refreshTokensEnabled))
                                .buildOrThrow())
                        .setAdditionalModule(oauth2Module(tokenServer))
                        .setSystemAccessControl(TestSystemAccessControl.NO_IMPERSONATION)
                        .build()) {
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
            if (refreshTokensEnabled) {
                TokenPairSerializer serializer = server.getInstance(Key.get(TokenPairSerializer.class));
                TokenPair tokenPair = serializer.deserialize(getOauthToken(client, bearer.getTokenServer()));
                assertThat(tokenPair.accessToken()).isEqualTo(tokenServer.getAccessToken());
                assertThat(tokenPair.refreshToken()).isEqualTo(Optional.of(tokenServer.getRefreshToken()));
            }
            else {
                assertThat(getOauthToken(client, bearer.getTokenServer())).isEqualTo(tokenServer.getAccessToken());
            }

            // if Web UI is using oauth so we should get a cookie
            if (webUiEnabled) {
                HttpCookie cookie = getCookie(cookieManager, OAUTH2_COOKIE);
                assertThat(cookie.getValue()).isEqualTo(tokenServer.getAccessToken());
                assertThat(cookie.getPath()).isEqualTo("/ui/");
                assertThat(cookie.getDomain()).isEqualTo(baseUri.getHost());
                assertThat(cookie.getMaxAge() > 0 && cookie.getMaxAge() < MINUTES.toSeconds(5)).isTrue();
                assertThat(cookie.isHttpOnly()).isTrue();

                HttpCookie idTokenCookie = getCookie(cookieManager, ID_TOKEN_COOKIE);
                assertThat(idTokenCookie.getValue()).isEqualTo(tokenServer.issueIdToken(Optional.of(hashNonce(bearer.getNonceCookie().getValue()))));
                assertThat(idTokenCookie.getPath()).isEqualTo("/ui/");
                assertThat(idTokenCookie.getDomain()).isEqualTo(baseUri.getHost());
                assertThat(idTokenCookie.getMaxAge() > 0 && cookie.getMaxAge() < MINUTES.toSeconds(5)).isTrue();
                assertThat(idTokenCookie.isHttpOnly()).isTrue();

                cookieManager.getCookieStore().removeAll();
            }
            else {
                List<HttpCookie> cookies = cookieManager.getCookieStore().getCookies();
                assertThat(cookies.isEmpty())
                        .describedAs("Expected no cookies when webUi is not enabled, but got: " + cookies)
                        .isTrue();
            }

            OkHttpClient clientWithOAuthToken = client.newBuilder()
                    .authenticator((route, response) -> response.request().newBuilder()
                            .header(AUTHORIZATION, "Bearer " + getOauthToken(client, bearer.getTokenServer()))
                            .build())
                    .build();
            assertAuthenticationAutomatic(httpServerInfo.getHttpsUri(), clientWithOAuthToken);

            // verify if error is encoded to prevent XSS vulnerability
            OAuthBearer maliciousBearer = assertAuthenticateOAuth2Bearer(client, getManagementLocation(baseUri), "http://example.com/authorize");
            assertErrorCodeIsEncoded(client, baseUri, maliciousBearer);
        }
    }

    private static void assertErrorCodeIsEncoded(OkHttpClient client, URI baseUri, OAuthBearer bearer)
            throws IOException
    {
        String maliciousErrorCode = "<script>alert(\"I love xss\")</script>";
        String encodedErrorCode = "%3Cscript%3Ealert%28%22I+love+xss%22%29%3C%2Fscript%3E";
        Request request = new Request.Builder()
                .url(uriBuilderFrom(baseUri)
                        .replacePath("/oauth2/callback/")
                        .addParameter("error", maliciousErrorCode)
                        .addParameter("state", bearer.getState())
                        .toString())
                .build();
        try (Response response = client.newCall(request).execute()) {
            assertThat(requireNonNull(response.body()).string())
                    .doesNotContain(maliciousErrorCode)
                    .contains(encodedErrorCode);
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
            assertThat(response.code())
                    .describedAs(url)
                    .isEqualTo(SC_UNAUTHORIZED);
            String authenticateHeader = response.header(WWW_AUTHENTICATE);
            assertThat(authenticateHeader).isNotNull();
            Pattern oauth2BearerPattern = Pattern.compile("Bearer x_redirect_server=\"(https://127.0.0.1:[0-9]+/oauth2/token/initiate/.+)\", x_token_server=\"(https://127.0.0.1:[0-9]+/oauth2/token/.+)\"");
            Matcher matcher = oauth2BearerPattern.matcher(authenticateHeader);
            assertThat(matcher.matches())
                    .describedAs(format("Invalid authentication header.\nExpected: %s\nPattern: %s", authenticateHeader, oauth2BearerPattern))
                    .isTrue();
            redirectTo = matcher.group(1);
            tokenServer = matcher.group(2);
        }

        request = new Request.Builder()
                .url(redirectTo)
                .build();
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.code()).isEqualTo(SC_SEE_OTHER);
            String locationHeader = response.header(LOCATION);
            assertThat(locationHeader).isNotNull();
            Pattern locationPattern = Pattern.compile(format("%s\\?(.+)", expectedRedirect));
            Matcher matcher = locationPattern.matcher(locationHeader);
            assertThat(matcher.matches())
                    .describedAs(format("Invalid location header.\nExpected: %s\nPattern: %s", expectedRedirect, locationPattern))
                    .isTrue();

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

    @Test
    public void testOAuth2Groups()
            throws Exception
    {
        testOAuth2Groups(Optional.empty());
        testOAuth2Groups(Optional.of(ImmutableSet.of()));
        testOAuth2Groups(Optional.of(ImmutableSet.of("admin", "public")));
    }

    private void testOAuth2Groups(Optional<Set<String>> groups)
            throws Exception
    {
        try (TokenServer tokenServer = new TokenServer(Optional.empty());
                TestingTrinoServer server = TestingTrinoServer.builder()
                        .setProperties(ImmutableMap.<String, String>builder()
                                .putAll(SECURE_PROPERTIES)
                                .put("web-ui.enabled", "true")
                                .put("http-server.authentication.type", "oauth2")
                                .putAll(getOAuth2Properties(tokenServer))
                                .put("deprecated.http-server.authentication.oauth2.groups-field", GROUPS_CLAIM)
                                .buildOrThrow())
                        .setAdditionalModule(oauth2Module(tokenServer))
                        .setSystemAccessControl(TestSystemAccessControl.NO_IMPERSONATION)
                        .build()) {
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
                assertThat(response.code()).isEqualTo(SC_OK);
                assertThat(response.header("user")).isEqualTo(TEST_USER);
                assertThat(response.header("principal")).isEqualTo(TEST_USER);
                assertThat(response.header("groups")).isEqualTo(groups.map(TestResource::toHeader).orElse(""));
            }

            OkHttpClient clientWithOAuthCookie = client.newBuilder()
                    .cookieJar(new CookieJar()
                    {
                        @Override
                        public void saveFromResponse(HttpUrl url, List<Cookie> cookies) {}

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
                assertThat(response.code()).isEqualTo(SC_OK);
                assertThat(response.header("user")).isEqualTo(TEST_USER);
                assertThat(response.header("principal")).isEqualTo(TEST_USER);
                assertThat(response.header("groups")).isEqualTo(groups.map(TestResource::toHeader).orElse(""));
            }
        }
    }

    @Test
    public void testJwtAndOAuth2AuthenticatorsSeparation()
            throws Exception
    {
        testJwtAndOAuth2AuthenticatorsSeparation("jwt,oauth2");
        testJwtAndOAuth2AuthenticatorsSeparation("oauth2,jwt");
    }

    private void testJwtAndOAuth2AuthenticatorsSeparation(String authenticators)
            throws Exception
    {
        TestingHttpServer jwkServer = createTestingJwkServer();
        jwkServer.start();
        try (TokenServer tokenServer = new TokenServer(Optional.of("preferred_username"));
                TestingTrinoServer server = TestingTrinoServer.builder()
                        .setProperties(
                                ImmutableMap.<String, String>builder()
                                        .putAll(SECURE_PROPERTIES)
                                        .put("http-server.authentication.type", authenticators)
                                        .put("http-server.authentication.jwt.key-file", jwkServer.getBaseUrl().toString())
                                        .put("http-server.authentication.jwt.principal-field", "sub")
                                        .putAll(getOAuth2Properties(tokenServer))
                                        .put("http-server.authentication.oauth2.principal-field", "preferred_username")
                                        .put("web-ui.enabled", "true")
                                        .buildOrThrow())
                        .setAdditionalModule(oauth2Module(tokenServer))
                        .setSystemAccessControl(TestSystemAccessControl.NO_IMPERSONATION)
                        .build()) {
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
                    .header().keyId(JWK_KEY_ID).and()
                    .subject("test-user")
                    .expiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                    .compact();

            OkHttpClient clientWithJwt = client.newBuilder()
                    .authenticator((route, response) -> response.request().newBuilder()
                            .header(AUTHORIZATION, "Bearer " + token)
                            .build())
                    .build();
            assertAuthenticationAutomatic(httpServerInfo.getHttpsUri(), clientWithJwt);
        }
    }

    @Test
    public void testJwtWithRefreshTokensForOAuth2Enabled()
            throws Exception
    {
        TestingHttpServer jwkServer = createTestingJwkServer();
        jwkServer.start();
        try (TokenServer tokenServer = new TokenServer(Optional.empty());
                TestingTrinoServer server = TestingTrinoServer.builder()
                        .setProperties(
                                ImmutableMap.<String, String>builder()
                                        .putAll(SECURE_PROPERTIES)
                                        .put("http-server.authentication.type", "oauth2,jwt")
                                        .put("http-server.authentication.jwt.key-file", jwkServer.getBaseUrl().toString())
                                        .putAll(ImmutableMap.<String, String>builder()
                                                .putAll(getOAuth2Properties(tokenServer))
                                                .put("http-server.authentication.oauth2.refresh-tokens", "true")
                                                .buildOrThrow())
                                        .put("web-ui.enabled", "true")
                                        .buildOrThrow())
                        .setAdditionalModule(oauth2Module(tokenServer))
                        .setSystemAccessControl(TestSystemAccessControl.NO_IMPERSONATION)
                        .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            assertAuthenticationDisabled(httpServerInfo.getHttpUri());

            String token = newJwtBuilder()
                    .signWith(JWK_PRIVATE_KEY)
                    .header().keyId(JWK_KEY_ID).and()
                    .subject("test-user")
                    .expiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                    .compact();

            OkHttpClient clientWithJwt = client.newBuilder()
                    .authenticator((route, response) -> response.request().newBuilder()
                            .header(AUTHORIZATION, "Bearer " + token)
                            .build())
                    .build();
            assertAuthenticationAutomatic(httpServerInfo.getHttpsUri(), clientWithJwt);
        }
    }

    @Test
    public void testResourceSecurityImpersonation()
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
                .setSystemAccessControl(TestSystemAccessControl.NO_IMPERSONATION)
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestResourceSecurity::authenticate);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            // Authenticated user TEST_USER_LOGIN impersonates impersonated-user by passing request header X-Trino-User
            Request request = new Request.Builder()
                    .url(getLocation(httpServerInfo.getHttpsUri(), "/protocol/identity"))
                    .addHeader("Authorization", Credentials.basic(TEST_USER_LOGIN, TEST_PASSWORD))
                    .addHeader("X-Trino-Original-User", TEST_USER_LOGIN)
                    .addHeader("X-Trino-User", "impersonated-user")
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertThat(response.code()).isEqualTo(SC_OK);
                assertThat(response.header("user")).isEqualTo("impersonated-user");
                assertThat(response.header("principal")).isEqualTo(TEST_USER_LOGIN);
            }
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
                .put("http-server.authentication.oauth2.oidc.discovery", "false")
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
        private static final String REFRESH_TOKEN = "REFRESH_TOKEN";
        private final String issuer = "http://example.com/";
        private final String clientId = "clientID";
        private final Date tokenExpiration = Date.from(ZonedDateTime.now().plusMinutes(5).toInstant());
        private final JwtParser jwtParser = newJwtParserBuilder().verifyWith(JWK_PUBLIC_KEY).build();
        private final Optional<String> principalField;
        private final TestingHttpServer jwkServer;
        private final String accessToken;

        public TokenServer(Optional<String> principalField)
                throws Exception
        {
            this.principalField = requireNonNull(principalField, "principalField is null");
            jwkServer = createTestingJwkServer();
            jwkServer.start();
            accessToken = issueAccessToken(Optional.empty());
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
                @Override
                public void load() {}

                @Override
                public Request createAuthorizationRequest(String state, URI callbackUri)
                {
                    return new Request(URI.create("http://example.com/authorize?" + state), Optional.of(UUID.randomUUID().toString()));
                }

                @Override
                public Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
                {
                    if (!"TEST_CODE".equals(code)) {
                        throw new IllegalArgumentException("Expected TEST_CODE");
                    }
                    return new Response(accessToken, now().plus(5, ChronoUnit.MINUTES), Optional.of(issueIdToken(nonce.map(TestResourceSecurity::hashNonce))), Optional.of(REFRESH_TOKEN));
                }

                @Override
                public Optional<Map<String, Object>> getClaims(String accessToken)
                {
                    return Optional.of(jwtParser.parseSignedClaims(accessToken).getPayload());
                }

                @Override
                public Response refreshTokens(String refreshToken)
                        throws ChallengeFailedException
                {
                    throw new UnsupportedOperationException("refresh tokens not supported");
                }

                @Override
                public Optional<URI> getLogoutEndpoint(Optional<String> idToken, URI callbackUrl)
                {
                    return Optional.empty();
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
            return REFRESH_TOKEN;
        }

        public String issueAccessToken(Optional<Set<String>> groups)
        {
            JwtBuilder accessToken = newJwtBuilder()
                    .signWith(JWK_PRIVATE_KEY)
                    .header().keyId(JWK_KEY_ID).and()
                    .issuer(issuer)
                    .audience().add(clientId).and()
                    .expiration(tokenExpiration);
            if (principalField.isPresent()) {
                accessToken.claim(principalField.get(), TEST_USER);
            }
            else {
                accessToken.subject(TEST_USER);
            }
            groups.ifPresent(groupsClaim -> accessToken.claim(GROUPS_CLAIM, groupsClaim));
            return accessToken.compact();
        }

        private String issueIdToken(Optional<String> nonceHash)
        {
            JwtBuilder idToken = newJwtBuilder()
                    .signWith(JWK_PRIVATE_KEY)
                    .header().keyId(JWK_KEY_ID).and()
                    .issuer(issuer)
                    .audience().add(clientId).and()
                    .expiration(tokenExpiration);
            if (principalField.isPresent()) {
                idToken.claim(principalField.get(), TEST_USER);
            }
            else {
                idToken.subject(TEST_USER);
            }
            nonceHash.ifPresent(nonce -> idToken.claim(NONCE, nonce));
            return idToken.compact();
        }
    }

    @jakarta.ws.rs.Path("/")
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
                    accessControl,
                    new ProtocolConfig(),
                    QueryDataEncoder.EncoderSelector.noEncoder());
        }

        @ResourceSecurity(AUTHENTICATED_USER)
        @GET
        @jakarta.ws.rs.Path("/protocol/identity")
        public jakarta.ws.rs.core.Response protocolIdentity(@Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
        {
            return echoIdentity(servletRequest, httpHeaders);
        }

        @ResourceSecurity(WEB_UI)
        @GET
        @jakarta.ws.rs.Path("/ui/api/identity")
        public jakarta.ws.rs.core.Response webUiIdentity(@Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
        {
            return echoIdentity(servletRequest, httpHeaders);
        }

        public jakarta.ws.rs.core.Response echoIdentity(HttpServletRequest servletRequest, HttpHeaders httpHeaders)
        {
            Identity identity = sessionContextFactory.extractAuthorizedIdentity(servletRequest, httpHeaders);
            return jakarta.ws.rs.core.Response.ok()
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
        Request request = new Request.Builder()
                .url(url)
                .headers(headers)
                .build();
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.code())
                    .describedAs(url)
                    .isEqualTo(expectedCode);
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

    private static HttpCookie getCookie(CookieManager cookieManager, String cookieName)
    {
        return cookieManager.getCookieStore().getCookies().stream()
                .filter(cookie -> cookie.getName().equals(cookieName))
                .findFirst()
                .orElseThrow();
    }

    private static String hashNonce(String nonce)
    {
        return sha256()
                .hashString(nonce, UTF_8)
                .toString();
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
        public void checkCanImpersonateUser(Identity identity, String userName)
        {
            if (!allowImpersonation) {
                denyImpersonateUser(identity.getUser(), userName);
            }
        }

        @Override
        public void checkCanReadSystemInformation(Identity identity)
        {
            if (!identity.getUser().equals(MANAGEMENT_USER)) {
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

        return new TestingHttpServer(httpServerInfo, nodeInfo, config, new JwkServlet());
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
    }
}
