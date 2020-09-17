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
package io.prestosql.server.security;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Key;
import io.airlift.http.server.HttpServerInfo;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.prestosql.plugin.base.security.AllowAllSystemAccessControl;
import io.prestosql.security.AccessControlManager;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.BasicPrincipal;
import io.prestosql.spi.security.SystemSecurityContext;
import okhttp3.Credentials;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Principal;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.prestosql.client.OkHttpUtil.setupSsl;
import static io.prestosql.client.PrestoHeaders.PRESTO_USER;
import static io.prestosql.spi.security.AccessDeniedException.denyReadSystemInformationAccess;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;

@Test
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
            .build();
    private static final String TEST_USER = "test-user";
    private static final String TEST_USER_LOGIN = TEST_USER + "@allowed";
    private static final String TEST_PASSWORD = "test-password";
    private static final String MANAGEMENT_USER = "management-user";
    private static final String MANAGEMENT_USER_LOGIN = MANAGEMENT_USER + "@allowed";
    private static final String MANAGEMENT_PASSWORD = "management-password";
    private static final String HMAC_KEY = Resources.getResource("hmac_key.txt").getPath();

    private OkHttpClient client;

    @BeforeClass
    public void setup()
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
    }

    @Test
    public void testInsecureAuthenticatorHttp()
            throws Exception
    {
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.authentication.insecure.user-mapping.pattern", ALLOWED_USER_MAPPING_PATTERN)
                        .build())
                .build()) {
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(new TestSystemAccessControl());
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertInsecureAuthentication(httpServerInfo.getHttpUri());
        }
    }

    @Test
    public void testInsecureAuthenticatorHttps()
            throws Exception
    {
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(SECURE_PROPERTIES)
                .build()) {
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(new TestSystemAccessControl());
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertInsecureAuthentication(httpServerInfo.getHttpUri());
            assertInsecureAuthentication(httpServerInfo.getHttpsUri());
        }
    }

    @Test
    public void testInsecureAuthenticatorHttpsOnly()
            throws Exception
    {
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.allow-insecure-over-http", "false")
                        .build())
                .build()) {
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(new TestSystemAccessControl());
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertAuthenticationDisabled(httpServerInfo.getHttpUri());
            assertInsecureAuthentication(httpServerInfo.getHttpsUri());
        }
    }

    @Test
    public void testPasswordAuthenticator()
            throws Exception
    {
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "password")
                        .put("http-server.authentication.password.user-mapping.pattern", ALLOWED_USER_MAPPING_PATTERN)
                        .build())
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticator(TestResourceSecurity::authenticate);
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(new TestSystemAccessControl());
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertAuthenticationDisabled(httpServerInfo.getHttpUri());
            assertPasswordAuthentication(httpServerInfo.getHttpsUri());
        }
    }

    @Test
    public void testPasswordAuthenticatorWithInsecureHttp()
            throws Exception
    {
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "password")
                        .put("http-server.authentication.allow-insecure-over-http", "true")
                        .put("http-server.authentication.password.user-mapping.pattern", ALLOWED_USER_MAPPING_PATTERN)
                        .build())
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticator(TestResourceSecurity::authenticate);
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(new TestSystemAccessControl());
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertInsecureAuthentication(httpServerInfo.getHttpUri());
            assertPasswordAuthentication(httpServerInfo.getHttpsUri());
        }
    }

    @Test
    public void testFixedManagerAuthenticatorHttpInsecureEnabledOnly()
            throws Exception
    {
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "password")
                        .put("http-server.authentication.allow-insecure-over-http", "true")
                        .put("http-server.authentication.password.user-mapping.pattern", ALLOWED_USER_MAPPING_PATTERN)
                        .put("management.user", MANAGEMENT_USER)
                        .build())
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticator(TestResourceSecurity::authenticate);
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(new TestSystemAccessControl());

            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertFixedManagementUser(httpServerInfo.getHttpUri(), true);
            assertPasswordAuthentication(httpServerInfo.getHttpsUri());
        }
    }

    @Test
    public void testFixedManagerAuthenticatorHttpInsecureDisabledOnly()
            throws Exception
    {
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "password")
                        .put("http-server.authentication.allow-insecure-over-http", "false")
                        .put("http-server.authentication.password.user-mapping.pattern", ALLOWED_USER_MAPPING_PATTERN)
                        .put("management.user", MANAGEMENT_USER)
                        .build())
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticator(TestResourceSecurity::authenticate);
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(new TestSystemAccessControl());

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
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "password")
                        .put("http-server.authentication.allow-insecure-over-http", "true")
                        .put("management.user", MANAGEMENT_USER)
                        .put("management.user.https-enabled", "true")
                        .build())
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticator(TestResourceSecurity::authenticate);
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(new TestSystemAccessControl());

            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertFixedManagementUser(httpServerInfo.getHttpUri(), true);
            assertFixedManagementUser(httpServerInfo.getHttpsUri(), false);
        }
    }

    @Test
    public void testCertAuthenticator()
            throws Exception
    {
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "certificate")
                        .put("http-server.https.truststore.path", LOCALHOST_KEYSTORE)
                        .put("http-server.https.truststore.key", "")
                        .build())
                .build()) {
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(new TestSystemAccessControl());
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
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "jwt")
                        .put("http-server.authentication.jwt.key-file", HMAC_KEY)
                        .build())
                .build()) {
            server.getInstance(Key.get(AccessControlManager.class)).addSystemAccessControl(new TestSystemAccessControl());
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            assertAuthenticationDisabled(httpServerInfo.getHttpUri());

            String hmac = Files.readString(Paths.get(HMAC_KEY));
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
            assertAuthenticationAutomatic(httpServerInfo.getHttpsUri(), clientWithJwt);
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
        // public
        assertOk(client, getPublicLocation(baseUri));
        // authorized user
        assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_UNAUTHORIZED);
        assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_UNAUTHORIZED, TEST_USER_LOGIN, null);
        assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_UNAUTHORIZED, TEST_USER_LOGIN, "invalid");
        assertResponseCode(client, getAuthorizedUserLocation(baseUri), SC_OK, TEST_USER_LOGIN, TEST_PASSWORD);
        // management
        assertResponseCode(client, getManagementLocation(baseUri), SC_UNAUTHORIZED);
        assertResponseCode(client, getManagementLocation(baseUri), SC_UNAUTHORIZED, TEST_USER_LOGIN, null);
        assertResponseCode(client, getManagementLocation(baseUri), SC_UNAUTHORIZED, TEST_USER_LOGIN, "invalid");
        assertResponseCode(client, getManagementLocation(baseUri), SC_FORBIDDEN, TEST_USER_LOGIN, TEST_PASSWORD);
        assertResponseCode(client, getManagementLocation(baseUri), SC_UNAUTHORIZED, MANAGEMENT_USER_LOGIN, null);
        assertResponseCode(client, getManagementLocation(baseUri), SC_UNAUTHORIZED, MANAGEMENT_USER_LOGIN, "invalid");
        assertResponseCode(client, getManagementLocation(baseUri), SC_OK, MANAGEMENT_USER_LOGIN, MANAGEMENT_PASSWORD);
        // internal
        assertResponseCode(client, getInternalLocation(baseUri), SC_FORBIDDEN);
        assertResponseCode(client, getInternalLocation(baseUri), SC_FORBIDDEN, TEST_USER_LOGIN, TEST_PASSWORD);
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
        assertResponseCode(authorizedClient, getManagementLocation(baseUri), SC_OK, Headers.of(PRESTO_USER, MANAGEMENT_USER));
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
            assertEquals(response.code(), expectedCode, url);
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

    private static class TestSystemAccessControl
            extends AllowAllSystemAccessControl
    {
        @Override
        public void checkCanReadSystemInformation(SystemSecurityContext context)
        {
            if (!context.getIdentity().getUser().equals(MANAGEMENT_USER)) {
                denyReadSystemInformationAccess();
            }
        }
    }
}
