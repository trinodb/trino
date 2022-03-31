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
package io.trino.jdbc;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logging;
import io.airlift.security.pem.PemReader;
import io.jsonwebtoken.security.Keys;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.security.Key;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.io.Files.asCharSource;
import static com.google.common.io.Resources.getResource;
import static io.jsonwebtoken.JwsHeader.KEY_ID;
import static io.jsonwebtoken.SignatureAlgorithm.HS512;
import static io.jsonwebtoken.security.Keys.hmacShaKeyFor;
import static io.trino.server.security.jwt.JwtUtil.newJwtBuilder;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Base64.getMimeDecoder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestTrinoDriverAuth
{
    private static final String TEST_CATALOG = "test_catalog";
    private TestingTrinoServer server;
    private Key defaultKey;
    private Key hmac222;
    private PrivateKey privateKey33;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();

        URL resource = getClass().getClassLoader().getResource("33.privateKey");
        assertNotNull(resource, "key directory not found");
        File keyDir = new File(resource.toURI()).getAbsoluteFile().getParentFile();

        defaultKey = hmacShaKeyFor(getMimeDecoder().decode(asCharSource(new File(keyDir, "default-key.key"), US_ASCII).read().getBytes(US_ASCII)));
        hmac222 = hmacShaKeyFor(getMimeDecoder().decode(asCharSource(new File(keyDir, "222.key"), US_ASCII).read().getBytes(US_ASCII)));
        privateKey33 = PemReader.loadPrivateKey(new File(keyDir, "33.privateKey"), Optional.empty());

        server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.authentication.type", "JWT")
                        .put("http-server.authentication.jwt.key-file", new File(keyDir, "${KID}.key").getPath())
                        .put("http-server.https.enabled", "true")
                        .put("http-server.https.keystore.path", new File(getResource("localhost.keystore").toURI()).getPath())
                        .put("http-server.https.keystore.key", "changeit")
                        .buildOrThrow())
                .build();
        server.installPlugin(new TpchPlugin());
        server.createCatalog(TEST_CATALOG, "tpch");
        server.waitForNodeRefresh(Duration.ofSeconds(10));
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        server.close();
    }

    @Test
    public void testSuccessDefaultKey()
            throws Exception
    {
        String accessToken = newJwtBuilder()
                .setSubject("test")
                .signWith(defaultKey)
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken))) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123"));
                ResultSet rs = statement.getResultSet();
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 123);
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testSuccessHmac()
            throws Exception
    {
        String accessToken = newJwtBuilder()
                .setSubject("test")
                .setHeaderParam(KEY_ID, "222")
                .signWith(hmac222)
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken))) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123"));
                ResultSet rs = statement.getResultSet();
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 123);
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testSuccessPublicKey()
            throws Exception
    {
        String accessToken = newJwtBuilder()
                .setSubject("test")
                .setHeaderParam(KEY_ID, "33")
                .signWith(privateKey33)
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken))) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123"));
                ResultSet rs = statement.getResultSet();
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 123);
                assertFalse(rs.next());
            }
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Authentication failed: Unauthorized")
    public void testFailedNoToken()
            throws Exception
    {
        try (Connection connection = createConnection(ImmutableMap.of())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT 123");
            }
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Authentication failed: Unsigned Claims JWTs are not supported.")
    public void testFailedUnsigned()
            throws Exception
    {
        String accessToken = newJwtBuilder()
                .setSubject("test")
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken))) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT 123");
            }
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Authentication failed: JWT signature does not match.*")
    public void testFailedBadHmacSignature()
            throws Exception
    {
        Key badKey = Keys.secretKeyFor(HS512);
        String accessToken = newJwtBuilder()
                .setSubject("test")
                .signWith(badKey)
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken))) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT 123");
            }
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Authentication failed: JWT signature does not match.*")
    public void testFailedWrongPublicKey()
            throws Exception
    {
        String accessToken = newJwtBuilder()
                .setSubject("test")
                .setHeaderParam(KEY_ID, "42")
                .signWith(privateKey33)
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken))) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT 123");
            }
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Authentication failed: Unknown signing key ID")
    public void testFailedUnknownPublicKey()
            throws Exception
    {
        String accessToken = newJwtBuilder()
                .setSubject("test")
                .setHeaderParam(KEY_ID, "unknown")
                .signWith(privateKey33)
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken))) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT 123");
            }
        }
    }

    @Test
    public void testSuccessFullSslVerification()
            throws Exception
    {
        String accessToken = newJwtBuilder()
                .setSubject("test")
                .setHeaderParam(KEY_ID, "33")
                .signWith(privateKey33)
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken, "SSLVerification", "FULL"))) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123"));
                ResultSet rs = statement.getResultSet();
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 123);
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testSuccessCaSslVerification()
            throws Exception
    {
        String accessToken = newJwtBuilder()
                .setSubject("test")
                .setHeaderParam(KEY_ID, "33")
                .signWith(privateKey33)
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken, "SSLVerification", "CA"))) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123"));
                ResultSet rs = statement.getResultSet();
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 123);
                assertFalse(rs.next());
            }
        }
    }

    // TODO: testSuccessCaSslVerificationMismatchedHostnameValidCA()

    // TODO: testSuccessNoneSslVerificationMismatchedHostname()

    // TODO: testSuccessNoneSslVerificationInvalidCA()

    @Test
    public void testFailedFullSslVerificationWithoutSSL()
    {
        assertThatThrownBy(() -> createBasicConnection(ImmutableMap.of("SSLVerification", "FULL")))
                .isInstanceOf(SQLException.class)
                .hasMessage("Connection property 'SSLVerification' is not allowed");
    }

    // TODO: testFailedFullSslVerificationMismatchedHostname()

    // TODO: testFailedFullSslVerificationInvalidCA()

    @Test
    public void testFailedCaSslVerificationWithoutSSL()
    {
        assertThatThrownBy(() -> createBasicConnection(ImmutableMap.of("SSLVerification", "CA")))
                .isInstanceOf(SQLException.class)
                .hasMessage("Connection property 'SSLVerification' is not allowed");
    }

    // TODO: testFailedCaSslVerificationInvalidCA()

    @Test
    public void testFailedNoneSslVerificationWithSSL()
    {
        assertThatThrownBy(() -> createConnection(ImmutableMap.of("SSLVerification", "NONE")))
                .isInstanceOf(SQLException.class)
                .hasMessage("Connection property 'SSLTrustStorePath' is not allowed");
    }

    @Test
    public void testFailedNoneSslVerificationWithSSLUnsigned()
            throws Exception
    {
        Connection connection = createBasicConnection(ImmutableMap.of("SSL", "true", "SSLVerification", "NONE"));
        Statement statement = connection.createStatement();
        assertThatThrownBy(() -> statement.execute("SELECT 123"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Authentication failed: Unauthorized");
    }

    private Connection createConnection(Map<String, String> additionalProperties)
            throws Exception
    {
        String url = format("jdbc:trino://localhost:%s", server.getHttpsAddress().getPort());
        Properties properties = new Properties();
        properties.setProperty("user", "test");
        properties.setProperty("SSL", "true");
        properties.setProperty("SSLTrustStorePath", new File(getResource("localhost.truststore").toURI()).getPath());
        properties.setProperty("SSLTrustStorePassword", "changeit");
        additionalProperties.forEach(properties::setProperty);
        return DriverManager.getConnection(url, properties);
    }

    private Connection createBasicConnection(Map<String, String> additionalProperties)
            throws SQLException
    {
        String url = format("jdbc:trino://localhost:%s", server.getHttpsAddress().getPort());
        Properties properties = new Properties();
        properties.setProperty("user", "test");
        additionalProperties.forEach(properties::setProperty);
        return DriverManager.getConnection(url, properties);
    }
}
