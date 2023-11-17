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
import io.jsonwebtoken.Jwts;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import javax.crypto.SecretKey;

import java.io.File;
import java.net.URL;
import java.security.Key;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.io.Files.asCharSource;
import static com.google.common.io.Resources.getResource;
import static io.jsonwebtoken.security.Keys.hmacShaKeyFor;
import static io.trino.server.security.jwt.JwtUtil.newJwtBuilder;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Base64.getMimeDecoder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestTrinoDriverAuth
{
    private static final String TEST_CATALOG = "test_catalog";
    private TestingTrinoServer server;
    private Key defaultKey;
    private Key hmac222;
    private PrivateKey privateKey33;

    @BeforeAll
    public void setup()
            throws Exception
    {
        Logging.initialize();

        URL resource = getClass().getClassLoader().getResource("33.privateKey");
        assertThat(resource)
                .describedAs("key directory not found")
                .isNotNull();
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
    }

    @AfterAll
    public void teardown()
            throws Exception
    {
        server.close();
        server = null;
    }

    @Test
    public void testSuccessDefaultKey()
            throws Exception
    {
        String accessToken = newJwtBuilder()
                .subject("test")
                .signWith(defaultKey)
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken));
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("SELECT 123")).isTrue();
            ResultSet rs = statement.getResultSet();
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isEqualTo(123);
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    public void testSuccessHmac()
            throws Exception
    {
        String accessToken = newJwtBuilder()
                .subject("test")
                .header().keyId("222")
                .and()
                .signWith(hmac222)
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken));
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("SELECT 123")).isTrue();
            ResultSet rs = statement.getResultSet();
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isEqualTo(123);
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    public void testSuccessPublicKey()
            throws Exception
    {
        String accessToken = newJwtBuilder()
                .subject("test")
                .header().keyId("33")
                .and()
                .signWith(privateKey33)
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken));
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("SELECT 123")).isTrue();
            ResultSet rs = statement.getResultSet();
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isEqualTo(123);
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    public void testFailedNoToken()
    {
        assertThatThrownBy(() -> {
            try (Connection connection = createConnection(ImmutableMap.of());
                    Statement statement = connection.createStatement()) {
                statement.execute("SELECT 123");
            }
        })
                .isInstanceOf(SQLException.class)
                .hasMessage("Authentication failed: Unauthorized");
    }

    @Test
    public void testFailedUnsigned()
            throws Exception
    {
        String accessToken = newJwtBuilder()
                .setSubject("test")
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken));
                Statement statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.execute("SELECT 123"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("Authentication failed: Unsecured JWSs (those with an 'alg' (Algorithm) header value of 'none') are disallowed by default");
        }
    }

    @Test
    public void testFailedBadHmacSignature()
            throws Exception
    {
        SecretKey badKey = Jwts.SIG.HS512.key().build();
        String accessToken = newJwtBuilder()
                .subject("test")
                .signWith(badKey)
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken));
                Statement statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.execute("SELECT 123"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("Authentication failed: JWT signature does not match locally computed signature. JWT validity cannot be asserted and should not be trusted.");
        }
    }

    @Test
    public void testFailedWrongPublicKey()
    {
        assertThatThrownBy(() -> {
            String accessToken = newJwtBuilder()
                    .subject("test")
                    .header().keyId("42")
                    .and()
                    .signWith(privateKey33)
                    .compact();

            try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken));
                    Statement statement = connection.createStatement()) {
                statement.execute("SELECT 123");
            }
        })
                .isInstanceOf(SQLException.class)
                .hasMessageMatching("Authentication failed: JWT signature does not match.*");
    }

    @Test
    public void testFailedUnknownPublicKey()
    {
        assertThatThrownBy(() -> {
            String accessToken = newJwtBuilder()
                    .subject("test")
                    .header().keyId("unknown")
                    .and()
                    .signWith(privateKey33)
                    .compact();

            try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken));
                    Statement statement = connection.createStatement()) {
                statement.execute("SELECT 123");
            }
        })
                .isInstanceOf(SQLException.class)
                .hasMessage("Authentication failed: Unknown signing key ID");
    }

    @Test
    public void testSuccessFullSslVerification()
            throws Exception
    {
        String accessToken = newJwtBuilder()
                .subject("test")
                .header().keyId("33")
                .and()
                .signWith(privateKey33)
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken, "SSLVerification", "FULL"));
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("SELECT 123")).isTrue();
            ResultSet rs = statement.getResultSet();
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isEqualTo(123);
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    public void testSuccessFullSslVerificationAlternateHostname()
            throws Exception
    {
        String accessToken = newJwtBuilder()
                .subject("test")
                .header().keyId("33")
                .and()
                .signWith(privateKey33)
                .compact();

        String url = format("jdbc:trino://127.0.0.1:%s", server.getHttpsAddress().getPort());
        Properties properties = new Properties();
        properties.setProperty("SSL", "true");
        properties.setProperty("SSLVerification", "FULL");
        properties.setProperty("SSLTrustStorePath", new File(getResource("localhost.truststore").toURI()).getPath());
        properties.setProperty("SSLTrustStorePassword", "changeit");
        properties.setProperty("accessToken", accessToken);
        properties.setProperty("hostnameInCertificate", "localhost");

        try (Connection connection = DriverManager.getConnection(url, properties);
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("SELECT 123")).isTrue();
            ResultSet rs = statement.getResultSet();
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isEqualTo(123);
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    public void testFailedFullSslVerificationAlternateHostnameNotProvided()
            throws Exception
    {
        String accessToken = newJwtBuilder()
                .subject("test")
                .header().keyId("33")
                .and()
                .signWith(privateKey33)
                .compact();

        String url = format("jdbc:trino://127.0.0.1:%s", server.getHttpsAddress().getPort());
        Properties properties = new Properties();
        properties.setProperty("SSL", "true");
        properties.setProperty("SSLVerification", "FULL");
        properties.setProperty("SSLTrustStorePath", new File(getResource("localhost.truststore").toURI()).getPath());
        properties.setProperty("SSLTrustStorePassword", "changeit");
        properties.setProperty("accessToken", accessToken);

        try (Connection connection = DriverManager.getConnection(url, properties);
                Statement statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.execute("SELECT 123"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("Error executing query: javax.net.ssl.SSLPeerUnverifiedException: Hostname 127.0.0.1 not verified");
        }
    }

    @Test
    public void testFailedCaSslVerificationAlternateHostname()
    {
        String accessToken = newJwtBuilder()
                .subject("test")
                .header().keyId("33")
                .and()
                .signWith(privateKey33)
                .compact();

        String url = format("jdbc:trino://127.0.0.1:%s", server.getHttpsAddress().getPort());
        Properties properties = new Properties();
        properties.setProperty("SSL", "true");
        properties.setProperty("SSLVerification", "CA");
        properties.setProperty("accessToken", accessToken);
        properties.setProperty("hostnameInCertificate", "localhost");

        assertThatThrownBy(() -> DriverManager.getConnection(url, properties))
                .isInstanceOf(SQLException.class)
                .hasMessage("Connection property hostnameInCertificate requires SSLVerification to be set to FULL");
    }

    @Test
    public void testFailedNoneSslVerificationAlternateHostname()
    {
        String accessToken = newJwtBuilder()
                .subject("test")
                .header().keyId("33")
                .and()
                .signWith(privateKey33)
                .compact();

        String url = format("jdbc:trino://127.0.0.1:%s", server.getHttpsAddress().getPort());
        Properties properties = new Properties();
        properties.setProperty("SSL", "true");
        properties.setProperty("SSLVerification", "NONE");
        properties.setProperty("accessToken", accessToken);
        properties.setProperty("hostnameInCertificate", "localhost");

        assertThatThrownBy(() -> DriverManager.getConnection(url, properties))
                .isInstanceOf(SQLException.class)
                .hasMessage("Connection property hostnameInCertificate requires SSLVerification to be set to FULL");
    }

    @Test
    public void testSuccessCaSslVerification()
            throws Exception
    {
        String accessToken = newJwtBuilder()
                .subject("test")
                .header().keyId("33")
                .and()
                .signWith(privateKey33)
                .compact();

        try (Connection connection = createConnection(ImmutableMap.of("accessToken", accessToken, "SSLVerification", "CA"));
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("SELECT 123")).isTrue();
            ResultSet rs = statement.getResultSet();
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isEqualTo(123);
            assertThat(rs.next()).isFalse();
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
                .hasMessage("Connection property SSLVerification requires TLS/SSL to be enabled");
    }

    // TODO: testFailedFullSslVerificationMismatchedHostname()

    // TODO: testFailedFullSslVerificationInvalidCA()

    @Test
    public void testFailedCaSslVerificationWithoutSSL()
    {
        assertThatThrownBy(() -> createBasicConnection(ImmutableMap.of("SSLVerification", "CA")))
                .isInstanceOf(SQLException.class)
                .hasMessage("Connection property SSLVerification requires TLS/SSL to be enabled");
    }

    // TODO: testFailedCaSslVerificationInvalidCA()

    @Test
    public void testFailedNoneSslVerificationWithSSL()
    {
        assertThatThrownBy(() -> createConnection(ImmutableMap.of("SSLVerification", "NONE")))
                .isInstanceOf(SQLException.class)
                .hasMessage("Connection property SSLTrustStorePath cannot be set if SSLVerification is set to NONE");
    }

    @Test
    public void testFailedNoneSslVerificationWithSSLUnsigned()
            throws Exception
    {
        try (Connection connection = createBasicConnection(ImmutableMap.of("SSL", "true", "SSLVerification", "NONE"));
                Statement statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.execute("SELECT 123"))
                    .isInstanceOf(SQLException.class)
                    .hasMessage("Authentication failed: Unauthorized");
        }
    }

    private Connection createConnection(Map<String, String> additionalProperties)
            throws Exception
    {
        String url = format("jdbc:trino://localhost:%s", server.getHttpsAddress().getPort());
        Properties properties = new Properties();
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
        additionalProperties.forEach(properties::setProperty);
        return DriverManager.getConnection(url, properties);
    }
}
