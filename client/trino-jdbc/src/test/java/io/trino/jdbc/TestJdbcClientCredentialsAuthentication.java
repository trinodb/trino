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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Inject;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logging;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.Authenticator;
import io.trino.server.security.SecurityConfig;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.google.common.io.Resources.getResource;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.server.security.ServerSecurityModule.authenticatorModule;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestJdbcClientCredentialsAuthentication
{
    private static final String TEST_CATALOG = "test_catalog";
    private static final String TOKEN_PATH = "/token";
    private static final String VALID_CLIENT_ID = "test-client";
    private static final String VALID_CLIENT_SECRET = "test-secret";
    private static final String ACCESS_TOKEN = "my-access-token";

    private MockWebServer idpServer;

    private TestingTrinoServer trinoServer;
    private TokenStore tokenStore;

    @BeforeAll
    public void setup()
            throws Exception
    {
        Logging.initialize();

        tokenStore = new TokenStore();
        tokenStore.issue(ACCESS_TOKEN);

        trinoServer = TestingTrinoServer.builder()
                .setAdditionalModule(new BearerTokenAuthModule(tokenStore, () -> idpServerBaseUrl()))
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.authentication.type", "bearer-token")
                        .put("http-server.https.enabled", "true")
                        .put("http-server.https.keystore.path", new File(getResource("localhost.keystore").toURI()).getPath())
                        .put("http-server.https.keystore.key", "changeit")
                        .put("web-ui.enabled", "false")
                        .buildOrThrow())
                .build();
        trinoServer.installPlugin(new TpchPlugin());
        trinoServer.createCatalog(TEST_CATALOG, "tpch");
    }

    @BeforeEach
    public void beforeEach()
            throws Exception
    {
        idpServer = new MockWebServer();
        idpServer.start();

        tokenStore.invalidateAll();
        tokenStore.issue(ACCESS_TOKEN);
    }

    @AfterEach
    public void afterEach()
            throws Exception
    {
        idpServer.close();
        idpServer = null;
    }

    @AfterAll
    public void teardown()
            throws Exception
    {
        closeAll(trinoServer);
        trinoServer = null;
    }

    @Test
    public void testSuccessfulQuery()
            throws Exception
    {
        enqueueToken(ACCESS_TOKEN, 3600);

        try (Connection connection = createConnection();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT 1")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }

        assertThat(idpServer.getRequestCount()).isEqualTo(1);
    }

    @Test
    public void testTokenIsCachedAcrossStatements()
            throws Exception
    {
        enqueueToken(ACCESS_TOKEN, 3600);

        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("SELECT 1");
            statement.execute("SELECT 2");
        }

        assertThat(idpServer.getRequestCount()).isEqualTo(1);
    }

    @Test
    public void testTokenRefreshedAfterServerRejects()
            throws Exception
    {
        enqueueToken(ACCESS_TOKEN, 3600);

        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("SELECT 1")).isTrue();

            tokenStore.invalidate(ACCESS_TOKEN);
            String rotatedToken = "rotated-token";
            tokenStore.issue(rotatedToken);
            enqueueToken(rotatedToken, 3600);

            assertThat(statement.execute("SELECT 2")).isTrue();
        }

        assertThat(idpServer.getRequestCount()).isEqualTo(2);
    }

    @Test
    public void testQueryFailsWhenAllTokensRevoked()
            throws Exception
    {
        enqueueToken(ACCESS_TOKEN, 3600);

        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT 1")) {
                assertThat(rs.next()).isTrue();
            }

            tokenStore.invalidateAll();
            enqueueToken(ACCESS_TOKEN, 3600);

            assertThatThrownBy(() -> statement.execute("SELECT 1"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("Authentication failed");
        }
    }

    @Test
    public void testMissingClientIdThrowsSqlException()
    {
        assertThatThrownBy(() -> {
            Properties props = baseProperties();
            props.setProperty("oauth2ClientSecret", VALID_CLIENT_SECRET);
            DriverManager.getConnection(trinoUrl(), props);
        }).isInstanceOf(SQLException.class);
    }

    @Test
    public void testMissingClientSecretThrowsSqlException()
    {
        assertThatThrownBy(() -> {
            Properties props = baseProperties();
            props.setProperty("oauth2ClientId", VALID_CLIENT_ID);
            DriverManager.getConnection(trinoUrl(), props);
        }).isInstanceOf(SQLException.class);
    }

    private String trinoUrl()
    {
        return format("jdbc:trino://localhost:%s", trinoServer.getHttpsAddress().getPort());
    }

    private String idpServerBaseUrl()
    {
        return "http://" + idpServer.getHostName() + ":" + idpServer.getPort();
    }

    private Properties baseProperties()
            throws Exception
    {
        Properties props = new Properties();
        props.setProperty("SSL", "true");
        props.setProperty("SSLTrustStorePath", new File(getResource("localhost.truststore").toURI()).getPath());
        props.setProperty("SSLTrustStorePassword", "changeit");
        return props;
    }

    private Connection createConnection()
            throws Exception
    {
        Properties props = baseProperties();
        props.setProperty("oauth2ClientId", VALID_CLIENT_ID);
        props.setProperty("oauth2ClientSecret", VALID_CLIENT_SECRET);
        return DriverManager.getConnection(trinoUrl(), props);
    }

    private void enqueueToken(String accessToken, long expiresIn)
    {
        idpServer.enqueue(new MockResponse.Builder()
                .code(HTTP_OK)
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .body(format("{\"access_token\":\"%s\",\"token_type\":\"Bearer\",\"expires_in\":%d}", accessToken, expiresIn))
                .build());
    }

    static class TokenStore
    {
        private final Set<String> validTokens = ConcurrentHashMap.newKeySet();

        public void issue(String token)
        {
            validTokens.add(token);
        }

        public boolean isValid(String token)
        {
            return validTokens.contains(token);
        }

        public void invalidate(String token)
        {
            validTokens.remove(token);
        }

        public void invalidateAll()
        {
            validTokens.clear();
        }
    }

    private static final class BearerTokenAuthModule
            extends AbstractConfigurationAwareModule
    {
        private final TokenStore tokenStore;
        private final Supplier<String> idpBaseUrl;

        BearerTokenAuthModule(TokenStore tokenStore, Supplier<String> idpBaseUrl)
        {
            this.tokenStore = requireNonNull(tokenStore, "tokenStore is null");
            this.idpBaseUrl = requireNonNull(idpBaseUrl, "idpBaseUrl is null");
        }

        @Override
        protected void setup(Binder binder)
        {
            install(authenticatorModule(
                    buildConfigObject(SecurityConfig.class),
                    "bearer-token",
                    BearerTokenAuthenticator.class,
                    b -> {
                        b.bind(TokenStore.class).toInstance(tokenStore);
                        b.bind(IdpBaseUrl.class).toInstance(new IdpBaseUrl(idpBaseUrl));
                    }));
        }
    }

    static final class IdpBaseUrl
    {
        private final Supplier<String> supplier;

        IdpBaseUrl(Supplier<String> supplier)
        {
            this.supplier = requireNonNull(supplier, "supplier is null");
        }

        String get()
        {
            return supplier.get();
        }
    }

    public static class BearerTokenAuthenticator
            implements Authenticator
    {
        private final TokenStore tokenStore;
        private final IdpBaseUrl idpBaseUrl;

        @Inject
        public BearerTokenAuthenticator(TokenStore tokenStore, IdpBaseUrl idpBaseUrl)
        {
            this.tokenStore = requireNonNull(tokenStore, "tokenStore is null");
            this.idpBaseUrl = requireNonNull(idpBaseUrl, "idpBaseUrl is null");
        }

        @Override
        public Identity authenticate(ContainerRequestContext request)
                throws AuthenticationException
        {
            List<String> headers = request.getHeaders().getOrDefault("Authorization", ImmutableList.of());
            for (String header : headers) {
                if (header.startsWith("Bearer ")) {
                    String token = header.substring("Bearer ".length());
                    if (tokenStore.isValid(token)) {
                        return Identity.ofUser("test-user");
                    }
                }
            }
            throw new AuthenticationException(
                    "Authentication required",
                    format("Bearer x_token_endpoint=\"%s\", scope=\"openid\"", idpBaseUrl.get() + TOKEN_PATH));
        }
    }
}
