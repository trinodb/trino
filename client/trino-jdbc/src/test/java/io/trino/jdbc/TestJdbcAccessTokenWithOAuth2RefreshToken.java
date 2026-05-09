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
import com.google.inject.Key;
import io.airlift.log.Logging;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.security.oauth2.ChallengeFailedException;
import io.trino.server.security.oauth2.OAuth2Client;
import io.trino.server.security.oauth2.TokenPairSerializer;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.io.Resources.getResource;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.testing.Closeables.closeAll;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Tests that JDBC connections using the {@code accessToken} property transparently refresh
 * expired composite tokens (JWE tokens containing both an access token and a refresh token)
 * when the server supports OAuth2 refresh tokens.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestJdbcAccessTokenWithOAuth2RefreshToken
{
    private static final String TEST_CATALOG = "test_catalog";
    private static final String REFRESH_TOKEN = "test-refresh-token";
    private static final int ACCESS_TOKEN_TTL_SECONDS = 2;

    private TestingTrinoServer server;
    private RefreshableOAuth2ClientStub oauthClient;

    @BeforeAll
    public void setup()
            throws Exception
    {
        Logging.initialize();

        oauthClient = new RefreshableOAuth2ClientStub();

        server = TestingTrinoServer.builder()
                .setAdditionalModule(binder -> newOptionalBinder(binder, OAuth2Client.class)
                        .setBinding()
                        .toInstance(oauthClient))
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.https.enabled", "true")
                        .put("http-server.https.keystore.path", new File(getResource("localhost.keystore").toURI()).getPath())
                        .put("http-server.https.keystore.key", "changeit")
                        .put("web-ui.enabled", "false")
                        .put("http-server.authentication.type", "oauth2")
                        // Required OAuth2 config properties (URLs are never fetched because OAuth2Client is overridden)
                        .put("http-server.authentication.oauth2.state-key", "test-state-key")
                        .put("http-server.authentication.oauth2.issuer", "http://example.com/")
                        .put("http-server.authentication.oauth2.auth-url", "http://example.com/")
                        .put("http-server.authentication.oauth2.token-url", "http://example.com/")
                        .put("http-server.authentication.oauth2.jwks-url", "http://example.com/jwks")
                        .put("http-server.authentication.oauth2.client-id", "test-client")
                        .put("http-server.authentication.oauth2.client-secret", "test-secret")
                        .put("http-server.authentication.oauth2.oidc.discovery", "false")
                        // Enable refresh tokens with a composite token TTL that exceeds the test duration
                        .put("http-server.authentication.oauth2.refresh-tokens", "true")
                        .put("http-server.authentication.oauth2.refresh-tokens.issued-token.timeout", "30s")
                        .buildOrThrow())
                .build();
        server.installPlugin(new TpchPlugin());
        server.createCatalog(TEST_CATALOG, "tpch");
    }

    @AfterAll
    public void teardown()
            throws Exception
    {
        closeAll(server);
        server = null;
    }

    @Test
    public void testAccessTokenWithRefreshTokenTransparentlyRefreshedAfterExpiry()
            throws Exception
    {
        // Create a composite JWE token (containing both an access token and a refresh token) with
        // a short access-token TTL (ACCESS_TOKEN_TTL_SECONDS), simulating the token a JDBC client
        // would receive from the server's x_token_server endpoint after a successful OAuth2 flow.
        String initialAccessToken = oauthClient.issueAccessToken();
        TokenPairSerializer serializer = server.getInstance(Key.get(TokenPairSerializer.class));
        String compositeToken = serializer.serializeTokenPair(
                initialAccessToken,
                Date.from(Instant.now().plusSeconds(ACCESS_TOKEN_TTL_SECONDS)),
                REFRESH_TOKEN);

        try (Connection connection = createConnection(compositeToken);
                Statement statement = connection.createStatement()) {
            // First query succeeds while the access token is still valid
            assertThat(statement.execute("SELECT 1")).isTrue();

            // Wait until the access token inside the composite token has expired
            Thread.sleep((ACCESS_TOKEN_TTL_SECONDS + 1) * 1000L);

            // Second query must still succeed: the server detects the expired access token,
            // uses the embedded refresh token to obtain a new one, and returns a 401 with
            // an x_token_server challenge. The JDBC client transparently polls for the
            // refreshed composite token and retries the request.
            assertThat(statement.execute("SELECT 1")).isTrue();
        }
    }

    @Test
    public void testAccessTokenFailsAfterRefreshTokenExpires()
            throws Exception
    {
        // Create a composite token with an invalid refresh token. When the access token expires
        // the server's TokenRefresher catches the ChallengeFailedException from refreshTokens()
        // and stores a token-exchange error. The JDBC client polls the x_token_server endpoint,
        // receives the error response, and must surface it as a SQLException rather than hanging.
        String initialAccessToken = oauthClient.issueAccessToken();
        TokenPairSerializer serializer = server.getInstance(Key.get(TokenPairSerializer.class));
        String compositeToken = serializer.serializeTokenPair(
                initialAccessToken,
                Date.from(Instant.now().plusSeconds(ACCESS_TOKEN_TTL_SECONDS)),
                "expired-refresh-token");

        try (Connection connection = createConnection(compositeToken);
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("SELECT 1")).isTrue();

            Thread.sleep((ACCESS_TOKEN_TTL_SECONDS + 1) * 1000L);

            // Server can no longer refresh the token and requires full re-authorization.
            // The client must fail with authentication error rather than hang.
            assertThatThrownBy(() -> statement.execute("SELECT 1"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("Token refreshing has failed: invalid refresh token");
        }
    }

    private Connection createConnection(String accessToken)
            throws Exception
    {
        String url = format("jdbc:trino://localhost:%s", server.getHttpsAddress().getPort());
        Properties properties = new Properties();
        properties.setProperty("SSL", "true");
        properties.setProperty("SSLTrustStorePath", new File(getResource("localhost.truststore").toURI()).getPath());
        properties.setProperty("SSLTrustStorePassword", "changeit");
        properties.setProperty("accessToken", accessToken);
        return DriverManager.getConnection(url, properties);
    }

    /**
     * OAuth2Client stub that issues opaque access tokens and supports refresh token exchange.
     * Tokens are tracked in memory; {@link #getAccessTokenClaims} validates by membership check.
     */
    static class RefreshableOAuth2ClientStub
            implements OAuth2Client
    {
        private final Set<String> validTokens = ConcurrentHashMap.newKeySet();

        /** Issues a fresh access token, registers it as valid, and returns its string value. */
        public String issueAccessToken()
        {
            String token = UUID.randomUUID().toString();
            validTokens.add(token);
            return token;
        }

        @Override
        public void load() {}

        @Override
        public Request createAuthorizationRequest(String state, URI callbackUri)
        {
            return new Request(URI.create("http://example.com/authorize"), Optional.empty());
        }

        @Override
        public Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
        {
            String token = issueAccessToken();
            return new Response(token, Instant.now().plusSeconds(ACCESS_TOKEN_TTL_SECONDS), Optional.empty(), Optional.of(REFRESH_TOKEN));
        }

        @Override
        public Optional<Map<String, Object>> getAccessTokenClaims(String accessToken)
        {
            if (validTokens.contains(accessToken)) {
                return Optional.of(ImmutableMap.of("sub", "test-user"));
            }
            return Optional.empty();
        }

        @Override
        public Response refreshTokens(String refreshToken)
                throws ChallengeFailedException
        {
            if (REFRESH_TOKEN.equals(refreshToken)) {
                String newToken = issueAccessToken();
                // Refreshed token gets a generous TTL so subsequent queries do not re-trigger refresh
                return new Response(newToken, Instant.now().plusSeconds(30), Optional.empty(), Optional.of(REFRESH_TOKEN));
            }
            throw new ChallengeFailedException("invalid refresh token");
        }

        @Override
        public Optional<URI> getLogoutEndpoint(Optional<String> idToken, URI callbackUrl)
        {
            return Optional.empty();
        }
    }
}
