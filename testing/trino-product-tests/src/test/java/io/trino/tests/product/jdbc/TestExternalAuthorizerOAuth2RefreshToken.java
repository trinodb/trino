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
package io.trino.tests.product.jdbc;

import io.trino.jdbc.TestingRedirectHandlerInjector;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.TpchTableResults;
import io.trino.tests.product.TestGroup;
import okhttp3.JavaNetCookieJar;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.CookieManager;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * OAuth2 authentication tests with refresh token support using JDBC.
 * <p>
 * Tests that refresh tokens work correctly:
 * - Token refresh happens automatically without user re-authentication
 * - Re-authentication required after refresh token expires
 * - Re-authentication required after internally issued token expires
 * <p>
 * Migrated from the Tempto-based TestExternalAuthorizerOAuth2RefreshToken to JUnit 5.
 */
@ProductTest
@RequiresEnvironment(JdbcOAuth2RefreshEnvironment.class)
@TestGroup.Oauth2Refresh
@TestGroup.ProfileSpecificTests
@Isolated  // These tests use TestingRedirectHandlerInjector which sets global state, so they cannot run in parallel
class TestExternalAuthorizerOAuth2RefreshToken
{
    @Test
    void shouldRefreshTokenAfterTokenExpire(JdbcOAuth2RefreshEnvironment env)
            throws SQLException, InterruptedException
    {
        CountingRedirectHandler redirectHandler = new CountingRedirectHandler(env.getHttpClient());
        TestingRedirectHandlerInjector.setRedirectHandler(redirectHandler);

        try (Connection connection = env.createTrinoConnection();
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                ResultSet results = statement.executeQuery()) {
            QueryResult result = QueryResult.forResultSet(results);
            assertThat(result).containsOnly(TpchTableResults.NATION_ROWS);

            assertThat(redirectHandler.getRedirectCount()).isEqualTo(1);

            // Wait until the token expires. See: JdbcOAuth2Environment.TTL_ACCESS_TOKEN_IN_SECONDS
            SECONDS.sleep(10);

            try (PreparedStatement repeatedStatement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                    ResultSet repeatedResults = repeatedStatement.executeQuery()) {
                QueryResult repeatedResult = QueryResult.forResultSet(repeatedResults);
                assertThat(repeatedResult).containsOnly(TpchTableResults.NATION_ROWS);
            }

            // Should still be 1 - refresh token used, no re-authentication needed
            assertThat(redirectHandler.getRedirectCount()).isEqualTo(1);
        }
    }

    @Test
    void shouldAuthenticateAfterRefreshTokenExpires(JdbcOAuth2RefreshEnvironment env)
            throws SQLException, InterruptedException
    {
        CountingRedirectHandler redirectHandler = new CountingRedirectHandler(env.getHttpClient());
        TestingRedirectHandlerInjector.setRedirectHandler(redirectHandler);

        try (Connection connection = env.createTrinoConnection();
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                ResultSet results = statement.executeQuery()) {
            QueryResult result = QueryResult.forResultSet(results);
            assertThat(result).containsOnly(TpchTableResults.NATION_ROWS);

            assertThat(redirectHandler.getRedirectCount()).isEqualTo(1);

            // Wait until the refresh token expires (15s). See: JdbcOAuth2Environment.TTL_REFRESH_TOKEN_IN_SECONDS
            SECONDS.sleep(20);

            try (PreparedStatement repeatedStatement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                    ResultSet repeatedResults = repeatedStatement.executeQuery()) {
                QueryResult repeatedResult = QueryResult.forResultSet(repeatedResults);
                assertThat(repeatedResult).containsOnly(TpchTableResults.NATION_ROWS);
            }

            // Should be 2 - refresh token expired, had to re-authenticate
            assertThat(redirectHandler.getRedirectCount()).isEqualTo(2);
        }
    }

    @Test
    void shouldAuthenticateAfterIssuedTokenExpires(JdbcOAuth2RefreshEnvironment env)
            throws SQLException, InterruptedException
    {
        CountingRedirectHandler redirectHandler = new CountingRedirectHandler(env.getHttpClient());
        TestingRedirectHandlerInjector.setRedirectHandler(redirectHandler);

        try (Connection connection = env.createTrinoConnection();
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                ResultSet results = statement.executeQuery()) {
            QueryResult result = QueryResult.forResultSet(results);
            assertThat(result).containsOnly(TpchTableResults.NATION_ROWS);

            assertThat(redirectHandler.getRedirectCount()).isEqualTo(1);

            // Wait until the internally issued token expires.
            // See: http-server.authentication.oauth2.refresh-tokens.issued-token.timeout (30s + buffer)
            SECONDS.sleep(35);

            try (PreparedStatement repeatedStatement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                    ResultSet repeatedResults = repeatedStatement.executeQuery()) {
                QueryResult repeatedResult = QueryResult.forResultSet(repeatedResults);
                assertThat(repeatedResult).containsOnly(TpchTableResults.NATION_ROWS);
            }

            // Should be 2 - issued token expired, had to re-authenticate
            assertThat(redirectHandler.getRedirectCount()).isEqualTo(2);
        }
    }

    /**
     * A redirect handler that counts how many times OAuth2 authentication was triggered.
     * Follows redirects manually since the environment's httpClient has followRedirects disabled.
     */
    private static class CountingRedirectHandler
            implements Consumer<URI>
    {
        private final OkHttpClient httpClient;
        private final AtomicInteger redirectCount = new AtomicInteger();

        CountingRedirectHandler(OkHttpClient httpClient)
        {
            this.httpClient = requireNonNull(httpClient, "httpClient is null");
        }

        @Override
        public void accept(URI uri)
        {
            redirectCount.incrementAndGet();
            // Create a fresh http client for each authentication attempt to avoid cookie/state issues
            OkHttpClient freshClient = httpClient.newBuilder()
                    .cookieJar(new JavaNetCookieJar(new CookieManager()))
                    .build();

            String currentUrl = uri.toString();
            try {
                // Follow redirects manually (up to 20 redirects for the OAuth2 flow)
                for (int i = 0; i < 20; i++) {
                    try (Response response = freshClient.newCall(
                            new Request.Builder()
                                    .get()
                                    .url(currentUrl)
                                    .build())
                            .execute()) {
                        int statusCode = response.code();
                        if (statusCode >= 300 && statusCode < 400) {
                            // Follow redirect
                            String location = response.header("Location");
                            checkState(location != null, "Redirect without Location header from %s", currentUrl);
                            currentUrl = location;
                            continue;
                        }
                        String body = response.body().string();
                        checkState(statusCode == 200, "Invalid status %s from %s: %s", statusCode, currentUrl, body);
                        checkState(body.contains("OAuth2 authentication succeeded"), "Invalid response from %s: %s", currentUrl, body);
                        return;
                    }
                }
                throw new IllegalStateException("Too many redirects, last URL: " + currentUrl);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public int getRedirectCount()
        {
            return redirectCount.get();
        }
    }
}
