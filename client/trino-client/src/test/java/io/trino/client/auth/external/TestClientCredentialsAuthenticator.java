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
package io.trino.client.auth.external;

import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import mockwebserver3.junit5.StartStop;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestClientCredentialsAuthenticator
{
    private static final String TOKEN_PATH = "/token";
    private static final String TRINO_PATH = "/v1/statement";

    @StartStop
    private final MockWebServer idpServer = new MockWebServer();

    @StartStop
    private final MockWebServer trinoServer = new MockWebServer();

    @Test
    public void testSuccessfulTokenFetchAndInjection()
            throws Exception
    {
        // Enqueue responses
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", fullChallenge())
                .build());
        enqueueToken("initial-token", 3600);
        trinoServer.enqueue(ok());

        // Execute
        OkHttpClient client = buildTrinoClient();
        Response response = client.newCall(trinoRequest()).execute();
        response.close();
        assertThat(response.code()).isEqualTo(HTTP_OK);

        // Verify IdP server
        assertThat(idpServer.getRequestCount()).isEqualTo(1);
        RecordedRequest tokenRequest = idpServer.takeRequest();
        assertThat(tokenRequest.getTarget()).isEqualTo(TOKEN_PATH);
        assertThat(tokenRequest.getMethod()).isEqualTo("POST");

        // Verify Trino server
        assertThat(trinoServer.getRequestCount()).isEqualTo(2);
        trinoServer.takeRequest();
        RecordedRequest authenticatedRequest = trinoServer.takeRequest();
        assertThat(authenticatedRequest.getHeaders().get(AUTHORIZATION)).isEqualTo("Bearer initial-token");
    }

    @Test
    public void testTokenIsCachedAcrossRequests()
            throws Exception
    {
        // First call: 401 exchange to learn token endpoint
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", fullChallenge())
                .build());
        enqueueToken("cached-token", 3600);
        trinoServer.enqueue(ok());
        // Second call: interceptor proactively injects cached token
        trinoServer.enqueue(ok());

        OkHttpClient client = buildTrinoClient();
        client.newCall(trinoRequest()).execute().close();
        client.newCall(trinoRequest()).execute().close();

        // Only one token fetch for two Trino calls
        assertThat(idpServer.getRequestCount()).isEqualTo(1);

        // Third Trino request (second call) has the cached token
        trinoServer.takeRequest(); // discard initial 401
        trinoServer.takeRequest(); // discard retry
        RecordedRequest secondCallRequest = trinoServer.takeRequest();
        assertThat(secondCallRequest.getHeaders().get(AUTHORIZATION)).isEqualTo("Bearer cached-token");
    }

    @Test
    public void testReactiveFetchOn401()
            throws Exception
    {
        // First call: establish token endpoint via 401 exchange
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", fullChallenge())
                .build());
        enqueueToken("first-token", 3600);
        trinoServer.enqueue(ok());

        OkHttpClient client = buildTrinoClient();
        client.newCall(trinoRequest()).execute().close();

        // Second call: interceptor injects cached token, but Trino rejects it
        trinoServer.enqueue(new MockResponse.Builder().code(HTTP_UNAUTHORIZED).build());
        enqueueToken("refreshed-token", 3600);
        trinoServer.enqueue(ok());

        Response response = client.newCall(trinoRequest()).execute();
        response.close();

        assertThat(response.code()).isEqualTo(HTTP_OK);
        assertThat(idpServer.getRequestCount()).isEqualTo(2);
    }

    @Test
    public void testExpiredTokenIsRefreshedProactively()
            throws Exception
    {
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", fullChallenge())
                .build());
        enqueueToken("expired-token", 0);
        // The network interceptor also fires on the authenticated retry; with expiresIn=0 the
        // token is already stale by then, so it fetches again before the retry reaches Trino.
        enqueueToken("interim-token", 0);
        trinoServer.enqueue(ok());

        OkHttpClient client = buildTrinoClient();
        client.newCall(trinoRequest()).execute().close();

        enqueueToken("fresh-token", 3600);
        trinoServer.enqueue(ok());

        Response response = client.newCall(trinoRequest()).execute();
        response.close();

        assertThat(response.code()).isEqualTo(HTTP_OK);
        assertThat(idpServer.getRequestCount()).isEqualTo(3);

        trinoServer.takeRequest();
        trinoServer.takeRequest();
        RecordedRequest secondCallRequest = trinoServer.takeRequest();
        assertThat(secondCallRequest.getHeaders().get(AUTHORIZATION)).isEqualTo("Bearer fresh-token");
    }

    @Test
    public void testMissingTokenEndpoint()
    {
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", "Bearer x_redirect_server=\"https://localhost:443/oauth2/token/initiate/550e8400-e29b-41d4-a716-446655440000\", x_token_server=\"https://localhost:443/oauth2/token/550e8400-e29b-41d4-a716-446655440000\", scope=\"openid\"")
                .build());

        OkHttpClient client = buildTrinoClient();
        assertThatThrownBy(() -> client.newCall(trinoRequest()).execute())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to obtain OAuth2 client credentials token")
                .cause()
                .hasMessageContaining("OAuth2 token endpoint is not available");
    }

    @Test
    public void testMissingAccessTokenInResponse()
    {
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", fullChallenge())
                .build());
        idpServer.enqueue(new MockResponse.Builder()
                .code(HTTP_OK)
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .body("{\"token_type\":\"Bearer\"}")
                .build());

        OkHttpClient client = buildTrinoClient();
        assertThatThrownBy(() -> client.newCall(trinoRequest()).execute())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to obtain OAuth2 client credentials token")
                .cause()
                .hasMessageContaining("Token response missing 'access_token'");
    }

    @Test
    public void testIdpErrorResponse()
    {
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", fullChallenge())
                .build());
        idpServer.enqueue(new MockResponse.Builder().code(HTTP_UNAUTHORIZED).build());

        OkHttpClient client = buildTrinoClient();
        assertThatThrownBy(() -> client.newCall(trinoRequest()).execute())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to obtain OAuth2 client credentials token")
                .cause()
                .hasMessageContaining("OAuth2 Client Credentials authentication failed, HTTP 401");
    }

    private OkHttpClient buildTrinoClient()
    {
        OkHttpClient baseClient = new OkHttpClient();
        ClientCredentialsAuthenticator authenticator = new ClientCredentialsAuthenticator(
                baseClient,
                "test-client",
                "test-secret");
        return baseClient.newBuilder()
                .addNetworkInterceptor(authenticator)
                .authenticator(authenticator)
                .build();
    }

    private String fullChallenge()
    {
        return "Bearer x_redirect_server=\"https://localhost:443/oauth2/token/initiate/550e8400-e29b-41d4-a716-446655440000\""
                + ", x_token_server=\"https://localhost:443/oauth2/token/550e8400-e29b-41d4-a716-446655440000\""
                + ", x_token_endpoint=\"" + idpTokenUrl() + "\""
                + ", scope=\"openid\"";
    }

    private String idpTokenUrl()
    {
        return "http://" + idpServer.getHostName() + ":" + idpServer.getPort() + TOKEN_PATH;
    }

    private Request trinoRequest()
    {
        return new Request.Builder()
                .url("http://" + trinoServer.getHostName() + ":" + trinoServer.getPort() + TRINO_PATH)
                .get()
                .build();
    }

    private void enqueueToken(String accessToken, long expiresIn)
    {
        idpServer.enqueue(new MockResponse.Builder()
                .code(HTTP_OK)
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .body("{\"access_token\":\"" + accessToken + "\",\"token_type\":\"Bearer\",\"expires_in\":" + expiresIn + "}")
                .build());
    }

    private static MockResponse ok()
    {
        return new MockResponse.Builder().code(HTTP_OK).build();
    }
}
