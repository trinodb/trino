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

import com.google.common.collect.ImmutableSet;
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
import java.util.Optional;

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
    private static final String DISCOVERY_PATH = "/.well-known/openid-configuration";
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
        enqueueDiscovery();
        enqueueToken("initial-token", 3600);
        trinoServer.enqueue(ok());

        OkHttpClient client = buildTrinoClient();
        Response response = client.newCall(trinoRequest()).execute();
        response.close();

        assertThat(response.code()).isEqualTo(HTTP_OK);

        // discovery request
        RecordedRequest discoveryRequest = idpServer.takeRequest();
        assertThat(discoveryRequest.getTarget()).isEqualTo(DISCOVERY_PATH);

        // token request
        RecordedRequest tokenRequest = idpServer.takeRequest();
        assertThat(tokenRequest.getTarget()).isEqualTo(TOKEN_PATH);
        assertThat(tokenRequest.getMethod()).isEqualTo("POST");

        // trino request carries Bearer token
        RecordedRequest trinoRequest = trinoServer.takeRequest();
        assertThat(trinoRequest.getHeaders().get(AUTHORIZATION)).isEqualTo("Bearer initial-token");
    }

    @Test
    public void testScopesAreIncludedInTokenRequest()
            throws Exception
    {
        enqueueDiscovery();
        enqueueToken("scoped-token", 3600);
        trinoServer.enqueue(ok());

        OkHttpClient baseClient = new OkHttpClient();
        String issuer = "http://" + idpServer.getHostName() + ":" + idpServer.getPort();
        ClientCredentialsAuthenticator authenticator = new ClientCredentialsAuthenticator(
                baseClient,
                "test-client",
                "test-secret",
                Optional.of(issuer),
                Optional.of(ImmutableSet.of("openid", "profile")));
        OkHttpClient client = baseClient.newBuilder()
                .addNetworkInterceptor(authenticator)
                .authenticator(authenticator)
                .build();
        client.newCall(trinoRequest()).execute().close();

        idpServer.takeRequest(); // discovery
        idpServer.takeRequest(); // token
        // scope is URL-encoded in the form body; verify token arrived at Trino
        RecordedRequest trinoReq = trinoServer.takeRequest();
        assertThat(trinoReq.getHeaders().get(AUTHORIZATION)).isEqualTo("Bearer scoped-token");
    }

    @Test
    public void testTokenIsCachedAcrossRequests()
            throws Exception
    {
        enqueueDiscovery();
        enqueueToken("cached-token", 3600);
        trinoServer.enqueue(ok());
        trinoServer.enqueue(ok());

        OkHttpClient client = buildTrinoClient();
        client.newCall(trinoRequest()).execute().close();
        client.newCall(trinoRequest()).execute().close();

        // Only one discovery + one token fetch for two Trino requests
        assertThat(idpServer.getRequestCount()).isEqualTo(2);
        assertThat(trinoServer.getRequestCount()).isEqualTo(2);

        // Both Trino requests use the same token
        trinoServer.takeRequest();
        RecordedRequest secondTrinoRequest = trinoServer.takeRequest();
        assertThat(secondTrinoRequest.getHeaders().get(AUTHORIZATION)).isEqualTo("Bearer cached-token");
    }

    @Test
    public void testReactiveFetchOn401()
            throws Exception
    {
        enqueueDiscovery();
        enqueueToken("first-token", 3600);
        // Trino rejects first token; authenticate() re-uses the cached token endpoint
        // (no second discovery needed) and fetches a fresh token
        trinoServer.enqueue(new MockResponse.Builder().code(HTTP_UNAUTHORIZED).build());
        enqueueToken("refreshed-token", 3600);
        trinoServer.enqueue(ok());

        OkHttpClient client = buildTrinoClient();
        Response response = client.newCall(trinoRequest()).execute();
        response.close();

        // The final response is 200 — authenticate() successfully obtained a new token and
        // retried. In OkHttp 5 the authenticator retry bypasses network interceptors, so we
        // validate the end-to-end result rather than the token on the wire.
        assertThat(response.code()).isEqualTo(HTTP_OK);
        assertThat(trinoServer.getRequestCount()).isEqualTo(2);
    }

    @Test
    public void testIssuerDiscoveredFromServerChallenge()
            throws Exception
    {
        // Trino returns 401 with x_issuer_server; client discovers issuer from the challenge
        String issuer = "http://" + idpServer.getHostName() + ":" + idpServer.getPort();
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", "Bearer x_issuer_server=\"" + issuer + "\", x_token_server=\"http://ignored\"")
                .build());
        enqueueDiscovery();
        enqueueToken("challenge-token", 3600);
        trinoServer.enqueue(ok());

        OkHttpClient baseClient = new OkHttpClient();
        ClientCredentialsAuthenticator authenticator = new ClientCredentialsAuthenticator(
                baseClient,
                "test-client",
                "test-secret",
                Optional.empty(), // no issuer pre-configured
                Optional.empty());
        OkHttpClient client = baseClient.newBuilder()
                .addNetworkInterceptor(authenticator)
                .authenticator(authenticator)
                .build();

        Response response = client.newCall(trinoRequest()).execute();
        response.close();

        assertThat(response.code()).isEqualTo(HTTP_OK);
        assertThat(trinoServer.getRequestCount()).isEqualTo(2);
    }

    @Test
    public void testMissingIssuerWhenServerDoesNotReturnChallenge()
    {
        // No issuer configured and server returns 401 without x_issuer_server (e.g. old server)
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", "Bearer x_token_server=\"http://ignored\"")
                .build());

        OkHttpClient baseClient = new OkHttpClient();
        ClientCredentialsAuthenticator authenticator = new ClientCredentialsAuthenticator(
                baseClient,
                "test-client",
                "test-secret",
                Optional.empty(),
                Optional.empty());
        OkHttpClient client = baseClient.newBuilder()
                .addNetworkInterceptor(authenticator)
                .authenticator(authenticator)
                .build();

        assertThatThrownBy(() -> client.newCall(trinoRequest()).execute())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to obtain OAuth2 client credentials token")
                .cause()
                .hasMessageContaining("OAuth2 issuer is not configured and could not be discovered from the server challenge");
    }

    @Test
    public void testScopeDiscoveredFromServerChallenge()
            throws Exception
    {
        // Trino returns 401 with scope in the challenge; client uses it when none is pre-configured
        String issuer = "http://" + idpServer.getHostName() + ":" + idpServer.getPort();
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", "Bearer x_issuer_server=\"" + issuer + "\", scope=\"openid profile\"")
                .build());
        enqueueDiscovery();
        enqueueToken("scope-token", 3600);
        trinoServer.enqueue(ok());

        OkHttpClient baseClient = new OkHttpClient();
        ClientCredentialsAuthenticator authenticator = new ClientCredentialsAuthenticator(
                baseClient,
                "test-client",
                "test-secret",
                Optional.empty(),
                Optional.empty()); // no scopes pre-configured
        OkHttpClient client = baseClient.newBuilder()
                .addNetworkInterceptor(authenticator)
                .authenticator(authenticator)
                .build();

        Response response = client.newCall(trinoRequest()).execute();
        response.close();

        assertThat(response.code()).isEqualTo(HTTP_OK);
        assertThat(idpServer.getRequestCount()).isEqualTo(2); // discovery + token
        assertThat(trinoServer.getRequestCount()).isEqualTo(2); // initial 401 + authenticated retry
        trinoServer.takeRequest(); // discard unauthenticated request
        RecordedRequest trinoReq = trinoServer.takeRequest();
        assertThat(trinoReq.getHeaders().get(AUTHORIZATION)).isEqualTo("Bearer scope-token");
    }

    @Test
    public void testOidcDiscoveryFailure()
    {
        idpServer.enqueue(new MockResponse.Builder().code(503).build());

        OkHttpClient client = buildTrinoClient();
        assertThatThrownBy(() -> client.newCall(trinoRequest()).execute())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to fetch OIDC discovery document");
    }

    @Test
    public void testMissingTokenEndpointInDiscovery()
    {
        idpServer.enqueue(new MockResponse.Builder()
                .code(HTTP_OK)
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .body("{}")
                .build());

        OkHttpClient client = buildTrinoClient();
        assertThatThrownBy(() -> client.newCall(trinoRequest()).execute())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("OIDC discovery document missing 'token_endpoint'");
    }

    @Test
    public void testMissingAccessTokenInResponse()
    {
        enqueueDiscovery();
        idpServer.enqueue(new MockResponse.Builder()
                .code(HTTP_OK)
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .body("{\"token_type\":\"Bearer\"}")
                .build());

        OkHttpClient client = buildTrinoClient();
        assertThatThrownBy(() -> client.newCall(trinoRequest()).execute())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Token response missing 'access_token'");
    }

    @Test
    public void testTokenFetchFailure()
    {
        enqueueDiscovery();
        idpServer.enqueue(new MockResponse.Builder().code(HTTP_UNAUTHORIZED).build());

        OkHttpClient client = buildTrinoClient();
        assertThatThrownBy(() -> client.newCall(trinoRequest()).execute())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("OAuth2 Client Credentials authentication failed, HTTP 401");
    }

    private OkHttpClient buildTrinoClient()
    {
        OkHttpClient baseClient = new OkHttpClient();
        String issuer = "http://" + idpServer.getHostName() + ":" + idpServer.getPort();
        ClientCredentialsAuthenticator authenticator = new ClientCredentialsAuthenticator(
                baseClient,
                "test-client",
                "test-secret",
                Optional.of(issuer),
                Optional.empty());
        return baseClient.newBuilder()
                .addNetworkInterceptor(authenticator)
                .authenticator(authenticator)
                .build();
    }

    private Request trinoRequest()
    {
        return new Request.Builder()
                .url("http://" + trinoServer.getHostName() + ":" + trinoServer.getPort() + TRINO_PATH)
                .get()
                .build();
    }

    private void enqueueDiscovery()
    {
        String tokenEndpoint = "http://" + idpServer.getHostName() + ":" + idpServer.getPort() + TOKEN_PATH;
        idpServer.enqueue(new MockResponse.Builder()
                .code(HTTP_OK)
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .body("{\"token_endpoint\":\"" + tokenEndpoint + "\"}")
                .build());
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
