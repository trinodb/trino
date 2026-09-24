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

import com.google.common.io.Resources;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import mockwebserver3.junit5.StartStop;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

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
    private static final char[] KEY_STORE_PASSWORD = "Pass1234".toCharArray();

    @StartStop
    private final MockWebServer idpServer = new MockWebServer();

    @StartStop
    private final MockWebServer trinoServer = new MockWebServer();

    private X509TrustManager trustManager;
    private SSLSocketFactory clientSocketFactory;
    private final AtomicReference<FormBody> tokenRequestBody = new AtomicReference<>();

    @BeforeEach
    public void setupTls()
            throws Exception
    {
        // The token endpoint carries the client secret, so the authenticator requires it to be https.
        KeyStore keyStore = loadKeyStore();

        KeyStore serverKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        serverKeyStore.load(null, null);
        serverKeyStore.setKeyEntry(
                "localhost",
                keyStore.getKey("localhost", KEY_STORE_PASSWORD),
                KEY_STORE_PASSWORD,
                keyStore.getCertificateChain("localhost"));
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(serverKeyStore, KEY_STORE_PASSWORD);

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);
        trustManager = (X509TrustManager) trustManagerFactory.getTrustManagers()[0];

        SSLContext serverContext = SSLContext.getInstance("TLS");
        serverContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());
        idpServer.useHttps(serverContext.getSocketFactory());

        SSLContext clientContext = SSLContext.getInstance("TLS");
        clientContext.init(null, new TrustManager[] {trustManager}, new SecureRandom());
        clientSocketFactory = clientContext.getSocketFactory();
    }

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
        assertThat(formFields(tokenRequestBody.get()))
                .containsExactly(
                        Map.entry("grant_type", "client_credentials"),
                        Map.entry("client_id", "test-client"),
                        Map.entry("client_secret", "test-secret"),
                        Map.entry("scope", "openid"));

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

        // Second call: interceptor injects cached token, but Trino rejects it and re-challenges
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", fullChallenge())
                .build());
        enqueueToken("refreshed-token", 3600);
        trinoServer.enqueue(ok());

        Response response = client.newCall(trinoRequest()).execute();
        response.close();

        assertThat(response.code()).isEqualTo(HTTP_OK);
        assertThat(idpServer.getRequestCount()).isEqualTo(2);
    }

    @Test
    public void testExpiredTokenIsRefreshedReactively()
            throws Exception
    {
        // First call: 401 exchange yields a token that is already expired (expires_in=0).
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", fullChallenge())
                .build());
        enqueueToken("expired-token", 0);
        trinoServer.enqueue(ok());

        OkHttpClient client = buildTrinoClient();
        client.newCall(trinoRequest()).execute().close();

        // Second call: the cached token is expired, so the interceptor does not inject it. Trino
        // challenges again and the authenticator fetches a fresh token reactively on the 401.
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", fullChallenge())
                .build());
        enqueueToken("fresh-token", 3600);
        trinoServer.enqueue(ok());

        Response response = client.newCall(trinoRequest()).execute();
        response.close();

        assertThat(response.code()).isEqualTo(HTTP_OK);
        assertThat(idpServer.getRequestCount()).isEqualTo(2);

        trinoServer.takeRequest(); // first call: initial 401 challenge
        trinoServer.takeRequest(); // first call: retry with expired-token
        trinoServer.takeRequest(); // second call: challenge (no token injected)
        RecordedRequest refreshedRequest = trinoServer.takeRequest();
        assertThat(refreshedRequest.getHeaders().get(AUTHORIZATION)).isEqualTo("Bearer fresh-token");
    }

    @Test
    public void testChallengeWithoutTokenEndpointUsesConfiguredEndpoint()
            throws Exception
    {
        // The challenge omits x_token_endpoint; the authenticator falls back to the configured endpoint.
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", "Bearer scope=\"openid\"")
                .build());
        enqueueToken("configured-token", 3600);
        trinoServer.enqueue(ok());

        OkHttpClient client = buildTrinoClient();
        Response response = client.newCall(trinoRequest()).execute();
        response.close();

        assertThat(response.code()).isEqualTo(HTTP_OK);
        assertThat(idpServer.getRequestCount()).isEqualTo(1);
        assertThat(idpServer.takeRequest().getTarget()).isEqualTo(TOKEN_PATH);
    }

    @Test
    public void testUsesChallengeEndpointWhenNotConfigured()
            throws Exception
    {
        // No configured endpoint: the authenticator uses the (https) endpoint from the challenge.
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", fullChallenge())
                .build());
        enqueueToken("challenge-token", 3600);
        trinoServer.enqueue(ok());

        OkHttpClient client = buildTrinoClient(Optional.empty());
        Response response = client.newCall(trinoRequest()).execute();
        response.close();

        assertThat(response.code()).isEqualTo(HTTP_OK);
        assertThat(idpServer.getRequestCount()).isEqualTo(1);
        assertThat(idpServer.takeRequest().getTarget()).isEqualTo(TOKEN_PATH);
    }

    @Test
    public void testRejectsPlainHttpChallengeEndpointWhenNotConfigured()
    {
        // No configured endpoint: a plain-HTTP endpoint from the challenge is refused.
        String plainHttpEndpoint = "http://localhost:" + idpServer.getPort() + TOKEN_PATH;
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", "Bearer x_token_endpoint=\"" + plainHttpEndpoint + "\", scope=\"openid\"")
                .build());

        OkHttpClient client = buildTrinoClient(Optional.empty());
        assertThatThrownBy(() -> client.newCall(trinoRequest()).execute())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to obtain OAuth2 client credentials token")
                .cause()
                .hasMessageContaining("is not https")
                .hasMessageContaining("refusing to send client credentials over an insecure connection");

        assertThat(idpServer.getRequestCount()).isEqualTo(0);
    }

    @Test
    public void testMissingTokenEndpointWhenNotConfigured()
    {
        // No configured endpoint and the challenge does not advertise one.
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", "Bearer scope=\"openid\"")
                .build());

        OkHttpClient client = buildTrinoClient(Optional.empty());
        assertThatThrownBy(() -> client.newCall(trinoRequest()).execute())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to obtain OAuth2 client credentials token")
                .cause()
                .hasMessageContaining("OAuth2 token endpoint is not available");

        assertThat(idpServer.getRequestCount()).isEqualTo(0);
    }

    @Test
    public void testRejectsChallengeTokenEndpointOnDifferentHost()
    {
        // A malicious server tries to redirect the client secret to a host it controls.
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", "Bearer x_token_endpoint=\"https://attacker.example.com/token\", scope=\"openid\"")
                .build());

        OkHttpClient client = buildTrinoClient();
        assertThatThrownBy(() -> client.newCall(trinoRequest()).execute())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to obtain OAuth2 client credentials token")
                .cause()
                .hasMessageContaining("does not match the configured oauth2TokenEndpoint")
                .hasMessageContaining("refusing to send client credentials");

        // The client secret was never sent anywhere.
        assertThat(idpServer.getRequestCount()).isEqualTo(0);
    }

    @Test
    public void testRejectsPlainHttpChallengeTokenEndpoint()
    {
        // Same host and path as the configured endpoint, but downgraded to plain HTTP.
        String plainHttpEndpoint = "http://localhost:" + idpServer.getPort() + TOKEN_PATH;
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", "Bearer x_token_endpoint=\"" + plainHttpEndpoint + "\", scope=\"openid\"")
                .build());

        OkHttpClient client = buildTrinoClient();
        assertThatThrownBy(() -> client.newCall(trinoRequest()).execute())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to obtain OAuth2 client credentials token")
                .cause()
                .hasMessageContaining("does not match the configured oauth2TokenEndpoint");

        assertThat(idpServer.getRequestCount()).isEqualTo(0);
    }

    @Test
    public void testDoesNotFollowRedirectFromTokenEndpoint()
    {
        trinoServer.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader("WWW-Authenticate", fullChallenge())
                .build());
        // 307 preserves the POST body, so following it would resend the client credentials elsewhere.
        idpServer.enqueue(new MockResponse.Builder()
                .code(307)
                .addHeader("Location", "https://localhost:" + idpServer.getPort() + "/elsewhere")
                .build());

        OkHttpClient client = buildTrinoClient();
        assertThatThrownBy(() -> client.newCall(trinoRequest()).execute())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to obtain OAuth2 client credentials token")
                .cause()
                .hasMessageContaining("OAuth2 Client Credentials authentication failed, HTTP 307");

        // The redirect was not followed: only the single token POST reached the IdP.
        assertThat(idpServer.getRequestCount()).isEqualTo(1);
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
                .hasMessageContaining("accessToken is null");
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
        return buildTrinoClient(Optional.of(idpTokenUrl()));
    }

    private OkHttpClient buildTrinoClient(Optional<String> configuredTokenEndpoint)
    {
        OkHttpClient baseClient = new OkHttpClient.Builder()
                .sslSocketFactory(clientSocketFactory, trustManager)
                // Capture the form body of the token request so tests can assert on it without reading the
                // recorded request body (which would require a direct dependency on okio).
                .addInterceptor(chain -> {
                    Request request = chain.request();
                    if (request.url().encodedPath().equals(TOKEN_PATH) && request.body() instanceof FormBody) {
                        tokenRequestBody.set((FormBody) request.body());
                    }
                    return chain.proceed(request);
                })
                .build();
        ClientCredentialsAuthenticator authenticator = new ClientCredentialsAuthenticator(
                baseClient,
                "test-client",
                "test-secret",
                configuredTokenEndpoint);
        return baseClient.newBuilder()
                .addNetworkInterceptor(authenticator)
                .authenticator(authenticator)
                .build();
    }

    private static Map<String, String> formFields(FormBody body)
    {
        Map<String, String> fields = new LinkedHashMap<>();
        for (int i = 0; i < body.size(); i++) {
            fields.put(body.name(i), body.value(i));
        }
        return fields;
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
        return "https://localhost:" + idpServer.getPort() + TOKEN_PATH;
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

    private static KeyStore loadKeyStore()
            throws Exception
    {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream in = Resources.getResource("certs/certs.jks").openStream()) {
            keyStore.load(in, KEY_STORE_PASSWORD);
        }
        return keyStore;
    }

    private static MockResponse ok()
    {
        return new MockResponse.Builder().code(HTTP_OK).build();
    }
}
