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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import okhttp3.Authenticator;
import okhttp3.Challenge;
import okhttp3.FormBody;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

/**
 * OkHttp Interceptor and Authenticator that implements OAuth2 client credentials flow.
 * <p>
 * On the first request the authenticator:
 * 1. Receives a 401 from Trino with a {@code WWW-Authenticate: Bearer x_token_endpoint="...", scope="..."} challenge.
 * 2. POSTs to the token endpoint with {@code grant_type=client_credentials}.
 * 3. Caches the resulting access token, its expiry time, the token endpoint URL, and the scope.
 * 4. Subsequent requests proactively inject the cached token (via the interceptor).
 * 5. Re-fetches transparently when the token expires (proactively via interceptor,
 *    reactively via authenticator on 401).
 * <p>
 * The client secret is only ever sent to the IdP token endpoint — never to Trino.
 */
public class ClientCredentialsAuthenticator
        implements Interceptor, Authenticator
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TOKEN_ENDPOINT_FIELD = "x_token_endpoint";
    private static final String SCOPE_FIELD = "scope";
    private static final int EXPIRY_BUFFER_SECONDS = 30;

    private final OkHttpClient httpClient;
    private final String clientId;
    private final String clientSecret;

    private final Lock lock = new ReentrantLock();
    private volatile String cachedToken;
    private volatile Instant tokenExpiry = Instant.EPOCH;
    private volatile String tokenEndpoint;
    private volatile String cachedScope;

    public ClientCredentialsAuthenticator(
            OkHttpClient httpClient,
            String clientId,
            String clientSecret)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.clientId = requireNonNull(clientId, "clientId is null");
        this.clientSecret = requireNonNull(clientSecret, "clientSecret is null");
    }

    @Override
    public Response intercept(Chain chain)
            throws IOException
    {
        if (tokenEndpoint == null) {
            return chain.proceed(chain.request());
        }
        String token = getValidToken(new ChallengeHints(Optional.empty(), Optional.empty()));
        return chain.proceed(withBearerToken(chain.request(), token));
    }

    @Nullable
    @Override
    public Request authenticate(Route route, Response response)
            throws IOException
    {
        if (response.priorResponse() != null && response.priorResponse().code() == 401) {
            return null;
        }

        tokenExpiry = Instant.EPOCH;

        ChallengeHints hints = extractFromChallenge(response);
        try {
            String token = getValidToken(hints);
            return withBearerToken(response.request(), token);
        }
        catch (IOException e) {
            throw new IOException("Failed to obtain OAuth2 client credentials token", e);
        }
    }

    private static final class ChallengeHints
    {
        private final Optional<String> tokenEndpoint;
        private final Optional<String> scope;

        ChallengeHints(Optional<String> tokenEndpoint, Optional<String> scope)
        {
            this.tokenEndpoint = tokenEndpoint;
            this.scope = scope;
        }

        Optional<String> tokenEndpoint()
        {
            return tokenEndpoint;
        }

        Optional<String> scope()
        {
            return scope;
        }
    }

    private static ChallengeHints extractFromChallenge(Response response)
    {
        for (Challenge challenge : response.challenges()) {
            if (challenge.scheme().equalsIgnoreCase("Bearer")) {
                Map<String, String> params = challenge.authParams();
                String tokenEndpoint = params.get(TOKEN_ENDPOINT_FIELD);
                String scope = params.get(SCOPE_FIELD);
                return new ChallengeHints(
                        Optional.ofNullable(tokenEndpoint).filter(not(String::isEmpty)),
                        Optional.ofNullable(scope).filter(not(String::isEmpty)));
            }
        }
        return new ChallengeHints(Optional.empty(), Optional.empty());
    }

    private String getValidToken(ChallengeHints hints)
            throws IOException
    {
        if (cachedToken != null && Instant.now().isBefore(tokenExpiry)) {
            return cachedToken;
        }
        lock.lock();
        try {
            if (cachedToken != null && Instant.now().isBefore(tokenExpiry)) {
                return cachedToken;
            }
            if (tokenEndpoint == null) {
                tokenEndpoint = hints.tokenEndpoint()
                        .orElseThrow(() -> new IOException("OAuth2 token endpoint is not available; the server did not return x_token_endpoint in the WWW-Authenticate challenge"));
            }
            hints.scope().ifPresent(scope -> cachedScope = scope);

            FormBody.Builder formBuilder = new FormBody.Builder()
                    .add("grant_type", "client_credentials")
                    .add("client_id", clientId)
                    .add("client_secret", clientSecret);
            if (cachedScope != null) {
                formBuilder.add("scope", cachedScope);
            }

            Request request = new Request.Builder()
                    .url(tokenEndpoint)
                    .post(formBuilder.build())
                    .build();

            try (Response response = httpClient.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    throw new IOException("OAuth2 Client Credentials authentication failed, HTTP " + response.code());
                }
                TokenResponse tokenResponse = OBJECT_MAPPER.readValue(response.body().string(), TokenResponse.class);
                if (tokenResponse.accessToken() == null) {
                    throw new IOException("Token response missing 'access_token' field");
                }
                cachedToken = tokenResponse.accessToken();
                tokenExpiry = Instant.now().plusSeconds(Math.max(0, tokenResponse.expiresIn() - EXPIRY_BUFFER_SECONDS));
            }
            catch (IOException e) {
                tokenEndpoint = null;
                throw e;
            }
            return cachedToken;
        }
        finally {
            lock.unlock();
        }
    }

    private static Request withBearerToken(Request request, String token)
    {
        return request.newBuilder()
                .header(AUTHORIZATION, "Bearer " + token)
                .build();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class TokenResponse
    {
        private final String accessToken;
        private final long expiresIn;

        @JsonCreator
        TokenResponse(
                @JsonProperty("access_token") String accessToken,
                @JsonProperty("expires_in") Long expiresIn)
        {
            this.accessToken = accessToken;
            this.expiresIn = expiresIn != null ? expiresIn : 300;
        }

        String accessToken()
        {
            return accessToken;
        }

        long expiresIn()
        {
            return expiresIn;
        }
    }
}
