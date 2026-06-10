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

import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.util.Objects.requireNonNull;

/**
 * OkHttp Interceptor and Authenticator that implements OAuth2 client credentials flow.
 * <p>
 * On the first request (or after token expiry) the authenticator:
 * 1. Resolves the issuer and scopes: uses pre-configured values if provided, otherwise reads
 *    {@code x_issuer_server} and {@code scope} from the 401 Bearer challenge returned by Trino.
 * 2. Fetches {@code {issuer}/.well-known/openid-configuration} to discover the token endpoint.
 * 3. POSTs to the token endpoint with {@code grant_type=client_credentials}.
 * 4. Caches the resulting access token and its expiry time.
 * 5. Re-fetches transparently when the token expires (proactively via interceptor,
 *    reactively via authenticator on 401).
 * <p>
 * The client secret is only ever sent to the IdP token endpoint — never to Trino.
 */
public class ClientCredentialsAuthenticator
        implements Interceptor, Authenticator
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String ISSUER_URI_FIELD = "x_issuer_server";
    private static final String SCOPE_FIELD = "scope";
    private static final int EXPIRY_BUFFER_SECONDS = 30;

    private final OkHttpClient httpClient;
    private final String clientId;
    private final String clientSecret;
    private final Optional<String> issuer;
    private final Optional<Set<String>> scopes;

    private final Lock lock = new ReentrantLock();
    private volatile String cachedToken;
    private volatile Instant tokenExpiry = Instant.EPOCH;
    private volatile String tokenEndpoint;

    public ClientCredentialsAuthenticator(
            OkHttpClient httpClient,
            String clientId,
            String clientSecret,
            Optional<String> issuer,
            Optional<Set<String>> scopes)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.clientId = requireNonNull(clientId, "clientId is null");
        this.clientSecret = requireNonNull(clientSecret, "clientSecret is null");
        this.issuer = requireNonNull(issuer, "issuer is null");
        this.scopes = requireNonNull(scopes, "scopes is null");
    }

    @Override
    public Response intercept(Chain chain)
            throws IOException
    {
        if (issuer.isEmpty() && tokenEndpoint == null) {
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

        ChallengeHints hints = issuer.isPresent() && scopes.isPresent()
                ? new ChallengeHints(Optional.empty(), Optional.empty())
                : extractFromChallenge(response);
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
        private final Optional<String> issuer;
        private final Optional<String> scope;

        ChallengeHints(Optional<String> issuer, Optional<String> scope)
        {
            this.issuer = issuer;
            this.scope = scope;
        }

        Optional<String> issuer()
        {
            return issuer;
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
                String issuer = params.get(ISSUER_URI_FIELD);
                String scope = params.get(SCOPE_FIELD);
                return new ChallengeHints(
                        issuer != null && !issuer.isEmpty() ? Optional.of(issuer) : Optional.empty(),
                        scope != null && !scope.isEmpty() ? Optional.of(scope) : Optional.empty());
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
                String resolvedIssuer = issuer.or(hints::issuer)
                        .orElseThrow(() -> new IOException("OAuth2 issuer is not configured and could not be discovered from the server challenge"));
                tokenEndpoint = discoverTokenEndpoint(resolvedIssuer);
            }

            FormBody.Builder formBuilder = new FormBody.Builder()
                    .add("grant_type", "client_credentials")
                    .add("client_id", clientId)
                    .add("client_secret", clientSecret);
            scopes.map(list -> String.join(" ", list)).or(hints::scope).ifPresent(s -> formBuilder.add("scope", s));

            Request request = new Request.Builder()
                    .url(tokenEndpoint)
                    .post(formBuilder.build())
                    .build();

            try (Response response = httpClient.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    throw new IOException("OAuth2 Client Credentials authentication failed, HTTP " + response.code());
                }
                JsonNode json = OBJECT_MAPPER.readTree(response.body().string());
                if (json == null || !json.has("access_token")) {
                    throw new IOException("Token response missing 'access_token' field");
                }
                cachedToken = json.get("access_token").asText();
                long expiresIn = json.has("expires_in") ? json.get("expires_in").asLong() : 300;
                tokenExpiry = Instant.now().plusSeconds(Math.max(0, expiresIn - EXPIRY_BUFFER_SECONDS));
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

    private String discoverTokenEndpoint(String resolvedIssuer)
            throws IOException
    {
        String discoveryUrl = (resolvedIssuer.endsWith("/") ? resolvedIssuer.substring(0, resolvedIssuer.length() - 1) : resolvedIssuer) + "/.well-known/openid-configuration";
        Request request = new Request.Builder()
                .url(discoveryUrl)
                .get()
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Failed to fetch OIDC discovery document from " + discoveryUrl + ", HTTP " + response.code());
            }
            JsonNode json = OBJECT_MAPPER.readTree(response.body().string());
            if (json == null || !json.has("token_endpoint")) {
                throw new IOException("OIDC discovery document missing 'token_endpoint' field");
            }
            return json.get("token_endpoint").asText();
        }
    }

    private static Request withBearerToken(Request request, String token)
    {
        return request.newBuilder()
                .header(AUTHORIZATION, "Bearer " + token)
                .build();
    }
}
