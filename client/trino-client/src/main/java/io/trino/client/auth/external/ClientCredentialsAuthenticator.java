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
import okhttp3.HttpUrl;
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

/**
 * OkHttp Interceptor and Authenticator that implements OAuth2 client credentials flow.
 * <p>
 * On the first request the authenticator:
 * 1. Receives a 401 from Trino with a {@code WWW-Authenticate: Bearer x_token_endpoint="...", scope="..."} challenge.
 * 2. POSTs {@code grant_type=client_credentials} to the token endpoint. By default the endpoint is taken
 *    from the {@code x_token_endpoint} value in the challenge, which must be an https URL. A client that
 *    sets the {@code oauth2TokenEndpoint} connection property pins the endpoint to that value: the secret
 *    is then only ever sent there, and a challenge that names a different endpoint is rejected.
 * 3. Caches the resulting access token and its expiry time. The scope is taken from each challenge and
 *    is not cached across challenges.
 * 4. Subsequent requests proactively inject the cached token (via the interceptor).
 * 5. Re-fetches transparently when the token expires (reactively via the authenticator on 401,
 *    using the scope from the fresh challenge).
 * <p>
 * The client secret is only ever sent to the token endpoint over https — never to Trino, and never over a
 * clear-text connection. When {@code oauth2TokenEndpoint} is set, it is also never sent to an endpoint that
 * does not match the configured one, so a malicious or compromised server cannot redirect the credentials
 * to a host it controls.
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
    private final Optional<HttpUrl> configuredTokenEndpoint;

    private final Lock lock = new ReentrantLock();
    private volatile String cachedToken;
    private volatile Instant tokenExpiry = Instant.EPOCH;

    public ClientCredentialsAuthenticator(
            OkHttpClient httpClient,
            String clientId,
            String clientSecret,
            Optional<String> tokenEndpoint)
    {
        // The token request carries the client secret, so it must not follow redirects: a redirect could
        // send the credentials to a different (or downgraded, non-https) host chosen by the server.
        this.httpClient = requireNonNull(httpClient, "httpClient is null")
                .newBuilder()
                .followRedirects(false)
                .followSslRedirects(false)
                .build();
        this.clientId = requireNonNull(clientId, "clientId is null");
        this.clientSecret = requireNonNull(clientSecret, "clientSecret is null");
        this.configuredTokenEndpoint = requireNonNull(tokenEndpoint, "tokenEndpoint is null")
                .map(ClientCredentialsAuthenticator::parseConfiguredEndpoint);
    }

    private static HttpUrl parseConfiguredEndpoint(String tokenEndpoint)
    {
        HttpUrl url = HttpUrl.parse(tokenEndpoint);
        checkArgument(url != null, "tokenEndpoint is not a valid URL: %s", tokenEndpoint);
        // The client secret is sent to this endpoint, so it must not travel in clear text.
        checkArgument(url.isHttps(), "tokenEndpoint must be an https URL: %s", tokenEndpoint);
        return url;
    }

    @Override
    public Response intercept(Chain chain)
            throws IOException
    {
        String token = cachedToken;
        if (token != null && Instant.now().isBefore(tokenExpiry)) {
            return chain.proceed(withBearerToken(chain.request(), token));
        }
        return chain.proceed(chain.request());
    }

    @Nullable
    @Override
    public Request authenticate(Route route, Response response)
            throws IOException
    {
        if (response.priorResponse() != null && response.priorResponse().code() == 401) {
            return null;
        }

        // The cached token used for this request was rejected by the server, so a fresh token is required.
        // Capture it so refreshToken can detect whether another thread has already refreshed it.
        String rejectedToken = cachedToken;
        ChallengeHints hints = extractFromChallenge(response);
        try {
            String token = refreshToken(rejectedToken, hints);
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
            this.tokenEndpoint = requireNonNull(tokenEndpoint, "tokenEndpoint is null");
            this.scope = requireNonNull(scope, "scope is null");
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

    private String refreshToken(@Nullable String rejectedToken, ChallengeHints hints)
            throws IOException
    {
        lock.lock();
        try {
            // Another thread may have refreshed the token while we waited for the lock.
            // If the cached token is no longer the rejected one, reuse it.
            if (cachedToken != null && !cachedToken.equals(rejectedToken)) {
                return cachedToken;
            }
            HttpUrl tokenEndpoint = resolveTokenEndpoint(hints);

            FormBody.Builder formBuilder = new FormBody.Builder()
                    .add("grant_type", "client_credentials")
                    .add("client_id", clientId)
                    .add("client_secret", clientSecret);
            hints.scope().ifPresent(scope -> formBuilder.add("scope", scope));

            Request request = new Request.Builder()
                    .url(tokenEndpoint)
                    .post(formBuilder.build())
                    .build();

            try (Response response = httpClient.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    throw new IOException("OAuth2 Client Credentials authentication failed, HTTP " + response.code());
                }
                TokenResponse tokenResponse = OBJECT_MAPPER.readValue(response.body().string(), TokenResponse.class);
                cachedToken = tokenResponse.accessToken();
                tokenExpiry = Instant.now().plusSeconds(Math.max(0, tokenResponse.expiresIn() - EXPIRY_BUFFER_SECONDS));
            }
            return cachedToken;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Determines the token endpoint the client credentials are exchanged at. When {@code oauth2TokenEndpoint}
     * is configured, that endpoint is used and a challenge that names a different one is rejected. Otherwise
     * the endpoint advertised by the server in the challenge is used, and it must be an https URL so the
     * secret is never sent over a clear-text connection.
     */
    private HttpUrl resolveTokenEndpoint(ChallengeHints hints)
            throws IOException
    {
        Optional<String> challengeTokenEndpoint = hints.tokenEndpoint();
        if (configuredTokenEndpoint.isPresent()) {
            HttpUrl configured = configuredTokenEndpoint.get();
            if (challengeTokenEndpoint.isPresent()) {
                HttpUrl challengeEndpoint = HttpUrl.parse(challengeTokenEndpoint.get());
                if (challengeEndpoint == null || !isSameEndpoint(configured, challengeEndpoint)) {
                    throw new IOException(format(
                            "Server-provided OAuth2 token endpoint '%s' does not match the configured oauth2TokenEndpoint '%s'; refusing to send client credentials",
                            challengeTokenEndpoint.get(),
                            configured));
                }
            }
            return configured;
        }

        if (challengeTokenEndpoint.isEmpty()) {
            throw new IOException("OAuth2 token endpoint is not available; the server did not return x_token_endpoint in the WWW-Authenticate challenge");
        }
        HttpUrl challengeEndpoint = HttpUrl.parse(challengeTokenEndpoint.get());
        if (challengeEndpoint == null) {
            throw new IOException(format("Server-provided OAuth2 token endpoint '%s' is not a valid URL", challengeTokenEndpoint.get()));
        }
        // Without a configured endpoint, the secret goes to the server-named endpoint, so it must be https.
        if (!challengeEndpoint.isHttps()) {
            throw new IOException(format(
                    "Server-provided OAuth2 token endpoint '%s' is not https; refusing to send client credentials over an insecure connection",
                    challengeTokenEndpoint.get()));
        }
        return challengeEndpoint;
    }

    private static Request withBearerToken(Request request, String token)
    {
        return request.newBuilder()
                .header(AUTHORIZATION, "Bearer " + token)
                .build();
    }

    /**
     * Compares two token endpoints for equality after normalization. {@link HttpUrl} already lower-cases the
     * scheme and host and resolves the default port, so only the path needs its trailing slash normalized.
     * The query and fragment are ignored because they are not part of the token endpoint's identity.
     */
    private static boolean isSameEndpoint(HttpUrl configured, HttpUrl fromChallenge)
    {
        return configured.scheme().equals(fromChallenge.scheme())
                && configured.host().equals(fromChallenge.host())
                && configured.port() == fromChallenge.port()
                && trimTrailingSlash(configured.encodedPath()).equals(trimTrailingSlash(fromChallenge.encodedPath()));
    }

    private static String trimTrailingSlash(String path)
    {
        if (path.length() > 1 && path.endsWith("/")) {
            return path.substring(0, path.length() - 1);
        }
        return path;
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
            this.accessToken = requireNonNull(accessToken, "accessToken is null");
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
