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

import com.google.common.annotations.VisibleForTesting;
import io.trino.client.ClientException;
import okhttp3.Authenticator;
import okhttp3.Challenge;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ExternalAuthenticator
        implements Authenticator, Interceptor
{
    public static final String TOKEN_URI_FIELD = "x_token_server";
    public static final String TOKEN_REFRESH_URI_FIELD = "x_token_refresh_server";
    public static final String REDIRECT_URI_FIELD = "x_redirect_server";

    private final TokenPoller tokenPoller;
    private final RedirectHandler redirectHandler;
    private final Duration timeout;
    private final KnownToken knownToken;
    private final int refreshAccessTokenInterval;

    public ExternalAuthenticator(
            RedirectHandler redirect,
            TokenPoller tokenPoller,
            KnownToken knownToken,
            Duration timeout,
            int refreshAccessTokenInterval)
    {
        this.tokenPoller = requireNonNull(tokenPoller, "tokenPoller is null");
        this.redirectHandler = requireNonNull(redirect, "redirect is null");
        this.knownToken = requireNonNull(knownToken, "knownToken is null");
        this.timeout = requireNonNull(timeout, "timeout is null");
        this.refreshAccessTokenInterval = refreshAccessTokenInterval;
    }

    @Nullable
    @Override
    public Request authenticate(Route route, Response response)
    {
        knownToken.setupToken(previousToken -> {
            Optional<Supplier<Optional<Token>>> refreshTokenSupplier = toRefreshedTokenSupplier(response, previousToken);
            if (refreshTokenSupplier.isPresent()) {
                // Token exists. Got expired and refresh challenge is in response.
                return refreshTokenSupplier.get().get();
            }

            Optional<ExternalAuthentication> authentication = toAuthentication(response);
            if (authentication.isPresent()) {
                // Authentication challenge in response
                return authentication.get().obtainToken(timeout, redirectHandler, tokenPoller);
            }

            return Optional.empty();
        });

        return knownToken.getToken()
                .map(value -> withBearerToken(response.request(), value))
                .orElse(null);
    }

    private Optional<Supplier<Optional<Token>>> toRefreshedTokenSupplier(Response response, Optional<Token> token)
    {
        if (refreshAccessTokenInterval < 0 || !token.isPresent()
                || !isTokenRefreshPossible(token.get())) {
            return Optional.empty();
        }
        Optional<String> refreshToken = token.get().getRefreshToken();
        if (refreshToken.isPresent()) {
            for (Challenge challenge : response.challenges()) {
                if (challenge.scheme().equalsIgnoreCase("Bearer")) {
                    return parseField(challenge.authParams(), TOKEN_REFRESH_URI_FIELD)
                            .map(tokenRefreshUri -> () -> toRefreshedToken(refreshToken.get(), tokenRefreshUri, tokenPoller));
                }
            }
        }
        throw new ClientException(format("Failed to refresh access token. Refresh token missing."));
    }

    @Override
    public Response intercept(Chain chain)
            throws IOException
    {
        Optional<Token> availableToken = knownToken.getToken();
        if (availableToken.isPresent()) {
            Token token = availableToken.get();
            if (refreshAccessTokenInterval > 0 && token.isExpired(refreshAccessTokenInterval)
                    && isTokenRefreshPossible(token)) {
                knownToken.setupToken(ignored -> toRefreshedToken(
                        token.getRefreshToken().get(),
                        token.getRefreshUri().get(),
                        tokenPoller));
            }
            return chain.proceed(withBearerToken(chain.request(), token));
        }

        return chain.proceed(chain.request());
    }

    private Optional<Token> toRefreshedToken(String refreshToken, URI tokenRefreshUri, TokenPoller tokenPoller)
    {
        return Optional.of(tokenPoller.pollForToken(
                URI.create(tokenRefreshUri + "?refresh_token=" + refreshToken),
                Duration.ofSeconds(3)).getToken());
    }

    private boolean isTokenRefreshPossible(Token token)
    {
        return token.getRefreshToken().isPresent() && token.getRefreshUri().isPresent();
    }

    private static Request withBearerToken(Request request, Token token)
    {
        return request.newBuilder()
                .header(AUTHORIZATION, "Bearer " + token.token())
                .build();
    }

    @VisibleForTesting
    static Optional<ExternalAuthentication> toAuthentication(Response response)
    {
        for (Challenge challenge : response.challenges()) {
            if (challenge.scheme().equalsIgnoreCase("Bearer")) {
                Optional<URI> tokenUri = parseField(challenge.authParams(), TOKEN_URI_FIELD);
                if (tokenUri.isPresent()) {
                    Optional<URI> redirectUri = parseField(challenge.authParams(), REDIRECT_URI_FIELD);
                    Optional<URI> refreshUri = parseField(challenge.authParams(), TOKEN_REFRESH_URI_FIELD);
                    return Optional.of(new ExternalAuthentication(tokenUri.get(), redirectUri, refreshUri));
                }
            }
        }

        return Optional.empty();
    }

    private static Optional<URI> parseField(Map<String, String> fields, String key)
    {
        return Optional.ofNullable(fields.get(key)).map(value -> {
            try {
                return new URI(value);
            }
            catch (URISyntaxException e) {
                throw new ClientException(format("Failed to parse URI for field '%s'", key), e);
            }
        });
    }
}
