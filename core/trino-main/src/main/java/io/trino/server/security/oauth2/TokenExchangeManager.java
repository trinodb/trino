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
package io.trino.server.security.oauth2;

import com.google.common.cache.Cache;
import com.google.inject.Inject;
import com.nimbusds.oauth2.sdk.AccessTokenResponse;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import com.nimbusds.oauth2.sdk.tokenexchange.TokenExchangeGrant;
import io.airlift.http.client.HttpClient;
import io.airlift.units.Duration;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.OAuth2Token;
import io.trino.spi.security.OAuth2TokenExchanger;
import io.trino.spi.security.TokenExchangeRequest;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.cache.CacheBuilder.newBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;

public class TokenExchangeManager
        implements OAuth2TokenExchanger
{
    private final boolean enabled;
    private final NimbusAirliftHttpClient httpClient;
    private final Duration cacheRefreshMargin;
    private final URI tokenEndpoint;
    private final Optional<ClientAuthentication> clientAuthentication;
    private final ClientID clientId;
    private final Cache<CacheKey, CachedToken> cache;

    @Inject
    public TokenExchangeManager(OAuth2TokenExchangeConfig config, @ForOAuth2TokenExchange HttpClient httpClient)
    {
        requireNonNull(config, "config is null");
        this.httpClient = new NimbusAirliftHttpClient(requireNonNull(httpClient, "httpClient is null"));
        this.cacheRefreshMargin = config.getCacheRefreshMargin();
        this.cache = buildNonEvictableCache(newBuilder()
                .maximumSize(1024)
                .expireAfterWrite(java.time.Duration.ofHours(1)));

        this.enabled = config.isEnabled();
        if (enabled) {
            this.tokenEndpoint = requireNonNull(config.getTokenEndpoint(), "oauth2-token-exchange.token-endpoint must be set when oauth2-token-exchange.enabled is set");
            this.clientId = new ClientID(requireNonNull(config.getClientId(), "oauth2-token-exchange.client-id must be set when oauth2-token-exchange.enabled is set"));
            this.clientAuthentication = config.getClientSecret()
                    .map(secret -> new ClientSecretBasic(clientId, new Secret(secret)));
        }
        else {
            this.tokenEndpoint = null;
            this.clientId = null;
            this.clientAuthentication = Optional.empty();
        }
    }

    @Override
    public Optional<OAuth2Token> exchangeToken(ConnectorIdentity identity, TokenExchangeRequest request)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(request, "request is null");

        checkState(enabled, "OAuth2 token exchange is not enabled. Set oauth2-token-exchange.enabled=true in config.properties");

        String subjectToken = identity.getExtraCredentials().get(INTERNAL_ACCESS_TOKEN_KEY);
        if (subjectToken == null) {
            throw new OAuth2TokenExchangeException("The identity does not carry an OAuth2 access token. Enable http-server.authentication.oauth2.capture-access-token and use an OAuth2 authenticator");
        }

        List<String> requestedAudience = request.audience();
        List<String> requestedScope = request.scope();
        List<Audience> exchangeAudience = requestedAudience.stream().map(Audience::new).collect(toImmutableList());
        Scope exchangeScope = Scope.parse(requestedScope);
        TokenTypeURI requestedTokenType = request.requestedTokenType()
                .map(TokenExchangeManager::parseTokenType)
                .orElse(TokenTypeURI.ACCESS_TOKEN);

        CacheKey cacheKey = new CacheKey(subjectToken, exchangeAudience.toString(), exchangeScope.toString());
        CachedToken cachedToken = cache.getIfPresent(cacheKey);
        if (cachedToken != null && !isExpired(cachedToken)) {
            return Optional.of(new OAuth2Token(cachedToken.accessToken(), cachedToken.expiration()));
        }

        CachedToken exchangedToken = exchange(subjectToken, exchangeAudience, exchangeScope, requestedTokenType);
        // Only positive-cache tokens with a known lifetime
        if (exchangedToken.expiration().isPresent()) {
            cache.put(cacheKey, exchangedToken);
        }
        return Optional.of(new OAuth2Token(exchangedToken.accessToken(), exchangedToken.expiration()));
    }

    private CachedToken exchange(String subjectToken, List<Audience> audience, Scope scope, TokenTypeURI requestedTokenType)
    {
        TokenExchangeGrant grant = new TokenExchangeGrant(
                new BearerAccessToken(subjectToken),
                TokenTypeURI.ACCESS_TOKEN,
                null,
                null,
                requestedTokenType,
                audience);

        TokenRequest tokenRequest = clientAuthentication
                .map(auth -> new TokenRequest(tokenEndpoint, auth, grant, scope))
                .orElseGet(() -> new TokenRequest(tokenEndpoint, clientId, grant, scope));

        AccessTokenResponse response;
        try {
            response = httpClient.execute(tokenRequest, AccessTokenResponse::parse);
        }
        catch (RuntimeException e) {
            throw new OAuth2TokenExchangeException("OAuth2 token exchange request failed", e);
        }
        if (!response.indicatesSuccess()) {
            throw new OAuth2TokenExchangeException("OAuth2 token exchange failed: " + response.toErrorResponse().toHTTPResponse().getBody());
        }

        BearerAccessToken accessToken = response.getTokens().getBearerAccessToken();
        long lifetime = accessToken.getLifetime();
        Optional<Instant> expiration = lifetime > 0
                ? Optional.of(Instant.now().plusSeconds(lifetime))
                : Optional.empty();
        return new CachedToken(accessToken.getValue(), expiration);
    }

    private boolean isExpired(CachedToken token)
    {
        return token.expiration()
                .map(expiration -> !expiration.isAfter(Instant.now().plusMillis(cacheRefreshMargin.toMillis())))
                .orElse(true);
    }

    private static TokenTypeURI parseTokenType(String value)
    {
        try {
            return TokenTypeURI.parse(value);
        }
        catch (ParseException e) {
            throw new IllegalArgumentException("Invalid token type: %s".formatted(value), e);
        }
    }

    private record CacheKey(String subjectToken, String audience, String scope) {}

    private record CachedToken(String accessToken, Optional<Instant> expiration) {}
}
