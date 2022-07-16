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

import io.airlift.log.Logger;
import io.trino.server.security.oauth2.OAuth2Client.Response;
import io.trino.server.security.oauth2.TokenPairSerializer.TokenPair;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static io.trino.server.security.oauth2.OAuth2TokenExchange.hashAuthId;
import static java.util.Objects.requireNonNull;

public class TokenRefresher
{
    private static final Logger LOG = Logger.get(TokenRefresher.class);
    private final TokenPairSerializer tokenAssembler;
    private final OAuth2TokenHandler tokenHandler;
    private final OAuth2Client client;
    private final Duration lastEligibleRefreshTokenTimeout;
    private final ExecutorService executorService;

    public TokenRefresher(TokenPairSerializer tokenAssembler, OAuth2TokenHandler tokenHandler, OAuth2Client client, Duration lastEligibleRefreshTokenTimeout, ExecutorService executorService)
    {
        this.tokenAssembler = requireNonNull(tokenAssembler, "tokenAssembler is null");
        this.tokenHandler = requireNonNull(tokenHandler, "tokenHandler is null");
        this.client = requireNonNull(client, "oAuth2Client is null");
        this.lastEligibleRefreshTokenTimeout = requireNonNull(lastEligibleRefreshTokenTimeout, "lastEligibleRefreshTokenTimeout is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
    }

    public Optional<UUID> refreshToken(TokenPair tokenPair)
    {
        requireNonNull(tokenPair, "tokenPair is null");

        Optional<String> refreshToken = Optional.of(tokenPair)
                .filter(tokens -> tokens.isBeforeExpirationForAtLeast(lastEligibleRefreshTokenTimeout, Clock.systemUTC()))
                .flatMap(TokenPair::getRefreshToken);
        if (refreshToken.isPresent()) {
            UUID refreshingId = UUID.randomUUID();
            refreshToken(refreshToken.get(), refreshingId);
            return Optional.of(refreshingId);
        }
        return Optional.empty();
    }

    private void refreshToken(String refreshToken, UUID refreshingId)
    {
        executorService.submit(() -> {
            try {
                Response response = client.refreshTokens(refreshToken);
                String serializedToken = tokenAssembler.serialize(TokenPair.fromOAuth2Response(response));
                tokenHandler.setAccessToken(hashAuthId(refreshingId), serializedToken);
            }
            catch (Throwable e) {
                tokenHandler.setTokenExchangeError(hashAuthId(refreshingId), "Token refreshing has failed: " + e.getMessage());
            }
        });
    }
}
