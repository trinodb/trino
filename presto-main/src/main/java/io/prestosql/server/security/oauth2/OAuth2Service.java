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
package io.prestosql.server.security.oauth2;

import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuth2AccessTokenErrorResponse;
import com.github.scribejava.core.model.OAuthConstants;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.prestosql.server.security.oauth2.Challenge.Started;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.server.security.oauth2.OAuth2Resource.CALLBACK_ENDPOINT;
import static io.prestosql.server.security.oauth2.OAuth2Resource.OAUTH2_API_PREFIX;
import static java.util.Objects.requireNonNull;

public class OAuth2Service
{
    private final DynamicCallbackOAuth2Service service;
    private final JwtParser jwtParser;
    private final Cache<State, Challenge> authorizationChallenges;
    private final Duration maxPollDuration;

    @Inject
    public OAuth2Service(OAuth2Config oauth2Config)
    {
        requireNonNull(oauth2Config, "oauth2Config is null");
        this.service = new DynamicCallbackOAuth2Service(
                OAuth2Api.create(oauth2Config),
                oauth2Config.getClientId(),
                oauth2Config.getClientSecret(),
                "openid");
        this.jwtParser = Jwts.parser()
                .setSigningKeyResolver(new JWKSSigningKeyResolver(oauth2Config.getServerUrl()));
        this.authorizationChallenges = CacheBuilder.newBuilder()
                .expireAfterWrite(oauth2Config.getChallengeTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .build();
        this.maxPollDuration = Duration.ofMillis(oauth2Config.getMaxSinglePollMs());
    }

    Started startChallenge(URI serverUri)
    {
        State state = State.randomState();
        String authorizationUrl = service.getAuthorizationUrl(
                ImmutableMap.of(
                        OAuthConstants.REDIRECT_URI, serverUri.resolve(OAUTH2_API_PREFIX + CALLBACK_ENDPOINT).toString(),
                        OAuthConstants.STATE, state.toString()));
        Started challenge = new Started(state, authorizationUrl);
        authorizationChallenges.put(state, challenge);
        return challenge;
    }

    CompletableFuture<Challenge> pollForFinish(State state, ExecutorService executor)
            throws ChallengeNotFoundException
    {
        return Failsafe.with(new RetryPolicy<Challenge>()
                .handleResultIf(Challenge::isPending)
                .withMaxAttempts(-1)
                .withMaxDuration(maxPollDuration)
                .withDelay(maxPollDuration.dividedBy(10)))
                .with(executor)
                .getAsync(() -> Optional.ofNullable(authorizationChallenges.getIfPresent(state))
                        .orElseThrow(() -> new ChallengeNotFoundException(state)));
    }

    Challenge finishChallenge(
            URI serverUri,
            State state,
            Optional<String> code,
            Optional<OAuth2ErrorResponse> error)
            throws ChallengeNotFoundException
    {
        requireNonNull(state, "state is null");
        requireNonNull(code, "code is null");
        requireNonNull(error, "error is null");
        checkArgument(code.isPresent() || error.isPresent(), "Either code or error should be present");
        checkArgument(code.isEmpty() || error.isEmpty(), "Either code or error should be empty");

        return authorizationChallenges.asMap()
                .compute(state, (key, challenge) -> {
                    Started started = Optional.ofNullable(challenge)
                            .flatMap(c -> c.isInStatus(Status.STARTED))
                            .orElseThrow(() -> new ChallengeNotFoundException(state));
                    if (error.isPresent()) {
                        return error.map(started::fail)
                                .orElseThrow();
                    }
                    return finishChallenge(started, code.orElseThrow(), serverUri);
                });
    }

    private Challenge finishChallenge(Started challenge, String code, URI serverUri)
    {
        try {
            OAuth2AccessToken token = getToken(code, serverUri);
            return challenge.succeed(token, parseClaimsJws(token.getAccessToken()));
        }
        catch (OAuth2AccessTokenErrorResponse e) {
            return challenge.fail(
                    new OAuth2ErrorResponse(
                            e.getError(),
                            Optional.ofNullable(e.getErrorDescription()),
                            Optional.ofNullable(e.getErrorUri())));
        }
    }

    private OAuth2AccessToken getToken(String code, URI serverUri)
    {
        try {
            return service.getAccessToken(code, serverUri.resolve(OAUTH2_API_PREFIX + CALLBACK_ENDPOINT).toString());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    Jws<Claims> parseClaimsJws(String token)
    {
        return jwtParser.parseClaimsJws(token);
    }
}
