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
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.prestosql.server.security.oauth2.Challenge.Started;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.server.security.oauth2.OAuth2Resource.CALLBACK_ENDPOINT;
import static io.prestosql.server.security.oauth2.OAuth2Resource.OAUTH2_API_PREFIX;
import static java.util.Objects.requireNonNull;

public class OAuth2Service
{
    private final DynamicCallbackOAuth2Service service;
    private final JwtParser jwtParser;
    private final Cache<State, Started> authorizationChallenges;

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

    Challenge finishChallenge(
            URI serverUri,
            State state,
            Optional<String> code,
            Optional<OAuth2ErrorResponse> error)
            throws InterruptedException, ExecutionException, IOException
    {
        requireNonNull(state, "state is null");
        requireNonNull(code, "code is null");
        requireNonNull(error, "error is null");
        checkArgument(code.isPresent() || error.isPresent(), "Either code or error should be present");
        checkArgument(code.isEmpty() || error.isEmpty(), "Either code or error should be empty");
        Started challenge = getStartedChallenge(state);
        authorizationChallenges.invalidate(state);
        if (error.isPresent()) {
            return challenge.fail(error.get());
        }
        try {
            OAuth2AccessToken token = service.getAccessToken(code.get(), serverUri.resolve(OAUTH2_API_PREFIX + CALLBACK_ENDPOINT).toString());
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

    Jws<Claims> parseClaimsJws(String token)
    {
        return jwtParser.parseClaimsJws(token);
    }

    private Started getStartedChallenge(State state)
    {
        return Optional
                .ofNullable(authorizationChallenges.getIfPresent(state))
                .orElseThrow(() -> new ChallengeNotFoundException(state));
    }
}
