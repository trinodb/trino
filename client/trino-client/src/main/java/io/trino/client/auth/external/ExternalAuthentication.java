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
import net.jodah.failsafe.ExecutionContext;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;

import static java.lang.Math.max;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;

class ExternalAuthentication
{
    private final Optional<URI> redirectUri;
    private final URI tokenUri;

    public ExternalAuthentication(URI redirectUri, URI tokenUri)
    {
        this.redirectUri = Optional.of(requireNonNull(redirectUri, "redirectUri is null"));
        this.tokenUri = requireNonNull(tokenUri, "tokenUri is null");
    }

    public ExternalAuthentication(URI tokenUri)
    {
        this.redirectUri = Optional.empty();
        this.tokenUri = requireNonNull(tokenUri, "tokenUri is null");
    }

    public Optional<AuthenticationToken> obtainToken(RetryPolicy<TokenPoll> retryPolicy, Duration maxPollingTime, RedirectHandler handler, Tokens tokens)
    {
        requireNonNull(retryPolicy, "retryPolicy is null");
        requireNonNull(maxPollingTime, "maxPollingTime is null");
        requireNonNull(handler, "handler is null");
        requireNonNull(tokens, "prestoAuthentications is null");

        redirectUri.ifPresent(handler::redirectTo);

        TokenPoll tokenPoll = Failsafe.with(retryPolicy
                .abortIf(TokenPoll::hasFailed)
                .handleIf(TokenPollException.class::isInstance))
                .get(context -> pollForToken(context, tokens, maxPollingTime));

        tokenPoll.getError()
                .ifPresent(error -> {
                    throw new ClientException(error);
                });

        return tokenPoll.getToken();
    }

    private TokenPoll pollForToken(ExecutionContext context, Tokens tokens, Duration maxPollingTime)
    {
        Duration remainingTime = Duration.of(max(maxPollingTime.toMillis() - context.getElapsedTime().toMillis(), 0), MILLIS);
        return Optional.<TokenPoll>ofNullable(context.getLastResult())
                .flatMap(TokenPoll::getNextTokenUri)
                .map(lastUri -> tokens.pollForTokenUntil(lastUri, remainingTime)) //In case off exception, we should retry polling from last obtained nextTokenUri
                .orElseGet(() -> tokens.pollForTokenUntil(tokenUri, remainingTime)); //If no nextTokenUri is available, then we should poll at the original tokenUri
    }

    @VisibleForTesting
    Optional<URI> getRedirectUri()
    {
        return redirectUri;
    }

    @VisibleForTesting
    URI getTokenUri()
    {
        return tokenUri;
    }
}
