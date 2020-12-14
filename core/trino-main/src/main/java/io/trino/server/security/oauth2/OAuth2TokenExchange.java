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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import io.trino.dispatcher.DispatchExecutor;

import javax.inject.Inject;

import java.util.Optional;
import java.util.UUID;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OAuth2TokenExchange
{
    public static final Duration MAX_POLL_TIME = new Duration(9, SECONDS);
    private static final TokenPoll TOKEN_POLL_TIMED_OUT = TokenPoll.error("Authentication has timed out");

    private final LoadingCache<UUID, SettableFuture<TokenPoll>> cache;
    private final ListeningExecutorService responseExecutor;

    @Inject
    public OAuth2TokenExchange(OAuth2Config config, DispatchExecutor executor)
    {
        this.responseExecutor = requireNonNull(executor, "responseExecutor is null").getExecutor();

        long challengeTimeoutMs = config.getChallengeTimeout().toMillis();
        ListeningScheduledExecutorService scheduledExecutor = executor.getScheduledExecutor();
        this.cache = CacheBuilder.newBuilder()
                .expireAfterWrite((challengeTimeoutMs + MAX_POLL_TIME.toMillis()) * 2, MILLISECONDS)
                .<UUID, SettableFuture<TokenPoll>>removalListener(notification -> notification.getValue().set(TOKEN_POLL_TIMED_OUT))
                .build(new CacheLoader<>()
                {
                    @Override
                    public SettableFuture<TokenPoll> load(UUID authId)
                    {
                        SettableFuture<TokenPoll> future = SettableFuture.create();
                        ListenableScheduledFuture<?> timeout = scheduledExecutor.schedule(() -> future.set(TOKEN_POLL_TIMED_OUT), challengeTimeoutMs, MILLISECONDS);
                        future.addListener(() -> timeout.cancel(true), responseExecutor);
                        return future;
                    }
                });
    }

    public void setAccessToken(UUID authId, String accessToken)
    {
        cache.getUnchecked(authId).set(TokenPoll.token(accessToken));
    }

    public void setTokenExchangeError(UUID authId, String message)
    {
        cache.getUnchecked(authId).set(TokenPoll.error(message));
    }

    public ListenableFuture<TokenPoll> getTokenPoll(UUID authId)
    {
        return nonCancellationPropagating(cache.getUnchecked(authId));
    }

    public void dropToken(UUID authId)
    {
        cache.invalidate(authId);
    }

    public static class TokenPoll
    {
        private final Optional<String> token;
        private final Optional<String> error;

        private TokenPoll(String token, String error)
        {
            this.token = Optional.ofNullable(token);
            this.error = Optional.ofNullable(error);
        }

        static TokenPoll token(String token)
        {
            requireNonNull(token, "token is null");

            return new TokenPoll(token, null);
        }

        static TokenPoll error(String error)
        {
            requireNonNull(error, "token is null");

            return new TokenPoll(null, error);
        }

        public Optional<String> getToken()
        {
            return token;
        }

        public Optional<String> getError()
        {
            return error;
        }
    }
}
