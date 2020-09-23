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

import com.google.common.collect.ImmutableList;
import io.trino.client.ClientException;
import net.jodah.failsafe.RetryPolicy;
import okhttp3.OkHttpClient;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.net.URI.create;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestExternalAuthentication
{
    private static final String AUTH_TOKEN = "authToken";
    private static final URI redirectUri = create("https://redirect.uri");
    private static final URI tokenUri = create("https://token.uri");
    private static final Duration IGNORED_DURATION = Duration.ofNanos(1);

    @Test
    public void testObtainTokenWhenTokenAlreadyExists()
    {
        MockRedirectHandler redirectHandler = new MockRedirectHandler();
        StubTokens tokens = new StubTokens(tokenUri, TokenPoll.successful(authToken()));

        Optional<AuthenticationToken> token = new ExternalAuthentication(redirectUri, tokenUri)
                .obtainToken(noRetries(), IGNORED_DURATION, redirectHandler, tokens);

        assertThat(redirectHandler.redirectedTo).isEqualTo(redirectUri);
        assertThat(token.map(AuthenticationToken::getToken)).isEqualTo(Optional.of(AUTH_TOKEN));
    }

    @Test
    public void testObtainTokenWhenTokenIsReadyAt2ndAttempt()
    {
        MockRedirectHandler redirectHandler = new MockRedirectHandler();
        StubTokens tokens = new StubTokens(() -> new TokenPollException(new RuntimeException()))
                .withTokenPolls(tokenUri, ImmutableList.of(TokenPoll.successful(authToken())));

        Optional<AuthenticationToken> token = new ExternalAuthentication(redirectUri, tokenUri)
                .obtainToken(retries(1), IGNORED_DURATION, redirectHandler, tokens);

        assertThat(token)
                .map(AuthenticationToken::getToken)
                .contains(AUTH_TOKEN);
    }

    @Test
    public void testObtainTokenWhenTokenIsNeverAvailable()
    {
        MockRedirectHandler redirectHandler = new MockRedirectHandler();
        StubTokens tokens = new StubTokens(tokenUri, TokenPoll.nextPollingResource(tokenUri));

        Optional<AuthenticationToken> token = new ExternalAuthentication(redirectUri, tokenUri)
                .obtainToken(retries(1), IGNORED_DURATION, redirectHandler, tokens);

        assertThat(token).isEmpty();
    }

    private AuthenticationToken authToken()
    {
        return new AuthenticationToken(AUTH_TOKEN);
    }

    @Test
    public void testObtainTokenWhenPollingFails()
    {
        MockRedirectHandler redirectHandler = new MockRedirectHandler();
        StubTokens tokens = new StubTokens(tokenUri, ImmutableList.of(TokenPoll.failed("error")));

        assertThatThrownBy(() -> new ExternalAuthentication(redirectUri, tokenUri)
                .obtainToken(retries(1), IGNORED_DURATION, redirectHandler, tokens))
                .isInstanceOf(ClientException.class)
                .hasMessage("error");
    }

    @Test
    public void testObtainTokenWhenPollingFailsWithException()
    {
        MockRedirectHandler redirectHandler = new MockRedirectHandler();
        StubTokens tokens = new StubTokens(() -> new RuntimeException("polling failed"));

        assertThatThrownBy(() -> new ExternalAuthentication(redirectUri, tokenUri)
                .obtainToken(retries(1), IGNORED_DURATION, redirectHandler, tokens))
                .hasMessage("polling failed");
    }

    @Test
    public void testObtainTokenWhenNoRedirectUrlHasBeenProvided()
    {
        MockRedirectHandler redirectHandler = new MockRedirectHandler();
        StubTokens tokens = new StubTokens(() -> new TokenPollException(new RuntimeException("polling failed")))
                .withTokenPolls(tokenUri, ImmutableList.of(TokenPoll.successful(authToken())));

        Optional<AuthenticationToken> token = new ExternalAuthentication(tokenUri)
                .obtainToken(retries(2), IGNORED_DURATION, redirectHandler, tokens);

        assertThat(token)
                .map(AuthenticationToken::getToken)
                .isEqualTo(Optional.of(AUTH_TOKEN));
        assertThat(redirectHandler.hasBeenTriggered()).isFalse();
    }

    private static class MockRedirectHandler
            implements RedirectHandler
    {
        private URI redirectedTo;

        @Override
        public void redirectTo(URI uri)
                throws RedirectException
        {
            this.redirectedTo = uri;
        }

        boolean hasBeenTriggered()
        {
            return redirectedTo != null;
        }
    }

    private static class StubTokens
            extends Tokens
    {
        private final Map<URI, List<TokenPoll>> tokenPolls = new HashMap<>();
        private Runnable throwingRunnable = () -> {};

        StubTokens(URI tokenUri, TokenPoll expectedTokenPoll)
        {
            super(new OkHttpClient.Builder().build());

            tokenPolls.put(tokenUri, ImmutableList.of(expectedTokenPoll));
        }

        StubTokens(URI tokenUri, List<TokenPoll> expectedTokenPoll)
        {
            super(new OkHttpClient.Builder().build());

            tokenPolls.put(tokenUri, expectedTokenPoll);
        }

        StubTokens(Supplier<RuntimeException> throwable)
        {
            super(new OkHttpClient.Builder().build());

            throwingRunnable = () -> {
                throw throwable.get();
            };
        }

        StubTokens withTokenPolls(URI tokenUri, List<TokenPoll> expectedTokenPoll)
        {
            tokenPolls.put(tokenUri, expectedTokenPoll);
            return this;
        }

        @Override
        public TokenPoll pollForTokenUntil(URI tokenUri, Duration ignored)
        {
            try {
                throwingRunnable.run();
            }
            finally {
                throwingRunnable = () -> {};
            }

            if (tokenPolls.containsKey(tokenUri)) {
                List<TokenPoll> tokens = this.tokenPolls.get(tokenUri);
                if (tokens.size() > 1) {
                    this.tokenPolls.put(tokenUri, ImmutableList.copyOf(tokens.subList(1, tokens.size())));
                }
                else {
                    this.tokenPolls.put(tokenUri, ImmutableList.of());
                }
                return tokens.get(0);
            }
            throw new IllegalArgumentException("Unknown tokenPath: " + tokenUri);
        }
    }

    private static <T> RetryPolicy<T> noRetries()
    {
        return new RetryPolicy<T>()
                .withMaxAttempts(1);
    }

    private static <T> RetryPolicy<T> retries(int noOfRetries)
    {
        return new RetryPolicy<T>()
                .withMaxAttempts(1 + noOfRetries);
    }
}
