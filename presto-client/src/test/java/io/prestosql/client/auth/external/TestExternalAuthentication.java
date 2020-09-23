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
package io.prestosql.client.auth.external;

import com.google.common.collect.ImmutableList;
import okhttp3.OkHttpClient;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static io.prestosql.client.auth.external.RetryPolicies.noRetries;
import static io.prestosql.client.auth.external.RetryPolicies.retries;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestExternalAuthentication
{
    private static final String AUTH_TOKEN = "authToken";

    @Test
    public void testObtainTokenWhenTokenAlreadyExists()
    {
        MockRedirectHandler redirectHandler = new MockRedirectHandler();
        StubTokens tokens = new StubTokens("tokenUrl", TokenPoll.successful(authToken()));

        Optional<AuthenticationToken> token = new ExternalAuthentication("redirectUrl", "tokenUrl")
                .obtainToken(noRetries(), redirectHandler, tokens);

        assertThat(redirectHandler.redirectToUrl).isEqualTo("redirectUrl");
        assertThat(token.map(AuthenticationToken::asString)).isEqualTo(Optional.of(AUTH_TOKEN));
    }

    @Test
    public void testObtainTokenWhenTokenIsReadyAt2ndAttempt()
    {
        MockRedirectHandler redirectHandler = new MockRedirectHandler();
        StubTokens tokens = new StubTokens("tokenUrl", ImmutableList.of(TokenPoll.pending(), TokenPoll.successful(authToken())));

        Optional<AuthenticationToken> token = new ExternalAuthentication("redirectUrl", "tokenUrl")
                .obtainToken(retries(1), redirectHandler, tokens);

        assertThat(token)
                .map(AuthenticationToken::asString)
                .isEqualTo(Optional.of(AUTH_TOKEN));
    }

    private AuthenticationToken authToken()
    {
        return new AuthenticationToken(AUTH_TOKEN);
    }

    @Test
    public void testObtainTokenWhenPollingFails()
    {
        MockRedirectHandler redirectHandler = new MockRedirectHandler();
        StubTokens tokens = new StubTokens("tokenUrl", ImmutableList.of(TokenPoll.pending(), TokenPoll.failed()));

        Optional<AuthenticationToken> token = new ExternalAuthentication("redirectUrl", "tokenUrl")
                .obtainToken(retries(1), redirectHandler, tokens);

        assertThat(token).isEqualTo(Optional.empty());
    }

    @Test
    public void testObtainTokenWhenPollingFailsWithException()
    {
        MockRedirectHandler redirectHandler = new MockRedirectHandler();
        StubTokens tokens = new StubTokens(() -> new RuntimeException("polling failed"));

        assertThatThrownBy(() -> new ExternalAuthentication("redirectUrl", "tokenUrl")
                .obtainToken(retries(1), redirectHandler, tokens))
                .hasMessage("polling failed");
    }

    @Test
    public void testObtainTokenWhenPollingFailsWithTokenPollException()
    {
        MockRedirectHandler redirectHandler = new MockRedirectHandler();
        StubTokens tokens = new StubTokens(() -> new TokenPollException(new RuntimeException("polling failed")))
                .withTokenPolls("tokenUrl", ImmutableList.of(TokenPoll.successful(authToken())));

        Optional<AuthenticationToken> token = new ExternalAuthentication("redirectUrl", "tokenUrl")
                .obtainToken(retries(2), redirectHandler, tokens);

        assertThat(token)
                .map(AuthenticationToken::asString)
                .isEqualTo(Optional.of(AUTH_TOKEN));
    }

    private static class MockRedirectHandler
            implements RedirectHandler
    {
        private String redirectToUrl;

        @Override
        public void redirectTo(String uri)
                throws RedirectException
        {
            this.redirectToUrl = uri;
        }
    }

    private static class StubTokens
            extends Tokens
    {
        private final Map<String, List<TokenPoll>> tokenPolls = new HashMap<>();
        private Runnable throwingRunnable = () -> {};

        StubTokens(String tokenUrl, TokenPoll expectedTokenPoll)
        {
            super(new OkHttpClient.Builder().build(), Duration.ZERO);

            tokenPolls.put(tokenUrl, ImmutableList.of(expectedTokenPoll));
        }

        StubTokens(String tokenPath, List<TokenPoll> expectedTokenPoll)
        {
            super(new OkHttpClient.Builder().build(), Duration.ZERO);

            tokenPolls.put(tokenPath, expectedTokenPoll);
        }

        StubTokens(Supplier<RuntimeException> throwable)
        {
            super(new OkHttpClient.Builder().build(), Duration.ZERO);

            throwingRunnable = () -> {
                throw throwable.get();
            };
        }

        StubTokens withTokenPolls(String tokenPath, List<TokenPoll> expectedTokenPoll)
        {
            tokenPolls.put(tokenPath, expectedTokenPoll);
            return this;
        }

        @Override
        public TokenPoll pollForToken(String tokenUrl)
        {
            try {
                throwingRunnable.run();
            }
            finally {
                throwingRunnable = () -> {};
            }

            if (tokenPolls.containsKey(tokenUrl)) {
                List<TokenPoll> tokens = this.tokenPolls.get(tokenUrl);
                if (tokens.size() > 1) {
                    this.tokenPolls.put(tokenUrl, ImmutableList.copyOf(tokens.subList(1, tokens.size())));
                }
                else {
                    this.tokenPolls.put(tokenUrl, ImmutableList.of());
                }
                return tokens.get(0);
            }
            throw new IllegalArgumentException("Unknown tokenPath: " + tokenUrl);
        }
    }
}
