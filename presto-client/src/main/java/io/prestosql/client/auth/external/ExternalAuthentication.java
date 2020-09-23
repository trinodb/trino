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

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

class ExternalAuthentication
{
    private final String redirectUrl;
    private final String tokenPath;

    ExternalAuthentication(String redirectUrl, String tokenPath)
    {
        this.redirectUrl = requireNonNull(redirectUrl, "redirectUrl is null");
        this.tokenPath = requireNonNull(tokenPath, "tokenUrl is null");
    }

    Optional<AuthenticationToken> obtainToken(RetryPolicy<TokenPoll> retryPolicy, RedirectHandler handler, Tokens tokens)
    {
        requireNonNull(retryPolicy, "retryPolicy is null");
        requireNonNull(handler, "handler is null");
        requireNonNull(tokens, "prestoAuthentications is null");

        handler.redirectTo(redirectUrl);

        TokenPoll tokenPoll = Failsafe.with(retryPolicy
                .abortIf(TokenPoll::hasFailed)
                .handleIf(ex -> ex instanceof TokenPollException)
                .handleResultIf(TokenPoll::isNotAvailableYet))
                .get(() -> tokens.pollForToken(tokenPath));

        return tokenPoll.getToken();
    }

    String getRedirectUrl()
    {
        return redirectUrl;
    }

    String getTokenPath()
    {
        return tokenPath;
    }
}
