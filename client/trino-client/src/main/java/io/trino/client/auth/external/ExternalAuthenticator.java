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

import io.trino.client.ClientException;
import net.jodah.failsafe.RetryPolicy;
import okhttp3.Authenticator;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static io.trino.client.auth.external.AuthenticationAssembler.toAuthentication;
import static java.util.Objects.requireNonNull;

public class ExternalAuthenticator
        implements Authenticator, Interceptor
{
    private static final String BEARER = "Bearer";

    private final Tokens tokens;
    private final RedirectHandler redirectHandler;
    private final RetryPolicy<TokenPoll> retryPolicy;
    private final Duration maxPollTimeout;
    private AuthenticationToken knownToken;

    public ExternalAuthenticator(RedirectHandler redirect, Tokens tokens, RetryPolicy<TokenPoll> retryPolicy, Duration maxPollTimeout)
    {
        this.tokens = requireNonNull(tokens, "tokens is null");
        this.redirectHandler = requireNonNull(redirect, "redirect is null");
        this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
        this.maxPollTimeout = requireNonNull(maxPollTimeout, "maxPollTimeout is null");
    }

    @Nullable
    @Override
    public Request authenticate(Route route, Response response)
    {
        Set<RuntimeException> headerParsingErrors = new HashSet<>();

        Optional<ExternalAuthentication> externalAuthentication = response.headers(WWW_AUTHENTICATE).stream()
                .filter(header -> header.startsWith(BEARER))
                .flatMap(header -> {
                    try {
                        return Stream.of(toAuthentication(header));
                    }
                    catch (RuntimeException ex) {
                        headerParsingErrors.add(ex);
                        return Stream.empty();
                    }
                })
                .findFirst();

        Optional<AuthenticationToken> obtainedToken = externalAuthentication.orElseThrow(() -> {
            ClientException clientException = new ClientException("Failed to parse Bearer headers for External Authenticator");
            headerParsingErrors.forEach(clientException::addSuppressed);
            return clientException;
        }).obtainToken(retryPolicy.copy(), maxPollTimeout, redirectHandler, tokens);

        if (!obtainedToken.isPresent()) {
            return null;
        }

        knownToken = obtainedToken.get();
        return addBearerToken(response.request().newBuilder(), knownToken);
    }

    @Override
    public Response intercept(Chain chain)
            throws IOException
    {
        if (knownToken != null) {
            Request request = addBearerToken(chain.request().newBuilder(), knownToken);
            return chain.proceed(request);
        }
        return chain.proceed(chain.request());
    }

    private Request addBearerToken(Request.Builder requestBuilder, AuthenticationToken token)
    {
        return requestBuilder
                .addHeader(AUTHORIZATION, BEARER + " " + token.getToken())
                .build();
    }
}
