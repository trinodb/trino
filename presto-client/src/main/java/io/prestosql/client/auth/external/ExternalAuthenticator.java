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

import net.jodah.failsafe.RetryPolicy;
import okhttp3.Authenticator;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

import javax.annotation.Nullable;

import java.net.URI;
import java.time.Duration;
import java.util.List;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static io.prestosql.client.auth.external.RetryPolicies.retryWithBackoffUntil;
import static java.util.Objects.requireNonNull;

public class ExternalAuthenticator
        implements Authenticator
{
    private final Tokens tokens;
    private final RedirectHandler redirect;
    private final RetryPolicy<TokenPoll> retryPolicy;
    private final AuthenticationAssembler assember;

    ExternalAuthenticator(RedirectHandler redirect, Tokens tokens, AuthenticationAssembler assember, RetryPolicy<TokenPoll> retryPolicy)
    {
        this.tokens = requireNonNull(tokens, "tokens is null");
        this.redirect = requireNonNull(redirect, "redirect is null");
        this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
        this.assember = assember;
    }

    public static void setupExternalAuthenticator(OkHttpClient.Builder builder, URI httpUri, Duration timeout, RedirectHandler redirectHandler)
    {
        builder.authenticator(new ExternalAuthenticator(
                redirectHandler,
                new Tokens(builder.build(), timeout),
                new AuthenticationAssembler(httpUri),
                retryWithBackoffUntil(timeout)));
    }

    @Nullable
    @Override
    public Request authenticate(Route route, Response response)
    {
        List<String> authorizationHeaders = response.request().headers(AUTHORIZATION);
        if (authorizationHeaders.stream()
                .anyMatch(header -> header.startsWith("Bearer"))) {
            return null;
        }
        return response.headers(WWW_AUTHENTICATE).stream()
                .filter(header -> header.startsWith("External-Bearer"))
                .findFirst()
                .map(assember::toAuthentication)
                .flatMap(authentication -> authentication.obtainToken(
                        retryPolicy.copy(),
                        redirect,
                        tokens))
                .map(token -> addBearerToken(response.request().newBuilder(), token))
                .orElse(null);
    }

    private Request addBearerToken(Request.Builder requestBuilder, AuthenticationToken token)
    {
        return requestBuilder
                .addHeader(AUTHORIZATION, "Bearer " + token.asString())
                .build();
    }
}
