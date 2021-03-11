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
import okhttp3.HttpUrl;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Optional;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static io.trino.client.auth.external.ExternalAuthenticator.TOKEN_URI_FIELD;
import static io.trino.client.auth.external.ExternalAuthenticator.toAuthentication;
import static io.trino.client.auth.external.TokenPollResult.successful;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.net.URI.create;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestExternalAuthenticator
{
    @Test
    public void testChallengeWithOnlyTokenServerUri()
    {
        assertThat(buildAuthentication("Bearer x_token_server=\"http://token.uri\""))
                .hasValueSatisfying(authentication -> {
                    assertThat(authentication.getRedirectUri()).isEmpty();
                    assertThat(authentication.getTokenUri()).isEqualTo(create("http://token.uri"));
                });
    }

    @Test
    public void testChallengeWithBothUri()
    {
        assertThat(buildAuthentication("Bearer x_redirect_server=\"http://redirect.uri\", x_token_server=\"http://token.uri\""))
                .hasValueSatisfying(authentication -> {
                    assertThat(authentication.getRedirectUri()).hasValue(create("http://redirect.uri"));
                    assertThat(authentication.getTokenUri()).isEqualTo(create("http://token.uri"));
                });
    }

    @Test
    public void testChallengeWithValuesWithoutQuotes()
    {
        // this is legal according to RFC 7235
        assertThat(buildAuthentication("Bearer x_redirect_server=http://redirect.uri, x_token_server=http://token.uri"))
                .hasValueSatisfying(authentication -> {
                    assertThat(authentication.getRedirectUri()).hasValue(create("http://redirect.uri"));
                    assertThat(authentication.getTokenUri()).isEqualTo(create("http://token.uri"));
                });
    }

    @Test
    public void testChallengeWithAdditionalFields()
    {
        assertThat(buildAuthentication("Bearer type=\"token\", x_redirect_server=\"http://redirect.uri\", x_token_server=\"http://token.uri\", description=\"oauth challenge\""))
                .hasValueSatisfying(authentication -> {
                    assertThat(authentication.getRedirectUri()).hasValue(create("http://redirect.uri"));
                    assertThat(authentication.getTokenUri()).isEqualTo(create("http://token.uri"));
                });
    }

    @Test
    public void testInvalidChallenges()
    {
        // no authentication parameters
        assertThat(buildAuthentication("Bearer")).isEmpty();

        // no Bearer scheme prefix
        assertThat(buildAuthentication("x_redirect_server=\"http://redirect.uri\", x_token_server=\"http://token.uri\"")).isEmpty();

        // space instead of comma
        assertThat(buildAuthentication("Bearer x_redirect_server=\"http://redirect.uri\" x_token_server=\"http://token.uri\"")).isEmpty();

        // equals sign instead of comma
        assertThat(buildAuthentication("Bearer x_redirect_server=\"http://redirect.uri\"=x_token_server=\"http://token.uri\"")).isEmpty();
    }

    @Test
    public void testChallengeWithMalformedUri()
    {
        assertThatThrownBy(() -> buildAuthentication("Bearer x_token_server=\"http://[1.1.1.1]\""))
                .isInstanceOf(ClientException.class)
                .hasMessageContaining(format("Failed to parse URI for field '%s'", TOKEN_URI_FIELD))
                .hasRootCauseInstanceOf(URISyntaxException.class)
                .hasRootCauseMessage("Malformed IPv6 address at index 8: http://[1.1.1.1]");
    }

    @Test
    public void testAuthentication()
    {
        MockTokenPoller tokenPoller = new MockTokenPoller()
                .withResult(URI.create("http://token.uri"), successful(new Token("valid-token")));
        ExternalAuthenticator authenticator = new ExternalAuthenticator(uri -> {}, tokenPoller, Duration.ofSeconds(1));

        Request authenticated = authenticator.authenticate(null, getUnauthorizedResponse("Bearer x_token_server=\"http://token.uri\""));

        assertThat(authenticated.headers())
                .extracting(headers -> headers.get(AUTHORIZATION))
                .isEqualTo("Bearer valid-token");
    }

    @Test
    public void testReAuthenticationAfterRejectingToken()
    {
        MockTokenPoller tokenPoller = new MockTokenPoller()
                .withResult(URI.create("http://token.uri"), successful(new Token("first-token")))
                .withResult(URI.create("http://token.uri"), successful(new Token("second-token")));
        ExternalAuthenticator authenticator = new ExternalAuthenticator(uri -> {}, tokenPoller, Duration.ofSeconds(1));

        Request request = authenticator.authenticate(null, getUnauthorizedResponse("Bearer x_token_server=\"http://token.uri\""));
        Request reAuthenticated = authenticator.authenticate(null, getUnauthorizedResponse("Bearer x_token_server=\"http://token.uri\"", request));

        assertThat(reAuthenticated.headers(AUTHORIZATION))
                .containsExactly("Bearer second-token");
    }

    private static Optional<ExternalAuthentication> buildAuthentication(String challengeHeader)
    {
        return toAuthentication(getUnauthorizedResponse(challengeHeader));
    }

    private static Response getUnauthorizedResponse(String challengeHeader)
    {
        return getUnauthorizedResponse(challengeHeader,
                new Request.Builder()
                        .url(HttpUrl.get("http://example.com"))
                        .build());
    }

    private static Response getUnauthorizedResponse(String challengeHeader, Request request)
    {
        return new Response.Builder()
                .request(request)
                .protocol(Protocol.HTTP_1_1)
                .code(HTTP_UNAUTHORIZED)
                .message("Unauthorized")
                .header(WWW_AUTHENTICATE, challengeHeader)
                .build();
    }
}
