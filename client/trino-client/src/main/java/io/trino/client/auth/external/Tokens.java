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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.json.JsonCodec;
import io.trino.client.ClientException;
import io.trino.client.JsonResponse;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeException;
import net.jodah.failsafe.RetryPolicy;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Optional;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.client.JsonResponse.execute;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static java.util.Objects.requireNonNull;

public class Tokens
{
    private static final JsonCodec<TokenPollRepresentation> TOKEN_POLL_CODEC = jsonCodec(TokenPollRepresentation.class);
    private static final String USER_AGENT_VALUE = "TokenClientV1" +
            "/" +
            firstNonNull(Tokens.class.getPackage().getImplementationVersion(), "unknown");

    private final OkHttpClient client;

    public Tokens(OkHttpClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    public TokenPoll pollForTokenUntil(URI tokenUri, Duration timeout)
            throws TokenPollException, ClientException
    {
        try {
            return Failsafe.with(new RetryPolicy<TokenPoll>()
                    .withMaxAttempts(-1)
                    .withMaxDuration(timeout)
                    .handleResultIf(TokenPoll::isPending)
                    .abortOn(Throwable.class))
                    .get(context -> {
                        Request request = Optional.<TokenPoll>ofNullable(context.getLastResult())
                                .flatMap(TokenPoll::getNextTokenUri)
                                .map(Tokens::createRequestFor)
                                .orElseGet(() -> createRequestFor(tokenUri));
                        return handleRequest(request);
                    });
        }
        catch (FailsafeException e) {
            throw new TokenPollException(e.getCause());
        }
    }

    private static Request createRequestFor(URI tokenUri)
    {
        HttpUrl requestUrl = HttpUrl.get(tokenUri);
        if (requestUrl == null) {
            throw new ClientException(format("Parsing \"%s\" to URL has failed", tokenUri.toString()));
        }

        return new Request.Builder()
                .addHeader(USER_AGENT, USER_AGENT_VALUE)
                .url(requestUrl)
                .get()
                .build();
    }

    private TokenPoll handleRequest(Request request)
            throws IOException
    {
        JsonResponse<TokenPollRepresentation> response = execute(TOKEN_POLL_CODEC, client, request);
        switch (response.getStatusCode()) {
            case HTTP_OK:
                return response.getValue().toTokenPoll();
            case HTTP_UNAVAILABLE:
                throw new IOException(format("Token poll failed with message: %s", response.getResponseBody()));
            default:
                return TokenPoll.failed(format("Unknown response code \"%s\", retrieved from token poll", response.getStatusCode()));
        }
    }

    public static class TokenPollRepresentation
    {
        private final String token;
        private final String nextUri;
        private final String error;

        @JsonCreator
        public TokenPollRepresentation(
                @JsonProperty("token") String token,
                @JsonProperty("nextUri") String nextUri,
                @JsonProperty("error") String error)
        {
            this.token = token;
            this.nextUri = nextUri;
            this.error = error;
        }

        TokenPoll toTokenPoll()
        {
            if (token != null) {
                return TokenPoll.successful(new AuthenticationToken(token));
            }
            if (error != null) {
                return TokenPoll.failed(error);
            }
            if (nextUri != null && !nextUri.trim().isEmpty()) {
                try {
                    return TokenPoll.nextPollingResource(new URI(nextUri));
                }
                catch (URISyntaxException e) {
                    throw new ClientException("Parsing nextUri field to URI has failed", e);
                }
            }
            return TokenPoll.failed("Token poll has failed, as it has not retrieved any know state. either token, error or nextUri fields are required");
        }
    }
}
