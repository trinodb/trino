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
import net.jodah.failsafe.FailsafeException;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Duration;

import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Objects.requireNonNull;

class Tokens
{
    public static final int HTTP_FAILED_DEPENDENCY = 424;
    private final OkHttpClient client;
    private Duration timeout;

    Tokens(OkHttpClient client, Duration timeout)
    {
        this.client = requireNonNull(client, "client is null");
        this.timeout = requireNonNull(timeout, "timeout is null");
    }

    public TokenPoll pollForToken(String tokenUrl)
            throws TokenPollException
    {
        Call call = client.newCall(new Request.Builder()
                .url(tokenUrl)
                .get()
                .build());
        try {
            return Failsafe.with(RetryPolicies.<TokenPoll>retryUntil(timeout)
                    .handleResultIf(TokenPoll::isNotAvailableYet)
                    .handleIf(tx -> false))
                    .get(() -> handleResponse(call.clone()));
        }
        catch (FailsafeException e) {
            // OkHttp throws this after clearing the interrupt status
            // TODO: remove after updating to Okio 1.15.0+
            Throwable cause = e.getCause();
            if (InterruptedIOException.class.equals(cause.getClass()) && "thread interrupted".equals(cause.getMessage())) {
                Thread.currentThread().interrupt();
            }
            throw new TokenPollException(cause);
        }
    }

    private TokenPoll handleResponse(Call call)
            throws IOException
    {
        try (Response response = call.execute()) {
            switch (response.code()) {
                case HTTP_OK:
                    return TokenPoll.successful(
                            new AuthenticationToken(response.body().string()));
                case HTTP_ACCEPTED:
                    return TokenPoll.pending();
                case HTTP_FAILED_DEPENDENCY:
                    return TokenPoll.failed();
                default:
                    if (response.code() > 399) {
                        String body = response.body().string();
                        throw new IOException("Token poll failed with message: " + body);
                    }
                    throw new IllegalArgumentException(format("Unknown response code \"%s\", retrieved from token poll", response.code()));
            }
        }
    }
}
