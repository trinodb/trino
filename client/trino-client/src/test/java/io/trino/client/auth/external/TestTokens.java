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
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.QueueDispatcher;
import okhttp3.mockwebserver.RecordedRequest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.URI.create;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestTokens
{
    private static final String TOKEN_PATH = "/v1/authentications/sso/test/token";
    private static final String JSON_PAIR = "{\"%s\" : \"%s\"}";
    private static final Duration ONE_SECOND = Duration.ofSeconds(1);
    private Tokens tokens;
    private MockWebServer server;

    @BeforeMethod(alwaysRun = true)
    public void setup()
            throws Exception
    {
        server = new MockWebServer();
        server.start();

        tokens = new Tokens(
                new OkHttpClient.Builder().build());
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws IOException
    {
        server.close();
    }

    @Test
    public void testTokenPollWhenTokenIsReady()
    {
        server.enqueue(statusAndBody(HTTP_OK, format(JSON_PAIR, "token", "token")));

        TokenPoll tokenPoll = tokens.pollForTokenUntil(tokenUri(), ONE_SECOND);

        assertThat(tokenPoll.getToken()).map(AuthenticationToken::getToken)
                .isEqualTo(Optional.of("token"));
    }

    @Test
    public void testTokenPollWhenItsNotReadyAtFirst()
    {
        Stream.concat(
                Stream.generate(() -> statusAndBody(HTTP_OK, format(JSON_PAIR, "nextUri", tokenUri()))).limit(100),
                Stream.of(statusAndBody(HTTP_OK, format(JSON_PAIR, "token", "token"))))
                .forEach(server::enqueue);

        TokenPoll tokenPoll = tokens.pollForTokenUntil(tokenUri(), ONE_SECOND);

        assertThat(tokenPoll.getToken()).map(AuthenticationToken::getToken)
                .contains("token");
    }

    @Test
    public void testTokenPollWhenItsNotReadyOverATimeout()
    {
        server.setDispatcher(delayedDispatcher(Duration.ofMillis(10)));
        Stream.generate(() -> statusAndBody(HTTP_OK, format(JSON_PAIR, "nextUri", tokenUri()))).limit(200)
                .forEach(server::enqueue);

        TokenPoll tokenPoll = tokens.pollForTokenUntil(tokenUri(), ONE_SECOND);

        assertThat(tokenPoll.getToken()).isEmpty();
    }

    @Test
    public void testTokenPollWhenThePollingHasFailed()
    {
        server.enqueue(statusAndBody(HTTP_OK, format(JSON_PAIR, "error", "error_message")));

        TokenPoll tokenPoll = tokens.pollForTokenUntil(tokenUri(), ONE_SECOND);

        assertThat(tokenPoll.hasFailed()).isTrue();
        assertThat(tokenPoll.getError()).contains("error_message");
    }

    @Test
    public void testTokenPollWhenItNoTokenPollAvailable()
    {
        server.enqueue(status(HTTP_NOT_FOUND));

        TokenPoll tokenPoll = tokens.pollForTokenUntil(tokenUri(), ONE_SECOND);

        assertThat(tokenPoll.hasFailed()).isTrue();
    }

    @Test
    public void testTokenPollWhenServerReturns201()
    {
        server.enqueue(status(HTTP_CREATED));

        TokenPoll tokenPoll = tokens.pollForTokenUntil(tokenUri(), ONE_SECOND);

        assertThat(tokenPoll.hasFailed()).isTrue();
        assertThat(tokenPoll.getError())
                .contains("Unknown response code \"201\", retrieved from token poll");
    }

    @Test
    public void testTokenPollWhenServerReturns200WithoutAnyValidState()
    {
        server.enqueue(statusAndBody(HTTP_OK, format(JSON_PAIR, "foo", "bar")));

        TokenPoll tokenPoll = tokens.pollForTokenUntil(tokenUri(), ONE_SECOND);

        assertThat(tokenPoll.hasFailed()).isTrue();
        assertThat(tokenPoll.getError())
                .contains("Token poll has failed, as it has not retrieved any know state. either token, error or nextUri fields are required");
    }

    @Test
    public void testTokenPollWhenServerReturns503()
    {
        server.enqueue(statusAndBody(503, "Server failed unexpectedly"));

        assertThatThrownBy(() -> tokens.pollForTokenUntil(tokenUri(), ONE_SECOND))
                .isInstanceOf(TokenPollException.class)
                .hasMessageEndingWith("Server failed unexpectedly")
                .hasCauseInstanceOf(IOException.class);
    }

    @Test
    public void testTokenPollWhenNextUriIsMalformed()
    {
        server.enqueue(statusAndBody(HTTP_OK, format(JSON_PAIR, "nextUri", ":::")));

        assertThatThrownBy(() -> tokens.pollForTokenUntil(tokenUri(), ONE_SECOND))
                .isInstanceOf(ClientException.class)
                .hasMessage("Parsing nextUri field to URI has failed")
                .hasCauseInstanceOf(URISyntaxException.class);
    }

    @Test
    public void testTokenPollWhenNextUriIsNotValidUrl()
    {
        server.enqueue(statusAndBody(HTTP_OK, format(JSON_PAIR, "nextUri", "not:an:url")));

        assertThatThrownBy(() -> tokens.pollForTokenUntil(tokenUri(), ONE_SECOND))
                .isInstanceOf(ClientException.class)
                .hasMessage("Parsing \"not:an:url\" to URL has failed");
    }

    private URI tokenUri()
    {
        return create("http://" + server.getHostName() + ":" + server.getPort() + TOKEN_PATH);
    }

    private MockResponse statusAndBody(int status, String body)
    {
        return new MockResponse()
                .setResponseCode(status)
                .addHeader(CONTENT_TYPE, JSON_UTF_8)
                .setBody(body);
    }

    private MockResponse status(int code)
    {
        return new MockResponse()
                .setResponseCode(code);
    }

    private Dispatcher delayedDispatcher(Duration timeOfDelay)
    {
        return new QueueDispatcher()
        {
            @Override
            public MockResponse dispatch(RecordedRequest request)
                    throws InterruptedException
            {
                MILLISECONDS.sleep(timeOfDelay.toMillis());
                return super.dispatch(request);
            }
        };
    }
}
