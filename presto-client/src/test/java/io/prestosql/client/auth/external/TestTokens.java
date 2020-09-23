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
import java.time.Duration;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.ANY_TEXT_TYPE;
import static io.prestosql.client.auth.external.Tokens.HTTP_FAILED_DEPENDENCY;
import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestTokens
{
    private static final String TOKEN_PATH = "/v1/authentications/sso/test/token";
    private Tokens tokens;
    private MockWebServer server;

    @BeforeMethod(alwaysRun = true)
    public void setup()
            throws Exception
    {
        server = new MockWebServer();
        server.start();

        tokens = new Tokens(
                new OkHttpClient.Builder().build(),
                Duration.ofSeconds(1));
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws IOException
    {
        server.close();
    }

    @Test(singleThreaded = true)
    public void testTokenPollWhenTokenIsReady()
    {
        server.enqueue(statusAndBody(HTTP_OK, "token"));

        TokenPoll tokenPoll = tokens.pollForToken(tokenUrl());

        assertThat(tokenPoll.getToken()).map(AuthenticationToken::asString)
                .isEqualTo(Optional.of("token"));
    }

    @Test(singleThreaded = true)
    public void testTokenPollWhenItsNotReadyAtFirst()
    {
        Stream.concat(
                Stream.generate(() -> status(HTTP_ACCEPTED)).limit(999),
                Stream.of(statusAndBody(HTTP_OK, "token")))
                .forEach(server::enqueue);

        TokenPoll tokenPoll = tokens.pollForToken(tokenUrl());

        assertThat(tokenPoll.getToken()).map(AuthenticationToken::asString)
                .isEqualTo(Optional.of("token"));
    }

    @Test(singleThreaded = true)
    public void testTokenPollWhenItsNotReadyOverATimeout()
    {
        server.setDispatcher(delayedDispatcher(Duration.ofMillis(10)));
        Stream.generate(() -> status(HTTP_ACCEPTED)).limit(200)
                .forEach(server::enqueue);

        TokenPoll tokenPoll = tokens.pollForToken(tokenUrl());

        assertThat(tokenPoll.getToken()).isEqualTo(Optional.empty());
    }

    @Test(singleThreaded = true)
    public void testTokenPollWhenItHasFailedDependency()
    {
        server.enqueue(status(HTTP_FAILED_DEPENDENCY));

        TokenPoll tokenPoll = tokens.pollForToken(tokenUrl());

        assertThat(tokenPoll.hasFailed()).isTrue();
    }

    @Test(singleThreaded = true)
    public void testTokenPollWhenServerReturns201()
    {
        server.enqueue(status(HTTP_CREATED));

        assertThatThrownBy(() -> tokens.pollForToken(tokenUrl()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageEndingWith("Unknown response code \"201\", retrieved from token poll");
    }

    private String tokenUrl()
    {
        return "http://" + server.getHostName() + ":" + server.getPort() + TOKEN_PATH;
    }

    @Test(singleThreaded = true)
    public void testTokenPollWhenServerReturns500()
    {
        server.enqueue(statusAndBody(500, "Server failed unexpectedly"));

        assertThatThrownBy(() -> tokens.pollForToken(tokenUrl()))
                .isInstanceOf(TokenPollException.class)
                .hasMessageEndingWith("Server failed unexpectedly")
                .hasCauseInstanceOf(IOException.class);
    }

    private MockResponse statusAndBody(int status, String body)
    {
        return new MockResponse()
                .setResponseCode(status)
                .addHeader(CONTENT_TYPE, ANY_TEXT_TYPE)
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
