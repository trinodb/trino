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
package io.prestosql.server.security.oauth2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.log.Logging;
import io.airlift.testing.Closeables;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.server.ui.WebUiModule;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.testing.Assertions.assertLessThan;
import static io.prestosql.client.OkHttpUtil.setupInsecureSsl;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.ws.rs.core.HttpHeaders.LOCATION;
import static javax.ws.rs.core.HttpHeaders.WWW_AUTHENTICATE;
import static javax.ws.rs.core.Response.Status.FOUND;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestOAuth2Authenticator
{
    private static final int HTTPS_PORT = findAvailablePort();

    private final TestingHydraService testingHydraService = new TestingHydraService();
    private final OkHttpClient httpClient;

    private TestingPrestoServer server;
    private URI serverUri;

    public TestOAuth2Authenticator()
    {
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();
        setupInsecureSsl(httpClientBuilder);
        httpClientBuilder.followRedirects(false);
        this.httpClient = httpClientBuilder.build();
    }

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();

        Testcontainers.exposeHostPorts(HTTPS_PORT);
        testingHydraService.start();

        createConsumer();

        server = TestingPrestoServer.builder()
                .setCoordinator(true)
                .setAdditionalModule(new WebUiModule())
                .setProperties(
                        ImmutableMap.<String, String>builder()
                                .put("web-ui.enabled", "true")
                                .put("web-ui.authentication.type", "oauth2")
                                .put("http-server.https.port", Integer.toString(HTTPS_PORT))
                                .put("http-server.https.enabled", "true")
                                .put("http-server.https.keystore.path", Resources.getResource("cert/localhost.pem").getPath())
                                .put("http-server.https.keystore.key", "")
                                .put("http-server.authentication.type", "oauth2")
                                .put("http-server.authentication.oauth2.auth-url", "http://127.0.0.1:9000/oauth2/auth")
                                .put("http-server.authentication.oauth2.token-url", "http://127.0.0.1:9000/oauth2/token")
                                .put("http-server.authentication.oauth2.client-id", "another-consumer")
                                .put("http-server.authentication.oauth2.client-secret", "consumer-secret")
                                .build())
                .build();
        waitForNodeRefresh(server);
        serverUri = server.getHttpsBaseUrl();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        Closeables.closeAll(server, testingHydraService);
    }

    @Test
    public void testUnauthorizedApiCall()
            throws IOException
    {
        try (Response response = httpClient
                .newCall(apiCall().build())
                .execute()) {
            assertUnauthorizedApiResponse(response);
        }
    }

    @Test
    public void testUnauthorizedUICall()
            throws IOException
    {
        try (Response response = httpClient
                .newCall(uiCall().build())
                .execute()) {
            assertThat(response.code()).isEqualTo(FOUND.getStatusCode());
            assertRedirectUrl(response.header(LOCATION));
        }
    }

    private void createConsumer()
    {
        testingHydraService.createHydraContainer()
                .withCommand(format("clients create " +
                        "--endpoint http://hydra:4445 " +
                        "--id another-consumer " +
                        "--secret consumer-secret " +
                        "-g authorization_code,refresh_token " +
                        "-r token,code,id_token " +
                        "--scope openid,offline " +
                        "--callbacks https://127.0.0.1:%s/callback", HTTPS_PORT))
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(30)))
                .start();
    }

    private Request.Builder uiCall()
    {
        return new Request.Builder()
                .url(serverUri.resolve("/ui/").toString())
                .get();
    }

    private Request.Builder apiCall()
    {
        return new Request.Builder()
                .url(serverUri.resolve("/v1/query/").toString())
                .get();
    }

    private static int findAvailablePort()
    {
        try (ServerSocket tempSocket = new ServerSocket(0)) {
            return tempSocket.getLocalPort();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void waitForNodeRefresh(TestingPrestoServer server)
            throws InterruptedException
    {
        Instant start = Instant.now();
        while (server.refreshNodes().getActiveNodes().size() < 1) {
            assertLessThan(Duration.between(start, Instant.now()), Duration.ofSeconds(10));
            MILLISECONDS.sleep(10);
        }
    }

    private static void assertUnauthorizedApiResponse(Response response)
            throws MalformedURLException
    {
        assertThat(response.code()).isEqualTo(UNAUTHORIZED.getStatusCode());
        String authenticateHeader = response.header(WWW_AUTHENTICATE);
        assertThat(authenticateHeader).isNotNull();
        Matcher authenticateHeaderMatcher = Pattern.compile("^Bearer realm=\"Presto\", redirectUrl=\"(.*)\", status=\"STARTED\"$")
                .matcher(authenticateHeader);
        assertThat(authenticateHeaderMatcher.matches()).isTrue();
        assertRedirectUrl(authenticateHeaderMatcher.group(1));
    }

    private static void assertRedirectUrl(String redirectUrl)
            throws MalformedURLException
    {
        assertThat(redirectUrl).isNotNull();
        URL location = new URL(redirectUrl);
        HttpUrl url = HttpUrl.parse(redirectUrl);
        assertThat(url).isNotNull();
        assertThat(location.getProtocol()).isEqualTo("http");
        assertThat(location.getHost()).isEqualTo("127.0.0.1");
        assertThat(location.getPort()).isEqualTo(9000);
        assertThat(location.getPath()).isEqualTo("/oauth2/auth");
        assertThat(url.queryParameterValues("response_type")).isEqualTo(ImmutableList.of("code"));
        assertThat(url.queryParameterValues("scope")).isEqualTo(ImmutableList.of("openid"));
        assertThat(url.queryParameterValues("redirect_uri")).isEqualTo(ImmutableList.of(format("https://127.0.0.1:%s/callback", HTTPS_PORT)));
        assertThat(url.queryParameterValues("client_id")).isEqualTo(ImmutableList.of("another-consumer"));
        assertThat(url.queryParameterValues("state")).isNotNull();
    }
}
