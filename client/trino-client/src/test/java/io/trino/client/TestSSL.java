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
package io.trino.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.node.testing.TestingNodeModule;
import jakarta.servlet.Servlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.net.ssl.SSLHandshakeException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.client.OkHttpUtil.setupSsl;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestSSL
{
    private static final String ALL_CERTS = Resources.getResource("certs/certs.crt").getPath();
    private static final String LOCALHOST_CERT = Resources.getResource("certs/localhost.crt").getPath();
    private static final String DIFFERENTHOST_CERT = Resources.getResource("certs/differenthost.crt").getPath();
    private LifeCycleManager lifeCycleManager;
    private int httpsPort;

    @BeforeAll
    public void setUp()
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingHttpServerModule("test-ssl"),
                binder -> binder.bind(Servlet.class).toInstance(new TestingHttpServlet()))
                .setRequiredConfigurationProperties(
                        ImmutableMap.<String, String>builder()
                                .put("http-server.http.enabled", "false")
                                .put("http-server.https.enabled", "true")
                                .put("http-server.https.keystore.path", Resources.getResource("certs/certs.jks").getPath())
                                .put("http-server.https.keystore.key", "Pass1234")
                                .buildOrThrow());

        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        httpsPort = injector.getInstance(HttpServerInfo.class).getHttpsUri().getPort();
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        lifeCycleManager.stop();
    }

    @ParameterizedTest
    @MethodSource("sslConfigurations")
    public void testSSL(String trustStorePath, List<String> trustedHosts, List<String> untrustedHosts)
            throws IOException
    {
        OkHttpClient client = newHttpClient(trustStorePath);

        for (String trustedHost : trustedHosts) {
            try (Response response = client.newCall(new Request.Builder()
                            .url(String.format("https://%s:%s", trustedHost, httpsPort))
                            .build())
                    .execute()) {
                assertThat(response.code()).isEqualTo(200);
                assertThat(response.body().string()).isEqualTo("OK");
            }
        }

        for (String untrustedHost : untrustedHosts) {
            assertThatThrownBy(() -> {
                client.newCall(new Request.Builder()
                                .url(String.format("https://%s:%s", untrustedHost, httpsPort))
                                .build())
                        .execute();
            }).isInstanceOf(SSLHandshakeException.class)
                    .hasMessageContaining("unable to find valid certification path to requested target");
        }
    }

    public static Stream<Arguments> sslConfigurations()
    {
        return Stream.of(
                Arguments.of(LOCALHOST_CERT, ImmutableList.of("localhost"), ImmutableList.of("differenthost.local")),
                Arguments.of(DIFFERENTHOST_CERT, ImmutableList.of("differenthost.local"), ImmutableList.of("localhost")),
                Arguments.of(ALL_CERTS, ImmutableList.of("localhost", "differenthost.local"), ImmutableList.of()));
    }

    private static OkHttpClient newHttpClient(String trustStorePath)
    {
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
                .followRedirects(false)
                .dns(hostname -> {
                    if ("localhost".equals(hostname) || "differenthost.local".equals(hostname)) {
                        return ImmutableList.of(InetAddress.getByName("127.0.0.1"));
                    }
                    throw new UnknownHostException("Unknown host: " + hostname);
                });
        setupSsl(
                clientBuilder,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.of(trustStorePath),
                Optional.empty(),
                Optional.empty(),
                false);
        return clientBuilder.build();
    }

    private static class TestingHttpServlet
            extends HttpServlet
    {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            response.setContentType("text/plain");
            response.getWriter().write("OK");
        }
    }
}
