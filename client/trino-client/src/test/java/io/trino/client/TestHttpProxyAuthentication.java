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

import com.google.common.net.HostAndPort;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import mockwebserver3.junit5.StartStop;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHttpProxyAuthentication
{
    private static final String PROXY_USER = "proxyuser";
    private static final String PROXY_PASSWORD = "proxypass";

    @StartStop
    private final MockWebServer proxy = new MockWebServer();

    @Test
    public void testReactiveProxyAuthorization()
            throws Exception
    {
        // OkHttp authenticates plain HTTP proxy requests only after a 407. Preemptive
        // Proxy-Authorization is applied to the CONNECT request that opens an HTTPS tunnel.
        proxy.enqueue(new MockResponse.Builder()
                .code(407)
                .addHeader("Proxy-Authenticate", "Basic realm=\"proxy\"")
                .build());
        proxy.enqueue(new MockResponse.Builder()
                .code(200)
                .body("ok")
                .build());

        try (Response response = clientWithProxyCredentials().newCall(new Request.Builder().url("http://127.0.0.1:9/").build()).execute()) {
            assertThat(response.code()).isEqualTo(200);
        }

        RecordedRequest challenge = proxy.takeRequest();
        assertThat(challenge.getHeaders().get("Proxy-Authorization")).isNull();
        RecordedRequest authenticated = proxy.takeRequest();
        assertThat(authenticated.getHeaders().get("Proxy-Authorization")).isEqualTo(Credentials.basic(PROXY_USER, PROXY_PASSWORD));
    }

    @Test
    public void testProxyAuthorizationOnConnect()
            throws Exception
    {
        proxy.enqueue(new MockResponse.Builder()
                .code(200)
                .build());

        assertThatThrownBy(() -> clientWithProxyCredentials().newCall(new Request.Builder().url("https://127.0.0.1/").build()).execute())
                .isInstanceOf(IOException.class);

        RecordedRequest request = proxy.takeRequest();
        assertThat(request.getMethod()).isEqualTo("CONNECT");
        assertThat(request.getHeaders().get("Proxy-Authorization")).isEqualTo(Credentials.basic(PROXY_USER, PROXY_PASSWORD));
    }

    @Test
    public void testRejectedProxyCredentialDoesNotRetryForever()
            throws Exception
    {
        for (int i = 0; i < 4; i++) {
            proxy.enqueue(new MockResponse.Builder()
                    .code(407)
                    .addHeader("Proxy-Authenticate", "Basic realm=\"proxy\"")
                    .build());
        }

        int requestsBefore = proxy.getRequestCount();
        try (Response response = clientWithProxyCredentials().newCall(new Request.Builder().url("http://127.0.0.1:9/").build()).execute()) {
            // The authenticator gives up once Proxy-Authorization is already present, and OkHttp
            // returns that 407 to the caller instead of retrying.
            assertThat(response.code()).isEqualTo(407);
        }
        assertThat(proxy.getRequestCount() - requestsBefore).isLessThanOrEqualTo(2);
    }

    private OkHttpClient clientWithProxyCredentials()
    {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        OkHttpUtil.setupHttpProxy(
                builder,
                Optional.of(HostAndPort.fromParts(proxy.getHostName(), proxy.getPort())),
                Optional.of(PROXY_USER),
                Optional.of(PROXY_PASSWORD));
        OkHttpUtil.setupTimeouts(builder, 5, TimeUnit.SECONDS);
        return builder.build();
    }
}
