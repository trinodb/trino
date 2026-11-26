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

import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.junit5.StartStop;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static io.trino.client.OkHttpUtil.excludeCredentials;
import static io.trino.client.OkHttpUtil.tokenAuth;
import static org.assertj.core.api.Assertions.assertThat;

final class TestOkHttpUtil
{
    @StartStop
    private final MockWebServer server = new MockWebServer();

    @Test
    void testTokenAuthSkipsRequestsWithoutCredentials()
            throws IOException, InterruptedException
    {
        server.enqueue(new MockResponse.Builder().build());
        server.enqueue(new MockResponse.Builder().build());
        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(tokenAuth("access_t0ken"))
                .build();
        Request.Builder request = new Request.Builder().url(server.url("/"));

        client.newCall(request.build()).execute().close();
        client.newCall(excludeCredentials(request).build()).execute().close();

        assertThat(server.takeRequest().getHeaders().get(AUTHORIZATION)).isEqualTo("Bearer access_t0ken");
        assertThat(server.takeRequest().getHeaders().get(AUTHORIZATION)).isNull();
    }
}
