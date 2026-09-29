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
package io.trino.plugin.httpcredentials;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.security.ExtraCredentialsProvider;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import mockwebserver3.junit5.StartStop;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.json.JsonCodec.mapJsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

final class TestHttpExtraCredentialsProvider
{
    @StartStop
    private final MockWebServer server = new MockWebServer();

    @Test
    void testResolvesCredentials()
            throws Exception
    {
        server.enqueue(jsonResponse(
                """
                {"db.user": "alice", "db.password": "alice-secret"}
                """));

        ExtraCredentialsProvider provider = createProvider(ImmutableMap.of());

        assertThat(provider.getExtraCredentials("alice"))
                .isEqualTo(ImmutableMap.of("db.user", "alice", "db.password", "alice-secret"));

        RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
        assertThat(request).isNotNull();
        assertThat(request.getMethod()).isEqualTo("POST");
        assertThat(mapJsonCodec(String.class, String.class).fromJson(request.getBody().utf8()))
                .isEqualTo(Map.of("user", "alice"));
        assertThat(request.getHeaders().get("X-Internal-Secret")).isNull();
    }

    @Test
    void testSharedSecretIsSent()
            throws Exception
    {
        server.enqueue(jsonResponse("{}"));

        ExtraCredentialsProvider provider = createProvider(ImmutableMap.of("http-extra-credentials.shared-secret", "hunter2"));
        provider.getExtraCredentials("alice");

        RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
        assertThat(request).isNotNull();
        assertThat(request.getHeaders().get("X-Internal-Secret")).isEqualTo("hunter2");
    }

    @Test
    void testSuccessfulLookupIsCached()
    {
        server.enqueue(jsonResponse(
                """
                {"db.user": "alice"}
                """));

        ExtraCredentialsProvider provider = createProvider(ImmutableMap.of("http-extra-credentials.cache-ttl", "30s"));

        assertThat(provider.getExtraCredentials("alice")).isEqualTo(ImmutableMap.of("db.user", "alice"));
        assertThat(provider.getExtraCredentials("alice")).isEqualTo(ImmutableMap.of("db.user", "alice"));

        assertThat(server.getRequestCount())
                .describedAs("second lookup within the TTL must be served from the cache")
                .isEqualTo(1);
    }

    @Test
    void testCredentialsAreCachedPerUser()
    {
        server.enqueue(jsonResponse(
                """
                {"db.user": "alice"}
                """));
        server.enqueue(jsonResponse(
                """
                {"db.user": "bob"}
                """));

        ExtraCredentialsProvider provider = createProvider(ImmutableMap.of("http-extra-credentials.cache-ttl", "30s"));

        assertThat(provider.getExtraCredentials("alice")).isEqualTo(ImmutableMap.of("db.user", "alice"));
        assertThat(provider.getExtraCredentials("bob")).isEqualTo(ImmutableMap.of("db.user", "bob"));
        assertThat(provider.getExtraCredentials("alice")).isEqualTo(ImmutableMap.of("db.user", "alice"));

        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    void testZeroTtlDisablesCaching()
    {
        server.enqueue(jsonResponse(
                """
                {"db.user": "alice"}
                """));
        server.enqueue(jsonResponse(
                """
                {"db.user": "alice2"}
                """));

        ExtraCredentialsProvider provider = createProvider(ImmutableMap.of("http-extra-credentials.cache-ttl", "0s"));

        assertThat(provider.getExtraCredentials("alice")).isEqualTo(ImmutableMap.of("db.user", "alice"));
        assertThat(provider.getExtraCredentials("alice")).isEqualTo(ImmutableMap.of("db.user", "alice2"));

        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    /**
     * The provider deliberately fails open: a credential service outage must not fail queries.
     * Failures are not cached, so the next lookup retries immediately.
     */
    @Test
    void testFailureReturnsNoCredentialsAndIsNotCached()
    {
        server.enqueue(new MockResponse.Builder().code(500).build());
        server.enqueue(jsonResponse(
                """
                {"db.user": "alice"}
                """));

        ExtraCredentialsProvider provider = createProvider(ImmutableMap.of("http-extra-credentials.cache-ttl", "30s"));

        assertThat(provider.getExtraCredentials("alice")).isEmpty();
        assertThat(provider.getExtraCredentials("alice")).isEqualTo(ImmutableMap.of("db.user", "alice"));

        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    private ExtraCredentialsProvider createProvider(Map<String, String> extraConfig)
    {
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("http-extra-credentials.uri", server.url("/credentials").toString())
                .putAll(extraConfig)
                .buildKeepingLast();
        return new HttpExtraCredentialsProviderFactory().create(config);
    }

    private static MockResponse jsonResponse(String body)
    {
        return new MockResponse.Builder()
                .code(200)
                .addHeader("Content-Type", "application/json")
                .body(body)
                .build();
    }
}
