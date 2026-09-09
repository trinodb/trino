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

import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.spi.security.ExtraCredentialsProvider;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * {@link ExtraCredentialsProvider} that resolves a user's connection credentials by
 * POSTing {@code {"user": "<user>"}} to a configured HTTP endpoint and returning the
 * JSON object of {@code {credential-name: value}} pairs it responds with.
 *
 * <p>Because it runs on the request path, successful responses are cached in-process
 * for the configured TTL; failures are logged and NOT cached, so a transient outage
 * recovers immediately.
 */
public class HttpExtraCredentialsProvider
        implements ExtraCredentialsProvider
{
    private static final Logger log = Logger.get(HttpExtraCredentialsProvider.class);
    private static final JsonCodec<Map<String, String>> MAP_CODEC = mapJsonCodec(String.class, String.class);

    private final URI uri;
    private final String sharedSecret;
    private final long cacheTtlNanos;
    private final HttpClient httpClient;
    private final ConcurrentHashMap<String, Entry> cache = new ConcurrentHashMap<>();

    private record Entry(long expiryNanos, Map<String, String> credentials) {}

    @Inject
    public HttpExtraCredentialsProvider(HttpExtraCredentialsConfig config, @ForHttpExtraCredentials HttpClient httpClient)
    {
        requireNonNull(config, "config is null");
        this.uri = requireNonNull(config.getUri(), "uri is null");
        this.sharedSecret = config.getSharedSecret();
        this.cacheTtlNanos = (long) config.getCacheTtl().getValue(NANOSECONDS);
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @Override
    public Map<String, String> getExtraCredentials(String user)
    {
        requireNonNull(user, "user is null");
        Entry cached = cache.get(user);
        if (cached != null && System.nanoTime() - cached.expiryNanos() < 0) {
            return cached.credentials();
        }
        try {
            Request.Builder builder = preparePost()
                    .setUri(uri)
                    .addHeader("Content-Type", "application/json")
                    .setBodyGenerator(jsonBodyGenerator(MAP_CODEC, Map.of("user", user)));
            if (sharedSecret != null && !sharedSecret.isBlank()) {
                builder.addHeader("X-Internal-Secret", sharedSecret);
            }
            Map<String, String> credentials = Map.copyOf(httpClient.execute(builder.build(), createJsonResponseHandler(MAP_CODEC)));
            cache.put(user, new Entry(System.nanoTime() + cacheTtlNanos, credentials));
            return credentials;
        }
        catch (Exception e) {
            log.warn(e, "Failed to resolve extra credentials for user '%s'; proceeding without injected credentials", user);
            return Map.of();
        }
    }
}
