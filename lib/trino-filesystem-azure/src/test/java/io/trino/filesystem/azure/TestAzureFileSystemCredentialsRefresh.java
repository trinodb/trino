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
package io.trino.filesystem.azure;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.util.TracingOptions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.filesystem.azure.AzureFileSystemConstants.EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class TestAzureFileSystemCredentialsRefresh
{
    private static final String ACCOUNT = "account";
    private static final DataSize WRITE_BLOCK_SIZE = DataSize.of(1, MEGABYTE);
    private static final Location LOCATION = Location.of("abfs://container@%s.dfs.core.windows.net/file".formatted(ACCOUNT));

    private final ExecutorService uploadExecutor = newCachedThreadPool(daemonThreadsNamed("test-azure-upload-%s"));

    @AfterAll
    void tearDown()
    {
        uploadExecutor.shutdownNow();
    }

    @Test
    void testSasTokenChangedByRefresherIsUsedForSubsequentRequests()
            throws IOException
    {
        RecordingHttpClient httpClient = new RecordingHttpClient();
        CountingCredentialsRefresher credentialsRefresher = new CountingCredentialsRefresher(2);
        TrinoFileSystem fileSystem = createFileSystem(httpClient, credentialsRefresher);

        OutputStream outputStream = fileSystem.newOutputFile(LOCATION).create();
        assertThat(credentialsRefresher.refreshCount()).isEqualTo(1);

        outputStream.write(randomData());
        outputStream.close();
        assertThat(credentialsRefresher.refreshCount()).isEqualTo(5);
        assertThat(httpClient.requestSignatures()).containsExactly("initial", "initial", "refreshed", "refreshed", "refreshed");
    }

    @Test
    void testUnchangedSasTokenIsUsedForAllRequests()
            throws IOException
    {
        RecordingHttpClient httpClient = new RecordingHttpClient();
        CountingCredentialsRefresher credentialsRefresher = new CountingCredentialsRefresher(Integer.MAX_VALUE);
        TrinoFileSystem fileSystem = createFileSystem(httpClient, credentialsRefresher);

        OutputStream outputStream = fileSystem.newOutputFile(LOCATION).create();
        assertThat(credentialsRefresher.refreshCount()).isEqualTo(1);

        outputStream.write(randomData());
        outputStream.close();
        assertThat(credentialsRefresher.refreshCount()).isEqualTo(5);
        assertThat(httpClient.requestSignatures()).containsExactly("initial", "initial", "initial", "initial", "initial");
    }

    private TrinoFileSystem createFileSystem(HttpClient httpClient, Supplier<Map<String, String>> credentialsRefresher)
    {
        return new AzureFileSystem(
                httpClient,
                new PassThroughHttpPipelinePolicy(),
                uploadExecutor,
                new TracingOptions(),
                new AzureAuthSasToken(ImmutableMap.of(ACCOUNT, sasToken("initial")), Optional.of(credentialsRefresher)),
                "core.windows.net",
                DataSize.of(4, MEGABYTE),
                WRITE_BLOCK_SIZE,
                1,
                WRITE_BLOCK_SIZE,
                false);
    }

    private static byte[] randomData()
    {
        byte[] data = new byte[toIntExact(WRITE_BLOCK_SIZE.toBytes() * 3)];
        ThreadLocalRandom.current().nextBytes(data);
        return data;
    }

    private static String sasToken(String signature)
    {
        return "sv=2024-08-04&sp=rwdlac&sig=" + signature;
    }

    private static final class CountingCredentialsRefresher
            implements Supplier<Map<String, String>>
    {
        private final int initialSasTokenRefreshCount;
        private final AtomicInteger refreshCount = new AtomicInteger();

        private CountingCredentialsRefresher(int initialSasTokenRefreshCount)
        {
            this.initialSasTokenRefreshCount = initialSasTokenRefreshCount;
        }

        @Override
        public Map<String, String> get()
        {
            String signature = "refreshed";
            if (refreshCount.incrementAndGet() <= initialSasTokenRefreshCount) {
                signature = "initial";
            }
            return ImmutableMap.of(EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX + ACCOUNT, sasToken(signature));
        }

        public int refreshCount()
        {
            return refreshCount.get();
        }
    }

    private static final class PassThroughHttpPipelinePolicy
            implements HttpPipelinePolicy
    {
        @Override
        public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next)
        {
            return next.process();
        }
    }

    private static final class RecordingHttpClient
            implements HttpClient
    {
        private final List<String> requestSignatures = new CopyOnWriteArrayList<>();

        @Override
        public Mono<HttpResponse> send(HttpRequest request)
        {
            requestSignatures.add(signature(request));
            if (request.getHttpMethod() == HttpMethod.HEAD) {
                return Mono.just(new EmptyHttpResponse(request, 404, new HttpHeaders().set(HttpHeaderName.fromString("x-ms-error-code"), "BlobNotFound")));
            }
            return Mono.just(new EmptyHttpResponse(request, 201, new HttpHeaders()
                    .set(HttpHeaderName.ETAG, "\"etag\"")
                    .set(HttpHeaderName.LAST_MODIFIED, "Wed, 01 Oct 2026 00:00:00 GMT")));
        }

        public List<String> requestSignatures()
        {
            return ImmutableList.copyOf(requestSignatures);
        }

        private static String signature(HttpRequest request)
        {
            return Splitter.on('&').splitToStream(request.getUrl().getQuery())
                    .filter(parameter -> parameter.startsWith("sig="))
                    .map(parameter -> parameter.substring("sig=".length()))
                    .collect(onlyElement());
        }
    }

    private static final class EmptyHttpResponse
            extends HttpResponse
    {
        private final int statusCode;
        private final HttpHeaders headers;

        private EmptyHttpResponse(HttpRequest request, int statusCode, HttpHeaders headers)
        {
            super(request);
            this.statusCode = statusCode;
            this.headers = headers;
        }

        @Override
        public int getStatusCode()
        {
            return statusCode;
        }

        @Override
        @SuppressWarnings("deprecation")
        public String getHeaderValue(String name)
        {
            return headers.getValue(name);
        }

        @Override
        public HttpHeaders getHeaders()
        {
            return headers;
        }

        @Override
        public Flux<ByteBuffer> getBody()
        {
            return Flux.empty();
        }

        @Override
        public Mono<byte[]> getBodyAsByteArray()
        {
            return Mono.just(new byte[0]);
        }

        @Override
        public Mono<String> getBodyAsString()
        {
            return Mono.just("");
        }

        @Override
        public Mono<String> getBodyAsString(Charset charset)
        {
            return Mono.just("");
        }
    }
}
