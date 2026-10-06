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
package io.trino.plugin.exchange.filesystem.azure;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.exchange.filesystem.ExchangeSourceFile;
import io.trino.plugin.exchange.filesystem.MetricsBuilder;
import io.trino.plugin.exchange.filesystem.azure.AzureBlobFileSystemExchangeStorage.AzureExchangeStorageReader;
import io.trino.spi.exchange.ExchangeId;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static io.trino.plugin.exchange.filesystem.azure.AzureBlobFileSystemExchangeStorage.toReadFailure;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAzureBlobExchangeReadFailure
{
    private static final URI FILE = URI.create("abfs://container@account.dfs.core.windows.net/exchange/file");
    private static final URI FIRST_FILE = URI.create("abfs://container@account.dfs.core.windows.net/exchange/0_0.data");
    private static final URI LAST_FILE = URI.create("abfs://container@account.dfs.core.windows.net/exchange/1_0.data");
    private static final ExchangeId EXCHANGE_ID = new ExchangeId("exchange");

    @Test
    public void testReaderMissingLastFileOfBufferFill()
    {
        // Both files fit into a single buffer fill, which consumes all source files before the requests complete
        try (AzureExchangeStorageReader reader = createReader(LAST_FILE, 404)) {
            // Azure client completes requests asynchronously
            assertThat(reader.isBlocked())
                    .failsWithin(Duration.ofSeconds(10))
                    .withThrowableOfType(ExecutionException.class);
            assertThatThrownBy(reader::read)
                    .isInstanceOf(NoSuchFileException.class)
                    .hasMessage(LAST_FILE.toString());
        }
    }

    @Test
    public void testReaderMissingEarlierFileOfBufferFill()
    {
        try (AzureExchangeStorageReader reader = createReader(FIRST_FILE, 404)) {
            // Azure client completes requests asynchronously
            assertThat(reader.isBlocked())
                    .failsWithin(Duration.ofSeconds(10))
                    .withThrowableOfType(ExecutionException.class);
            assertThatThrownBy(reader::read)
                    .isInstanceOf(NoSuchFileException.class)
                    .hasMessage(FIRST_FILE.toString());
        }
    }

    @Test
    public void testReaderOtherFailureStaysGeneric()
    {
        try (AzureExchangeStorageReader reader = createReader(LAST_FILE, 403)) {
            // Azure client completes requests asynchronously
            assertThat(reader.isBlocked())
                    .failsWithin(Duration.ofSeconds(10))
                    .withThrowableOfType(ExecutionException.class);
            assertThatThrownBy(reader::read)
                    .isExactlyInstanceOf(IOException.class)
                    .hasRootCauseInstanceOf(BlobStorageException.class);
        }
    }

    @Test
    public void testMissingBlob()
    {
        assertThat(toReadFailure(blobStorageException(404), FILE))
                .isInstanceOf(NoSuchFileException.class)
                .hasMessage(FILE.toString());
    }

    @Test
    public void testNestedCauseIsInspected()
    {
        RuntimeException failure = new CompletionException(blobStorageException(404));
        assertThat(toReadFailure(failure, FILE))
                .isInstanceOf(NoSuchFileException.class)
                .hasCause(failure);
    }

    @Test
    public void testOtherFailureStaysGeneric()
    {
        RuntimeException failure = blobStorageException(503);
        assertThat(toReadFailure(failure, FILE))
                .isExactlyInstanceOf(IOException.class)
                .hasCause(failure);
    }

    private static BlobStorageException blobStorageException(int statusCode)
    {
        return new BlobStorageException("storage failure", new StatusOnlyResponse(null, statusCode), null);
    }

    private static AzureExchangeStorageReader createReader(URI failingFile, int statusCode)
    {
        List<ExchangeSourceFile> sourceFiles = ImmutableList.of(
                new ExchangeSourceFile(FIRST_FILE, 16, EXCHANGE_ID, 0, 0),
                new ExchangeSourceFile(LAST_FILE, 16, EXCHANGE_ID, 1, 0));
        return new AzureExchangeStorageReader(blobServiceClient(failingFile, statusCode), sourceFiles, new MetricsBuilder(), 1024, 1024);
    }

    /**
     * Fails requests for {@code failingFile} with {@code statusCode}, requests for other files never complete
     */
    private static BlobServiceAsyncClient blobServiceClient(URI failingFile, int statusCode)
    {
        // The client URL-encodes '/' in blob names, match on the file name only
        String failingFileName = failingFile.getPath().substring(failingFile.getPath().lastIndexOf('/') + 1);
        HttpClient httpClient = request -> {
            if (request.getUrl().getPath().endsWith(failingFileName)) {
                return Mono.just(new StatusOnlyResponse(request, statusCode));
            }
            return Mono.never();
        };
        return new BlobServiceClientBuilder()
                .endpoint("https://account.blob.core.windows.net")
                .httpClient(httpClient)
                .buildAsyncClient();
    }

    private static class StatusOnlyResponse
            extends HttpResponse
    {
        private final int statusCode;

        public StatusOnlyResponse(HttpRequest request, int statusCode)
        {
            super(request);
            this.statusCode = statusCode;
        }

        @Override
        public int getStatusCode()
        {
            return statusCode;
        }

        @Override
        @Deprecated
        public String getHeaderValue(String name)
        {
            return null;
        }

        @Override
        public HttpHeaders getHeaders()
        {
            return new HttpHeaders();
        }

        @Override
        public Flux<ByteBuffer> getBody()
        {
            return Flux.empty();
        }

        @Override
        public Mono<byte[]> getBodyAsByteArray()
        {
            return Mono.empty();
        }

        @Override
        public Mono<String> getBodyAsString()
        {
            return Mono.empty();
        }

        @Override
        public Mono<String> getBodyAsString(Charset charset)
        {
            return Mono.empty();
        }
    }
}
