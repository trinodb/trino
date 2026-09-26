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

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.models.BlobStorageException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.CompletionException;

import static io.trino.plugin.exchange.filesystem.azure.AzureBlobFileSystemExchangeStorage.toReadFailure;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAzureBlobExchangeReadFailure
{
    private static final URI FILE = URI.create("abfs://container@account.dfs.core.windows.net/exchange/file");

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
        return new BlobStorageException("storage failure", new StatusOnlyResponse(statusCode), null);
    }

    private static class StatusOnlyResponse
            extends HttpResponse
    {
        private final int statusCode;

        public StatusOnlyResponse(int statusCode)
        {
            super((HttpRequest) null);
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
