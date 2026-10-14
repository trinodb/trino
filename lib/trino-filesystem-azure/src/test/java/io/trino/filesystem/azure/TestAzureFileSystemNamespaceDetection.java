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
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.util.TracingOptions;
import io.airlift.units.DataSize;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestAzureFileSystemNamespaceDetection
{
    private static final Location DIRECTORY = Location.of("abfss://container@account.dfs.core.windows.net/directory");

    @Test
    void testHierarchicalNamespaceNotEnabled()
            throws IOException
    {
        assertListsBlobs("HierarchicalNamespaceNotEnabled");
    }

    @Test
    void testEndpointUnsupportedAccountFeatures()
            throws IOException
    {
        // returned instead of HierarchicalNamespaceNotEnabled by a flat namespace account with blob soft delete or blob storage events enabled
        assertListsBlobs("EndpointUnsupportedAccountFeatures");
    }

    @Test
    void testUnrelatedConflict()
    {
        assertThatThrownBy(() -> createFileSystem("InvalidSourceOrDestinationResourceType").listFiles(DIRECTORY))
                .isInstanceOf(IOException.class)
                .hasMessage("Checking whether hierarchical namespace is enabled for the location %s failed", DIRECTORY);
    }

    private static void assertListsBlobs(String dataLakeErrorCode)
            throws IOException
    {
        FileIterator files = createFileSystem(dataLakeErrorCode).listFiles(DIRECTORY);

        assertThat(files.hasNext()).isTrue();
        FileEntry file = files.next();
        assertThat(file.location()).isEqualTo(DIRECTORY.appendPath("file"));
        assertThat(file.length()).isEqualTo(3);
        assertThat(files.hasNext()).isFalse();
    }

    private static AzureFileSystem createFileSystem(String dataLakeErrorCode)
    {
        return new AzureFileSystem(
                new FlatNamespaceHttpClient(dataLakeErrorCode),
                (_, next) -> next.process(),
                newDirectExecutorService(),
                new TracingOptions(),
                new AzureAuthAccessKey("a2V5"),
                "core.windows.net",
                DataSize.of(4, MEGABYTE),
                DataSize.of(4, MEGABYTE),
                1,
                DataSize.of(4, MEGABYTE),
                true);
    }

    /**
     * Rejects every Data Lake endpoint request with the given error code and lists a single blob from the Blob endpoint.
     */
    private static class FlatNamespaceHttpClient
            implements HttpClient
    {
        private static final String BLOB_LISTING =
                """
                <?xml version="1.0" encoding="utf-8"?>
                <EnumerationResults ServiceEndpoint="https://account.blob.core.windows.net/" ContainerName="container">
                  <Prefix>directory/</Prefix>
                  <Blobs>
                    <Blob>
                      <Name>directory/file</Name>
                      <Properties>
                        <Last-Modified>Thu, 01 Jan 2026 00:00:00 GMT</Last-Modified>
                        <Content-Length>3</Content-Length>
                      </Properties>
                    </Blob>
                  </Blobs>
                  <NextMarker />
                </EnumerationResults>
                """;

        private final String dataLakeErrorCode;

        public FlatNamespaceHttpClient(String dataLakeErrorCode)
        {
            this.dataLakeErrorCode = dataLakeErrorCode;
        }

        @Override
        public Mono<HttpResponse> send(HttpRequest request)
        {
            String host = request.getUrl().getHost();
            if (host.equals("account.dfs.core.windows.net")) {
                return Mono.just(new FixedHttpResponse(request, 409, new HttpHeaders().set(HttpHeaderName.fromString("x-ms-error-code"), dataLakeErrorCode), ""));
            }
            if (host.equals("account.blob.core.windows.net")) {
                if (request.getUrl().getQuery().contains("comp=list")) {
                    return Mono.just(new FixedHttpResponse(request, 200, new HttpHeaders().set(HttpHeaderName.CONTENT_TYPE, "application/xml"), BLOB_LISTING));
                }
                // container existence check
                if (request.getUrl().getQuery().equals("restype=container")) {
                    return Mono.just(new FixedHttpResponse(request, 200, new HttpHeaders(), ""));
                }
            }
            return Mono.error(new AssertionError("Unexpected request: %s %s".formatted(request.getHttpMethod(), request.getUrl())));
        }
    }

    private static class FixedHttpResponse
            extends HttpResponse
    {
        private final int statusCode;
        private final HttpHeaders headers;
        private final byte[] body;

        public FixedHttpResponse(HttpRequest request, int statusCode, HttpHeaders headers, String body)
        {
            super(request);
            this.statusCode = statusCode;
            this.headers = headers;
            this.body = body.getBytes(UTF_8);
        }

        @Override
        public int getStatusCode()
        {
            return statusCode;
        }

        @Deprecated
        @Override
        public String getHeaderValue(String name)
        {
            return headers.getValue(HttpHeaderName.fromString(name));
        }

        @Override
        public HttpHeaders getHeaders()
        {
            return headers;
        }

        @Override
        public Flux<ByteBuffer> getBody()
        {
            return Flux.just(ByteBuffer.wrap(body));
        }

        @Override
        public Mono<byte[]> getBodyAsByteArray()
        {
            return Mono.just(body);
        }

        @Override
        public Mono<String> getBodyAsString()
        {
            return getBodyAsString(UTF_8);
        }

        @Override
        public Mono<String> getBodyAsString(Charset charset)
        {
            return Mono.just(new String(body, charset));
        }
    }
}
