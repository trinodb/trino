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
package io.trino.filesystem.s3;

import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.filesystem.s3.S3FileSystemConfig.StorageClassType.STANDARD;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

final class TestS3OutputStream
{
    private static final int PART_SIZE = 5 * 1024 * 1024;

    @Test
    void testCompletionFailureAbortsMultipartUpload()
            throws Exception
    {
        RuntimeException failure = new IllegalStateException("completion failed");
        ExecutionInterceptor interceptor = new ExecutionInterceptor()
        {
            @Override
            public void beforeExecution(Context.BeforeExecution context, ExecutionAttributes executionAttributes)
            {
                if (context.request() instanceof CompleteMultipartUploadRequest) {
                    throw failure;
                }
            }
        };
        try (MultipartUploadHttpClient httpClient = new MultipartUploadHttpClient();
                S3Client client = S3Client.builder()
                        .region(US_EAST_1)
                        .endpointOverride(URI.create("http://localhost"))
                        .forcePathStyle(true)
                        .credentialsProvider(AnonymousCredentialsProvider.create())
                        .httpClient(httpClient)
                        .overrideConfiguration(configuration -> configuration.addExecutionInterceptor(interceptor))
                        .build()) {
            S3Context context = new S3Context(
                    PART_SIZE,
                    false,
                    S3Context.S3SseContext.of(S3FileSystemConfig.S3SseType.NONE, null, null),
                    Optional.empty(),
                    STANDARD,
                    S3FileSystemConfig.ObjectCannedAcl.NONE,
                    S3FileSystemConfig.S3ChecksumAlgorithm.DEFAULT);
            try (S3OutputStream output = new S3OutputStream(
                    newSimpleAggregatedMemoryContext(),
                    directExecutor(),
                    client,
                    context,
                    new S3Location(Location.of("s3://bucket/object")),
                    Optional.empty())) {
                // The extra byte forces a second multipart part.
                output.write(new byte[PART_SIZE + 1]);

                assertThatThrownBy(output::close).isSameAs(failure);
                assertThat(httpClient.abortRequests).singleElement().satisfies(request -> {
                    assertThat(request.encodedPath()).isEqualTo("/bucket/object");
                    assertThat(request.rawQueryParameters().get("uploadId")).containsExactly("upload-id");
                });
            }
        }
    }

    private static final class MultipartUploadHttpClient
            implements SdkHttpClient
    {
        private final List<SdkHttpRequest> abortRequests = new ArrayList<>();

        @Override
        public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request)
        {
            return new ExecutableHttpRequest()
            {
                @Override
                public HttpExecuteResponse call()
                {
                    SdkHttpResponse.Builder response = SdkHttpResponse.builder().statusCode(200);
                    byte[] body = new byte[0];
                    switch (request.httpRequest().method()) {
                        case POST -> body =
                                """
                                <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                                    <UploadId>upload-id</UploadId>
                                </InitiateMultipartUploadResult>
                                """.getBytes(UTF_8);
                        case PUT -> response.putHeader("ETag", "\"part-etag\"");
                        case DELETE -> {
                            abortRequests.add(request.httpRequest());
                            response.statusCode(204);
                        }
                        default -> throw new AssertionError("Unexpected request: " + request.httpRequest());
                    }
                    return HttpExecuteResponse.builder()
                            .response(response.putHeader("Content-Length", Integer.toString(body.length)).build())
                            .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
                            .build();
                }

                @Override
                public void abort() {}
            };
        }

        @Override
        public void close() {}
    }
}
