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
import io.trino.filesystem.s3.S3FileSystemConfig.RetryMode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.filesystem.s3.S3FileSystemConfig.RetryMode.STANDARD;
import static io.trino.filesystem.s3.S3FileSystemConfig.RetryMode.getRetryStrategy;
import static io.trino.filesystem.s3.S3FileSystemConfig.S3ChecksumAlgorithm.CRC32C;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static software.amazon.awssdk.awscore.retry.AwsRetryStrategy.adaptiveRetryStrategy;
import static software.amazon.awssdk.http.SdkHttpMethod.HEAD;
import static software.amazon.awssdk.http.SdkHttpMethod.POST;
import static software.amazon.awssdk.retries.api.BackoffStrategy.retryImmediately;
import static software.amazon.awssdk.services.s3.model.ChecksumAlgorithm.CRC32_C;
import static software.amazon.awssdk.services.s3.model.RequestPayer.REQUESTER;

@TestInstance(PER_CLASS)
class TestS3FileSystemDelete
{
    private final S3Presigner preSigner = S3Presigner.builder()
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .region(Region.US_EAST_1)
            .build();

    @AfterAll
    void close()
    {
        preSigner.close();
    }

    @Test
    void testDeleteSuccessfulFiles()
            throws IOException
    {
        TestingS3Client client = new TestingS3Client(3, response());
        fileSystem(client).deleteFiles(List.of(location("first"), location("second")));

        assertThat(client.requests).hasSize(1);
        assertThat(keys(client.requests.getFirst())).containsExactlyInAnyOrder("first", "second");
    }

    @ParameterizedTest
    @EnumSource(RetryMode.class)
    void testRetryOnlyFailedFiles(RetryMode retryMode)
            throws IOException
    {
        try (TestingS3Client client = new TestingS3Client(
                retryMode,
                3,
                response(error("retry", "InternalError", "Please try again")),
                response())) {
            fileSystem(client).deleteFiles(List.of(location("deleted"), location("retry")));

            assertThat(client.requests).hasSize(2);
            assertThat(keys(client.requests.getFirst())).containsExactlyInAnyOrder("deleted", "retry");
            assertThat(keys(client.requests.getLast())).containsExactly("retry");
            assertThat(client.requests.getLast().delete().quiet()).isTrue();
            assertThat(client.requests.getLast().bucket()).isEqualTo("bucket");
            assertThat(client.requests.getLast().requestPayer()).isEqualTo(REQUESTER);
            assertThat(client.requests.getLast().checksumAlgorithm()).isEqualTo(CRC32_C);
        }
    }

    @Test
    void testPermanentFailureIsNotRetried()
    {
        TestingS3Client client = new TestingS3Client(
                3,
                response(error("denied", "AccessDenied", "Access denied"), error("retry", "InternalError", "Please try again")),
                response());

        assertThatThrownBy(() -> fileSystem(client).deleteFiles(List.of(location("deleted"), location("denied"), location("retry"))))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to delete one or more files: {s3://bucket/denied=Access denied (AccessDenied)}");
        assertThat(client.requests).hasSize(2);
        assertThat(keys(client.requests.getLast())).containsExactly("retry");
    }

    @Test
    void testPermanentFailureWithoutTransientFailure()
    {
        TestingS3Client client = new TestingS3Client(3, response(error("denied", "AccessDenied", null)));

        assertThatThrownBy(() -> fileSystem(client).deleteFiles(List.of(location("denied"))))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to delete one or more files: {s3://bucket/denied=AccessDenied}");
        assertThat(client.requests).hasSize(1);
    }

    @Test
    void testRetryLimit()
    {
        TestingS3Client client = new TestingS3Client(
                3,
                response(error("retry", "InternalError", "first failure")),
                response(error("retry", "InternalError", "second failure")),
                response(error("retry", "InternalError", "last failure")));

        assertThatThrownBy(() -> fileSystem(client).deleteFiles(List.of(location("retry"))))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to delete one or more files: {s3://bucket/retry=last failure (InternalError)}");
        assertThat(client.requests).hasSize(3);
    }

    @Test
    void testRetriesDisabled()
    {
        TestingS3Client client = new TestingS3Client(1, response(error("retry", "InternalError", null)));

        assertThatThrownBy(() -> fileSystem(client).deleteFiles(List.of(location("retry"))))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to delete one or more files: {s3://bucket/retry=InternalError}");
        assertThat(client.requests).hasSize(1);
    }

    @Test
    void testInterruptedRetry()
    {
        TestingS3Client client = new TestingS3Client(3, response(error("retry", "InternalError", null)));
        client.interruptAfterDelete = true;

        try {
            assertThatThrownBy(() -> fileSystem(client).deleteFiles(List.of(location("retry"))))
                    .isInstanceOf(InterruptedIOException.class)
                    .hasMessage("Interrupted while batch deleting files")
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(client.requests).hasSize(1);
        }
        finally {
            Thread.interrupted();
        }
    }

    @Test
    void testAdaptiveClientRemainsUsableAfterDelete()
            throws IOException
    {
        try (TestingHttpClient httpClient = new TestingHttpClient(
                httpResponse(200,
                        """
                        <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                            <Error><Key>retry</Key><Code>InternalError</Code></Error>
                        </DeleteResult>
                        """),
                httpResponse(200, "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>"),
                httpResponse(429, "<Error><Code>SlowDown</Code></Error>"),
                httpResponse(200, ""));
                S3Client client = S3Client.builder()
                        .httpClient(httpClient)
                        .credentialsProvider(AnonymousCredentialsProvider.create())
                        .region(Region.US_EAST_1)
                        .endpointOverride(URI.create("http://localhost"))
                        .forcePathStyle(true)
                        .overrideConfiguration(builder -> builder.retryStrategy(adaptiveRetryStrategy().toBuilder()
                                .maxAttempts(3)
                                .backoffStrategy(retryImmediately())
                                .throttlingBackoffStrategy(retryImmediately())
                                .build()))
                        .build()) {
            fileSystem(client).deleteFiles(List.of(location("retry")));

            assertThat(client.headObject(builder -> builder.bucket("bucket").key("remaining")).sdkHttpResponse().statusCode()).isEqualTo(200);
            assertThat(httpClient.methods).containsExactly(POST, POST, HEAD, HEAD);
        }
    }

    private static HttpExecuteResponse httpResponse(int statusCode, String body)
    {
        byte[] bytes = body.getBytes(UTF_8);
        return HttpExecuteResponse.builder()
                .response(SdkHttpResponse.builder()
                        .statusCode(statusCode)
                        .putHeader("Content-Type", "application/xml")
                        .putHeader("Content-Length", Integer.toString(bytes.length))
                        .build())
                .responseBody(AbortableInputStream.create(new ByteArrayInputStream(bytes)))
                .build();
    }

    private S3FileSystem fileSystem(S3Client client)
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setRequesterPays(true)
                .setChecksumAlgorithm(CRC32C);
        S3Context context = new S3Context(
                toIntExact(config.getStreamingPartSize().toBytes()),
                config.isRequesterPays(),
                S3Context.S3SseContext.of(config.getSseType(), config.getSseKmsKeyId(), config.getSseCustomerKey()),
                Optional.empty(),
                config.getStorageClass(),
                config.getCannedAcl(),
                config.getChecksumAlgorithm());
        return new S3FileSystem(directExecutor(), client, preSigner, context);
    }

    private static Location location(String key)
    {
        return Location.of("s3://bucket/" + key);
    }

    private static S3Error error(String key, String code, String message)
    {
        return S3Error.builder().key(key).code(code).message(message).build();
    }

    private static DeleteObjectsResponse response(S3Error... errors)
    {
        return DeleteObjectsResponse.builder().errors(errors).build();
    }

    private static List<String> keys(DeleteObjectsRequest request)
    {
        return request.delete().objects().stream().map(ObjectIdentifier::key).toList();
    }

    private static final class TestingS3Client
            implements S3Client
    {
        private final RetryStrategy retryStrategy;
        private final Deque<DeleteObjectsResponse> responses;
        private final List<DeleteObjectsRequest> requests = new ArrayList<>();
        private boolean interruptAfterDelete;

        private TestingS3Client(int maxAttempts, DeleteObjectsResponse... responses)
        {
            this(STANDARD, maxAttempts, responses);
        }

        private TestingS3Client(RetryMode retryMode, int maxAttempts, DeleteObjectsResponse... responses)
        {
            this.retryStrategy = getRetryStrategy(retryMode).toBuilder()
                    .maxAttempts(maxAttempts)
                    .build();
            this.responses = new ArrayDeque<>(Arrays.asList(responses));
        }

        @Override
        public DeleteObjectsResponse deleteObjects(DeleteObjectsRequest request)
        {
            requests.add(request);
            if (interruptAfterDelete) {
                Thread.currentThread().interrupt();
            }
            return responses.removeFirst();
        }

        @Override
        public S3ServiceClientConfiguration serviceClientConfiguration()
        {
            return S3ServiceClientConfiguration.builder()
                    .overrideConfiguration(ClientOverrideConfiguration.builder().retryStrategy(retryStrategy).build())
                    .build();
        }

        @Override
        public String serviceName()
        {
            return SERVICE_NAME;
        }

        @Override
        public void close()
        {
            retryStrategy.close();
        }
    }

    private static final class TestingHttpClient
            implements SdkHttpClient
    {
        private final Deque<HttpExecuteResponse> responses;
        private final List<SdkHttpMethod> methods = new ArrayList<>();

        private TestingHttpClient(HttpExecuteResponse... responses)
        {
            this.responses = new ArrayDeque<>(Arrays.asList(responses));
        }

        @Override
        public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request)
        {
            return new ExecutableHttpRequest()
            {
                @Override
                public HttpExecuteResponse call()
                {
                    methods.add(request.httpRequest().method());
                    return responses.removeFirst();
                }

                @Override
                public void abort() {}
            };
        }

        @Override
        public void close() {}
    }
}
