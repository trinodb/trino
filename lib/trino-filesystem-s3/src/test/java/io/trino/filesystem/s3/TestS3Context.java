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

import com.google.common.collect.ImmutableList;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.checksums.DefaultChecksumAlgorithm;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.services.s3.LegacyMd5Plugin;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeParams;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.zip.CRC32;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.filesystem.s3.S3FileSystemConfig.SignerType.Aws4UnsignedPayloadSigner;
import static io.trino.filesystem.s3.S3FileSystemConfig.StorageClassType.STANDARD;
import static io.trino.filesystem.s3.S3RemoteSigningAuthScheme.REQUEST_SIGNER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static software.amazon.awssdk.core.checksums.RequestChecksumCalculation.WHEN_REQUIRED;
import static software.amazon.awssdk.http.SdkHttpMethod.PUT;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

final class TestS3Context
{
    @Test
    void testRemoteSignerPreservesConfiguredSigningProperties()
    {
        HttpSigner<AwsCredentialsIdentity> signer = AwsV4HttpSigner.create();
        S3Context context = context().withCredentials(ConnectorIdentity.ofUser("test"), Optional.of(signer));
        AwsRequestOverrideConfiguration.Builder builder = AwsRequestOverrideConfiguration.builder();
        context.withKmsKeyId("kms-key").applyRequestOverrides(builder);
        AwsRequestOverrideConfiguration configuration = builder.build();

        assertThat(configuration.credentialsProvider()).containsInstanceOf(AnonymousCredentialsProvider.class);
        S3ServiceClientConfiguration.Builder serviceConfiguration = S3ServiceClientConfiguration.builder()
                .authSchemeProvider(S3FileSystemLoader.createAuthSchemeProvider(Aws4UnsignedPayloadSigner));
        configuration.plugins().forEach(plugin -> plugin.configureClient(serviceConfiguration));
        AuthSchemeOption option = getOnlyElement(serviceConfiguration.authSchemeProvider().resolveAuthScheme(
                S3AuthSchemeParams.builder().operation("GetObject").region(US_EAST_1).build()));
        assertThat(option.signerProperty(REQUEST_SIGNER)).isSameAs(signer);
        assertThat(option.signerProperty(AwsV4HttpSigner.DOUBLE_URL_ENCODE)).isTrue();
        assertThat(option.signerProperty(AwsV4HttpSigner.NORMALIZE_PATH)).isTrue();
        assertThat(option.signerProperty(AwsV4HttpSigner.PAYLOAD_SIGNING_ENABLED)).isFalse();
    }

    @Test
    void testRemoteSigningRejectsOtherAuthSchemes()
    {
        AwsRequestOverrideConfiguration.Builder builder = AwsRequestOverrideConfiguration.builder();
        context().withCredentials(ConnectorIdentity.ofUser("test"), Optional.of(AwsV4HttpSigner.create()))
                .applyRequestOverrides(builder);
        S3ServiceClientConfiguration.Builder serviceConfiguration = S3ServiceClientConfiguration.builder()
                .authSchemeProvider(_ -> ImmutableList.of(AuthSchemeOption.builder().schemeId("aws.auth#sigv4a").build()));
        builder.build().plugins().forEach(plugin -> plugin.configureClient(serviceConfiguration));

        assertThatThrownBy(() -> serviceConfiguration.authSchemeProvider().resolveAuthScheme(
                S3AuthSchemeParams.builder().operation("GetObject").region(US_EAST_1).build()))
                .isInstanceOf(SdkClientException.class)
                .hasMessage("Remote signing requires the SigV4 authentication scheme");
    }

    @Test
    void testConcurrentRemoteSignersDoNotChangeClientDefaults()
            throws Exception
    {
        CyclicBarrier barrier = new CyclicBarrier(2);
        S3Context first = context().withCredentials(ConnectorIdentity.ofUser("first"), Optional.of(signer("first", barrier)));
        S3Context second = context().withCredentials(ConnectorIdentity.ofUser("second"), Optional.of(signer("second", barrier)));
        try (CapturingHttpClient httpClient = new CapturingHttpClient();
                S3Client client = S3Client.builder()
                        .httpClient(httpClient)
                        .region(US_EAST_1)
                        .endpointOverride(URI.create("https://storage.example"))
                        .forcePathStyle(true)
                        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("client-access", "client-secret")))
                        .authSchemeProvider(S3FileSystemLoader.createAuthSchemeProvider(Aws4UnsignedPayloadSigner))
                        .putAuthScheme(new S3RemoteSigningAuthScheme())
                        .build();
                ExecutorService executor = newFixedThreadPool(2)) {
            Future<?> firstRequest = executor.submit(() -> client.headObject(HeadObjectRequest.builder()
                    .bucket("bucket")
                    .key("first")
                    .overrideConfiguration(first::applyRequestOverrides)
                    .build()));
            Future<?> secondRequest = executor.submit(() -> client.headObject(HeadObjectRequest.builder()
                    .bucket("bucket")
                    .key("second")
                    .overrideConfiguration(second::applyRequestOverrides)
                    .build()));
            firstRequest.get(10, SECONDS);
            secondRequest.get(10, SECONDS);

            assertThat(httpClient.requests).hasSize(2).allSatisfy(request ->
                    assertThat(request.firstMatchingHeader("Authorization"))
                            .contains(request.encodedPath().substring("/bucket/".length())));

            client.headObject(request -> request.bucket("bucket").key("default"));
            assertThat(httpClient.requests).hasSize(3);
            assertThat(httpClient.requests.getLast().firstMatchingHeader("Authorization")).hasValueSatisfying(value ->
                    assertThat(value).startsWith("AWS4-HMAC-SHA256 Credential=client-access/"));
        }
    }

    @Test
    void testRemoteSigningPreservesRequiredChecksumsAndPayload()
            throws Exception
    {
        List<SignRequest<? extends AwsCredentialsIdentity>> signingRequests = new CopyOnWriteArrayList<>();
        HttpSigner<AwsCredentialsIdentity> signer = signer(request -> {
            signingRequests.add(request);
            return SignedRequest.builder()
                    .request(request.request().toBuilder().putHeader("Authorization", "remote-signature").build())
                    .payload(request.payload().orElse(null))
                    .build();
        });
        S3Context context = context().withCredentials(ConnectorIdentity.ofUser("test"), Optional.of(signer));
        try (CapturingHttpClient httpClient = new CapturingHttpClient();
                S3Client client = S3Client.builder()
                        .httpClient(httpClient)
                        .region(US_EAST_1)
                        .endpointOverride(URI.create("http://storage.example"))
                        .forcePathStyle(true)
                        .credentialsProvider(AnonymousCredentialsProvider.create())
                        .requestChecksumCalculation(WHEN_REQUIRED)
                        .addPlugin(LegacyMd5Plugin.create())
                        .putAuthScheme(new S3RemoteSigningAuthScheme())
                        .build()) {
            client.deleteObjects(DeleteObjectsRequest.builder()
                    .bucket("bucket")
                    .delete(delete -> delete.objects(
                            ObjectIdentifier.builder().key("first").build(),
                            ObjectIdentifier.builder().key("second").build()))
                    .overrideConfiguration(context::applyRequestOverrides)
                    .build());
            byte[] deletePayload = httpClient.bodies.getFirst();
            String contentMd5 = Base64.getEncoder().encodeToString(MessageDigest.getInstance("MD5").digest(deletePayload));
            CRC32 crc32 = new CRC32();
            crc32.update(deletePayload);
            String contentCrc32 = Base64.getEncoder().encodeToString(ByteBuffer.allocate(Integer.BYTES).putInt((int) crc32.getValue()).array());
            assertThat(signingRequests.getFirst().request().firstMatchingHeader("Content-MD5")).contains(contentMd5);
            assertThat(httpClient.requests.getFirst().firstMatchingHeader("Content-MD5")).contains(contentMd5);
            assertThat(signingRequests.getFirst().request().firstMatchingHeader("x-amz-sdk-checksum-algorithm")).contains("CRC32");
            assertThat(signingRequests.getFirst().request().firstMatchingHeader("x-amz-checksum-crc32")).contains(contentCrc32);
            assertThat(httpClient.requests.getFirst().firstMatchingHeader("x-amz-sdk-checksum-algorithm")).contains("CRC32");
            assertThat(httpClient.requests.getFirst().firstMatchingHeader("x-amz-checksum-crc32")).contains(contentCrc32);
            assertThat(new String(deletePayload, UTF_8)).contains("<Key>first</Key>", "<Key>second</Key>");
            try (InputStream input = signingRequests.getFirst().payload().orElseThrow().newStream()) {
                assertThat(input.readAllBytes()).isEqualTo(deletePayload);
            }
            byte[] contents = "ordinary object contents".getBytes(UTF_8);
            client.putObject(PutObjectRequest.builder()
                    .bucket("bucket")
                    .key("object")
                    .overrideConfiguration(context::applyRequestOverrides)
                    .build(), RequestBody.fromBytes(contents));
            assertThat(signingRequests).hasSize(2);
            assertThat(httpClient.requests).hasSize(2);
            assertThat(signingRequests.getLast().request().firstMatchingHeader("Content-MD5")).isEmpty();
            assertThat(signingRequests.getLast().request().firstMatchingHeader("x-amz-sdk-checksum-algorithm")).isEmpty();
            assertThat(signingRequests.getLast().request().firstMatchingHeader("x-amz-checksum-crc32")).isEmpty();
            assertThat(httpClient.requests.getLast().firstMatchingHeader("Content-Encoding")).isEmpty();
            assertThat(httpClient.bodies.getLast()).isEqualTo(contents);
            try (InputStream input = signingRequests.getLast().payload().orElseThrow().newStream()) {
                assertThat(input.readAllBytes()).isEqualTo(contents);
            }
        }
    }

    @Test
    void testRemoteSigningPreservesProvidedChecksum()
    {
        SignRequest<? extends AwsCredentialsIdentity> request = SignRequest.builder(AnonymousCredentialsProvider.create().resolveCredentials())
                .request(SdkHttpRequest.builder()
                        .method(PUT)
                        .uri(URI.create("https://storage.example/object"))
                        .putHeader("X-Amz-Checksum-Crc32", "precomputed")
                        .build())
                .payload(() -> {
                    throw new AssertionError("A precomputed checksum must not read the payload");
                })
                .putProperty(AwsV4HttpSigner.CHECKSUM_ALGORITHM, DefaultChecksumAlgorithm.CRC32)
                .putProperty(REQUEST_SIGNER, signer(signRequest -> SignedRequest.builder()
                        .request(signRequest.request())
                        .payload(signRequest.payload().orElse(null))
                        .build()))
                .build();

        SignedRequest signed = new S3RemoteSigningAuthScheme().sign(request);
        assertThat(signed.request().firstMatchingHeader("x-amz-checksum-crc32")).contains("precomputed");
        assertThat(signed.payload()).isEqualTo(request.payload());
    }

    private static S3Context context()
    {
        return new S3Context(
                5 * 1024 * 1024,
                false,
                S3Context.S3SseContext.of(S3FileSystemConfig.S3SseType.NONE, null, null),
                Optional.empty(),
                STANDARD,
                S3FileSystemConfig.ObjectCannedAcl.NONE);
    }

    private static HttpSigner<AwsCredentialsIdentity> signer(String authorization, CyclicBarrier barrier)
    {
        return signer(request -> {
            assertThat(request.identity().accessKeyId()).isNull();
            assertThat(request.identity().secretAccessKey()).isNull();
            assertThat(request.property(AwsV4HttpSigner.REGION_NAME)).isEqualTo("us-east-1");
            assertThat(request.property(AwsV4HttpSigner.NORMALIZE_PATH)).isTrue();
            assertThat(request.property(AwsV4HttpSigner.PAYLOAD_SIGNING_ENABLED)).isFalse();
            try {
                barrier.await(10, SECONDS);
            }
            catch (Exception e) {
                throw new AssertionError(e);
            }
            return SignedRequest.builder()
                    .request(request.request().toBuilder().putHeader("Authorization", authorization).build())
                    .payload(request.payload().orElse(null))
                    .build();
        });
    }

    private static HttpSigner<AwsCredentialsIdentity> signer(Function<SignRequest<? extends AwsCredentialsIdentity>, SignedRequest> signingFunction)
    {
        return new HttpSigner<>()
        {
            @Override
            public SignedRequest sign(SignRequest<? extends AwsCredentialsIdentity> request)
            {
                return signingFunction.apply(request);
            }

            @Override
            public CompletableFuture<AsyncSignedRequest> signAsync(AsyncSignRequest<? extends AwsCredentialsIdentity> request)
            {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };
    }

    private static final class CapturingHttpClient
            implements SdkHttpClient
    {
        private final List<SdkHttpRequest> requests = new CopyOnWriteArrayList<>();
        private final List<byte[]> bodies = new CopyOnWriteArrayList<>();

        @Override
        public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request)
        {
            requests.add(request.httpRequest());
            return new ExecutableHttpRequest()
            {
                @Override
                public HttpExecuteResponse call()
                        throws IOException
                {
                    byte[] body = new byte[0];
                    if (request.contentStreamProvider().isPresent()) {
                        try (InputStream input = request.contentStreamProvider().orElseThrow().newStream()) {
                            body = input.readAllBytes();
                        }
                    }
                    bodies.add(body);
                    HttpExecuteResponse.Builder response = HttpExecuteResponse.builder()
                            .response(SdkHttpResponse.builder().statusCode(200).putHeader("Content-Length", "0").build());
                    if (request.httpRequest().rawQueryParameters().containsKey("delete")) {
                        byte[] result = "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>".getBytes(UTF_8);
                        response.response(SdkHttpResponse.builder().statusCode(200).putHeader("Content-Length", Integer.toString(result.length)).build())
                                .responseBody(AbortableInputStream.create(new ByteArrayInputStream(result)));
                    }
                    return response.build();
                }

                @Override
                public void abort() {}
            };
        }

        @Override
        public void close() {}
    }
}
