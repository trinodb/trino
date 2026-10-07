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

import com.google.common.io.Closer;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoInputFile;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.containers.Floci;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static io.trino.filesystem.s3.S3FileSystemConfig.S3AuthType.ANONYMOUS;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
public class TestS3FileSystemFlociWithRemoteSigning
        extends AbstractTestS3FileSystem
{
    private static final String BUCKET = "test-bucket";
    // Floci's enforced-auth mode recognizes the legacy "test" access key.
    private static final AwsCredentials CREDENTIALS = AwsBasicCredentials.create("test", "test");

    @Container
    private static final Floci FLOCI = new Floci().withEnv("FLOCI_SERVICES_S3_ENFORCE_AUTH", "true");

    @Override
    protected void initEnvironment()
    {
        try (S3Client client = createS3Client()) {
            client.createBucket(builder -> builder.bucket(BUCKET));
        }
    }

    @Override
    protected String bucket()
    {
        return BUCKET;
    }

    @Override
    protected S3Client createS3Client()
    {
        return S3Client.builder()
                .applyMutation(FLOCI::updateClient)
                .credentialsProvider(StaticCredentialsProvider.create(CREDENTIALS))
                .build();
    }

    @Override
    protected S3FileSystemFactory createS3FileSystemFactory()
    {
        return createFileSystemFactory(config().setAuthType(ANONYMOUS), TestS3FileSystemFlociWithRemoteSigning::sign);
    }

    @Override
    protected boolean supportsPreSignedUri()
    {
        return false;
    }

    @Test
    @Override
    public void testPreSignedUris()
            throws IOException
    {
        try (TempBlob blob = randomBlobLocation("pre-signed")) {
            blob.createOrOverwrite("private content");
            assertThat(getFileSystem().preSignedUri(blob.location(), new Duration(1, SECONDS))).isEmpty();
        }
    }

    @Test
    void testUnsignedSignerDoesNotUseClientCredentials()
            throws IOException
    {
        try (TempBlob blob = randomBlobLocation("unsigned-signer"); Closer closer = Closer.create()) {
            blob.createOrOverwrite("private content");
            S3FileSystemFactory factory = createFileSystemFactory(
                    config().setAwsAccessKey(CREDENTIALS.accessKeyId()).setAwsSecretKey(CREDENTIALS.secretAccessKey()),
                    request -> SignedRequest.builder()
                            .request(request.request())
                            .payload(request.payload().orElse(null))
                            .build());
            closer.register(factory::destroy);
            TrinoInputFile input = factory.create(ConnectorIdentity.ofUser("test")).newInputFile(blob.location());

            assertThatThrownBy(input::exists)
                    .isInstanceOf(IOException.class)
                    .cause()
                    .isInstanceOf(SdkServiceException.class)
                    .extracting(cause -> ((SdkServiceException) cause).statusCode())
                    .isEqualTo(403);
        }
    }

    @Test
    void testSignerFailureDoesNotUseClientCredentials()
            throws IOException
    {
        try (TempBlob blob = randomBlobLocation("failed-signer"); Closer closer = Closer.create()) {
            blob.createOrOverwrite("private content");
            S3FileSystemFactory factory = createFileSystemFactory(
                    config().setAwsAccessKey(CREDENTIALS.accessKeyId()).setAwsSecretKey(CREDENTIALS.secretAccessKey()),
                    _ -> {
                        throw SdkClientException.create("Signing denied");
                    });
            closer.register(factory::destroy);
            TrinoInputFile input = factory.create(ConnectorIdentity.ofUser("test")).newInputFile(blob.location());

            assertThatThrownBy(input::exists)
                    .isInstanceOf(IOException.class)
                    .rootCause()
                    .isInstanceOf(SdkClientException.class)
                    .hasMessageStartingWith("Signing denied");
        }
    }

    private static S3FileSystemConfig config()
    {
        return new S3FileSystemConfig()
                .setEndpoint(FLOCI.endpoint().toString())
                .setRegion(FLOCI_REGION)
                .setPathStyleAccess(true)
                .setStreamingPartSize(STREAMING_PART_SIZE)
                .setMaxErrorRetries(1);
    }

    private static S3FileSystemFactory createFileSystemFactory(
            S3FileSystemConfig config,
            Function<SignRequest<? extends AwsCredentialsIdentity>, SignedRequest> signingFunction)
    {
        HttpSigner<AwsCredentialsIdentity> signer = new HttpSigner<>()
        {
            @Override
            public SignedRequest sign(SignRequest<? extends AwsCredentialsIdentity> request)
            {
                assertThat(request.identity().accessKeyId()).isNull();
                assertThat(request.identity().secretAccessKey()).isNull();
                SignedRequest signed = signingFunction.apply(request);
                assertThat(signed.payload().orElse(null)).isSameAs(request.payload().orElse(null));
                return signed;
            }

            @Override
            public CompletableFuture<AsyncSignedRequest> signAsync(AsyncSignRequest<? extends AwsCredentialsIdentity> request)
            {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };
        return new S3FileSystemFactory(
                OpenTelemetry.noop(),
                config,
                new S3FileSystemStats(),
                Optional.of(_ -> Optional.of(signer)));
    }

    private static SignedRequest sign(SignRequest<? extends AwsCredentialsIdentity> request)
    {
        return AwsV4HttpSigner.create().sign(builder -> builder
                .identity(CREDENTIALS)
                .request(request.request())
                .payload(request.payload().orElse(null))
                .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, "s3")
                .putProperty(AwsV4HttpSigner.REGION_NAME, request.requireProperty(AwsV4HttpSigner.REGION_NAME))
                .putProperty(AwsV4HttpSigner.DOUBLE_URL_ENCODE, false)
                .putProperty(AwsV4HttpSigner.NORMALIZE_PATH, false)
                .putProperty(AwsV4HttpSigner.PAYLOAD_SIGNING_ENABLED, false)
                .putProperty(AwsV4HttpSigner.CHUNK_ENCODING_ENABLED, false));
    }
}
