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

import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.s3.S3FileSystemConfig.S3AuthType;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.containers.Minio;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises {@code s3.auth-type=ANONYMOUS} against MinIO, which enforces auth.
 * Anonymous reads succeed only against a bucket with a public-read policy, so this
 * verifies the request is genuinely unsigned rather than falling back to credentials.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestS3FileSystemAnonymous
{
    private static final String PUBLIC_BUCKET = "public-bucket";
    private static final String PRIVATE_BUCKET = "private-bucket";
    private static final String KNOWN_KEY = "directory/known-file.txt";
    private static final byte[] KNOWN_CONTENT = "anonymous read works".getBytes(StandardCharsets.UTF_8);

    private Minio minio;
    private S3FileSystemFactory fileSystemFactory;
    private TrinoFileSystem fileSystem;

    @BeforeAll
    void init()
    {
        minio = Minio.builder().build();
        minio.start();
        try (S3Client s3Client = createAdminClient()) {
            s3Client.createBucket(builder -> builder.bucket(PUBLIC_BUCKET));
            s3Client.putBucketPolicy(builder -> builder.bucket(PUBLIC_BUCKET).policy(publicReadPolicy(PUBLIC_BUCKET)));
            s3Client.putObject(builder -> builder.bucket(PUBLIC_BUCKET).key(KNOWN_KEY), RequestBody.fromBytes(KNOWN_CONTENT));

            s3Client.createBucket(builder -> builder.bucket(PRIVATE_BUCKET));
            s3Client.putObject(builder -> builder.bucket(PRIVATE_BUCKET).key(KNOWN_KEY), RequestBody.fromBytes(KNOWN_CONTENT));
        }

        fileSystemFactory = new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setAuthType(S3AuthType.ANONYMOUS)
                        .setEndpoint(minio.getMinioAddress())
                        .setRegion(Minio.MINIO_REGION)
                        .setPathStyleAccess(true),
                new S3FileSystemStats());
        fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
    }

    @AfterAll
    void cleanup()
    {
        fileSystem = null;
        if (fileSystemFactory != null) {
            fileSystemFactory.destroy();
            fileSystemFactory = null;
        }
        if (minio != null) {
            minio.close();
            minio = null;
        }
    }

    @Test
    void testAnonymousRead()
            throws IOException
    {
        TrinoInputFile inputFile = fileSystem.newInputFile(Location.of("s3://%s/%s".formatted(PUBLIC_BUCKET, KNOWN_KEY)));
        assertThat(inputFile.exists()).isTrue();
        assertThat(inputFile.length()).isEqualTo(KNOWN_CONTENT.length);
        try (TrinoInputStream inputStream = inputFile.newStream()) {
            assertThat(inputStream.readAllBytes()).isEqualTo(KNOWN_CONTENT);
        }
    }

    @Test
    void testAnonymousListDirectories()
            throws IOException
    {
        assertThat(fileSystem.listDirectories(Location.of("s3://%s/".formatted(PUBLIC_BUCKET))))
                .contains(Location.of("s3://%s/directory/".formatted(PUBLIC_BUCKET)));
    }

    @Test
    void testAnonymousReadDeniedOnPrivateBucket()
    {
        // Object exists in the bucket, so a 403 proves the request was genuinely anonymous rather than falling back to credentials.
        TrinoInputFile inputFile = fileSystem.newInputFile(Location.of("s3://%s/%s".formatted(PRIVATE_BUCKET, KNOWN_KEY)));
        assertThatThrownBy(() -> {
            try (TrinoInputStream inputStream = inputFile.newStream()) {
                inputStream.readAllBytes();
            }
        })
                .isInstanceOf(IOException.class)
                .cause()
                .isInstanceOf(SdkServiceException.class)
                .extracting(cause -> ((SdkServiceException) cause).statusCode())
                .isEqualTo(403);
    }

    @Test
    void testAnonymousListDirectoriesDeniedOnPrivateBucket()
    {
        assertThatThrownBy(() -> fileSystem.listDirectories(Location.of("s3://%s/".formatted(PRIVATE_BUCKET))))
                .isInstanceOf(IOException.class)
                .cause()
                .isInstanceOf(SdkServiceException.class)
                .extracting(cause -> ((SdkServiceException) cause).statusCode())
                .isEqualTo(403);
    }

    private S3Client createAdminClient()
    {
        return S3Client.builder()
                .endpointOverride(URI.create(minio.getMinioAddress()))
                .region(Region.of(Minio.MINIO_REGION))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(Minio.MINIO_ROOT_USER, Minio.MINIO_ROOT_PASSWORD)))
                .build();
    }

    private static String publicReadPolicy(String bucket)
    {
        return """
               {
                   "Version": "2012-10-17",
                   "Statement": [{
                       "Effect": "Allow",
                       "Principal": {"AWS": ["*"]},
                       "Action": ["s3:GetObject", "s3:ListBucket"],
                       "Resource": ["arn:aws:s3:::%s/*", "arn:aws:s3:::%s"]
                   }]
               }""".formatted(bucket, bucket);
    }
}
