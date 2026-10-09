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
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.s3.S3FileSystemConfig.S3ChecksumAlgorithm;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.containers.Minio;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;

import static io.trino.filesystem.s3.S3FileSystem.DELETE_BATCH_SIZE;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;
import static software.amazon.awssdk.services.s3.model.ChecksumMode.ENABLED;

public class TestS3FileSystemMinIo
        extends AbstractTestS3FileSystem
{
    private final String bucket = "test-bucket-test-s3-file-system-minio";

    private Minio minio;

    @Override
    protected void initEnvironment()
    {
        minio = Minio.builder().build();
        minio.start();
        minio.createBucket(bucket);
    }

    @AfterAll
    void tearDown()
    {
        if (minio != null) {
            minio.close();
            minio = null;
        }
    }

    @Override
    protected String bucket()
    {
        return bucket;
    }

    @Override
    protected S3Client createS3Client()
    {
        return S3Client.builder()
                .endpointOverride(URI.create(minio.getMinioAddress()))
                .region(Region.of(Minio.MINIO_REGION))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(Minio.MINIO_ROOT_USER, Minio.MINIO_ROOT_PASSWORD)))
                .build();
    }

    @Override
    protected S3FileSystemFactory createS3FileSystemFactory()
    {
        return new S3FileSystemFactory(OpenTelemetry.noop(), createS3FileSystemConfig(), new S3FileSystemStats());
    }

    private S3FileSystemConfig createS3FileSystemConfig()
    {
        return new S3FileSystemConfig()
                .setEndpoint(minio.getMinioAddress())
                .setRegion(Minio.MINIO_REGION)
                .setPathStyleAccess(true)
                .setAwsAccessKey(Minio.MINIO_ROOT_USER)
                .setAwsSecretKey(Minio.MINIO_ROOT_PASSWORD)
                .setStreamingPartSize(STREAMING_PART_SIZE);
    }

    @Test
    @Override
    public void testPaths()
    {
        assertThatThrownBy(super::testPaths)
                .isInstanceOf(IOException.class)
                // MinIO does not support object keys with directory navigation ("/./" or "/../") or with double slashes ("//")
                .hasMessage("S3 HEAD request failed for file: s3://" + bucket + "/test/.././/file");
    }

    @Test
    @Override
    public void testListFiles()
            throws IOException
    {
        // MinIO is not hierarchical but has hierarchical naming constraints. For example it's not possible to have two blobs "level0" and "level0/level1".
        testListFiles(true);
    }

    @Test
    @Override
    public void testListFilesStartingFrom()
            throws IOException
    {
        // MinIO is not hierarchical but has hierarchical naming constraints. For example it's not possible to have two blobs "level0" and "level0/level1".
        testListFilesStartingFrom(true);
    }

    @Test
    @Override
    public void testDeleteDirectory()
            throws IOException
    {
        // MinIO is not hierarchical but has hierarchical naming constraints. For example it's not possible to have two blobs "level0" and "level0/level1".
        testDeleteDirectory(true);
    }

    @Test
    @Override
    public void testListDirectories()
            throws IOException
    {
        // MinIO is not hierarchical but has hierarchical naming constraints. For example it's not possible to have two blobs "level0" and "level0/level1".
        testListDirectories(true);
    }

    @Test
    void testDeleteManyFiles()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            // create a large number of files to test batch deletion over multiple batches
            // we run this test only on MinIO to avoid API costs and long execution time on AWS S3
            List<TempBlob> blobs = randomBlobs(closer, DELETE_BATCH_SIZE + 100);
            List<Location> locations = blobs.stream()
                    .map(TempBlob::location)
                    .toList();

            getFileSystem().deleteFiles(locations);
            for (Location location : locations) {
                assertThat(getFileSystem().newInputFile(location).exists()).isFalse();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = S3ChecksumAlgorithm.class, names = "DEFAULT", mode = EXCLUDE)
    void testChecksumAlgorithm(S3ChecksumAlgorithm checksumAlgorithm)
            throws IOException
    {
        S3FileSystemFactory fileSystemFactory = new S3FileSystemFactory(
                OpenTelemetry.noop(),
                createS3FileSystemConfig().setChecksumAlgorithm(checksumAlgorithm),
                new S3FileSystemStats());
        try (S3Client s3Client = createS3Client()) {
            TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
            Location singlePartLocation = getRootLocation().appendPath("checksum/single-part-" + randomUUID());
            Location multipartLocation = getRootLocation().appendPath("checksum/multipart-" + randomUUID());

            fileSystem.newOutputFile(singlePartLocation).createOrOverwrite("test data".getBytes(UTF_8));
            try (OutputStream outputStream = fileSystem.newOutputFile(multipartLocation).create()) {
                outputStream.write(new byte[toIntExact(STREAMING_PART_SIZE.toBytes()) * 2 + 1]);
            }

            for (Location location : List.of(singlePartLocation, multipartLocation)) {
                HeadObjectResponse response = s3Client.headObject(request -> request
                        .bucket(bucket)
                        .key(new S3Location(location).key())
                        .checksumMode(ENABLED));
                assertThat(storedChecksum(response, checksumAlgorithm))
                        .as("%s checksum of %s", checksumAlgorithm, location)
                        .isNotNull();
            }

            fileSystem.deleteFiles(List.of(singlePartLocation, multipartLocation));
            assertThat(fileSystem.newInputFile(singlePartLocation).exists()).isFalse();
            assertThat(fileSystem.newInputFile(multipartLocation).exists()).isFalse();
        }
        finally {
            fileSystemFactory.destroy();
        }
    }

    private static String storedChecksum(HeadObjectResponse response, S3ChecksumAlgorithm checksumAlgorithm)
    {
        return switch (checksumAlgorithm) {
            case DEFAULT -> throw new IllegalArgumentException("No checksum for DEFAULT algorithm");
            case CRC32 -> response.checksumCRC32();
            case CRC32C -> response.checksumCRC32C();
            case SHA1 -> response.checksumSHA1();
            case SHA256 -> response.checksumSHA256();
        };
    }
}
