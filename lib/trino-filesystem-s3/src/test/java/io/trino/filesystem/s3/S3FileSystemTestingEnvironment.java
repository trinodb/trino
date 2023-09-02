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

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import io.airlift.log.Logging;
import io.airlift.units.DataSize;
import io.trino.filesystem.AbstractTrinoFileSystemTestingEnvironment;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.containers.Minio;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class S3FileSystemTestingEnvironment
        extends AbstractTrinoFileSystemTestingEnvironment
{
    private final S3FileSystemFactory fileSystemFactory;
    private final TrinoFileSystem fileSystem;

    public S3FileSystemTestingEnvironment()
    {
        Logging.initialize();
        initEnvironment();
        this.fileSystemFactory = createS3FileSystemFactory();
        this.fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
    }

    public void close()
    {
        fileSystemFactory.destroy();
    }

    @Override
    protected boolean isHierarchical()
    {
        return false;
    }

    @Override
    public TrinoFileSystem getFileSystem()
    {
        return fileSystem;
    }

    @Override
    protected Location getRootLocation()
    {
        return Location.of("s3://%s/".formatted(bucket()));
    }

    @Override
    protected final boolean supportsCreateWithoutOverwrite()
    {
        return false;
    }

    @Override
    protected final boolean supportsRenameFile()
    {
        return false;
    }

    @Override
    protected final boolean deleteFileFailsIfNotExists()
    {
        return false;
    }

    @Override
    protected void verifyFileSystemIsEmpty()
    {
        try (S3Client client = createS3Client()) {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucket())
                    .build();
            assertThat(client.listObjectsV2(request).contents()).isEmpty();
        }
    }

    protected abstract void initEnvironment();

    protected abstract String bucket();

    protected abstract S3FileSystemFactory createS3FileSystemFactory();

    protected abstract S3Client createS3Client();

    public static class S3FileSystemTestingEnvironmentAwsS3
            extends S3FileSystemTestingEnvironment
    {
        private String accessKey;
        private String secretKey;
        private String region;
        private String bucket;

        @Override
        protected void initEnvironment()
        {
            accessKey = getRequiredEnvironmentVariable("AWS_ACCESS_KEY_ID");
            secretKey = getRequiredEnvironmentVariable("AWS_SECRET_ACCESS_KEY");
            region = getRequiredEnvironmentVariable("AWS_REGION");
            bucket = getRequiredEnvironmentVariable("S3_BUCKET");
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
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                    .region(Region.of(region))
                    .build();
        }

        @Override
        protected S3FileSystemFactory createS3FileSystemFactory()
        {
            return new S3FileSystemFactory(new S3FileSystemConfig()
                    .setAwsAccessKey(accessKey)
                    .setAwsSecretKey(secretKey)
                    .setRegion(region)
                    .setStreamingPartSize(DataSize.valueOf("5.5MB")));
        }
    }

    public static class S3FileSystemTestingEnvironmentLocalStack
            extends S3FileSystemTestingEnvironment
    {
        private static final String BUCKET = "test-bucket";

        private LocalStackContainer localStack;

        @Override
        protected void initEnvironment()
        {
            localStack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.0.2"))
                    .withServices(Service.S3);
            localStack.start();
            try (S3Client s3Client = createS3Client()) {
                s3Client.createBucket(builder -> builder.bucket(BUCKET).build());
            }
        }

        @Override
        public void close()
        {
            if (localStack != null) {
                localStack.close();
                localStack = null;
            }
            super.close();
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
                    .endpointOverride(localStack.getEndpointOverride(Service.S3))
                    .region(Region.of(localStack.getRegion()))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
                    .build();
        }

        @Override
        protected S3FileSystemFactory createS3FileSystemFactory()
        {
            return new S3FileSystemFactory(new S3FileSystemConfig()
                    .setAwsAccessKey(localStack.getAccessKey())
                    .setAwsSecretKey(localStack.getSecretKey())
                    .setEndpoint(localStack.getEndpointOverride(Service.S3).toString())
                    .setRegion(localStack.getRegion())
                    .setStreamingPartSize(DataSize.valueOf("5.5MB")));
        }
    }

    public static class S3FileSystemTestingEnvironmentMinIo
            extends S3FileSystemTestingEnvironment
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

        @Override
        public void close()
        {
            if (minio != null) {
                minio.close();
                minio = null;
            }
            super.close();
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
                            AwsBasicCredentials.create(Minio.MINIO_ACCESS_KEY, Minio.MINIO_SECRET_KEY)))
                    .build();
        }

        @Override
        protected S3FileSystemFactory createS3FileSystemFactory()
        {
            return new S3FileSystemFactory(new S3FileSystemConfig()
                    .setEndpoint(minio.getMinioAddress())
                    .setRegion(Minio.MINIO_REGION)
                    .setPathStyleAccess(true)
                    .setAwsAccessKey(Minio.MINIO_ACCESS_KEY)
                    .setAwsSecretKey(Minio.MINIO_SECRET_KEY)
                    .setStreamingPartSize(DataSize.valueOf("5.5MB")));
        }
    }

    public static class S3FileSystemTestingEnvironmentS3Mock
            extends S3FileSystemTestingEnvironment
    {
        private static final String BUCKET = "test-bucket";

        private S3MockContainer s3Mock;

        @Override
        protected void initEnvironment()
        {
            s3Mock = new S3MockContainer("3.0.1")
                    .withInitialBuckets(BUCKET);
            s3Mock.start();
        }

        @Override
        public void close()
        {
            if (s3Mock != null) {
                s3Mock.close();
                s3Mock = null;
            }
            super.close();
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
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("accesskey", "secretkey")))
                    .endpointOverride(URI.create(s3Mock.getHttpEndpoint()))
                    .region(Region.US_EAST_1)
                    .forcePathStyle(true)
                    .build();
        }

        @Override
        protected S3FileSystemFactory createS3FileSystemFactory()
        {
            return new S3FileSystemFactory(new S3FileSystemConfig()
                    .setAwsAccessKey("accesskey")
                    .setAwsSecretKey("secretkey")
                    .setEndpoint(s3Mock.getHttpEndpoint())
                    .setRegion(Region.US_EAST_1.id())
                    .setPathStyleAccess(true)
                    .setStreamingPartSize(DataSize.valueOf("5.5MB")));
        }
    }
}
