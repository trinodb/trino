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

import io.airlift.log.Logging;
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.net.URI;

import static io.trino.testing.SystemEnvironmentUtils.isEnvSet;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOSSFileSystemAliyunOss
        extends AbstractTestTrinoFileSystem
{
    private String accessKey;
    private String secretKey;
    private String region;
    private String bucket;
    private String endpoint;
    private S3FileSystemFactory fileSystemFactory;
    private TrinoFileSystem fileSystem;

    @BeforeAll
    void init()
    {
        Logging.initialize();

        accessKey = requireEnv("ALIYUN_OSS_ACCESS_KEY_ID");
        secretKey = requireEnv("ALIYUN_OSS_ACCESS_KEY_SECRET");
        region = requireEnv("ALIYUN_OSS_REGION");
        bucket = requireEnv("EMPTY_OSS_BUCKET");

        if (isEnvSet("ALIYUN_OSS_ENDPOINT")) {
            endpoint = requireEnv("ALIYUN_OSS_ENDPOINT");
        }
        else {
            endpoint = "https://oss-" + region + ".aliyuncs.com";
        }

        DataSize streamingPartSize = DataSize.valueOf("5.5MB");
        assertThat(streamingPartSize).describedAs("Configured part size should be less than test's larger file size")
                .isLessThan(LARGER_FILE_DATA_SIZE);
        fileSystemFactory = new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setAwsAccessKey(accessKey)
                        .setAwsSecretKey(secretKey)
                        .setRegion(region)
                        .setEndpoint(endpoint)
                        .setSignerType(S3FileSystemConfig.SignerType.AwsS3V4Signer)
                        .setStreamingPartSize(streamingPartSize),
                new S3FileSystemStats());
        fileSystem = new OSSFileSystem(fileSystemFactory.create(ConnectorIdentity.ofUser("test")));
    }

    @AfterAll
    void cleanup()
    {
        fileSystem = null;
        if (fileSystemFactory != null) {
            fileSystemFactory.destroy();
        }
        fileSystemFactory = null;
    }

    @Override
    protected boolean isHierarchical()
    {
        return false;
    }

    @Override
    protected TrinoFileSystem getFileSystem()
    {
        return fileSystem;
    }

    @Override
    protected Location getRootLocation()
    {
        return Location.of("oss://%s/".formatted(bucket));
    }

    @Override
    protected boolean isCreateExclusive()
    {
        return false;
    }

    @Override
    protected boolean supportsCreateExclusive()
    {
        return false;
    }

    @Override
    protected boolean supportsRenameFile()
    {
        return false;
    }

    @Override
    protected boolean supportsPreSignedUri()
    {
        return true;
    }

    @Override
    protected void verifyFileSystemIsEmpty()
    {
        try (S3Client client = createS3Client()) {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .build();
            assertThat(client.listObjectsV2(request).contents()).isEmpty();
        }
    }

    private S3Client createS3Client()
    {
        return S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .region(Region.of(region))
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .endpointOverride(URI.create(endpoint))
                .build();
    }
}
