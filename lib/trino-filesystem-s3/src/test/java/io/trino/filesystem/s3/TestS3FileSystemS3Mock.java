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
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
public class TestS3FileSystemS3Mock
        extends AbstractTestS3FileSystem
{
    private static final String BUCKET = "test-bucket";

    @Container
    private static final S3MockContainer S3_MOCK = new S3MockContainer("4.10.0")
            .withInitialBuckets(BUCKET);

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
                .endpointOverride(URI.create(S3_MOCK.getHttpEndpoint()))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .build();
    }

    @Override
    protected S3FileSystemFactory createS3FileSystemFactory()
    {
        DataSize streamingPartSize = DataSize.valueOf("5.5MB");
        assertThat(streamingPartSize).describedAs("Configured part size should be less than test's larger file size")
                .isLessThan(LARGER_FILE_DATA_SIZE);
        return new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setAwsAccessKey("accesskey")
                        .setAwsSecretKey("secretkey")
                        .setEndpoint(S3_MOCK.getHttpEndpoint())
                        .setRegion(Region.US_EAST_1.id())
                        .setPathStyleAccess(true)
                        .setStreamingPartSize(streamingPartSize)
                        .setSignerType(S3FileSystemConfig.SignerType.AwsS3V4Signer),
                new S3FileSystemStats());
    }

    @Test
    @Override
    public void testPreSignedUris()
    {
        // S3 mock doesn't expire pre-signed URLs
        assertThatThrownBy(super::testPreSignedUris)
                .hasMessageContaining("Expecting code to raise a throwable");
    }

    @Test
    @Override
    public void testPaths()
    {
        // this is S3Mock bug, see https://github.com/adobe/S3Mock/issues/2788
        assertThatThrownBy(super::testPaths)
                .hasMessageContaining("S3 HEAD request failed for file: s3://test-bucket/test/.././/file");
    }

    @Test
    @Override
    public void testReadingEmptyFile()
    {
        // this is S3Mock bug, see https://github.com/adobe/S3Mock/issues/2789
        assertThatThrownBy(super::testReadingEmptyFile)
                .hasMessageContaining("Failed to open S3 file: s3://test-bucket/inputStream/");
    }
}
