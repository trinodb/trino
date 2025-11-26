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

import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.FileEntry;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectStorageClass;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.testing.SystemEnvironmentUtils.isEnvSet;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static org.assertj.core.api.Assertions.assertThat;

public class TestS3FileSystemAwsS3
        extends AbstractTestS3FileSystem
{
    private String accessKey;
    private String secretKey;
    private String region;
    private String bucket;
    private String endpoint;

    @Override
    protected void initEnvironment()
    {
        accessKey = requireEnv("AWS_ACCESS_KEY_ID");
        secretKey = requireEnv("AWS_SECRET_ACCESS_KEY");
        region = requireEnv("AWS_REGION");

        bucket = requireEnv("EMPTY_S3_BUCKET");

        if (isEnvSet("AWS_ENDPOINT")) {
            endpoint = requireEnv("AWS_ENDPOINT");
        }
        else {
            endpoint = "https://s3." + region + ".amazonaws.com";
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
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .region(Region.of(region))
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .endpointOverride(URI.create(endpoint))
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
                        .setAwsAccessKey(accessKey)
                        .setAwsSecretKey(secretKey)
                        .setRegion(region)
                        .setEndpoint(endpoint)
                        .setSignerType(S3FileSystemConfig.SignerType.AwsS3V4Signer)
                        .setStreamingPartSize(streamingPartSize),
                new S3FileSystemStats());
    }

    @Test
    void testS3FileIteratorFileEntryTags()
            throws IOException
    {
        try (S3Client s3Client = createS3Client()) {
            String key = "test/tagsGlacier";
            ObjectStorageClass storageClass = ObjectStorageClass.GLACIER;
            PutObjectRequest putObjectRequestBuilder = PutObjectRequest.builder()
                    .bucket(bucket())
                    .key(key)
                    .storageClass(storageClass.toString())
                    .build();
            s3Client.putObject(
                    putObjectRequestBuilder,
                    RequestBody.empty());

            try {
                List<FileEntry> listing = toList(getFileSystem().listFiles(getRootLocation().appendPath("test")));
                FileEntry fileEntry = getOnlyElement(listing);

                assertThat(fileEntry.tags()).contains("s3:glacier");
            }
            finally {
                s3Client.deleteObject(delete -> delete.bucket(bucket()).key(key));
            }
        }
    }
}
