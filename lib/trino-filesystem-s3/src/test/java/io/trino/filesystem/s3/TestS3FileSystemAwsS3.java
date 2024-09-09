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
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectStorageClass;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestS3FileSystemAwsS3
        extends AbstractTestS3FileSystem
{
    private String accessKey;
    private String secretKey;
    private String region;
    private String bucket;

    @Override
    protected void initEnvironment()
    {
        accessKey = environmentVariable("AWS_ACCESS_KEY_ID");
        secretKey = environmentVariable("AWS_SECRET_ACCESS_KEY");
        region = environmentVariable("AWS_REGION");
        bucket = environmentVariable("EMPTY_S3_BUCKET");
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
        return new S3FileSystemFactory(OpenTelemetry.noop(), new S3FileSystemConfig()
                .setAwsAccessKey(accessKey)
                .setAwsSecretKey(secretKey)
                .setRegion(region)
                .setSupportsExclusiveCreate(true)
                .setStreamingPartSize(DataSize.valueOf("5.5MB")), new S3FileSystemStats());
    }

    private static String environmentVariable(String name)
    {
        return requireNonNull(System.getenv(name), "Environment variable not set: " + name);
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

                assertThat(fileEntry.tags().contains("s3:glacier")).isTrue();
            }
            finally {
                s3Client.deleteObject(delete -> delete.bucket(bucket()).key(key));
            }
        }
    }
}
