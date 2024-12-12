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
import org.junit.jupiter.api.AfterAll;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteMarkerEntry;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.paginators.ListObjectVersionsIterable;

import java.net.URI;

public class TestS3FileSystemBackblazeB2
        extends AbstractTestS3FileSystem
{
    private String accessKey;
    private String secretKey;
    private String endpointUrl;
    private String region;
    private String bucket;

    @Override
    protected void initEnvironment()
    {
        accessKey = environmentVariable("AWS_ACCESS_KEY_ID");
        secretKey = environmentVariable("AWS_SECRET_ACCESS_KEY");
        endpointUrl = environmentVariable("AWS_ENDPOINT_URL");
        region = environmentVariable("AWS_REGION");
        bucket = environmentVariable("EMPTY_S3_BUCKET");
    }

    @Override
    protected boolean isCreateExclusive()
    {
        return false; // not supported by Backblaze B2
    }

    @Override
    protected boolean supportsCreateExclusive()
    {
        return false; // not supported by Backblaze B2
    }

    @AfterAll
    void tearDown()
    {
        // Backblaze B2 buckets are always versioned, and the default setting is
        // to keep all object versions until they are 'hard deleted'.
        // The tests use the default DeleteObject operation which, in versioned
        // buckets, 'soft deletes', or 'hides', the objects, rather than hard
        // deleting them. The result is that the bucket looks empty, but
        // still contains data.
        //
        // This code deletes the hidden files themselves and the associated delete
        // markers. Non-hidden files are NOT deleted, so verifyFileSystemIsEmpty()
        // will still find any files that should have been deleted, but were not.
        try (S3Client client = createS3Client()) {
            // Delete hidden versions
            ListObjectVersionsRequest request = ListObjectVersionsRequest.builder()
                    .bucket(bucket())
                    .build();
            ListObjectVersionsIterable response = client.listObjectVersionsPaginator(request);
            for (ObjectVersion version : response.versions()) {
                if (!version.isLatest()) {
                    DeleteObjectRequest doRequest = DeleteObjectRequest.builder()
                            .bucket(bucket())
                            .key(version.key())
                            .versionId(version.versionId())
                            .build();
                    client.deleteObject(doRequest);
                }
            }
            // Delete delete markers
            for (DeleteMarkerEntry deleteMarker : response.deleteMarkers()) {
                DeleteObjectRequest doRequest = DeleteObjectRequest.builder()
                        .bucket(bucket())
                        .key(deleteMarker.key())
                        .versionId(deleteMarker.versionId())
                        .build();
                client.deleteObject(doRequest);
            }
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
                .endpointOverride(URI.create(endpointUrl))
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
                .setSupportsExclusiveCreate(false)
                .setStreamingPartSize(DataSize.valueOf("5.5MB")), new S3FileSystemStats());
    }
}
