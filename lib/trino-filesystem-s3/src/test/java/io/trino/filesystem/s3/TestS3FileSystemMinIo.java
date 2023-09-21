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
import io.trino.testing.containers.Minio;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.URI;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    public void testDeleteDirectory()
            throws IOException
    {
        // MinIO is not hierarchical but has hierarchical naming constraints. For example it's not possible to have two blobs "level0" and "level0/level1".
        testDeleteDirectory(true);
    }
}
