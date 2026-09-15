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
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.Location;
import io.trino.testing.containers.Floci;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.stream.IntStream;

import static io.trino.filesystem.s3.S3FileSystem.DELETE_BATCH_SIZE;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class TestS3FileSystemFloci
        extends AbstractTestS3FileSystem
{
    protected static final String BUCKET = "test-bucket";

    @Container
    private static final Floci FLOCI = new Floci();

    @Override
    protected void initEnvironment()
    {
        FLOCI.createBucket(BUCKET);
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
                .build();
    }

    @Override
    protected S3FileSystemFactory createS3FileSystemFactory()
    {
        return new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setAwsAccessKey(FLOCI_ACCESS_KEY)
                        .setAwsSecretKey(FLOCI_SECRET_KEY)
                        .setEndpoint(endpoint().toString())
                        .setRegion(FLOCI_REGION)
                        .setPathStyleAccess(true)
                        .setStreamingPartSize(STREAMING_PART_SIZE),
                new S3FileSystemStats());
    }

    protected static URI endpoint()
    {
        return FLOCI.endpoint();
    }

    @Test
    void testDeleteManyFiles()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            // Run batch deletion tests locally to avoid API costs and long execution time on AWS S3.
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

    @Test
    void testListManyFiles()
            throws IOException
    {
        try (Closer closer = Closer.create();
                S3Client client = createS3Client()) {
            // S3 returns at most 1000 objects per page.
            List<Location> locations = IntStream.range(0, 1100)
                    .mapToObj(index -> createBlob(closer, "files/%04d".formatted(index)))
                    .toList();
            createBlob(closer, "files-other/file");

            var firstPage = client.listObjectsV2(builder -> builder.bucket(bucket()).prefix("files/"));
            assertThat(firstPage.isTruncated()).isTrue();
            assertThat(firstPage.contents()).hasSize(1000);

            assertThat(toList(getFileSystem().listFiles(createLocation("files"))))
                    .extracting(FileEntry::location)
                    .containsExactlyElementsOf(locations);
            assertThat(toList(getFileSystem().listFilesStartingFrom(createLocation("files"), "0001")))
                    .extracting(FileEntry::location)
                    .containsExactlyElementsOf(locations.subList(1, locations.size()));
            assertThat(toList(getFileSystem().listFilesStartingFrom(createLocation("files"), "1000")))
                    .extracting(FileEntry::location)
                    .containsExactlyElementsOf(locations.subList(1000, locations.size()));
        }
    }

    @Test
    void testListManyDirectories()
            throws IOException
    {
        try (Closer closer = Closer.create();
                S3Client client = createS3Client()) {
            // Common prefixes count towards the 1000 entries per page limit.
            List<Location> directories = IntStream.range(0, 1100)
                    .mapToObj(index -> {
                        String directory = "directories/%04d/".formatted(index);
                        createBlob(closer, directory + "file");
                        return createLocation(directory);
                    })
                    .toList();
            createBlob(closer, "directories-other/file");

            var firstPage = client.listObjectsV2(builder -> builder.bucket(bucket()).prefix("directories/").delimiter("/"));
            assertThat(firstPage.isTruncated()).isTrue();
            assertThat(firstPage.commonPrefixes()).hasSize(1000);

            assertThat(getFileSystem().listDirectories(createLocation("directories")))
                    .containsExactlyInAnyOrderElementsOf(directories);
        }
    }

    @Test
    void testDeleteManyFilesInDirectory()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            List<Location> locations = IntStream.range(0, DELETE_BATCH_SIZE + 100)
                    .mapToObj(index -> createBlob(closer, "directory/%04d".formatted(index)))
                    .toList();
            Location parent = createBlob(closer, "directory");
            Location sibling = createBlob(closer, "directory-other/file");

            // Deleting a page of objects must not skip the remaining pages.
            getFileSystem().deleteDirectory(createLocation("directory"));
            for (Location location : locations) {
                assertThat(getFileSystem().newInputFile(location).exists()).isFalse();
            }
            assertThat(toList(getFileSystem().listFiles(createLocation("directory")))).isEmpty();
            assertThat(getFileSystem().newInputFile(parent).exists()).isTrue();
            assertThat(getFileSystem().newInputFile(sibling).exists()).isTrue();
        }
    }
}
