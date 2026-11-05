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
package io.trino.plugin.exchange.filesystem.s3;

import io.trino.plugin.exchange.filesystem.AbstractTestExchangeManager;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeManager;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeManagerFactory;
import io.trino.plugin.exchange.filesystem.TestExchangeManagerContext;
import io.trino.plugin.exchange.filesystem.containers.FlociStorage;
import io.trino.spi.exchange.ExchangeManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.util.List;

import static io.trino.plugin.exchange.filesystem.s3.ExchangeS3Config.S3SseType.NONE;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

public class TestS3FileSystemExchangeManager
        extends AbstractTestExchangeManager
{
    private final String bucketName = "test-exchange-spooling-" + randomUUID();

    private FlociStorage storage;
    private FileSystemExchangeManager fileSystemExchangeManager;

    @Override
    protected ExchangeManager createExchangeManager()
    {
        storage = new FlociStorage(bucketName, NONE);
        storage.start();

        fileSystemExchangeManager = (FileSystemExchangeManager) new FileSystemExchangeManagerFactory().create(
                storage.getExchangeManagerProperties(),
                new TestExchangeManagerContext());
        return fileSystemExchangeManager;
    }

    @Override
    protected void deleteFile(URI file)
    {
        storage.deleteObject(file);
    }

    @Override
    @AfterAll
    public void destroy()
            throws Exception
    {
        super.destroy();
        if (storage != null) {
            storage.close();
            storage = null;
        }
    }

    @Test
    public void testListDirectoriesReturnsImmediateSubdirectoriesOfExchangeDirectory()
            throws Exception
    {
        URI baseDirectory = fileSystemExchangeManager.getBaseDirectories().getFirst();
        String exchangeDirName = "listdirs-test-" + randomUUID() + ".exchange-1.0/";
        URI exchangeDirectory = baseDirectory.resolve(exchangeDirName);

        try (S3Client s3Client = createS3Client()) {
            putEmptyObject(s3Client, exchangeDirName + "0/0_0.data");
            putEmptyObject(s3Client, exchangeDirName + "1/0_0.data");
        }

        List<URI> directories = fileSystemExchangeManager.listDirectories(exchangeDirectory);

        assertThat(directories).containsExactlyInAnyOrder(
                exchangeDirectory.resolve("0/"),
                exchangeDirectory.resolve("1/"));
    }

    @Test
    public void testListDirectoriesOnBaseDirectoryReturnsExchangeDirectories()
            throws Exception
    {
        URI baseDirectory = fileSystemExchangeManager.getBaseDirectories().getFirst();
        String query1DirName = "listdirs-test-" + randomUUID() + ".exchange-1.0/";
        String query2DirName = "listdirs-test-" + randomUUID() + ".exchange-1.0/";

        try (S3Client s3Client = createS3Client()) {
            putEmptyObject(s3Client, query1DirName + "0/0_0.data");
            putEmptyObject(s3Client, query2DirName + "0/0_0.data");
        }

        List<URI> directories = fileSystemExchangeManager.listDirectories(baseDirectory);

        assertThat(directories).contains(
                baseDirectory.resolve(query1DirName),
                baseDirectory.resolve(query2DirName));
    }

    @Test
    public void testListDirectoriesOnMissingDirectoryReturnsEmpty()
            throws Exception
    {
        URI baseDirectory = fileSystemExchangeManager.getBaseDirectories().getFirst();
        URI missingDirectory = baseDirectory.resolve("listdirs-test-" + randomUUID() + ".does-not-exist/");

        List<URI> directories = fileSystemExchangeManager.listDirectories(missingDirectory);

        assertThat(directories).isEmpty();
    }

    @Test
    public void testDeleteRecursivelyRemovesDirectoriesFoundByListDirectories()
            throws Exception
    {
        URI baseDirectory = fileSystemExchangeManager.getBaseDirectories().getFirst();
        String orphanDirName = "listdirs-test-" + randomUUID() + ".exchange-1.0/";
        URI orphanDirectory = baseDirectory.resolve(orphanDirName);

        try (S3Client s3Client = createS3Client()) {
            putEmptyObject(s3Client, orphanDirName + "0/0_0.data");

            List<URI> directoriesBeforeDelete = fileSystemExchangeManager.listDirectories(baseDirectory);
            assertThat(directoriesBeforeDelete).contains(orphanDirectory);

            fileSystemExchangeManager.deleteRecursively(List.of(orphanDirectory));

            List<URI> directoriesAfterDelete = fileSystemExchangeManager.listDirectories(baseDirectory);
            assertThat(directoriesAfterDelete).doesNotContain(orphanDirectory);

            assertThat(s3Client.listObjectsV2(ListObjectsV2Request.builder()
                            .bucket(bucketName)
                            .prefix(orphanDirName)
                            .build())
                    .contents())
                    .isEmpty();
        }
    }

    private S3Client createS3Client()
    {
        return S3Client.builder()
                .endpointOverride(URI.create(storage.getExchangeManagerProperties().get("exchange.s3.endpoint")))
                .region(Region.of(FLOCI_REGION))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(FLOCI_ACCESS_KEY, FLOCI_SECRET_KEY)))
                .forcePathStyle(true)
                .build();
    }

    private void putEmptyObject(S3Client s3Client, String key)
    {
        s3Client.putObject(
                PutObjectRequest.builder().bucket(bucketName).key(key).build(),
                RequestBody.empty());
    }
}
