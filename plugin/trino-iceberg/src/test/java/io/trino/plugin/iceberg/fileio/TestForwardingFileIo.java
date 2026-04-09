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
package io.trino.plugin.iceberg.fileio;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestForwardingFileIo
{
    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(FileIO.class, ForwardingFileIo.class);
        assertAllMethodsOverridden(SupportsBulkOperations.class, ForwardingFileIo.class);
        assertAllMethodsOverridden(SupportsStorageCredentials.class, ForwardingFileIo.class);
    }

    @Test
    public void testUseFileSizeFromMetadata()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory("test_forwarding_fileio");
        Path filePath = tempDir.resolve("data.txt");
        Files.writeString(filePath, "test-data");

        LocalFileSystemFactory factory = new LocalFileSystemFactory(tempDir);
        TrinoFileSystem fileSystem = factory.create(SESSION);

        long actualLength = Files.size(filePath);

        try (ForwardingFileIo ignoringFileIo = new ForwardingFileIo(fileSystem, false)) {
            assertThat(ignoringFileIo.newInputFile("file:///data.txt", 1).getLength())
                    .isEqualTo(actualLength);
        }

        try (ForwardingFileIo usingFileIo = new ForwardingFileIo(fileSystem, true)) {
            assertThat(usingFileIo.newInputFile("file:///data.txt", 1).getLength())
                    .isEqualTo(1);
        }
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Test
    public void testStorageCredentialsRemainSeparateFromProperties()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory("test_forwarding_fileio_storage_creds");
        List<Map<String, String>> seenProperties = new ArrayList<>();
        LocalFileSystemFactory localFactory = new LocalFileSystemFactory(tempDir);
        IcebergFileSystemFactory capturingFactory = (identity, fileIoProperties) -> {
            seenProperties.add(ImmutableMap.copyOf(fileIoProperties));
            return localFactory.create(SESSION);
        };

        try (ForwardingFileIo fileIo = new ForwardingFileIo(
                localFactory.create(SESSION),
                ImmutableMap.of("base.key", "base-value"),
                true,
                newDirectExecutorService(),
                capturingFactory,
                ConnectorIdentity.ofUser("test-user"))) {
            fileIo.setCredentials(List.of(
                    StorageCredential.create("s3://bucket-a/", ImmutableMap.of(
                            S3FileIOProperties.ACCESS_KEY_ID, "access-a",
                            S3FileIOProperties.SECRET_ACCESS_KEY, "secret-a",
                            S3FileIOProperties.SESSION_TOKEN, "token-a")),
                    StorageCredential.create("s3://bucket-a/warehouse/", ImmutableMap.of(
                            S3FileIOProperties.ACCESS_KEY_ID, "access-b",
                            S3FileIOProperties.SECRET_ACCESS_KEY, "secret-b",
                            S3FileIOProperties.SESSION_TOKEN, "token-b"))));

            assertThat(fileIo.properties()).containsExactlyEntriesOf(ImmutableMap.of("base.key", "base-value"));
            assertThat(fileIo.credentials()).hasSize(2);
            assertThat(seenProperties).hasSize(2);
            assertThat(seenProperties).contains(ImmutableMap.of(
                    "base.key", "base-value",
                    S3FileIOProperties.ACCESS_KEY_ID, "access-a",
                    S3FileIOProperties.SECRET_ACCESS_KEY, "secret-a",
                    S3FileIOProperties.SESSION_TOKEN, "token-a"));
            assertThat(seenProperties).contains(ImmutableMap.of(
                    "base.key", "base-value",
                    S3FileIOProperties.ACCESS_KEY_ID, "access-b",
                    S3FileIOProperties.SECRET_ACCESS_KEY, "secret-b",
                    S3FileIOProperties.SESSION_TOKEN, "token-b"));
        }
        finally {
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }

    @Test
    public void testSetCredentialsAfterInitializeRebuildsPrefixFileSystems()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory("test_forwarding_fileio_refresh_creds");
        List<Map<String, String>> seenProperties = new ArrayList<>();
        LocalFileSystemFactory localFactory = new LocalFileSystemFactory(tempDir);
        IcebergFileSystemFactory capturingFactory = (identity, fileIoProperties) -> {
            seenProperties.add(ImmutableMap.copyOf(fileIoProperties));
            return localFactory.create(SESSION);
        };

        try (ForwardingFileIo fileIo = new ForwardingFileIo(
                localFactory.create(SESSION),
                ImmutableMap.of("base.key", "base-value"),
                true,
                newDirectExecutorService(),
                capturingFactory,
                ConnectorIdentity.ofUser("test-user"))) {
            fileIo.setCredentials(List.of(
                    StorageCredential.create("s3://bucket-a/", ImmutableMap.of(
                            S3FileIOProperties.ACCESS_KEY_ID, "access-a",
                            S3FileIOProperties.SECRET_ACCESS_KEY, "secret-a",
                            S3FileIOProperties.SESSION_TOKEN, "token-a"))));
            assertThat(seenProperties).hasSize(1);
            assertThat(seenProperties.get(0)).containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "access-a");

            fileIo.setCredentials(List.of(
                    StorageCredential.create("s3://bucket-b/", ImmutableMap.of(
                            S3FileIOProperties.ACCESS_KEY_ID, "access-b",
                            S3FileIOProperties.SECRET_ACCESS_KEY, "secret-b",
                            S3FileIOProperties.SESSION_TOKEN, "token-b"))));

            assertThat(seenProperties).hasSize(2);
            assertThat(seenProperties.get(1)).containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "access-b");
        }
        finally {
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }

    @Test
    public void testLongestPrefixRoutingAndFallback()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory("test_forwarding_fileio_prefix_routing");
        Path baseRoot = tempDir.resolve("base");
        Path prefixARoot = tempDir.resolve("prefix-a");
        Path prefixBRoot = tempDir.resolve("prefix-b");
        Files.createDirectories(baseRoot);
        Files.createDirectories(prefixARoot);
        Files.createDirectories(prefixBRoot);

        Files.createDirectories(baseRoot.resolve("other"));
        Files.writeString(baseRoot.resolve("other/fallback.txt"), "ccc");
        Files.createDirectories(prefixARoot.resolve("bucket"));
        Files.writeString(prefixARoot.resolve("bucket/data.txt"), "a");
        Files.createDirectories(prefixBRoot.resolve("bucket/warehouse"));
        Files.writeString(prefixBRoot.resolve("bucket/warehouse/data.txt"), "bb");

        LocalFileSystemFactory baseFactory = new LocalFileSystemFactory(baseRoot);
        LocalFileSystemFactory prefixAFactory = new LocalFileSystemFactory(prefixARoot);
        LocalFileSystemFactory prefixBFactory = new LocalFileSystemFactory(prefixBRoot);
        IcebergFileSystemFactory routingFactory = (identity, fileIoProperties) -> {
            String accessKey = fileIoProperties.get(S3FileIOProperties.ACCESS_KEY_ID);
            if ("access-a".equals(accessKey)) {
                return prefixAFactory.create(SESSION);
            }
            if ("access-b".equals(accessKey)) {
                return prefixBFactory.create(SESSION);
            }
            throw new IllegalArgumentException("Unexpected access key: " + accessKey);
        };

        try (ForwardingFileIo fileIo = new ForwardingFileIo(
                baseFactory.create(SESSION),
                ImmutableMap.of("base.key", "base-value"),
                true,
                newDirectExecutorService(),
                routingFactory,
                ConnectorIdentity.ofUser("test-user"))) {
            fileIo.setCredentials(List.of(
                    StorageCredential.create("file:///bucket/", ImmutableMap.of(
                            S3FileIOProperties.ACCESS_KEY_ID, "access-a",
                            S3FileIOProperties.SECRET_ACCESS_KEY, "secret-a",
                            S3FileIOProperties.SESSION_TOKEN, "token-a")),
                    StorageCredential.create("file:///bucket/warehouse/", ImmutableMap.of(
                            S3FileIOProperties.ACCESS_KEY_ID, "access-b",
                            S3FileIOProperties.SECRET_ACCESS_KEY, "secret-b",
                            S3FileIOProperties.SESSION_TOKEN, "token-b"))));

            // Longest matching prefix should select the more specific credential filesystem.
            assertThat(fileIo.newInputFile("file:///bucket/warehouse/data.txt").getLength()).isEqualTo(2);
            assertThat(fileIo.newInputFile("file:///bucket/data.txt").getLength()).isEqualTo(1);
            assertThat(fileIo.newInputFile("file:///other/fallback.txt").getLength()).isEqualTo(3);
        }
        finally {
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }
}
