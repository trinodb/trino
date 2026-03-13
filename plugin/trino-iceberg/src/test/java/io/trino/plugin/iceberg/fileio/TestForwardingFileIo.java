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
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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
    public void testStorageCredentialsMergedIntoProperties()
    {
        try (ForwardingFileIo fileIo = new ForwardingFileIo(
                new LocalFileSystemFactory(Path.of(System.getProperty("java.io.tmpdir"))).create(SESSION),
                ImmutableMap.of("base.key", "base.value"),
                true,
                newDirectExecutorService())) {
            // initially no credentials
            assertThat(fileIo.credentials()).isEmpty();
            assertThat(fileIo.properties()).containsExactlyEntriesOf(ImmutableMap.of("base.key", "base.value"));

            // set storage credentials — config entries should appear in properties
            List<StorageCredential> credentials = List.of(
                    StorageCredential.create("s3://bucket/", ImmutableMap.of(
                            "s3.access-key-id", "test-access-key",
                            "s3.secret-access-key", "test-secret-key",
                            "s3.session-token", "test-session-token")),
                    StorageCredential.create("gs://bucket/", ImmutableMap.of(
                            "gcs.oauth2.token", "test-gcs-token")));
            fileIo.setCredentials(credentials);

            assertThat(fileIo.credentials()).isEqualTo(credentials);
            assertThat(fileIo.properties())
                    .containsEntry("base.key", "base.value")
                    .containsEntry("s3.access-key-id", "test-access-key")
                    .containsEntry("s3.secret-access-key", "test-secret-key")
                    .containsEntry("s3.session-token", "test-session-token")
                    .containsEntry("gcs.oauth2.token", "test-gcs-token");
        }
    }

    @Test
    public void testStorageCredentialsTakePrecedenceOverBaseProperties()
    {
        try (ForwardingFileIo fileIo = new ForwardingFileIo(
                new LocalFileSystemFactory(Path.of(System.getProperty("java.io.tmpdir"))).create(SESSION),
                ImmutableMap.of("s3.access-key-id", "static-key", "unrelated.key", "keep-me"),
                true,
                newDirectExecutorService())) {
            fileIo.setCredentials(List.of(
                    StorageCredential.create("s3://bucket/", ImmutableMap.of(
                            "s3.access-key-id", "vended-key"))));

            assertThat(fileIo.properties())
                    .containsEntry("s3.access-key-id", "vended-key")
                    .containsEntry("unrelated.key", "keep-me");
        }
    }

    @Test
    public void testEmptyStorageCredentialsRevertToBaseProperties()
    {
        try (ForwardingFileIo fileIo = new ForwardingFileIo(
                new LocalFileSystemFactory(Path.of(System.getProperty("java.io.tmpdir"))).create(SESSION),
                ImmutableMap.of("base.key", "base.value"),
                true,
                newDirectExecutorService())) {
            fileIo.setCredentials(List.of(
                    StorageCredential.create("s3://bucket/", ImmutableMap.of(
                            "s3.access-key-id", "vended-key"))));

            assertThat(fileIo.properties()).containsKey("s3.access-key-id");

            // reset to empty
            fileIo.setCredentials(List.of());

            assertThat(fileIo.credentials()).isEmpty();
            assertThat(fileIo.properties()).containsExactlyEntriesOf(ImmutableMap.of("base.key", "base.value"));
        }
    }

    @Test
    public void testInitializeWithRegisteredContext()
    {
        String catalogName = "test-init-" + Thread.currentThread().threadId();
        Path tempDir = Path.of(System.getProperty("java.io.tmpdir"));
        LocalFileSystemFactory localFactory = new LocalFileSystemFactory(tempDir);
        ForwardingFileIo.registerContext(catalogName,
                (identity, props) -> localFactory.create(SESSION),
                newDirectExecutorService());
        try (ForwardingFileIo.IdentityScope ignored = ForwardingFileIo.withIdentity(ConnectorIdentity.ofUser("test-user"));
                ForwardingFileIo fileIo = new ForwardingFileIo()) {
            fileIo.initialize(ImmutableMap.of(
                    ForwardingFileIo.TRINO_CATALOG_NAME, catalogName,
                    "base.key", "base.value"));

            assertThat(fileIo.properties())
                    .containsEntry("base.key", "base.value");
        }
        finally {
            ForwardingFileIo.deregisterContext(catalogName);
        }
    }

    @Test
    public void testSetCredentialsThenInitialize()
    {
        String catalogName = "test-creds-" + Thread.currentThread().threadId();
        Path tempDir = Path.of(System.getProperty("java.io.tmpdir"));
        LocalFileSystemFactory localFactory = new LocalFileSystemFactory(tempDir);
        ForwardingFileIo.registerContext(catalogName,
                (identity, props) -> localFactory.create(SESSION),
                newDirectExecutorService());
        try (ForwardingFileIo.IdentityScope ignored = ForwardingFileIo.withIdentity(ConnectorIdentity.ofUser("test-user"));
                ForwardingFileIo fileIo = new ForwardingFileIo()) {
            // CatalogUtil.loadFileIO calls setCredentials BEFORE initialize
            List<StorageCredential> credentials = List.of(
                    StorageCredential.create("s3://bucket/", ImmutableMap.of(
                            "s3.access-key-id", "vended-key",
                            "s3.secret-access-key", "vended-secret")));
            fileIo.setCredentials(credentials);
            fileIo.initialize(ImmutableMap.of(
                    ForwardingFileIo.TRINO_CATALOG_NAME, catalogName,
                    "s3.access-key-id", "static-key"));

            // storage credentials take precedence over base properties
            assertThat(fileIo.properties())
                    .containsEntry("s3.access-key-id", "vended-key")
                    .containsEntry("s3.secret-access-key", "vended-secret");
            assertThat(fileIo.credentials()).isEqualTo(credentials);
        }
        finally {
            ForwardingFileIo.deregisterContext(catalogName);
        }
    }
}
