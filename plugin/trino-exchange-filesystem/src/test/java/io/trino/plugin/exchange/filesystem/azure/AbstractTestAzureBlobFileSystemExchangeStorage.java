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
package io.trino.plugin.exchange.filesystem.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.exchange.filesystem.ExchangeSourceFile;
import io.trino.plugin.exchange.filesystem.ExchangeStorageReader;
import io.trino.plugin.exchange.filesystem.ExchangeStorageWriter;
import io.trino.plugin.exchange.filesystem.FileStatus;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeConfig;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeStorage;
import io.trino.plugin.exchange.filesystem.MetricsBuilder;
import io.trino.spi.exchange.ExchangeId;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractTestAzureBlobFileSystemExchangeStorage
{
    protected enum AccountKind
    {
        HIERARCHICAL, FLAT
    }

    private BlobContainerClient blobContainerClient;
    private FileSystemExchangeStorage storage;
    private String containerName;
    private String account;

    protected void initializeWithAccessKey(String account, String accountKey, AccountKind accountKind)
            throws IOException
    {
        requireNonNull(account, "account is null");
        requireNonNull(accountKey, "accountKey is null");
        requireNonNull(accountKind, "accountKind is null");

        this.account = account;
        this.containerName = "test-exchange-%s-%s".formatted(accountKind.name().toLowerCase(ENGLISH), randomUUID());

        this.blobContainerClient = new BlobContainerClientBuilder()
                .endpoint("https://%s.blob.core.windows.net".formatted(account))
                .containerName(containerName)
                .credential(new StorageSharedKeyCredential(account, accountKey))
                .buildClient();
        blobContainerClient.create();
        boolean isHierarchicalNamespaceEnabled = isHierarchicalNamespaceEnabled();
        if (accountKind == AccountKind.HIERARCHICAL) {
            checkState(isHierarchicalNamespaceEnabled, "Expected hierarchical namespaces to be enabled for storage account %s and container %s with account kind %s".formatted(account, containerName, accountKind));
        }
        else {
            checkState(!isHierarchicalNamespaceEnabled, "Expected hierarchical namespaces to be disabled for storage account %s and container %s with account kind %s".formatted(account, containerName, accountKind));
        }

        String connectionString = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net".formatted(account, accountKey);
        this.storage = new AzureBlobFileSystemExchangeStorage(
                new ExchangeAzureConfig().setAzureStorageConnectionString(connectionString),
                new FileSystemExchangeConfig().setBaseDirectories(directoryUri("").toString()));
    }

    private boolean isHierarchicalNamespaceEnabled()
            throws IOException
    {
        try {
            BlockBlobClient blockBlobClient = blobContainerClient
                    .getBlobClient("/")
                    .getBlockBlobClient();
            return blockBlobClient.exists();
        }
        catch (RuntimeException e) {
            throw new IOException("Failed to check whether hierarchical namespaces is enabled for the storage account %s and container %s".formatted(account, containerName), e);
        }
    }

    @AfterAll
    void tearDown()
            throws IOException
    {
        if (storage != null) {
            storage.close();
            storage = null;
        }
        if (blobContainerClient != null) {
            blobContainerClient.deleteIfExists();
            blobContainerClient = null;
        }
    }

    @Test
    void testCreateEmptyFile()
    {
        URI fileUri = fileUri("create-empty-file-dir/empty-file");
        getFutureValue(storage.createEmptyFile(fileUri));

        List<FileStatus> files = getFutureValue(storage.listFilesRecursively(directoryUri("create-empty-file-dir/")));
        assertThat(files).hasSize(1);
        assertThat(files.getFirst().getFileSize()).isEqualTo(0);
    }

    @Test
    void testWriteAndListFiles()
    {
        URI fileUri = fileUri("test-dir/test-file");
        ExchangeStorageWriter writer = storage.createExchangeStorageWriter(fileUri);
        getFutureValue(writer.write(Slices.utf8Slice("test-data")));
        getFutureValue(writer.finish());

        List<FileStatus> files = getFutureValue(storage.listFilesRecursively(directoryUri("test-dir/")));
        assertThat(files).hasSize(1);
        assertThat(files.getFirst().getFileSize()).isEqualTo("test-data".length());
    }

    @Test
    void testWriteAndReadRoundTrip()
            throws IOException
    {
        String data = "hello-exchange-storage";
        URI fileUri = fileUri("roundtrip/data-file");

        // Write data using the serialization format expected by the reader:
        // each page is prefixed with a 4-byte length
        Slice dataSlice = Slices.utf8Slice(data);
        Slice serialized = Slices.allocate(Integer.BYTES + dataSlice.length());
        serialized.setInt(0, dataSlice.length());
        serialized.setBytes(Integer.BYTES, dataSlice);

        ExchangeStorageWriter writer = storage.createExchangeStorageWriter(fileUri);
        getFutureValue(writer.write(serialized));
        getFutureValue(writer.finish());

        List<FileStatus> files = getFutureValue(storage.listFilesRecursively(directoryUri("roundtrip/")));
        assertThat(files).hasSize(1);

        ExchangeSourceFile sourceFile = new ExchangeSourceFile(
                fileUri,
                files.getFirst().getFileSize(),
                ExchangeId.createRandomExchangeId(),
                0,
                0);

        try (ExchangeStorageReader reader = storage.createExchangeStorageReader(
                ImmutableList.of(sourceFile),
                serialized.length(),
                new MetricsBuilder())) {
            getFutureValue(reader.isBlocked());
            assertThat(reader.isFinished()).isFalse();
            Slice result = reader.read();
            assertThat(result).isNotNull();
            assertThat(result.toStringUtf8()).isEqualTo(data);
        }
    }

    @Test
    void testDeleteRecursivelySpecificDirectory()
    {
        URI file1 = fileUri("delete-dir/file1");
        URI file2 = fileUri("delete-dir/file2");
        URI file3 = fileUri("delete-dir/subdir/file3");

        getFutureValue(storage.createEmptyFile(file1));
        getFutureValue(storage.createEmptyFile(file2));
        getFutureValue(storage.createEmptyFile(file3));

        List<FileStatus> files = getFutureValue(storage.listFilesRecursively(directoryUri("delete-dir/")));
        assertThat(files).hasSize(3);

        getFutureValue(storage.deleteRecursively(ImmutableList.of(directoryUri("delete-dir/"))));

        files = getFutureValue(storage.listFilesRecursively(directoryUri("delete-dir/")));
        assertThat(files).isEmpty();
    }

    @Test
    void testDeleteRecursivelyContainerRoot()
    {
        URI file1 = fileUri("dir-a/file1");
        URI file2 = fileUri("dir-b/file2");
        URI file3 = fileUri("dir-b/subdir/file3");
        URI file4 = fileUri("dir-c/file4");

        getFutureValue(storage.createEmptyFile(file1));
        getFutureValue(storage.createEmptyFile(file2));
        getFutureValue(storage.createEmptyFile(file3));
        getFutureValue(storage.createEmptyFile(file4));

        List<FileStatus> files = getFutureValue(storage.listFilesRecursively(directoryUri("")));
        assertThat(files).hasSize(4);

        getFutureValue(storage.deleteRecursively(ImmutableList.of(directoryUri(""))));

        files = getFutureValue(storage.listFilesRecursively(directoryUri("")));
        assertThat(files).isEmpty();
    }

    @Test
    void testListFilesRecursivelyEmpty()
    {
        List<FileStatus> files = getFutureValue(storage.listFilesRecursively(directoryUri("nonexistent-dir/")));
        assertThat(files).isEmpty();
    }

    @Test
    void testCreateDirectories()
            throws IOException
    {
        // createDirectories is a no-op for Azure
        storage.createDirectories(directoryUri("some-dir/"));
    }

    @Test
    void testWriterAbort()
    {
        URI fileUri = fileUri("abort-dir/aborted-file");
        ExchangeStorageWriter writer = storage.createExchangeStorageWriter(fileUri);
        // Write more than the default 4MB block size to trigger multipart upload path
        // (single small write uses direct upload which commits immediately and cannot be aborted)
        getFutureValue(writer.write(Slices.allocate(4 * 1024 * 1024 + 1)));
        getFutureValue(writer.abort());

        List<FileStatus> files = getFutureValue(storage.listFilesRecursively(directoryUri("abort-dir/")));
        assertThat(files).isEmpty();
    }

    @Test
    void testCreateDirectoriesRejectsUriOutsideBaseDirectory()
    {
        URI outsideUri = URI.create("abfs://other-container@otheraccount.dfs.core.windows.net/some-dir/");
        assertUriOutsideBaseDirectoryRejected(() -> storage.createDirectories(outsideUri));
    }

    @Test
    void testCreateExchangeStorageWriterRejectsUriOutsideBaseDirectory()
    {
        URI outsideUri = URI.create("abfs://other-container@otheraccount.dfs.core.windows.net/some-file");
        assertUriOutsideBaseDirectoryRejected(() -> storage.createExchangeStorageWriter(outsideUri));
    }

    @Test
    void testCreateEmptyFileRejectsUriOutsideBaseDirectory()
    {
        URI outsideUri = URI.create("abfs://other-container@otheraccount.dfs.core.windows.net/some-file");
        assertUriOutsideBaseDirectoryRejected(() -> storage.createEmptyFile(outsideUri));
    }

    @Test
    void testDeleteRecursivelyRejectsUriOutsideBaseDirectory()
    {
        URI outsideUri = URI.create("abfs://other-container@otheraccount.dfs.core.windows.net/some-dir/");
        assertUriOutsideBaseDirectoryRejected(() -> storage.deleteRecursively(ImmutableList.of(outsideUri)));
    }

    @Test
    void testListFilesRecursivelyRejectsUriOutsideBaseDirectory()
    {
        URI outsideUri = URI.create("abfs://other-container@otheraccount.dfs.core.windows.net/some-dir/");
        assertUriOutsideBaseDirectoryRejected(() -> storage.listFilesRecursively(outsideUri));
    }

    @Test
    void testCreateExchangeStorageReaderRejectsUriOutsideBaseDirectory()
    {
        URI outsideUri = URI.create("abfs://other-container@otheraccount.dfs.core.windows.net/some-file");
        ExchangeSourceFile sourceFile = new ExchangeSourceFile(outsideUri, 100, ExchangeId.createRandomExchangeId(), 0, 0);
        assertUriOutsideBaseDirectoryRejected(() -> storage.createExchangeStorageReader(ImmutableList.of(sourceFile), 1024, new MetricsBuilder()));
    }

    private static void assertUriOutsideBaseDirectoryRejected(ThrowingCallable callable)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not within any base directory");
    }

    private URI fileUri(String path)
    {
        return URI.create("abfs://%s@%s.dfs.core.windows.net/%s".formatted(containerName, account, path));
    }

    private URI directoryUri(String path)
    {
        return URI.create("abfs://%s@%s.dfs.core.windows.net/%s".formatted(containerName, account, path));
    }
}
