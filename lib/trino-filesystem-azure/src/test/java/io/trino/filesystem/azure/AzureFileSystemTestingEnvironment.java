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
package io.trino.filesystem.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.StorageAccountInfo;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.options.DataLakePathDeleteOptions;
import io.trino.filesystem.AbstractTrinoFileSystemTestingEnvironment;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;

import static com.azure.storage.common.Utility.urlEncode;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.ROOT;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

public class AzureFileSystemTestingEnvironment
        extends AbstractTrinoFileSystemTestingEnvironment
{
    public enum AccountKind
    {
        HIERARCHICAL, FLAT, BLOB
    }

    private final String account;
    private final StorageSharedKeyCredential credential;
    private final AccountKind accountKind;
    private final String containerName;
    private final Location rootLocation;
    private final BlobContainerClient blobContainerClient;
    private final TrinoFileSystem fileSystem;

    public AzureFileSystemTestingEnvironment(String account, String accountKey, AccountKind expectedAccountKind)
            throws IOException
    {
        this.account = account;
        credential = new StorageSharedKeyCredential(account, accountKey);

        String blobEndpoint = "https://%s.blob.core.windows.net".formatted(account);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(blobEndpoint)
                .credential(credential)
                .buildClient();
        accountKind = getAccountKind(blobServiceClient);
        checkState(accountKind == expectedAccountKind, "Expected %s account, but found %s".formatted(expectedAccountKind, accountKind));

        containerName = "test-%s-%s".formatted(accountKind.name().toLowerCase(ROOT), randomUUID());
        rootLocation = Location.of("abfs://%s@%s.dfs.core.windows.net/".formatted(containerName, account));

        blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
        // this will fail if the container already exists, which is what we want
        blobContainerClient.create();

        fileSystem = new AzureFileSystemFactory(new AzureAuthAccessKey(accountKey), new AzureFileSystemConfig()).create(ConnectorIdentity.ofUser("test"));

        cleanupFiles();
    }

    private static AccountKind getAccountKind(BlobServiceClient blobServiceClient)
            throws IOException
    {
        StorageAccountInfo accountInfo = blobServiceClient.getAccountInfo();
        if (accountInfo.getAccountKind() == com.azure.storage.blob.models.AccountKind.STORAGE_V2) {
            if (accountInfo.isHierarchicalNamespaceEnabled()) {
                return AccountKind.HIERARCHICAL;
            }
            return AccountKind.FLAT;
        }
        if (accountInfo.getAccountKind() == com.azure.storage.blob.models.AccountKind.BLOB_STORAGE) {
            return AccountKind.BLOB;
        }
        throw new IOException("Unsupported account kind '%s'".formatted(accountInfo.getAccountKind()));
    }

    public void cleanupFiles()
    {
        if (accountKind == AccountKind.HIERARCHICAL) {
            DataLakeFileSystemClient fileSystemClient = new DataLakeFileSystemClientBuilder()
                    .endpoint("https://%s.dfs.core.windows.net".formatted(account))
                    .fileSystemName(containerName)
                    .credential(credential)
                    .buildClient();

            DataLakePathDeleteOptions deleteRecursiveOptions = new DataLakePathDeleteOptions().setIsRecursive(true);
            for (PathItem pathItem : fileSystemClient.listPaths()) {
                if (pathItem.isDirectory()) {
                    fileSystemClient.deleteDirectoryIfExistsWithResponse(pathItem.getName(), deleteRecursiveOptions, null, null);
                }
                else {
                    fileSystemClient.deleteFileIfExists(pathItem.getName());
                }
            }
        }
        else {
            blobContainerClient.listBlobs().forEach(item -> blobContainerClient.getBlobClient(urlEncode(item.getName())).deleteIfExists());
        }
    }

    public void close()
    {
        if (blobContainerClient != null) {
            blobContainerClient.deleteIfExists();
        }
    }

    @Override
    protected final boolean isHierarchical()
    {
        return accountKind == AccountKind.HIERARCHICAL;
    }

    @Override
    public final TrinoFileSystem getFileSystem()
    {
        return fileSystem;
    }

    @Override
    protected final Location getRootLocation()
    {
        return rootLocation;
    }

    @Override
    protected final void verifyFileSystemIsEmpty()
    {
        assertThat(blobContainerClient.listBlobs()).isEmpty();
    }
}
