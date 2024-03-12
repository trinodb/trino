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
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.options.DataLakePathDeleteOptions;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.io.IOException;

import static com.azure.storage.common.Utility.urlEncode;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractTestAzureFileSystem
        extends AbstractTestTrinoFileSystem
{
    protected static String getRequiredEnvironmentVariable(String name)
    {
        return requireNonNull(System.getenv(name), "Environment variable not set: " + name);
    }

    protected enum AccountKind
    {
        HIERARCHICAL, FLAT
    }

    private String account;
    private AzureAuth azureAuth;
    private AccountKind accountKind;
    private String containerName;
    private Location rootLocation;
    private BlobContainerClient blobContainerClient;
    private TrinoFileSystem fileSystem;

    protected void initializeWithAccessKey(String account, String accountKey, AccountKind accountKind)
            throws IOException
    {
        initialize(account, new AzureAuthAccessKey(accountKey), accountKind);
    }

    protected void initializeWithOAuth(String account, String tenantId, String clientId, String clientSecret, AccountKind accountKind)
            throws IOException
    {
        String clientEndpoint = "https://login.microsoftonline.com/%s/oauth2/v2.0/token".formatted(tenantId);
        initialize(account, new AzureAuthOauth(clientEndpoint, tenantId, clientId, clientSecret), accountKind);
    }

    private void initialize(String account, AzureAuth azureAuth, AccountKind accountKind)
            throws IOException
    {
        this.account = requireNonNull(account, "account is null");
        this.azureAuth = requireNonNull(azureAuth, "azureAuth is null");
        this.accountKind = requireNonNull(accountKind, "accountKind is null");
        containerName = "test-%s-%s".formatted(accountKind.name().toLowerCase(ROOT), randomUUID());
        rootLocation = Location.of("abfs://%s@%s.dfs.core.windows.net/".formatted(containerName, account));

        BlobContainerClientBuilder builder = new BlobContainerClientBuilder()
                .endpoint("https://%s.blob.core.windows.net".formatted(account))
                .containerName(containerName);
        azureAuth.setAuth(account, builder);
        blobContainerClient = builder.buildClient();
        // this will fail if the container already exists, which is what we want
        blobContainerClient.create();
        boolean isHierarchicalNamespaceEnabled = isHierarchicalNamespaceEnabled();
        if (accountKind == AccountKind.HIERARCHICAL) {
            checkState(isHierarchicalNamespaceEnabled, "Expected hierarchical namespaces to be enabled for storage account %s and container %s with account kind %s".formatted(account, containerName, accountKind));
        }
        else {
            checkState(!isHierarchicalNamespaceEnabled, "Expected hierarchical namespaces to not be enabled for storage account %s and container %s with account kind %s".formatted(account, containerName, accountKind));
        }

        fileSystem = new AzureFileSystemFactory(
                OpenTelemetry.noop(),
                azureAuth,
                new AzureFileSystemConfig()).create(ConnectorIdentity.ofUser("test"));

        cleanupFiles();
    }

    private boolean isHierarchicalNamespaceEnabled()
            throws IOException
    {
        DataLakeFileSystemClient fileSystemClient = createDataLakeFileSystemClient();
        try {
            return fileSystemClient.getDirectoryClient("/").exists();
        }
        catch (RuntimeException e) {
            throw new IOException("Failed to check whether hierarchical namespaces is enabled for the storage account %s and container %s".formatted(account, containerName));
        }
    }

    @AfterAll
    void tearDown()
    {
        azureAuth = null;
        fileSystem = null;
        if (blobContainerClient != null) {
            blobContainerClient.deleteIfExists();
            blobContainerClient = null;
        }
    }

    @AfterEach
    void afterEach()
    {
        cleanupFiles();
    }

    private void cleanupFiles()
    {
        if (accountKind == AccountKind.HIERARCHICAL) {
            DataLakeFileSystemClient fileSystemClient = createDataLakeFileSystemClient();
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

    private DataLakeFileSystemClient createDataLakeFileSystemClient()
    {
        DataLakeServiceClientBuilder serviceClientBuilder = new DataLakeServiceClientBuilder()
                .endpoint("https://%s.dfs.core.windows.net".formatted(account));
        azureAuth.setAuth(account, serviceClientBuilder);
        DataLakeServiceClient serviceClient = serviceClientBuilder.buildClient();
        return serviceClient.getFileSystemClient(containerName);
    }

    @Override
    protected final boolean isHierarchical()
    {
        return accountKind == AccountKind.HIERARCHICAL;
    }

    @Override
    protected final TrinoFileSystem getFileSystem()
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

    @Override
    protected boolean supportsCreateExclusive()
    {
        return true;
    }

    @Test
    @Override
    public void testPaths()
            throws IOException
    {
        // Azure file paths are always hierarchical
        testPathHierarchical();
    }
}
