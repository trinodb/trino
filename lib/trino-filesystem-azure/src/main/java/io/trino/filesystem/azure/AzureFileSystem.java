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

import com.azure.core.http.HttpClient;
import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.AccountKind;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.StorageAccountInfo;
import com.azure.storage.common.Utility;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.options.DataLakePathDeleteOptions;
import io.airlift.units.DataSize;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static com.azure.storage.common.implementation.Constants.HeaderConstants.ETAG_WILDCARD;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.filesystem.azure.AzureUtils.handleAzureException;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

public class AzureFileSystem
        implements TrinoFileSystem
{
    private final HttpClient httpClient;
    private final AzureAuth azureAuth;
    private final int readBlockSizeBytes;
    private final long writeBlockSizeBytes;
    private final int maxWriteConcurrency;
    private final long maxSingleUploadSizeBytes;

    public AzureFileSystem(HttpClient httpClient, AzureAuth azureAuth, DataSize readBlockSize, DataSize writeBlockSize, int maxWriteConcurrency, DataSize maxSingleUploadSize)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.azureAuth = requireNonNull(azureAuth, "azureAuth is null");
        this.readBlockSizeBytes = toIntExact(readBlockSize.toBytes());
        this.writeBlockSizeBytes = writeBlockSize.toBytes();
        checkArgument(maxWriteConcurrency >= 0, "maxWriteConcurrency is negative");
        this.maxWriteConcurrency = maxWriteConcurrency;
        this.maxSingleUploadSizeBytes = maxSingleUploadSize.toBytes();
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        AzureLocation azureLocation = new AzureLocation(location);
        BlobClient client = createBlobClient(azureLocation);
        return new AzureInputFile(azureLocation, OptionalLong.empty(), client, readBlockSizeBytes);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        AzureLocation azureLocation = new AzureLocation(location);
        BlobClient client = createBlobClient(azureLocation);
        return new AzureInputFile(azureLocation, OptionalLong.of(length), client, readBlockSizeBytes);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        AzureLocation azureLocation = new AzureLocation(location);
        BlobClient client = createBlobClient(azureLocation);
        return new AzureOutputFile(azureLocation, client, writeBlockSizeBytes, maxWriteConcurrency, maxSingleUploadSizeBytes);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        location.verifyValidFileLocation();
        AzureLocation azureLocation = new AzureLocation(location);
        BlobClient client = createBlobClient(azureLocation);
        try {
            client.delete();
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "deleting file", azureLocation);
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        AzureLocation azureLocation = new AzureLocation(location);
        try {
            if (isHierarchicalNamespaceEnabled(azureLocation)) {
                deleteGen2Directory(azureLocation);
            }
            else {
                deleteBlobDirectory(azureLocation);
            }
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "deleting directory", azureLocation);
        }
    }

    private void deleteGen2Directory(AzureLocation location)
            throws IOException
    {
        DataLakeFileSystemClient fileSystemClient = createFileSystemClient(location);
        DataLakePathDeleteOptions deleteRecursiveOptions = new DataLakePathDeleteOptions().setIsRecursive(true);
        if (location.path().isEmpty()) {
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
            DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient(location.path());
            if (directoryClient.exists()) {
                if (!directoryClient.getProperties().isDirectory()) {
                    throw new IOException("Location is not a directory: " + location);
                }
                directoryClient.deleteIfExistsWithResponse(deleteRecursiveOptions, null, null);
            }
        }
    }

    private void deleteBlobDirectory(AzureLocation location)
    {
        String path = location.path();
        if (!path.isEmpty() && !path.endsWith("/")) {
            path += "/";
        }
        BlobContainerClient blobContainerClient = createBlobContainerClient(location);
        PagedIterable<BlobItem> blobItems = blobContainerClient.listBlobs(new ListBlobsOptions().setPrefix(path), null);
        for (BlobItem item : blobItems) {
            String blobUrl = Utility.urlEncode(item.getName());
            blobContainerClient.getBlobClient(blobUrl).deleteIfExists();
        }
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        source.verifyValidFileLocation();
        target.verifyValidFileLocation();

        AzureLocation sourceLocation = new AzureLocation(source);
        AzureLocation targetLocation = new AzureLocation(target);
        if (!sourceLocation.account().equals(targetLocation.account())) {
            throw new IOException("Cannot rename across storage accounts");
        }
        if (!Objects.equals(sourceLocation.container(), targetLocation.container())) {
            throw new IOException("Cannot rename across storage account containers");
        }

        // DFS rename file works with all storage types
        renameGen2File(sourceLocation, targetLocation);
    }

    private void renameGen2File(AzureLocation source, AzureLocation target)
            throws IOException
    {
        try {
            DataLakeFileSystemClient fileSystemClient = createFileSystemClient(source);
            DataLakeFileClient dataLakeFileClient = fileSystemClient.getFileClient(source.path());
            if (dataLakeFileClient.getProperties().isDirectory()) {
                throw new IOException("Rename file from %s to %s, source is a directory".formatted(source, target));
            }

            fileSystemClient.createDirectoryIfNotExists(target.location().parentDirectory().path());
            dataLakeFileClient.renameWithResponse(
                    null,
                    target.path(),
                    null,
                    new DataLakeRequestConditions().setIfNoneMatch(ETAG_WILDCARD),
                    null,
                    null);
        }
        catch (RuntimeException e) {
            throw new IOException("Rename file from %s to %s failed".formatted(source, target), e);
        }
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        AzureLocation azureLocation = new AzureLocation(location);
        try {
            // blob api returns directories as blobs, so it can not be used when Gen2 is enabled
            if (isHierarchicalNamespaceEnabled(azureLocation)) {
                return listGen2Files(azureLocation);
            }
            else {
                return listBlobFiles(azureLocation);
            }
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "listing files", azureLocation);
        }
    }

    private FileIterator listGen2Files(AzureLocation location)
            throws IOException
    {
        DataLakeFileSystemClient fileSystemClient = createFileSystemClient(location);
        PagedIterable<PathItem> pathItems;
        if (location.path().isEmpty()) {
            pathItems = fileSystemClient.listPaths(new ListPathsOptions().setRecursive(true), null);
        }
        else {
            DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient(location.path());
            if (!directoryClient.exists()) {
                return FileIterator.empty();
            }
            if (!directoryClient.getProperties().isDirectory()) {
                throw new IOException("Location is not a directory: " + location);
            }
            pathItems = directoryClient.listPaths(true, false, null, null);
        }
        return new AzureDataLakeFileIterator(
                location,
                pathItems.stream()
                        .filter(not(PathItem::isDirectory))
                        .iterator());
    }

    private AzureBlobFileIterator listBlobFiles(AzureLocation location)
    {
        String path = location.path();
        if (!path.isEmpty() && !path.endsWith("/")) {
            path += "/";
        }
        PagedIterable<BlobItem> blobItems = createBlobContainerClient(location).listBlobs(new ListBlobsOptions().setPrefix(path), null);
        return new AzureBlobFileIterator(location, blobItems.iterator());
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        AzureLocation azureLocation = new AzureLocation(location);
        if (location.path().isEmpty()) {
            return Optional.of(true);
        }
        if (!isHierarchicalNamespaceEnabled(azureLocation)) {
            if (listFiles(location).hasNext()) {
                return Optional.of(true);
            }
            return Optional.empty();
        }

        try {
            DataLakeFileSystemClient fileSystemClient = createFileSystemClient(azureLocation);
            DataLakeFileClient fileClient = fileSystemClient.getFileClient(azureLocation.path());
            return Optional.of(fileClient.getProperties().isDirectory());
        }
        catch (DataLakeStorageException e) {
            if (e.getStatusCode() == 404) {
                return Optional.of(false);
            }
            throw handleAzureException(e, "checking directory existence", azureLocation);
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "checking directory existence", azureLocation);
        }
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        AzureLocation azureLocation = new AzureLocation(location);
        if (!isHierarchicalNamespaceEnabled(azureLocation)) {
            return;
        }
        try {
            DataLakeFileSystemClient fileSystemClient = createFileSystemClient(azureLocation);
            DataLakeDirectoryClient directoryClient = fileSystemClient.createDirectoryIfNotExists(azureLocation.path());
            if (!directoryClient.getProperties().isDirectory()) {
                throw new IOException("Location is not a directory: " + azureLocation);
            }
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "creating directory", azureLocation);
        }
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        AzureLocation sourceLocation = new AzureLocation(source);
        AzureLocation targetLocation = new AzureLocation(target);
        if (!sourceLocation.account().equals(targetLocation.account())) {
            throw new IOException("Cannot rename across storage accounts");
        }
        if (!Objects.equals(sourceLocation.container(), targetLocation.container())) {
            throw new IOException("Cannot rename across storage account containers");
        }
        if (!isHierarchicalNamespaceEnabled(sourceLocation)) {
            throw new IOException("Azure non-hierarchical does not support directory renames");
        }
        if (sourceLocation.path().isEmpty() || targetLocation.path().isEmpty()) {
            throw new IOException("Cannot rename %s to %s".formatted(source, target));
        }

        try {
            DataLakeFileSystemClient fileSystemClient = createFileSystemClient(sourceLocation);
            DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient(sourceLocation.path());
            if (!directoryClient.exists()) {
                throw new IOException("Source directory does not exist: " + source);
            }
            if (!directoryClient.getProperties().isDirectory()) {
                throw new IOException("Source is not a directory: " + source);
            }
            directoryClient.rename(null, targetLocation.path());
        }
        catch (RuntimeException e) {
            throw new IOException("Rename directory from %s to %s failed".formatted(source, target), e);
        }
    }

    private boolean isHierarchicalNamespaceEnabled(AzureLocation location)
            throws IOException
    {
        StorageAccountInfo accountInfo = createBlobContainerClient(location).getServiceClient().getAccountInfo();

        AccountKind accountKind = accountInfo.getAccountKind();
        if (accountKind == AccountKind.BLOB_STORAGE) {
            return false;
        }
        if (accountKind != AccountKind.STORAGE_V2) {
            throw new IOException("Unsupported account kind '%s': %s".formatted(accountKind, location));
        }
        return accountInfo.isHierarchicalNamespaceEnabled();
    }

    private BlobClient createBlobClient(AzureLocation location)
    {
        // encode the path using the Azure url encoder utility
        String path = Utility.urlEncode(location.path());
        return createBlobContainerClient(location).getBlobClient(path);
    }

    private BlobContainerClient createBlobContainerClient(AzureLocation location)
    {
        requireNonNull(location, "location is null");

        BlobContainerClientBuilder builder = new BlobContainerClientBuilder()
                .httpClient(httpClient)
                .endpoint(String.format("https://%s.blob.core.windows.net", location.account()));
        azureAuth.setAuth(location.account(), builder);
        location.container().ifPresent(builder::containerName);
        return builder.buildClient();
    }

    private DataLakeFileSystemClient createFileSystemClient(AzureLocation location)
    {
        requireNonNull(location, "location is null");

        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder()
                .httpClient(httpClient)
                .endpoint(String.format("https://%s.dfs.core.windows.net", location.account()));
        azureAuth.setAuth(location.account(), builder);
        DataLakeServiceClient client = builder.buildClient();
        DataLakeFileSystemClient fileSystemClient = client.getFileSystemClient(location.container().orElseThrow());
        if (!fileSystemClient.exists()) {
            throw new IllegalArgumentException();
        }
        return fileSystemClient;
    }
}
