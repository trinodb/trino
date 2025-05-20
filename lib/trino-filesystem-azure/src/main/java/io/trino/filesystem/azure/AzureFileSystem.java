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
import com.azure.core.util.ClientOptions;
import com.azure.core.util.TracingOptions;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.sas.SasProtocol;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemException;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.UriLocation;
import io.trino.filesystem.encryption.EncryptionKey;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.azure.storage.common.implementation.Constants.HeaderConstants.ETAG_WILDCARD;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.filesystem.azure.AzureUtils.blobCustomerProvidedKey;
import static io.trino.filesystem.azure.AzureUtils.encodedKey;
import static io.trino.filesystem.azure.AzureUtils.handleAzureException;
import static io.trino.filesystem.azure.AzureUtils.isFileNotFoundException;
import static io.trino.filesystem.azure.AzureUtils.keySha256Checksum;
import static io.trino.filesystem.azure.AzureUtils.lakeCustomerProvidedKey;
import static java.lang.Math.toIntExact;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.function.Predicate.not;

public class AzureFileSystem
        implements TrinoFileSystem
{
    private final HttpClient httpClient;
    private final TracingOptions tracingOptions;
    private final AzureAuth azureAuth;
    private final String endpoint;
    private final int readBlockSizeBytes;
    private final long writeBlockSizeBytes;
    private final int maxWriteConcurrency;
    private final long maxSingleUploadSizeBytes;

    public AzureFileSystem(
            HttpClient httpClient,
            TracingOptions tracingOptions,
            AzureAuth azureAuth,
            String endpoint,
            DataSize readBlockSize,
            DataSize writeBlockSize,
            int maxWriteConcurrency,
            DataSize maxSingleUploadSize)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.tracingOptions = requireNonNull(tracingOptions, "tracingOptions is null");
        this.azureAuth = requireNonNull(azureAuth, "azureAuth is null");
        this.endpoint = requireNonNull(endpoint, "endpoint is null");
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
        BlobClient client = createBlobClient(azureLocation, Optional.empty());
        return new AzureInputFile(azureLocation, OptionalLong.empty(), Optional.empty(), client, readBlockSizeBytes);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, EncryptionKey key)
    {
        AzureLocation azureLocation = new AzureLocation(location);
        BlobClient client = createBlobClient(azureLocation, Optional.of(key));
        return new AzureInputFile(azureLocation, OptionalLong.empty(), Optional.empty(), client, readBlockSizeBytes);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        AzureLocation azureLocation = new AzureLocation(location);
        BlobClient client = createBlobClient(azureLocation, Optional.empty());
        return new AzureInputFile(azureLocation, OptionalLong.of(length), Optional.empty(), client, readBlockSizeBytes);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, EncryptionKey key)
    {
        AzureLocation azureLocation = new AzureLocation(location);
        BlobClient client = createBlobClient(azureLocation, Optional.of(key));
        return new AzureInputFile(azureLocation, OptionalLong.of(length), Optional.empty(), client, readBlockSizeBytes);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        AzureLocation azureLocation = new AzureLocation(location);
        BlobClient client = createBlobClient(azureLocation, Optional.empty());
        return new AzureInputFile(azureLocation, OptionalLong.of(length), Optional.of(lastModified), client, readBlockSizeBytes);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, Instant lastModified, EncryptionKey key)
    {
        AzureLocation azureLocation = new AzureLocation(location);
        BlobClient client = createBlobClient(azureLocation, Optional.of(key));
        return new AzureInputFile(azureLocation, OptionalLong.of(length), Optional.of(lastModified), client, readBlockSizeBytes);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        AzureLocation azureLocation = new AzureLocation(location);
        BlobClient client = createBlobClient(azureLocation, Optional.empty());
        return new AzureOutputFile(azureLocation, client, writeBlockSizeBytes, maxWriteConcurrency, maxSingleUploadSizeBytes);
    }

    @Override
    public TrinoOutputFile newEncryptedOutputFile(Location location, EncryptionKey key)
    {
        AzureLocation azureLocation = new AzureLocation(location);
        BlobClient client = createBlobClient(azureLocation, Optional.of(key));
        return new AzureOutputFile(azureLocation, client, writeBlockSizeBytes, maxWriteConcurrency, maxSingleUploadSizeBytes);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        location.verifyValidFileLocation();
        AzureLocation azureLocation = new AzureLocation(location);
        BlobClient client = createBlobClient(azureLocation, Optional.empty());
        try {
            client.delete();
        }
        catch (RuntimeException e) {
            if (isFileNotFoundException(e)) {
                return;
            }
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
        DataLakeFileSystemClient fileSystemClient = createFileSystemClient(location, Optional.empty());
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
            DataLakeDirectoryClient directoryClient = createDirectoryClient(fileSystemClient, location.path());
            if (directoryClient.exists()) {
                if (!directoryClient.getProperties().isDirectory()) {
                    throw new TrinoFileSystemException("Location is not a directory: " + location);
                }
                directoryClient.deleteIfExistsWithResponse(deleteRecursiveOptions, null, null);
            }
        }
    }

    private void deleteBlobDirectory(AzureLocation location)
    {
        BlobContainerClient blobContainerClient = createBlobContainerClient(location, Optional.empty());
        PagedIterable<BlobItem> blobItems = blobContainerClient.listBlobs(new ListBlobsOptions().setPrefix(location.directoryPath()), null);
        for (BlobItem item : blobItems) {
            blobContainerClient.getBlobClient(item.getName()).deleteIfExists();
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
            throw new TrinoFileSystemException("Cannot rename across storage accounts");
        }
        if (!Objects.equals(sourceLocation.container(), targetLocation.container())) {
            throw new TrinoFileSystemException("Cannot rename across storage account containers");
        }

        // DFS rename file works with all storage types
        renameGen2File(sourceLocation, targetLocation);
    }

    private void renameGen2File(AzureLocation source, AzureLocation target)
            throws IOException
    {
        try {
            DataLakeFileSystemClient fileSystemClient = createFileSystemClient(source, Optional.empty());
            DataLakeFileClient dataLakeFileClient = createFileClient(fileSystemClient, source.path());
            if (dataLakeFileClient.getProperties().isDirectory()) {
                throw new TrinoFileSystemException("Rename file from %s to %s, source is a directory".formatted(source, target));
            }

            createDirectoryIfNotExists(fileSystemClient, target.location().parentDirectory().path());
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
            // blob API returns directories as blobs, so it cannot be used when Gen2 is enabled
            return isHierarchicalNamespaceEnabled(azureLocation)
                    ? listGen2Files(azureLocation)
                    : listBlobFiles(azureLocation);
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "listing files", azureLocation);
        }
    }

    private FileIterator listGen2Files(AzureLocation location)
            throws IOException
    {
        DataLakeFileSystemClient fileSystemClient = createFileSystemClient(location, Optional.empty());
        PagedIterable<PathItem> pathItems;
        if (location.path().isEmpty()) {
            pathItems = fileSystemClient.listPaths(new ListPathsOptions().setRecursive(true), null);
        }
        else {
            DataLakeDirectoryClient directoryClient = createDirectoryClient(fileSystemClient, location.path());
            if (!directoryClient.exists()) {
                return FileIterator.empty();
            }
            if (!directoryClient.getProperties().isDirectory()) {
                throw new TrinoFileSystemException("Location is not a directory: " + location);
            }
            pathItems = directoryClient.listPaths(true, false, null, null);
        }
        return new AzureDataLakeFileIterator(
                location,
                pathItems.stream()
                        .filter(not(PathItem::isDirectory))
                        .iterator());
    }

    private FileIterator listBlobFiles(AzureLocation location)
    {
        PagedIterable<BlobItem> blobItems = createBlobContainerClient(location, Optional.empty()).listBlobs(new ListBlobsOptions().setPrefix(location.directoryPath()), null);
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
            DataLakeFileSystemClient fileSystemClient = createFileSystemClient(azureLocation, Optional.empty());
            DataLakeFileClient fileClient = createFileClient(fileSystemClient, azureLocation.path());
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
            DataLakeFileSystemClient fileSystemClient = createFileSystemClient(azureLocation, Optional.empty());
            DataLakeDirectoryClient directoryClient = createDirectoryIfNotExists(fileSystemClient, azureLocation.path());
            if (!directoryClient.getProperties().isDirectory()) {
                throw new TrinoFileSystemException("Location is not a directory: " + azureLocation);
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
            throw new TrinoFileSystemException("Cannot rename across storage accounts");
        }
        if (!Objects.equals(sourceLocation.container(), targetLocation.container())) {
            throw new TrinoFileSystemException("Cannot rename across storage account containers");
        }
        if (!isHierarchicalNamespaceEnabled(sourceLocation)) {
            throw new TrinoFileSystemException("Azure non-hierarchical does not support directory renames");
        }
        if (sourceLocation.path().isEmpty() || targetLocation.path().isEmpty()) {
            throw new TrinoFileSystemException("Cannot rename %s to %s".formatted(source, target));
        }

        try {
            DataLakeFileSystemClient fileSystemClient = createFileSystemClient(sourceLocation, Optional.empty());
            DataLakeDirectoryClient directoryClient = createDirectoryClient(fileSystemClient, sourceLocation.path());
            if (!directoryClient.exists()) {
                throw new TrinoFileSystemException("Source directory does not exist: " + source);
            }
            if (!directoryClient.getProperties().isDirectory()) {
                throw new TrinoFileSystemException("Source is not a directory: " + source);
            }
            directoryClient.rename(null, targetLocation.path());
        }
        catch (RuntimeException e) {
            throw new IOException("Rename directory from %s to %s failed".formatted(source, target), e);
        }
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        AzureLocation azureLocation = new AzureLocation(location);
        try {
            // blob API returns directories as blobs, so it cannot be used when Gen2 is enabled
            return isHierarchicalNamespaceEnabled(azureLocation)
                    ? listGen2Directories(azureLocation)
                    : listBlobDirectories(azureLocation);
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "listing files", azureLocation);
        }
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        AzureLocation azureLocation = new AzureLocation(targetPath);
        if (!isHierarchicalNamespaceEnabled(azureLocation)) {
            return Optional.empty();
        }

        // allow for absolute or relative temporary prefix
        Location temporary;
        if (temporaryPrefix.startsWith("/")) {
            String prefix = temporaryPrefix;
            while (prefix.startsWith("/")) {
                prefix = prefix.substring(1);
            }
            temporary = azureLocation.baseLocation().appendPath(prefix);
        }
        else {
            temporary = targetPath.appendPath(temporaryPrefix);
        }

        temporary = temporary.appendPath(randomUUID().toString());

        createDirectory(temporary);
        return Optional.of(temporary);
    }

    @Override
    public Optional<UriLocation> preSignedUri(Location location, Duration ttl)
            throws IOException
    {
        return switch (azureAuth) {
            case AzureAuthOauth _ -> oauth2PresignedUri(location, ttl, Optional.empty());
            case AzureAuthAccessKey _ -> accessKeyPresignedUri(location, ttl, Optional.empty());
            default -> throw new UnsupportedOperationException("Unsupported azure auth: " + azureAuth);
        };
    }

    @Override
    public Optional<UriLocation> encryptedPreSignedUri(Location location, Duration ttl, EncryptionKey key)
            throws IOException
    {
        return switch (azureAuth) {
            case AzureAuthOauth _ -> oauth2PresignedUri(location, ttl, Optional.of(key));
            case AzureAuthAccessKey _ -> accessKeyPresignedUri(location, ttl, Optional.of(key));
            default -> throw new UnsupportedOperationException("Unsupported azure auth: " + azureAuth);
        };
    }

    private Optional<UriLocation> oauth2PresignedUri(Location location, Duration ttl, Optional<EncryptionKey> key)
            throws IOException
    {
        AzureLocation azureLocation = new AzureLocation(location);
        BlobContainerClient client = createBlobContainerClient(azureLocation, Optional.empty());

        OffsetDateTime startTime = OffsetDateTime.now();
        OffsetDateTime expiryTime = startTime.plus(ttl.toMillis(), MILLIS);
        UserDelegationKey userDelegationKey = client.getServiceClient().getUserDelegationKey(startTime, expiryTime);

        BlobSasPermission blobSasPermission = new BlobSasPermission()
                .setReadPermission(true);

        BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(expiryTime, blobSasPermission)
                .setStartTime(startTime)
                .setProtocol(SasProtocol.HTTPS_ONLY);

        BlobClient blobClient = createBlobClient(azureLocation, key);
        String sasToken = blobClient.generateUserDelegationSas(sasValues, userDelegationKey);
        try {
            return Optional.of(new UriLocation(URI.create(blobClient.getBlobUrl() + "?" + sasToken), preSignedHeaders(key)));
        }
        catch (Exception e) {
            throw new IOException("Failed to generate pre-signed URI", e);
        }
    }

    private Optional<UriLocation> accessKeyPresignedUri(Location location, Duration ttl, Optional<EncryptionKey> key)
            throws IOException
    {
        AzureLocation azureLocation = new AzureLocation(location);
        BlobClient client = createBlobClient(azureLocation, key);
        BlobSasPermission blobSasPermission = new BlobSasPermission()
                .setReadPermission(true);

        OffsetDateTime startTime = OffsetDateTime.now();
        OffsetDateTime expiryTime = startTime.plus(ttl.toMillis(), MILLIS);

        BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(expiryTime, blobSasPermission)
                .setStartTime(startTime)
                .setExpiryTime(expiryTime)
                .setPermissions(blobSasPermission);

        createBlobContainerClient(azureLocation, key).generateSas(values);
        try {
            return Optional.of(new UriLocation(URI.create(client.getBlobUrl() + "?" + client.generateSas(values)), preSignedHeaders(key)));
        }
        catch (Exception e) {
            throw new IOException("Failed to generate pre-signed URI", e);
        }
    }

    private static Map<String, List<String>> preSignedHeaders(Optional<EncryptionKey> key)
    {
        if (key.isEmpty()) {
            return ImmutableMap.of();
        }

        EncryptionKey encryption = key.get();
        ImmutableMap.Builder<String, List<String>> headers = ImmutableMap.builderWithExpectedSize(3);
        headers.put("x-ms-encryption-algorithm", List.of(encryption.algorithm()));
        headers.put("x-ms-encryption-key", List.of(encodedKey(encryption)));
        headers.put("x-ms-encryption-key-sha256", List.of(keySha256Checksum(encryption)));
        return headers.buildOrThrow();
    }

    private Set<Location> listGen2Directories(AzureLocation location)
            throws IOException
    {
        DataLakeFileSystemClient fileSystemClient = createFileSystemClient(location, Optional.empty());
        PagedIterable<PathItem> pathItems;
        if (location.path().isEmpty()) {
            pathItems = fileSystemClient.listPaths();
        }
        else {
            DataLakeDirectoryClient directoryClient = createDirectoryClient(fileSystemClient, location.path());
            if (!directoryClient.exists()) {
                return ImmutableSet.of();
            }
            if (!directoryClient.getProperties().isDirectory()) {
                throw new TrinoFileSystemException("Location is not a directory: " + location);
            }
            pathItems = directoryClient.listPaths(false, false, null, null);
        }
        Location baseLocation = location.baseLocation();
        return pathItems.stream()
                .filter(PathItem::isDirectory)
                .map(item -> baseLocation.appendPath(item.getName() + "/"))
                .collect(toImmutableSet());
    }

    private Set<Location> listBlobDirectories(AzureLocation location)
    {
        Location baseLocation = location.baseLocation();
        return createBlobContainerClient(location, Optional.empty())
                .listBlobsByHierarchy(location.directoryPath()).stream()
                .filter(BlobItem::isPrefix)
                .map(item -> baseLocation.appendPath(item.getName()))
                .collect(toImmutableSet());
    }

    private boolean isHierarchicalNamespaceEnabled(AzureLocation location)
            throws IOException
    {
        try {
            BlockBlobClient blockBlobClient = createBlobContainerClient(location, Optional.empty())
                    .getBlobClient("/")
                    .getBlockBlobClient();
            return blockBlobClient.exists();
        }
        catch (RuntimeException e) {
            throw new IOException("Checking whether hierarchical namespace is enabled for the location %s failed".formatted(location), e);
        }
    }

    private String validatedEndpoint(AzureLocation location)
    {
        if (!location.endpoint().equals(endpoint)) {
            throw new IllegalArgumentException("Location does not match configured Azure endpoint: " + location);
        }
        return location.endpoint();
    }

    private BlobClient createBlobClient(AzureLocation location, Optional<EncryptionKey> key)
    {
        return createBlobContainerClient(location, key).getBlobClient(location.path());
    }

    private BlobContainerClient createBlobContainerClient(AzureLocation location, Optional<EncryptionKey> key)
    {
        requireNonNull(location, "location is null");

        BlobContainerClientBuilder builder = new BlobContainerClientBuilder()
                .httpClient(httpClient)
                .clientOptions(new ClientOptions().setTracingOptions(tracingOptions))
                .endpoint("https://%s.blob.%s".formatted(location.account(), validatedEndpoint(location)));

        key.ifPresent(encryption -> builder.customerProvidedKey(blobCustomerProvidedKey(encryption)));

        azureAuth.setAuth(location.account(), builder);
        location.container().ifPresent(builder::containerName);
        return builder.buildClient();
    }

    private DataLakeFileSystemClient createFileSystemClient(AzureLocation location, Optional<EncryptionKey> key)
    {
        requireNonNull(location, "location is null");

        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder()
                .httpClient(httpClient)
                .clientOptions(new ClientOptions().setTracingOptions(tracingOptions))
                .endpoint("https://%s.dfs.%s".formatted(location.account(), validatedEndpoint(location)));
        key.ifPresent(encryption -> builder.customerProvidedKey(lakeCustomerProvidedKey(encryption)));
        azureAuth.setAuth(location.account(), builder);
        DataLakeServiceClient client = builder.buildClient();
        DataLakeFileSystemClient fileSystemClient = client.getFileSystemClient(location.container().orElseThrow());
        if (!fileSystemClient.exists()) {
            throw new IllegalArgumentException();
        }
        return fileSystemClient;
    }

    private static DataLakeDirectoryClient createDirectoryClient(DataLakeFileSystemClient fileSystemClient, String directoryName)
    {
        return fileSystemClient.getDirectoryClient(directoryName);
    }

    private static DataLakeFileClient createFileClient(DataLakeFileSystemClient fileSystemClient, String fileName)
    {
        return fileSystemClient.getFileClient(fileName);
    }

    private static DataLakeDirectoryClient createDirectoryIfNotExists(DataLakeFileSystemClient fileSystemClient, String name)
    {
        return fileSystemClient.createDirectoryIfNotExists(name);
    }
}
