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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestListFile;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsStorageCredentials;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.plugin.base.util.ExecutorUtil.processWithAdditionalThreads;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class ForwardingFileIo
        implements SupportsBulkOperations, SupportsStorageCredentials
{
    private static final int DELETE_BATCH_SIZE = 1000;
    private static final int BATCH_DELETE_PATHS_MESSAGE_LIMIT = 5;

    private final TrinoFileSystem fileSystem;
    private final Map<String, String> properties;
    private final boolean useFileSizeFromMetadata;
    private final ExecutorService deleteExecutor;
    private final IcebergFileSystemFactory storageCredentialFileSystemFactory;
    private final ConnectorIdentity storageCredentialIdentity;

    private volatile List<StorageCredential> storageCredentials = ImmutableList.of();
    private volatile Map<String, TrinoFileSystem> prefixedFileSystems = ImmutableMap.of();

    @VisibleForTesting
    public ForwardingFileIo(TrinoFileSystem fileSystem, boolean useFileSizeFromMetadata)
    {
        this(fileSystem, ImmutableMap.of(), useFileSizeFromMetadata, newDirectExecutorService());
    }

    public ForwardingFileIo(TrinoFileSystem fileSystem, Map<String, String> properties, boolean useFileSizeFromMetadata, ExecutorService deleteExecutor)
    {
        this(fileSystem, properties, useFileSizeFromMetadata, deleteExecutor, null, null);
    }

    public ForwardingFileIo(
            TrinoFileSystem fileSystem,
            Map<String, String> properties,
            boolean useFileSizeFromMetadata,
            ExecutorService deleteExecutor,
            IcebergFileSystemFactory storageCredentialFileSystemFactory,
            ConnectorIdentity storageCredentialIdentity)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.deleteExecutor = requireNonNull(deleteExecutor, "executorService is null");
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        this.useFileSizeFromMetadata = useFileSizeFromMetadata;
        this.storageCredentialFileSystemFactory = storageCredentialFileSystemFactory;
        this.storageCredentialIdentity = storageCredentialIdentity;
    }

    @Override
    public InputFile newInputFile(String path)
    {
        return new ForwardingInputFile(fileSystemForPath(path).newInputFile(Location.of(path)));
    }

    @Override
    public InputFile newInputFile(String path, long length)
    {
        TrinoFileSystem fileSystem = fileSystemForPath(path);
        if (!useFileSizeFromMetadata) {
            return new ForwardingInputFile(fileSystem.newInputFile(Location.of(path)));
        }

        return new ForwardingInputFile(fileSystem.newInputFile(Location.of(path), length));
    }

    @Override
    public OutputFile newOutputFile(String path)
    {
        return new ForwardingOutputFile(fileSystemForPath(path), Location.of(path));
    }

    @Override
    public void deleteFile(String path)
    {
        try {
            fileSystemForPath(path).deleteFile(Location.of(path));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to delete file: " + path, e);
        }
    }

    @Override
    public void deleteFile(InputFile file)
    {
        SupportsBulkOperations.super.deleteFile(file);
    }

    @Override
    public void deleteFile(OutputFile file)
    {
        SupportsBulkOperations.super.deleteFile(file);
    }

    @Override
    public void deleteFiles(Iterable<String> pathsToDelete)
            throws BulkDeletionFailureException
    {
        List<Callable<Void>> tasks = Streams.stream(Iterables.partition(pathsToDelete, DELETE_BATCH_SIZE))
                .map(batch -> (Callable<Void>) () -> {
                    deleteBatch(batch);
                    return null;
                }).collect(toImmutableList());
        try {
            processWithAdditionalThreads(tasks, deleteExecutor);
        }
        catch (ExecutionException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to delete files", e.getCause());
        }
    }

    @Override
    public InputFile newInputFile(ManifestFile manifest)
    {
        return SupportsBulkOperations.super.newInputFile(manifest);
    }

    @Override
    public InputFile newInputFile(DataFile file)
    {
        return SupportsBulkOperations.super.newInputFile(file);
    }

    @Override
    public InputFile newInputFile(DeleteFile file)
    {
        return SupportsBulkOperations.super.newInputFile(file);
    }

    @Override
    public InputFile newInputFile(ManifestListFile manifestList)
    {
        return SupportsBulkOperations.super.newInputFile(manifestList);
    }

    private void deleteBatch(List<String> filesToDelete)
    {
        try {
            Map<TrinoFileSystem, List<Location>> locationsByFileSystem = new IdentityHashMap<>();
            for (String path : filesToDelete) {
                TrinoFileSystem fileSystem = fileSystemForPath(path);
                locationsByFileSystem.computeIfAbsent(fileSystem, ignored -> new ArrayList<>())
                        .add(Location.of(path));
            }
            for (Map.Entry<TrinoFileSystem, List<Location>> entry : locationsByFileSystem.entrySet()) {
                entry.getKey().deleteFiles(entry.getValue());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to delete some or all of files: " +
                            Stream.concat(
                                            filesToDelete.stream()
                                                    .limit(BATCH_DELETE_PATHS_MESSAGE_LIMIT),
                                            filesToDelete.size() > BATCH_DELETE_PATHS_MESSAGE_LIMIT ? Stream.of("...") : Stream.of())
                                    .collect(joining(", ", "[", "]")),
                    e);
        }
    }

    @Override
    public Map<String, String> properties()
    {
        return properties;
    }

    @Override
    public void setCredentials(List<StorageCredential> credentials)
    {
        storageCredentials = ImmutableList.copyOf(requireNonNull(credentials, "credentials is null"));
        rebuildPrefixedFileSystems();
    }

    @Override
    public List<StorageCredential> credentials()
    {
        return storageCredentials;
    }

    private TrinoFileSystem fileSystemForPath(String path)
    {
        TrinoFileSystem matchingFileSystem = fileSystem;
        int matchingPrefixLength = -1;
        for (Map.Entry<String, TrinoFileSystem> prefixedFileSystem : prefixedFileSystems.entrySet()) {
            String prefix = prefixedFileSystem.getKey();
            if (path.startsWith(prefix) && prefix.length() > matchingPrefixLength) {
                matchingPrefixLength = prefix.length();
                matchingFileSystem = prefixedFileSystem.getValue();
            }
        }
        return matchingFileSystem;
    }

    private void rebuildPrefixedFileSystems()
    {
        if (storageCredentials.isEmpty() || storageCredentialFileSystemFactory == null || storageCredentialIdentity == null) {
            prefixedFileSystems = ImmutableMap.of();
            return;
        }

        ImmutableMap.Builder<String, TrinoFileSystem> rebuiltFileSystems = ImmutableMap.builder();
        for (StorageCredential storageCredential : storageCredentials) {
            Map<String, String> mergedProperties = ImmutableMap.<String, String>builder()
                    .putAll(properties)
                    .putAll(storageCredential.config())
                    .buildKeepingLast();
            rebuiltFileSystems.put(storageCredential.prefix(), storageCredentialFileSystemFactory.create(storageCredentialIdentity, mergedProperties));
        }
        prefixedFileSystems = rebuiltFileSystems.buildKeepingLast();
    }

    @Override
    public void initialize(Map<String, String> properties)
    {
        throw new UnsupportedOperationException("ForwardingFileIO does not support initialization by properties");
    }

    @Override
    public void close() {}
}
