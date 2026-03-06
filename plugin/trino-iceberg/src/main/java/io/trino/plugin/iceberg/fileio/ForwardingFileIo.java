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
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsStorageCredentials;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
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

    public static final String TRINO_CATALOG_NAME = "trino.catalog.name";

    private static final ConcurrentHashMap<String, CreationContext> contextRegistry = new ConcurrentHashMap<>();
    private static final ThreadLocal<ConnectorIdentity> currentIdentity = new ThreadLocal<>();

    private TrinoFileSystem fileSystem;
    private Map<String, String> properties = ImmutableMap.of();
    private boolean useFileSizeFromMetadata;
    private ExecutorService deleteExecutor = newDirectExecutorService();
    private volatile List<StorageCredential> storageCredentials = ImmutableList.of();

    public ForwardingFileIo() {}

    @VisibleForTesting
    public ForwardingFileIo(TrinoFileSystem fileSystem, boolean useFileSizeFromMetadata)
    {
        this(fileSystem, ImmutableMap.of(), useFileSizeFromMetadata, newDirectExecutorService());
    }

    public ForwardingFileIo(TrinoFileSystem fileSystem, Map<String, String> properties, boolean useFileSizeFromMetadata, ExecutorService deleteExecutor)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.deleteExecutor = requireNonNull(deleteExecutor, "executorService is null");
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        this.useFileSizeFromMetadata = useFileSizeFromMetadata;
    }

    @Override
    public InputFile newInputFile(String path)
    {
        return new ForwardingInputFile(fileSystem.newInputFile(Location.of(path)));
    }

    @Override
    public InputFile newInputFile(String path, long length)
    {
        if (!useFileSizeFromMetadata) {
            return new ForwardingInputFile(fileSystem.newInputFile(Location.of(path)));
        }

        return new ForwardingInputFile(fileSystem.newInputFile(Location.of(path), length));
    }

    @Override
    public OutputFile newOutputFile(String path)
    {
        return new ForwardingOutputFile(fileSystem, Location.of(path));
    }

    @Override
    public void deleteFile(String path)
    {
        try {
            fileSystem.deleteFile(Location.of(path));
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

    private void deleteBatch(List<String> filesToDelete)
    {
        try {
            fileSystem.deleteFiles(filesToDelete.stream().map(Location::of).toList());
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
        List<StorageCredential> credentials = storageCredentials;
        if (credentials.isEmpty()) {
            return properties;
        }
        ImmutableMap.Builder<String, String> merged = ImmutableMap.builder();
        merged.putAll(properties);
        for (StorageCredential credential : credentials) {
            merged.putAll(credential.config());
        }
        return merged.buildKeepingLast();
    }

    @Override
    public void setCredentials(List<StorageCredential> credentials)
    {
        this.storageCredentials = ImmutableList.copyOf(requireNonNull(credentials, "credentials is null"));
    }

    @Override
    public List<StorageCredential> credentials()
    {
        return storageCredentials;
    }

    @Override
    public void initialize(Map<String, String> properties)
    {
        String catalogName = properties.get(TRINO_CATALOG_NAME);
        checkState(catalogName != null, "ForwardingFileIo requires '%s' property for initialization", TRINO_CATALOG_NAME);
        CreationContext context = contextRegistry.get(catalogName);
        checkState(context != null, "No creation context registered for catalog: %s", catalogName);

        ConnectorIdentity identity = currentIdentity.get();
        if (identity == null) {
            // During RESTSessionCatalog.initialize(), no user session exists yet.
            // Use a default identity for catalog-level operations.
            identity = ConnectorIdentity.ofUser("trino");
        }

        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        // Use merged properties (base + storage credentials) so that the file system factory
        // can see vended credentials like s3.access-key-id from storage-credentials
        this.fileSystem = context.fileSystemFactory().create(identity, properties());
        this.useFileSizeFromMetadata = true;
        this.deleteExecutor = context.deleteExecutor();
    }

    public static void registerContext(String catalogName, IcebergFileSystemFactory fileSystemFactory, ExecutorService deleteExecutor)
    {
        requireNonNull(catalogName, "catalogName is null");
        contextRegistry.put(catalogName, new CreationContext(
                requireNonNull(fileSystemFactory, "fileSystemFactory is null"),
                requireNonNull(deleteExecutor, "deleteExecutor is null")));
    }

    public static void deregisterContext(String catalogName)
    {
        contextRegistry.remove(requireNonNull(catalogName, "catalogName is null"));
    }

    public static IdentityScope withIdentity(ConnectorIdentity identity)
    {
        ConnectorIdentity previous = currentIdentity.get();
        currentIdentity.set(requireNonNull(identity, "identity is null"));
        return new IdentityScope(previous);
    }

    public static final class IdentityScope
            implements AutoCloseable
    {
        private final ConnectorIdentity previous;

        private IdentityScope(ConnectorIdentity previous)
        {
            this.previous = previous;
        }

        @Override
        public void close()
        {
            if (previous == null) {
                currentIdentity.remove();
            }
            else {
                currentIdentity.set(previous);
            }
        }
    }

    @Override
    public void close() {}

    private record CreationContext(IcebergFileSystemFactory fileSystemFactory, ExecutorService deleteExecutor)
    {
        CreationContext
        {
            requireNonNull(fileSystemFactory, "fileSystemFactory is null");
            requireNonNull(deleteExecutor, "deleteExecutor is null");
        }
    }
}
