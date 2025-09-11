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
package io.trino.plugin.exchange.filesystem.alluxio;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.Source;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ListStatusPOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.Slice;
import io.trino.annotation.NotThreadSafe;
import io.trino.plugin.exchange.filesystem.ExchangeSourceFile;
import io.trino.plugin.exchange.filesystem.ExchangeStorageReader;
import io.trino.plugin.exchange.filesystem.ExchangeStorageWriter;
import io.trino.plugin.exchange.filesystem.FileStatus;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeStorage;
import io.trino.plugin.exchange.filesystem.MetricsBuilder;
import io.trino.spi.TrinoException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.exchange.filesystem.MetricsBuilder.SOURCE_FILES_PROCESSED;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class AlluxioFileSystemExchangeStorage
        implements FileSystemExchangeStorage
{
    private final FileSystem fileSystem;
    private final int blockSize;

    @Inject
    public AlluxioFileSystemExchangeStorage(ExchangeAlluxioConfig config)
    {
        AlluxioConfiguration alluxioConf = loadAlluxioConfiguration(config.getAlluxioSiteConfPath());
        fileSystem = FileSystem.Factory.create(FileSystemContext.create(alluxioConf));
        blockSize = toIntExact(config.getAlluxioStorageBlockSize().toBytes());
    }

    @Override
    public void createDirectories(URI dir)
            throws IOException
    {
        try {
            AlluxioURI alluxioUri = convertToAlluxioURI(dir);
            fileSystem.createDirectory(
                    alluxioUri,
                    CreateDirectoryPOptions.newBuilder()
                            .setAllowExists(true)
                            .setRecursive(true)
                            .build());
        }
        catch (Exception e) {
            throw new IOException("Error createDirectory " + dir, e);
        }
    }

    @Override
    public ExchangeStorageReader createExchangeStorageReader(List<ExchangeSourceFile> sourceFiles, int maxPageStorageSize, MetricsBuilder metricsBuilder)
    {
        return new AlluxioExchangeStorageReader(fileSystem, sourceFiles, metricsBuilder, blockSize);
    }

    @Override
    public ExchangeStorageWriter createExchangeStorageWriter(URI file)
    {
        return new AlluxioExchangeStorageWriter(fileSystem, file);
    }

    @Override
    public ListenableFuture<Void> createEmptyFile(URI file)
    {
        try {
            AlluxioURI locationUri = convertToAlluxioURI(file);
            if (!fileSystem.exists(locationUri)) {
                fileSystem.createFile(
                        locationUri,
                        CreateFilePOptions.newBuilder()
                                .setOverwrite(false)
                                .setRecursive(true)
                                .build());
            }
        }
        catch (Exception e) {
            return immediateFailedFuture(e);
        }
        return immediateVoidFuture();
    }

    @Override
    public ListenableFuture<Void> deleteRecursively(List<URI> directories)
    {
        for (URI dir : directories) {
            try {
                deleteDirectory(dir);
            }
            catch (Exception e) {
                return immediateFailedFuture(e);
            }
        }
        return immediateVoidFuture();
    }

    public void deleteDirectory(URI location)
            throws IOException
    {
        try {
            AlluxioURI uri = convertToAlluxioURI(location);
            URIStatus status = fileSystem.getStatus(uri);
            if (status == null) {
                return;
            }
            DeletePOptions deletePOptions = DeletePOptions.newBuilder().setRecursive(true).build();
            fileSystem.delete(uri, deletePOptions);
        }
        catch (FileDoesNotExistException | NotFoundRuntimeException e) {
        }
        catch (Exception e) {
            throw new IOException("Error deleteDirectory " + location, e);
        }
    }

    @Override
    public ListenableFuture<List<FileStatus>> listFilesRecursively(URI dir)
    {
        ImmutableList.Builder<FileStatus> builder = ImmutableList.builder();
        try {
            fileSystem.listStatus(convertToAlluxioURI(dir),
                    ListStatusPOptions.newBuilder().setRecursive(true).build())
                    .stream()
                    .map(status -> new FileStatus(status.getPath(), status.getLength()))
                    .forEach(builder::add);
        }
        catch (Exception e) {
            return immediateFailedFuture(e);
        }
        return immediateFuture(builder.build());
    }

    @Override
    public int getWriteBufferSize()
    {
        return blockSize;
    }

    @Override
    public void close()
            throws IOException
    {
    }

    private static AlluxioURI convertToAlluxioURI(URI location)
    {
        return new AlluxioURI(requireNonNull(location.getPath(), "path is null"));
    }

    public static AlluxioConfiguration loadAlluxioConfiguration(Optional<String> alluxioSiteConfPath)
    {
        AlluxioProperties alluxioProperties = new AlluxioProperties();
        if (!alluxioSiteConfPath.isPresent()) {
            return new InstancedConfiguration(alluxioProperties);
        }

        try (FileInputStream fileInputStream = new FileInputStream(alluxioSiteConfPath.get())) {
            Properties properties = new Properties();
            properties.load(fileInputStream);
            alluxioProperties.merge(properties, Source.siteProperty(alluxioSiteConfPath.get()));
            AlluxioConfiguration alluxioConfiguration = new InstancedConfiguration(alluxioProperties);
            alluxioConfiguration.validate();
            return alluxioConfiguration;
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_USER_ERROR, "alluxio site config load failed", e);
        }
    }

    @ThreadSafe
    private static class AlluxioExchangeStorageReader
            implements ExchangeStorageReader
    {
        private static final int INSTANCE_SIZE = instanceSize(AlluxioExchangeStorageReader.class);

        private final FileSystem fileSystem;
        @GuardedBy("this")
        private final Queue<ExchangeSourceFile> sourceFiles;
        private final MetricsBuilder.CounterMetricBuilder sourceFilesProcessedMetric;
        private final int blockSize;

        @GuardedBy("this")
        private InputStreamSliceInput sliceInput;
        @GuardedBy("this")
        private boolean closed;

        public AlluxioExchangeStorageReader(FileSystem fileSystem, List<ExchangeSourceFile> sourceFiles, MetricsBuilder metricsBuilder, int blockSize)
        {
            this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
            this.sourceFiles = new ArrayDeque<>(requireNonNull(sourceFiles, "sourceFiles is null"));
            requireNonNull(metricsBuilder, "metricsBuilder is null");
            sourceFilesProcessedMetric = metricsBuilder.getCounterMetric(SOURCE_FILES_PROCESSED);
            this.blockSize = blockSize;
        }

        @Override
        public synchronized Slice read()
                throws IOException
        {
            if (closed) {
                return null;
            }

            if (sliceInput != null) {
                if (sliceInput.isReadable()) {
                    return sliceInput.readSlice(sliceInput.readInt());
                }
                else {
                    sliceInput.close();
                    sourceFilesProcessedMetric.increment();
                }
            }

            ExchangeSourceFile sourceFile = sourceFiles.poll();
            if (sourceFile == null) {
                close();
                return null;
            }

            sliceInput = getSliceInput(sourceFile);
            return sliceInput.readSlice(sliceInput.readInt());
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            return immediateVoidFuture();
        }

        @Override
        public synchronized long getRetainedSize()
        {
            return INSTANCE_SIZE + (sliceInput == null ? 0 : sliceInput.getRetainedSize());
        }

        @Override
        public synchronized boolean isFinished()
        {
            return closed;
        }

        @Override
        public synchronized void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            if (sliceInput != null) {
                sliceInput.close();
                sliceInput = null;
            }
        }

        private InputStreamSliceInput getSliceInput(ExchangeSourceFile sourceFile)
                throws IOException
        {
            try {
                AlluxioURI fileURI = convertToAlluxioURI(sourceFile.getFileUri());
                return new InputStreamSliceInput(fileSystem.openFile(fileURI), blockSize);
            }
            catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    @NotThreadSafe
    private static class AlluxioExchangeStorageWriter
            implements ExchangeStorageWriter
    {
        private static final int INSTANCE_SIZE = instanceSize(AlluxioExchangeStorageWriter.class);
        private final OutputStream outputStream;

        public AlluxioExchangeStorageWriter(FileSystem fileSystem, URI file)
        {
            try {
                CreateFilePOptions options = CreateFilePOptions.newBuilder()
                        .setOverwrite(true).setRecursive(true).build();
                this.outputStream = fileSystem.createFile(convertToAlluxioURI(file), options);
            }
            catch (Exception e) {
                throw new UncheckedIOException(new IOException(e));
            }
        }

        @Override
        public ListenableFuture<Void> write(Slice slice)
        {
            try {
                outputStream.write(slice.getBytes());
            }
            catch (IOException | RuntimeException e) {
                return immediateFailedFuture(e);
            }
            return immediateVoidFuture();
        }

        @Override
        public ListenableFuture<Void> finish()
        {
            try {
                outputStream.close();
            }
            catch (IOException | RuntimeException e) {
                return immediateFailedFuture(e);
            }
            return immediateVoidFuture();
        }

        @Override
        public ListenableFuture<Void> abort()
        {
            try {
                outputStream.close();
            }
            catch (IOException | RuntimeException e) {
                return immediateFailedFuture(e);
            }
            return immediateVoidFuture();
        }

        @Override
        public long getRetainedSize()
        {
            return INSTANCE_SIZE;
        }
    }
}
