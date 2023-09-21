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

import com.azure.core.http.rest.PagedResponse;
import com.azure.core.util.BinaryData;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.batch.BlobBatchAsyncClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.trino.annotation.NotThreadSafe;
import io.trino.plugin.exchange.filesystem.ExchangeSourceFile;
import io.trino.plugin.exchange.filesystem.ExchangeStorageReader;
import io.trino.plugin.exchange.filesystem.ExchangeStorageWriter;
import io.trino.plugin.exchange.filesystem.FileStatus;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeStorage;
import jakarta.annotation.PreDestroy;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.exchange.filesystem.FileSystemExchangeFutures.translateFailures;
import static io.trino.plugin.exchange.filesystem.FileSystemExchangeManager.PATH_SEPARATOR;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;

public class AzureBlobFileSystemExchangeStorage
        implements FileSystemExchangeStorage
{
    private final int blockSize;
    private final BlobServiceAsyncClient blobServiceAsyncClient;

    @Inject
    public AzureBlobFileSystemExchangeStorage(ExchangeAzureConfig config)
    {
        this.blockSize = toIntExact(config.getAzureStorageBlockSize().toBytes());

        BlobServiceClientBuilder blobServiceClientBuilder = new BlobServiceClientBuilder()
                .retryOptions(new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, config.getMaxErrorRetries(), (Integer) null, null, null, null));
        Optional<String> connectionString = config.getAzureStorageConnectionString();
        if (connectionString.isPresent()) {
            blobServiceClientBuilder.connectionString(connectionString.get());
        }
        else {
            blobServiceClientBuilder.credential(new DefaultAzureCredentialBuilder().build());
        }
        this.blobServiceAsyncClient = blobServiceClientBuilder.buildAsyncClient();
    }

    @Override
    public void createDirectories(URI dir)
            throws IOException
    {
        // Nothing to do for Azure
    }

    @Override
    public ExchangeStorageReader createExchangeStorageReader(List<ExchangeSourceFile> sourceFiles, int maxPageStorageSize)
    {
        return new AzureExchangeStorageReader(blobServiceAsyncClient, sourceFiles, blockSize, maxPageStorageSize);
    }

    @Override
    public ExchangeStorageWriter createExchangeStorageWriter(URI file)
    {
        String containerName = getContainerName(file);
        String blobName = getPath(file);
        BlockBlobAsyncClient blockBlobAsyncClient = blobServiceAsyncClient
                .getBlobContainerAsyncClient(containerName)
                .getBlobAsyncClient(blobName)
                .getBlockBlobAsyncClient();
        return new AzureExchangeStorageWriter(blockBlobAsyncClient, blockSize);
    }

    @Override
    public ListenableFuture<Void> createEmptyFile(URI file)
    {
        String containerName = getContainerName(file);
        String blobName = getPath(file);
        return translateFailures(toListenableFuture(blobServiceAsyncClient
                .getBlobContainerAsyncClient(containerName)
                .getBlobAsyncClient(blobName)
                .upload(BinaryData.fromString(""))
                .toFuture()));
    }

    @Override
    public ListenableFuture<Void> deleteRecursively(List<URI> directories)
    {
        ImmutableMultimap.Builder<String, ListenableFuture<List<PagedResponse<BlobItem>>>> containerToListObjectsFuturesBuilder = ImmutableMultimap.builder();
        directories.forEach(dir -> containerToListObjectsFuturesBuilder.put(
                getContainerName(dir),
                listObjectsRecursively(dir)));
        Multimap<String, ListenableFuture<List<PagedResponse<BlobItem>>>> containerToListObjectsFutures = containerToListObjectsFuturesBuilder.build();

        ImmutableList.Builder<ListenableFuture<List<Void>>> deleteObjectsFutures = ImmutableList.builder();
        for (String containerName : containerToListObjectsFutures.keySet()) {
            BlobContainerAsyncClient blobContainerAsyncClient = blobServiceAsyncClient.getBlobContainerAsyncClient(containerName);
            deleteObjectsFutures.add(Futures.transformAsync(
                    Futures.allAsList(containerToListObjectsFutures.get(containerName)),
                    nestedPagedResponseList -> {
                        ImmutableList.Builder<String> blobUrls = ImmutableList.builder();
                        for (List<PagedResponse<BlobItem>> pagedResponseList : nestedPagedResponseList) {
                            for (PagedResponse<BlobItem> pagedResponse : pagedResponseList) {
                                pagedResponse.getValue().forEach(blobItem -> {
                                    blobUrls.add(blobContainerAsyncClient.getBlobAsyncClient(blobItem.getName()).getBlobUrl());
                                });
                            }
                        }
                        return deleteObjects(blobUrls.build());
                    },
                    directExecutor()));
        }

        return translateFailures(Futures.allAsList(deleteObjectsFutures.build()));
    }

    @Override
    public ListenableFuture<List<FileStatus>> listFilesRecursively(URI dir)
    {
        return Futures.transform(listObjectsRecursively(dir), pagedResponseList -> {
            ImmutableList.Builder<FileStatus> fileStatuses = ImmutableList.builder();
            for (PagedResponse<BlobItem> pagedResponse : pagedResponseList) {
                for (BlobItem blobItem : pagedResponse.getValue()) {
                    if (blobItem.isPrefix() != Boolean.TRUE) {
                        URI uri;
                        try {
                            uri = new URI(dir.getScheme(), dir.getUserInfo(), dir.getHost(), -1, PATH_SEPARATOR + blobItem.getName(), null, dir.getFragment());
                        }
                        catch (URISyntaxException e) {
                            throw new IllegalArgumentException(e);
                        }
                        fileStatuses.add(new FileStatus(uri.toString(), blobItem.getProperties().getContentLength()));
                    }
                }
            }
            return fileStatuses.build();
        }, directExecutor());
    }

    @Override
    public int getWriteBufferSize()
    {
        return blockSize;
    }

    @PreDestroy
    @Override
    public void close()
            throws IOException
    {
    }

    private ListenableFuture<List<PagedResponse<BlobItem>>> listObjectsRecursively(URI dir)
    {
        checkArgument(isDirectory(dir), "listObjectsRecursively called on file uri %s", dir);

        String containerName = getContainerName(dir);
        String directoryPath = getPath(dir);

        return toListenableFuture(blobServiceAsyncClient
                .getBlobContainerAsyncClient(containerName)
                .listBlobsByHierarchy(null, (new ListBlobsOptions()).setPrefix(directoryPath))
                .byPage()
                .collectList()
                .toFuture());
    }

    private ListenableFuture<List<Void>> deleteObjects(List<String> blobUrls)
    {
        BlobBatchAsyncClient blobBatchAsyncClient = new BlobBatchClientBuilder(blobServiceAsyncClient).buildAsyncClient();
        // deleteBlobs can delete at most 256 blobs at a time
        return Futures.allAsList(Lists.partition(blobUrls, 256).stream()
                .map(list -> toListenableFuture(blobBatchAsyncClient.deleteBlobs(list, DeleteSnapshotsOptionType.INCLUDE).then().toFuture()))
                .collect(toImmutableList()));
    }

    // URI format: abfs[s]://<container_name>@<account_name>.dfs.core.windows.net/<path>/<file_name>
    private static String getContainerName(URI uri)
    {
        return uri.getUserInfo();
    }

    private static String getPath(URI uri)
    {
        checkArgument(uri.isAbsolute(), "Uri is not absolute: %s", uri);
        String blobName = nullToEmpty(uri.getPath());
        if (blobName.startsWith(PATH_SEPARATOR)) {
            blobName = blobName.substring(PATH_SEPARATOR.length());
        }
        if (blobName.endsWith(PATH_SEPARATOR)) {
            blobName = blobName.substring(0, blobName.length() - PATH_SEPARATOR.length());
        }
        return blobName;
    }

    private static boolean isDirectory(URI uri)
    {
        return uri.toString().endsWith(PATH_SEPARATOR);
    }

    @ThreadSafe
    private static class AzureExchangeStorageReader
            implements ExchangeStorageReader
    {
        private static final int INSTANCE_SIZE = instanceSize(AzureExchangeStorageReader.class);

        private final BlobServiceAsyncClient blobServiceAsyncClient;
        @GuardedBy("this")
        private final Queue<ExchangeSourceFile> sourceFiles;
        private final int blockSize;
        private final int bufferSize;

        @GuardedBy("this")
        private ExchangeSourceFile currentFile;
        @GuardedBy("this")
        private long fileOffset;
        @GuardedBy("this")
        private SliceInput sliceInput;
        @GuardedBy("this")
        private int sliceSize = -1;
        private volatile boolean closed;
        private volatile long bufferRetainedSize;
        private volatile ListenableFuture<Void> inProgressReadFuture = immediateVoidFuture();

        public AzureExchangeStorageReader(
                BlobServiceAsyncClient blobServiceAsyncClient,
                List<ExchangeSourceFile> sourceFiles,
                int blockSize,
                int maxPageStorageSize)
        {
            this.blobServiceAsyncClient = requireNonNull(blobServiceAsyncClient, "blobServiceAsyncClient is null");
            this.sourceFiles = new ArrayDeque<>(requireNonNull(sourceFiles, "sourceFiles is null"));
            this.blockSize = blockSize;
            // Make sure buffer can accommodate at least one complete Slice, and keep reads aligned to block boundaries
            this.bufferSize = maxPageStorageSize + blockSize;

            // Safe publication of S3ExchangeStorageReader is required as it's a mutable class
            fillBuffer();
        }

        @Override
        public synchronized Slice read()
                throws IOException
        {
            if (closed || !inProgressReadFuture.isDone()) {
                return null;
            }

            try {
                getFutureValue(inProgressReadFuture);
            }
            catch (RuntimeException e) {
                throw new IOException(e);
            }

            if (sliceSize < 0) {
                sliceSize = sliceInput.readInt();
            }
            Slice data = sliceInput.readSlice(sliceSize);

            if (sliceInput.available() > Integer.BYTES) {
                sliceSize = sliceInput.readInt();
                if (sliceInput.available() < sliceSize) {
                    fillBuffer();
                }
            }
            else {
                sliceSize = -1;
                fillBuffer();
            }

            return data;
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            // rely on FileSystemExchangeSource implementation to wrap with nonCancellationPropagating
            return inProgressReadFuture;
        }

        @Override
        public synchronized long getRetainedSize()
        {
            return INSTANCE_SIZE + bufferRetainedSize;
        }

        @Override
        public boolean isFinished()
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

            currentFile = null;
            sliceInput = null;
            bufferRetainedSize = 0;
            inProgressReadFuture.cancel(true);
            inProgressReadFuture = immediateVoidFuture(); // such that we don't retain reference to the buffer
        }

        @GuardedBy("this")
        private void fillBuffer()
        {
            if (currentFile == null || fileOffset == currentFile.getFileSize()) {
                currentFile = sourceFiles.poll();
                if (currentFile == null) {
                    close();
                    return;
                }
                fileOffset = 0;
            }

            byte[] buffer = new byte[bufferSize];
            int bufferFill = 0;
            if (sliceInput != null) {
                int length = sliceInput.available();
                sliceInput.readBytes(buffer, 0, length);
                bufferFill += length;
            }

            ImmutableList.Builder<ListenableFuture<Void>> downloadFutures = ImmutableList.builder();
            while (true) {
                long fileSize = currentFile.getFileSize();
                // Make sure Azure Blob Storage read request byte ranges align with block sizes for best performance
                int readableBlocks = (buffer.length - bufferFill) / blockSize;
                if (readableBlocks == 0) {
                    if (buffer.length - bufferFill >= fileSize - fileOffset) {
                        readableBlocks = 1;
                    }
                    else {
                        break;
                    }
                }

                BlockBlobAsyncClient blockBlobAsyncClient = blobServiceAsyncClient
                        .getBlobContainerAsyncClient(getContainerName(currentFile.getFileUri()))
                        .getBlobAsyncClient(getPath(currentFile.getFileUri()))
                        .getBlockBlobAsyncClient();
                for (int i = 0; i < readableBlocks && fileOffset < fileSize; ++i) {
                    int length = (int) min(blockSize, fileSize - fileOffset);

                    int finalBufferFill = bufferFill;
                    FluentFuture<Void> downloadFuture = FluentFuture.from(toListenableFuture(blockBlobAsyncClient.downloadWithResponse(new BlobRange(fileOffset, (long) length), null, null, false).toFuture()))
                            .transformAsync(response -> toListenableFuture(response.getValue().collectList().toFuture()), directExecutor())
                            .transform(byteBuffers -> {
                                int offset = finalBufferFill;
                                for (ByteBuffer byteBuffer : byteBuffers) {
                                    int readableBytes = byteBuffer.remaining();
                                    if (byteBuffer.hasArray()) {
                                        arraycopy(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), buffer, offset, readableBytes);
                                    }
                                    else {
                                        byteBuffer.asReadOnlyBuffer().get(buffer, offset, readableBytes);
                                    }
                                    offset += readableBytes;
                                }
                                return null;
                            }, directExecutor());
                    downloadFutures.add(downloadFuture);
                    bufferFill += length;
                    fileOffset += length;
                }

                if (fileOffset == fileSize) {
                    currentFile = sourceFiles.poll();
                    if (currentFile == null) {
                        break;
                    }
                    fileOffset = 0;
                }
            }

            inProgressReadFuture = asVoid(Futures.allAsList(downloadFutures.build()));
            sliceInput = Slices.wrappedBuffer(buffer, 0, bufferFill).getInput();
            bufferRetainedSize = sliceInput.getRetainedSize();
        }
    }

    @NotThreadSafe
    private static class AzureExchangeStorageWriter
            implements ExchangeStorageWriter
    {
        private static final int INSTANCE_SIZE = instanceSize(AzureExchangeStorageWriter.class);

        private final BlockBlobAsyncClient blockBlobAsyncClient;
        private final int blockSize;

        private ListenableFuture<Void> directUploadFuture;
        private final List<ListenableFuture<Void>> multiPartUploadFutures = new ArrayList<>();
        private final List<String> blockIds = new ArrayList<>();
        private volatile boolean closed;

        public AzureExchangeStorageWriter(
                BlockBlobAsyncClient blockBlobAsyncClient,
                int blockSize)
        {
            this.blockBlobAsyncClient = requireNonNull(blockBlobAsyncClient, "blockBlobAsyncClient is null");
            this.blockSize = blockSize;
        }

        @Override
        public ListenableFuture<Void> write(Slice slice)
        {
            checkState(directUploadFuture == null, "Direct upload already started");
            if (closed) {
                // Ignore writes after writer is closed
                return immediateVoidFuture();
            }

            // Skip multipart upload if there would only be one part
            if (slice.length() < blockSize && multiPartUploadFutures.isEmpty()) {
                directUploadFuture = translateFailures(toListenableFuture(blockBlobAsyncClient.upload(Flux.just(slice.toByteBuffer()), slice.length()).toFuture()));
                return directUploadFuture;
            }

            String blockId = Base64.getEncoder().encodeToString(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
            ListenableFuture<Void> uploadFuture = toListenableFuture(blockBlobAsyncClient.stageBlock(blockId, Flux.just(slice.toByteBuffer()), slice.length()).toFuture());
            multiPartUploadFutures.add(uploadFuture);
            blockIds.add(blockId);
            return translateFailures(uploadFuture);
        }

        @Override
        public ListenableFuture<Void> finish()
        {
            if (closed) {
                return immediateVoidFuture();
            }

            if (multiPartUploadFutures.isEmpty()) {
                return requireNonNullElseGet(directUploadFuture, Futures::immediateVoidFuture);
            }

            ListenableFuture<Void> finishFuture = translateFailures(Futures.transformAsync(
                    Futures.allAsList(multiPartUploadFutures),
                    ignored -> toListenableFuture(blockBlobAsyncClient.commitBlockList(blockIds).toFuture()),
                    directExecutor()));
            Futures.addCallback(finishFuture, new FutureCallback<>() {
                @Override
                public void onSuccess(Void result)
                {
                    closed = true;
                }

                @Override
                public void onFailure(Throwable ignored)
                {
                    // Rely on caller to abort in case of exceptions during finish
                }
            }, directExecutor());
            return finishFuture;
        }

        @Override
        public ListenableFuture<Void> abort()
        {
            if (closed) {
                return immediateVoidFuture();
            }
            closed = true;

            if (multiPartUploadFutures.isEmpty()) {
                if (directUploadFuture != null) {
                    directUploadFuture.cancel(true);
                }
                return immediateVoidFuture();
            }

            verify(directUploadFuture == null);
            multiPartUploadFutures.forEach(future -> future.cancel(true));

            // No explicit way to delete staged blocks; uncommitted blocks are automatically deleted after 7 days
            return immediateVoidFuture();
        }

        @Override
        public long getRetainedSize()
        {
            return INSTANCE_SIZE + estimatedSizeOf(blockIds, SizeOf::estimatedSizeOf);
        }
    }
}
