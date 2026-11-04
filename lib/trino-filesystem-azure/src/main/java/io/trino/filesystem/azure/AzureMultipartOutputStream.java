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

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.filesystem.LinkedBuffer;
import io.trino.filesystem.TrinoOutputStream;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.submit;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.filesystem.azure.AzureUtils.handleAzureException;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class AzureMultipartOutputStream
        extends TrinoOutputStream
{
    private static final int BUFFER_CHUNK_SIZE = toIntExact(DataSize.of(1, MEGABYTE).toBytes());

    private final AzureLocation location;
    private final BlockBlobClient blockClient;
    private final LocalMemoryContext memoryContext;
    private final Executor uploadExecutor;

    private final int writeBlockSizeBytes;
    private final boolean overwrite;
    private boolean closed;
    private boolean multiStagingStarted;
    private final List<String> stagedBlocks = new ArrayList<>();
    private final List<ListenableFuture<?>> stagedBlockFutures = new ArrayList<>();
    private int blockNum;

    @Nullable // is null once all data is handed off for upload, or after abort
    private LinkedBuffer buffer;

    public AzureMultipartOutputStream(
            AzureLocation location,
            BlobClient blobClient,
            ExecutorService uploadExecutor,
            boolean overwrite,
            AggregatedMemoryContext memoryContext,
            int writeBlockSizeBytes)
    {
        requireNonNull(location, "location is null");
        requireNonNull(blobClient, "blobClient is null");
        checkArgument(writeBlockSizeBytes >= 0, "writeBlockSizeBytes is negative");

        this.location = location;
        this.writeBlockSizeBytes = writeBlockSizeBytes;
        this.overwrite = overwrite;
        this.memoryContext = memoryContext.newLocalMemoryContext(AzureMultipartOutputStream.class.getSimpleName());
        this.blockClient = blobClient.getBlockBlobClient();
        this.uploadExecutor = listeningDecorator(requireNonNull(uploadExecutor, "uploadExecutor is null"));
        // fixed size chunks, so a block never allocates more than one chunk beyond its size
        int chunkSize = min(BUFFER_CHUNK_SIZE, writeBlockSizeBytes);
        this.buffer = new LinkedBuffer(chunkSize, chunkSize);
        this.memoryContext.setBytes(buffer.allocated());
    }

    @Override
    public void write(int b)
            throws IOException
    {
        ensureOpen();
        int allocated = buffer.allocated();
        buffer.write(b);
        if (buffer.allocated() != allocated) {
            memoryContext.addBytes(buffer.allocated() - allocated);
        }
        flushBuffer(false);
    }

    @Override
    public void write(byte[] bytes, int offset, int length)
            throws IOException
    {
        ensureOpen();

        while (length > 0) {
            int copied = min(writeBlockSizeBytes - buffer.size(), length);
            int allocated = buffer.allocated();
            buffer.write(bytes, offset, copied);
            memoryContext.addBytes(buffer.allocated() - allocated);

            flushBuffer(false);

            offset += copied;
            length -= copied;
        }
    }

    @Override
    public void flush()
            throws IOException
    {
        ensureOpen();
        flushBuffer(false);
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Output stream closed: " + location);
        }
    }

    private void flushBuffer(boolean finished)
            throws IOException
    {
        // skip multipart upload if there would only be one staged block
        if (finished && !multiStagingStarted) {
            BlobOutputStream blobOutputStream = blockClient
                    .getBlobOutputStream(overwrite);

            buffer.takeInputStream().transferTo(blobOutputStream);
            blobOutputStream.close();
            memoryContext.addBytes(-buffer.allocated());
            buffer = null;
            return;
        }

        if (buffer.size() != writeBlockSizeBytes && (!finished || buffer.size() == 0)) {
            // If the buffer isn't full yet and we are not finished, do not stage the block
            return;
        }

        multiStagingStarted = true;

        LinkedBuffer data = buffer;

        if (finished) {
            this.buffer = null;
        }
        else {
            this.buffer = data.startNextPart();
            memoryContext.addBytes(buffer.allocated());
        }
        String nextBlockId = nextBlockId();
        stagedBlockFutures.add(submit(stageBlock(nextBlockId, data), uploadExecutor));
    }

    private Callable<String> stageBlock(String blockId, LinkedBuffer data)
    {
        return () -> {
            // the block must be replayable for retries, which a list of byte buffers is without copying
            blockClient.stageBlock(blockId, BinaryData.fromListByteBuffer(data.takeByteBuffers()));
            memoryContext.addBytes(-data.allocated());
            return blockId;
        };
    }

    private synchronized String nextBlockId()
    {
        String blockId = Base64.getEncoder().encodeToString(String.format("%06d", blockNum).getBytes(UTF_8));
        blockNum++;
        stagedBlocks.add(blockId);
        return blockId;
    }

    private void waitForUploadsToFinish()
            throws IOException
    {
        if (stagedBlockFutures.isEmpty()) {
            return;
        }

        try {
            Futures.allAsList(stagedBlockFutures).get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        }
        catch (ExecutionException e) {
            throw handleAzureException(e.getCause(), "upload", location);
        }
    }

    private void commitBlocksIfNeeded()
    {
        if (stagedBlocks.isEmpty()) {
            return;
        }

        blockClient.commitBlockList(stagedBlocks, overwrite);
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            flushBuffer(true);
            waitForUploadsToFinish();
            commitBlocksIfNeeded();
            memoryContext.setBytes(0);
            memoryContext.close();
        }
        catch (IOException | RuntimeException e) {
            throw handleAzureException(e, "upload", location);
        }
    }

    @Override
    public void abort()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        buffer = null;

        // staged blocks are discarded unless the block list is committed
        try {
            waitForUploadsToFinish();
        }
        finally {
            memoryContext.setBytes(0);
            memoryContext.close();
        }
    }
}
