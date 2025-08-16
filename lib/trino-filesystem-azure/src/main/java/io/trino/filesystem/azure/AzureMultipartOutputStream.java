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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.submit;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.trino.filesystem.azure.AzureUtils.handleAzureException;
import static java.lang.Math.clamp;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class AzureMultipartOutputStream
        extends OutputStream
{
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

    private byte[] buffer = new byte[0];
    private int bufferSize;
    private int initialBufferSize = 64;

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
    }

    @Override
    public void write(int b)
            throws IOException
    {
        ensureOpen();
        ensureCapacity(1);
        buffer[bufferSize] = (byte) b;
        bufferSize++;
        flushBuffer(false);
    }

    @Override
    public void write(byte[] bytes, int offset, int length)
            throws IOException
    {
        ensureOpen();

        while (length > 0) {
            ensureCapacity(length);

            int copied = min(buffer.length - bufferSize, length);
            arraycopy(bytes, offset, buffer, bufferSize, copied);
            bufferSize += copied;

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

    private void ensureCapacity(int extra)
    {
        int currentCapacity = buffer.length;
        int capacity = min(writeBlockSizeBytes, bufferSize + extra);
        if (buffer.length < capacity) {
            int target = max(buffer.length, initialBufferSize);
            if (target < capacity) {
                target += target / 2; // increase 50%
                target = clamp(target, capacity, writeBlockSizeBytes);
            }
            buffer = Arrays.copyOf(buffer, target);
            memoryContext.addBytes(target - currentCapacity);
        }
    }

    private void flushBuffer(boolean finished)
            throws IOException
    {
        // skip multipart upload if there would only be one staged block
        if (finished && !multiStagingStarted) {
            BlobOutputStream blobOutputStream = blockClient
                    .getBlobOutputStream(overwrite);

            blobOutputStream.write(buffer, 0, bufferSize);
            blobOutputStream.close();
            memoryContext.addBytes(-buffer.length);
            buffer = new byte[0];
            return;
        }

        if (bufferSize != writeBlockSizeBytes && (!finished || bufferSize == 0)) {
            // If the buffer isn't full yet and we are not finished, do not stage the block
            return;
        }

        multiStagingStarted = true;

        byte[] data = buffer;
        int length = bufferSize;

        if (finished) {
            this.buffer = null;
        }
        else {
            this.buffer = new byte[0];
            this.initialBufferSize = writeBlockSizeBytes;
            bufferSize = 0;
        }
        String nextBlockId = nextBlockId();
        stagedBlockFutures.add(submit(stageBlock(nextBlockId, data, length), uploadExecutor));
    }

    private Callable<String> stageBlock(String blockId, byte[] data, int length)
    {
        return () -> {
            blockClient.stageBlock(blockId, new ByteArrayInputStream(data, 0, length), length);
            memoryContext.addBytes(-length);
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
}
