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

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.CpkInfo;
import com.azure.storage.blob.models.CustomerProvidedKey;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.common.implementation.Constants.HeaderConstants;
import io.trino.filesystem.TrinoOutputStream;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import reactor.core.publisher.Sinks;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.filesystem.azure.AzureUtils.handleAzureException;
import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

class AzureOutputStream
        extends TrinoOutputStream
{
    private static final int BUFFER_SIZE = 8192;

    private final AzureLocation location;
    private final long writeBlockSizeBytes;
    private final Sinks.Many<ByteBuffer> sink = Sinks.many().unicast().onBackpressureBuffer(new ProducerBlockingQueue());
    private final CompletableFuture<Response<BlockBlobItem>> upload;
    private final OutputStream stream;
    private final LocalMemoryContext memoryContext;
    private long writtenBytes;
    private boolean closed;
    private boolean failed;

    public AzureOutputStream(
            AzureLocation location,
            BlobClient blobClient,
            boolean overwrite,
            AggregatedMemoryContext memoryContext,
            long writeBlockSizeBytes,
            int maxWriteConcurrency,
            long maxSingleUploadSizeBytes)
            throws IOException
    {
        requireNonNull(location, "location is null");
        requireNonNull(blobClient, "blobClient is null");
        checkArgument(writeBlockSizeBytes >= 0, "writeBlockSizeBytes is negative");
        checkArgument(maxWriteConcurrency >= 0, "maxWriteConcurrency is negative");
        checkArgument(maxSingleUploadSizeBytes >= 0, "maxSingleUploadSizeBytes is negative");

        this.location = location;
        this.writeBlockSizeBytes = writeBlockSizeBytes;
        BlobParallelUploadOptions uploadOptions = new BlobParallelUploadOptions(sink.asFlux())
                .setParallelTransferOptions(new ParallelTransferOptions()
                        .setBlockSizeLong(writeBlockSizeBytes)
                        .setMaxConcurrency(maxWriteConcurrency)
                        .setMaxSingleUploadSizeLong(maxSingleUploadSizeBytes));
        if (!overwrite) {
            // This is not enforced until data is written
            uploadOptions.setRequestConditions(new BlobRequestConditions().setIfNoneMatch(HeaderConstants.ETAG_WILDCARD));
        }

        try {
            upload = createAsyncClient(blobClient).uploadWithResponse(uploadOptions).toFuture();
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "creating file", location);
        }
        // the inner stream copies each write into the upload, so the buffer coalesces small writes
        stream = new BufferedOutputStream(new UploadOutputStream(), BUFFER_SIZE);

        this.memoryContext = memoryContext.newLocalMemoryContext(AzureOutputStream.class.getSimpleName());
        this.memoryContext.setBytes(BUFFER_SIZE);
    }

    @Override
    public void write(int b)
            throws IOException
    {
        ensureOpen();
        try {
            stream.write(b);
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "writing file", location);
        }
        recordBytesWritten(1);
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        checkFromIndexSize(offset, length, buffer.length);

        ensureOpen();
        try {
            stream.write(buffer, offset, length);
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "writing file", location);
        }
        recordBytesWritten(length);
    }

    @Override
    public void flush()
            throws IOException
    {
        ensureOpen();
        try {
            stream.flush();
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "writing file", location);
        }
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Output stream closed: " + location);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            try {
                stream.close();
            }
            catch (IOException | RuntimeException e) {
                // the block list is not committed yet, so cancelling keeps a partial blob from appearing
                upload.cancel(true);
                // Azure close sometimes rethrows IOExceptions from worker threads, so the
                // stack traces are disconnected from this call. Wrapping here solves that problem.
                throw new IOException("Error closing file: " + location, e);
            }
            finally {
                memoryContext.close();
            }
        }
    }

    @Override
    public void abort()
    {
        if (!closed) {
            closed = true;
            // cancelling stops in-flight block uploads, and the block list is committed only after the sink completes
            upload.cancel(true);
            memoryContext.close();
        }
    }

    private void recordBytesWritten(int size)
    {
        if (writtenBytes < writeBlockSizeBytes) {
            // assume that there is only one pending block buffer, and that it grows as written bytes grow
            memoryContext.setBytes(BUFFER_SIZE + min(writtenBytes + size, writeBlockSizeBytes));
        }
        writtenBytes += size;
    }

    private void waitForUpload()
            throws IOException
    {
        try {
            upload.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        }
        catch (ExecutionException e) {
            throw handleAzureException(e.getCause(), "uploading", location);
        }
    }

    // Matches the client that BlockBlobClient.getBlobOutputStream builds for its upload
    private static BlobAsyncClient createAsyncClient(BlobClient blobClient)
    {
        BlobClientBuilder builder = new BlobClientBuilder()
                .pipeline(blobClient.getHttpPipeline())
                .endpoint(blobClient.getBlobUrl())
                .serviceVersion(blobClient.getServiceVersion());
        CpkInfo customerProvidedKey = blobClient.getCustomerProvidedKey();
        if (customerProvidedKey != null) {
            builder.customerProvidedKey(new CustomerProvidedKey(customerProvidedKey.getEncryptionKey()));
        }
        return builder.buildAsyncClient();
    }

    private final class UploadOutputStream
            extends OutputStream
    {
        @Override
        public void write(int b)
                throws IOException
        {
            write(new byte[] {(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] buffer, int offset, int length)
                throws IOException
        {
            // the upload reads the bytes asynchronously, so the caller's buffer is copied
            ByteBuffer copy = ByteBuffer.wrap(Arrays.copyOfRange(buffer, offset, offset + length));
            Sinks.EmitResult result;
            try {
                result = sink.tryEmitNext(copy);
            }
            catch (RuntimeException e) {
                // an interrupted wait for buffer space leaves the upload running, so close must not commit
                failed = true;
                throw e;
            }
            if (result.isFailure()) {
                // the sink rejects data only after the upload has failed, so waiting returns at once
                // and rethrows the upload's actual exception instead of a generic message
                failed = true;
                waitForUpload();
                throw new IOException("Upload stopped accepting data (%s): %s".formatted(result, location));
            }
        }

        @Override
        public void close()
                throws IOException
        {
            if (failed) {
                // data went missing, so the block list must not be committed
                throw new IOException("Error closing file after failed write: " + location);
            }
            // a failed upload rejects completion, and waitForUpload reports its failure
            sink.tryEmitComplete();
            waitForUpload();
        }
    }

    // Blocks the writer until the upload takes the previous buffer, which bounds buffered data
    private static final class ProducerBlockingQueue
            extends LinkedBlockingQueue<ByteBuffer>
    {
        ProducerBlockingQueue()
        {
            super(1);
        }

        @Override
        public boolean offer(ByteBuffer buffer)
        {
            try {
                put(buffer);
                return true;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
