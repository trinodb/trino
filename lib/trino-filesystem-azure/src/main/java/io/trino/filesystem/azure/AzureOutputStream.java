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
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlockBlobOutputStreamOptions;
import com.azure.storage.common.implementation.Constants.HeaderConstants;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.filesystem.azure.AzureUtils.handleAzureException;
import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

class AzureOutputStream
        extends OutputStream
{
    private static final int BUFFER_SIZE = 8192;

    private final AzureLocation location;
    private final long writeBlockSizeBytes;
    private final OutputStream stream;
    private final LocalMemoryContext memoryContext;
    private long writtenBytes;
    private boolean closed;

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
        BlockBlobOutputStreamOptions streamOptions = new BlockBlobOutputStreamOptions();
        streamOptions.setParallelTransferOptions(new ParallelTransferOptions()
                .setBlockSizeLong(writeBlockSizeBytes)
                .setMaxConcurrency(maxWriteConcurrency)
                .setMaxSingleUploadSizeLong(maxSingleUploadSizeBytes));
        if (!overwrite) {
            // This is not enforced until data is written
            streamOptions.setRequestConditions(new BlobRequestConditions().setIfNoneMatch(HeaderConstants.ETAG_WILDCARD));
        }

        try {
            // TODO It is not clear if the buffered stream helps or hurts... the underlying implementation seems to copy every write to a byte buffer so small writes will suffer
            stream = new BufferedOutputStream(blobClient.getBlockBlobClient().getBlobOutputStream(streamOptions), BUFFER_SIZE);
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "creating file", location);
        }

        // TODO to track memory we will need to fork com.azure.storage.blob.specialized.BlobOutputStream.BlockBlobOutputStream
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
            catch (IOException e) {
                // Azure close sometimes rethrows IOExceptions from worker threads, so the
                // stack traces are disconnected from this call. Wrapping here solves that problem.
                throw new IOException("Error closing file: " + location, e);
            }
            finally {
                memoryContext.close();
            }
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
}
