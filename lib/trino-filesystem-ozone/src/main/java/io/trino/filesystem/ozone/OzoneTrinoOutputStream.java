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
package io.trino.filesystem.ozone;

import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;

import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

class OzoneTrinoOutputStream
        extends OutputStream
{
    private static final int BUFFER_SIZE = 8192;
    // TODO
    private long writeBlockSizeBytes = DataSize.of(4, DataSize.Unit.MEGABYTE).toBytes();

    private final OzoneLocation location;
    private final OutputStream stream;
    private final LocalMemoryContext memoryContext;
    private long writtenBytes;
    private boolean closed;

    public OzoneTrinoOutputStream(
            OzoneLocation location,
            ObjectStore store,
            AggregatedMemoryContext memoryContext)
            throws IOException
    {
        requireNonNull(location, "location is null");
        requireNonNull(store, "blobClient is null");

        this.location = location;

        try {
            OzoneVolume ozoneVolume = store.getVolume(location.volume());
            OzoneBucket bucket = ozoneVolume.getBucket(location.bucket());
            // TODO
//            ReplicationConfig replicationConfig = RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.valueOf(1));
//            OzoneDataStreamOutput ozoneOutputStream = bucket.createStreamKey(location.key(), 0, replicationConfig, Collections.emptyMap());
            // createMultipartKey?
            ReplicationConfig replicationConfig = StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.valueOf(1));
            OzoneOutputStream ozoneOutputStream = bucket.createKey(location.key(), 0, replicationConfig, Collections.emptyMap());

            // TODO It is not clear if the buffered stream helps or hurts... the underlying implementation seems to copy every write to a byte buffer so small writes will suffer
            stream = new BufferedOutputStream(ozoneOutputStream, BUFFER_SIZE);
        }
        catch (RuntimeException e) {
            // TODO
            throw e;
        }

        // TODO to track memory we will need to fork OzoneOutputStream(KeyOutputStream)
        this.memoryContext = memoryContext.newLocalMemoryContext(OzoneTrinoOutputStream.class.getSimpleName());
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
            // TODO
            throw e;
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
            // TODO
            throw e;
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
            // TODO
            throw e;
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
                // TODO: update
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
        // TODO
        if (writtenBytes < writeBlockSizeBytes) {
            // assume that there is only one pending block buffer, and that it grows as written bytes grow
            memoryContext.setBytes(BUFFER_SIZE + min(writtenBytes + size, writeBlockSizeBytes));
        }
        writtenBytes += size;
    }
}
