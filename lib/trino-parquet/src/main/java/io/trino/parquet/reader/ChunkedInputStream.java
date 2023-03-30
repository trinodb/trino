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
package io.trino.parquet.reader;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ChunkReader;
import io.trino.parquet.ParquetReaderOptions;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.io.ByteStreams.readFully;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static java.util.Objects.requireNonNull;

/**
 * A single continuous {@link InputStream} over multiple {@link Slice}s read on demand using given collection of {@link ChunkReader}s.
 * It is used to read parquet column chunk in limited (small) byte chunks (8MB by default, controlled by {@link ParquetReaderOptions#getMaxReadBlockSize()}).
 * Column chunks consists of multiple pages.
 * This abstraction is used because the page size is unknown until the page header is read
 * and page header and page data can be split between two or more byte chunks.
 */
public final class ChunkedInputStream
        extends InputStream
{
    private final Iterator<? extends ChunkReader> chunks;
    private ChunkReader currentChunkReader;
    // current is explicitly initialized to EMPTY_SLICE as this field is set to null when the stream is closed
    private BasicSliceInput current = EMPTY_SLICE.getInput();

    public ChunkedInputStream(Collection<? extends ChunkReader> chunks)
    {
        requireNonNull(chunks, "chunks is null");
        checkArgument(!chunks.isEmpty(), "At least one chunk is expected but got none");
        this.chunks = chunks.iterator();
    }

    public Slice getSlice(int length)
            throws IOException
    {
        if (length == 0) {
            return EMPTY_SLICE;
        }
        ensureOpen();
        while (!current.isReadable()) {
            checkArgument(chunks.hasNext(), "Requested %s bytes but 0 was available", length);
            readNextChunk();
        }
        if (current.available() >= length) {
            return current.readSlice(length);
        }
        // requested length crosses the slice boundary
        byte[] bytes = new byte[length];
        try {
            readFully(this, bytes, 0, bytes.length);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read " + length + " bytes", e);
        }
        return Slices.wrappedBuffer(bytes);
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        checkPositionIndexes(off, off + len, b.length);
        if (len == 0) {
            return 0;
        }
        ensureOpen();
        while (!current.isReadable()) {
            if (!chunks.hasNext()) {
                return -1;
            }
            readNextChunk();
        }

        return current.read(b, off, len);
    }

    @Override
    public int read()
            throws IOException
    {
        ensureOpen();
        while (!current.isReadable() && chunks.hasNext()) {
            readNextChunk();
        }

        return current.read();
    }

    @Override
    public int available()
            throws IOException
    {
        ensureOpen();
        return current.available();
    }

    @Override
    public void close()
    {
        if (current == null) {
            // already closed
            return;
        }
        if (currentChunkReader != null) {
            currentChunkReader.free();
        }
        while (chunks.hasNext()) {
            chunks.next().free();
        }
        current = null;
    }

    private void ensureOpen()
            throws IOException
    {
        if (current == null) {
            throw new IOException("Stream closed");
        }
    }

    private void readNextChunk()
    {
        if (currentChunkReader != null) {
            currentChunkReader.free();
        }
        currentChunkReader = chunks.next();
        Slice slice = currentChunkReader.readUnchecked();
        checkArgument(slice.length() > 0, "all chunks have to be not empty");
        current = slice.getInput();
    }
}
