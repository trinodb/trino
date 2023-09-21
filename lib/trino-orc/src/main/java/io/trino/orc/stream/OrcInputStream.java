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
package io.trino.orc.stream;

import com.google.common.primitives.Ints;
import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.OrcDataSourceId;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.orc.checkpoint.InputStreamCheckpoint.createInputStreamCheckpoint;
import static io.trino.orc.checkpoint.InputStreamCheckpoint.decodeCompressedBlockOffset;
import static io.trino.orc.checkpoint.InputStreamCheckpoint.decodeDecompressedOffset;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class OrcInputStream
        extends InputStream
{
    private final OrcChunkLoader chunkLoader;

    // 8 byte temp buffer for reading multibyte values that straddle a buffer boundary
    private final Slice tempBuffer8 = Slices.allocate(8);

    @Nullable
    private FixedLengthSliceInput current = EMPTY_SLICE.getInput();
    private long lastCheckpoint;

    public OrcInputStream(OrcChunkLoader chunkLoader)
    {
        this.chunkLoader = requireNonNull(chunkLoader, "chunkLoader is null");
    }

    @Override
    public void close()
    {
        // close is never called, so do not add code here
    }

    @Override
    public int available()
    {
        if (current == null) {
            return 0;
        }
        return current.available();
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    @Override
    public int read()
            throws IOException
    {
        if (current == null) {
            return -1;
        }

        int result = current.read();
        if (result != -1) {
            return result;
        }

        advance();
        return read();
    }

    @Override
    public int read(byte[] b, int off, int length)
            throws IOException
    {
        if (current == null) {
            return -1;
        }

        if (current.remaining() == 0) {
            advance();
            if (current == null) {
                return -1;
            }
        }

        return current.read(b, off, length);
    }

    public void skipFully(long length)
            throws IOException
    {
        while (length > 0) {
            long result = skip(length);
            if (result < 0) {
                throw new OrcCorruptionException(chunkLoader.getOrcDataSourceId(), "Unexpected end of stream");
            }
            length -= result;
        }
    }

    public void readFully(byte[] buffer, int offset, int length)
            throws IOException
    {
        while (offset < length) {
            int result = read(buffer, offset, length - offset);
            if (result < 0) {
                throw new OrcCorruptionException(chunkLoader.getOrcDataSourceId(), "Unexpected end of stream");
            }
            offset += result;
        }
    }

    public void readFully(int[] values, int offset, int length)
            throws IOException
    {
        if (current == null) {
            throw new OrcCorruptionException(chunkLoader.getOrcDataSourceId(), "Unexpected end of stream");
        }

        while (length > 0) {
            int remaining = Ints.saturatedCast(current.remaining());
            if (remaining < Integer.BYTES) {
                // there might be a value split across the buffers
                Slice slice = null;
                if (remaining != 0) {
                    slice = tempBuffer8;
                    current.readBytes(slice, 0, remaining);
                }

                advance();
                if (current == null) {
                    throw new OrcCorruptionException(chunkLoader.getOrcDataSourceId(), "Unexpected end of stream");
                }

                if (remaining != 0) {
                    current.readBytes(slice, remaining, Integer.BYTES - remaining);
                    values[offset] = slice.getInt(0);
                    length--;
                    offset++;
                }
                remaining = Ints.saturatedCast(current.remaining());
            }

            int chunkSize = min(length, remaining / Integer.BYTES);
            current.readInts(values, offset, chunkSize);
            length -= chunkSize;
            offset += chunkSize;
        }
    }

    public void readFully(long[] values, int offset, int length)
            throws IOException
    {
        if (current == null) {
            throw new OrcCorruptionException(chunkLoader.getOrcDataSourceId(), "Unexpected end of stream");
        }

        while (length > 0) {
            int remaining = Ints.saturatedCast(current.remaining());
            if (remaining < Long.BYTES) {
                // there might be a value split across the buffers
                Slice slice = null;
                if (remaining != 0) {
                    slice = tempBuffer8;
                    current.readBytes(slice, 0, remaining);
                }

                advance();
                if (current == null) {
                    throw new OrcCorruptionException(chunkLoader.getOrcDataSourceId(), "Unexpected end of stream");
                }

                if (remaining != 0) {
                    current.readBytes(slice, remaining, Long.BYTES - remaining);
                    values[offset] = slice.getLong(0);
                    length--;
                    offset++;
                }
                remaining = Ints.saturatedCast(current.remaining());
            }

            int chunkSize = min(length, remaining / Long.BYTES);
            current.readLongs(values, offset, chunkSize);
            length -= chunkSize;
            offset += chunkSize;
        }
    }

    public OrcDataSourceId getOrcDataSourceId()
    {
        return chunkLoader.getOrcDataSourceId();
    }

    public long getCheckpoint()
    {
        long checkpoint = chunkLoader.getLastCheckpoint();
        if (current != null && current.position() > 0) {
            checkpoint = createInputStreamCheckpoint(decodeCompressedBlockOffset(checkpoint), toIntExact(decodeDecompressedOffset(checkpoint) + current.position()));
        }
        return checkpoint;
    }

    public void seekToCheckpoint(long checkpoint)
            throws IOException
    {
        int compressedOffset = decodeCompressedBlockOffset(checkpoint);
        int decompressedOffset = decodeDecompressedOffset(checkpoint);
        // if checkpoint is within the current buffer, seek locally
        int currentDecompressedBufferOffset = decodeDecompressedOffset(lastCheckpoint);
        if (current != null && compressedOffset == decodeCompressedBlockOffset(lastCheckpoint) && decompressedOffset < currentDecompressedBufferOffset + current.length()) {
            current.setPosition(decompressedOffset - currentDecompressedBufferOffset);
            return;
        }
        // otherwise, drop the current buffer and seek the underlying data loader
        current = EMPTY_SLICE.getInput();
        chunkLoader.seekToCheckpoint(checkpoint);
        lastCheckpoint = checkpoint;
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        if (current == null || n <= 0) {
            return -1;
        }

        long result = current.skip(n);
        if (result != 0) {
            return result;
        }
        if (read() == -1) {
            return 0;
        }
        return 1 + current.skip(n - 1);
    }

    // This comes from the Apache Hive ORC code
    private void advance()
            throws IOException
    {
        if (!chunkLoader.hasNextChunk()) {
            current = null;
            return;
        }
        current = chunkLoader.nextChunk().getInput();
        lastCheckpoint = chunkLoader.getLastCheckpoint();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("source", chunkLoader)
                .add("uncompressedOffset", current == null ? null : current.position())
                .toString();
    }
}
