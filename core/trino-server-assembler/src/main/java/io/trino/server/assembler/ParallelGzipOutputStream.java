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
package io.trino.server.assembler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * A gzip output stream that compresses fixed size chunks in parallel.
 * <p>
 * The gzip format allows members to be concatenated, and every decompressor
 * treats the concatenation as a single stream. Each chunk is therefore deflated
 * independently on a worker thread and the resulting members are written in
 * submission order, which keeps the output byte for byte deterministic no matter
 * how the work is scheduled.
 * <p>
 * Compressing the {@code trino-server} distribution is otherwise the longest step
 * of the build, and it is entirely CPU bound.
 */
final class ParallelGzipOutputStream
        extends OutputStream
{
    private static final int GZIP_MAGIC_FIRST = 0x1F;
    private static final int GZIP_MAGIC_SECOND = 0x8B;
    private static final int DEFLATE_METHOD = 8;
    private static final int UNKNOWN_OPERATING_SYSTEM = 255;

    private final OutputStream output;
    private final ExecutorService executor;
    private final Deque<Future<byte[]>> pending = new ArrayDeque<>();
    private final int compressionLevel;
    private final int chunkSize;
    private final int maximumPending;

    private byte[] chunk;
    private int position;
    private boolean closed;

    public ParallelGzipOutputStream(OutputStream output, int compressionLevel, int chunkSize, int threads)
    {
        checkArgument(threads > 0, "threads must be at least one");
        checkArgument(chunkSize > 0, "chunkSize must be positive");

        this.output = requireNonNull(output, "output is null");
        this.compressionLevel = compressionLevel;
        this.chunkSize = chunkSize;
        // Bounding the queue bounds memory: at most this many chunks are held at once.
        this.maximumPending = threads * 2;
        this.executor = Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable, "gzip-compressor");
            thread.setDaemon(true);
            return thread;
        });
        this.chunk = new byte[chunkSize];
    }

    @Override
    public void write(int value)
            throws IOException
    {
        write(new byte[] {(byte) value}, 0, 1);
    }

    @Override
    public void write(byte[] source, int offset, int length)
            throws IOException
    {
        checkArgument(!closed, "Stream is closed");
        int remaining = length;
        int start = offset;
        while (remaining > 0) {
            int count = Math.min(remaining, chunkSize - position);
            System.arraycopy(source, start, chunk, position, count);
            position += count;
            start += count;
            remaining -= count;
            if (position == chunkSize) {
                submit();
            }
        }
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
            if (position > 0) {
                submit();
            }
            drain(0);
        }
        finally {
            executor.shutdownNow();
            output.close();
        }
    }

    private void submit()
            throws IOException
    {
        byte[] source = chunk;
        int length = position;
        pending.addLast(executor.submit(() -> deflate(source, length, compressionLevel)));
        chunk = new byte[chunkSize];
        position = 0;
        drain(maximumPending);
    }

    /**
     * Writes completed members until no more than {@code limit} remain outstanding.
     * Members are always written in submission order.
     */
    private void drain(int limit)
            throws IOException
    {
        while (pending.size() > limit) {
            Future<byte[]> member = pending.removeFirst();
            try {
                output.write(member.get());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while compressing", e);
            }
            catch (ExecutionException e) {
                if (e.getCause() instanceof UncheckedIOException cause) {
                    throw cause.getCause();
                }
                throw new IOException("Failed to compress", e.getCause());
            }
        }
    }

    /**
     * Deflates one chunk into a self contained gzip member.
     */
    private static byte[] deflate(byte[] source, int length, int compressionLevel)
    {
        ByteArrayOutputStream member = new ByteArrayOutputStream(length / 2);
        // A fixed header keeps the output reproducible; the modification time and
        // operating system fields carry no information worth preserving here.
        member.write(GZIP_MAGIC_FIRST);
        member.write(GZIP_MAGIC_SECOND);
        member.write(DEFLATE_METHOD);
        for (int i = 0; i < 6; i++) {
            member.write(0);
        }
        member.write(UNKNOWN_OPERATING_SYSTEM);

        Deflater deflater = new Deflater(compressionLevel, true);
        try (DeflaterOutputStream deflaterStream = new DeflaterOutputStream(member, deflater, 8192)) {
            deflaterStream.write(source, 0, length);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            deflater.end();
        }

        CRC32 crc = new CRC32();
        crc.update(source, 0, length);
        writeLittleEndian(member, crc.getValue());
        writeLittleEndian(member, length);
        return member.toByteArray();
    }

    private static void writeLittleEndian(ByteArrayOutputStream output, long value)
    {
        output.write((int) (value & 0xFF));
        output.write((int) ((value >> 8) & 0xFF));
        output.write((int) ((value >> 16) & 0xFF));
        output.write((int) ((value >> 24) & 0xFF));
    }
}
