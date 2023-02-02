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
package io.trino.hive.formats.compression;

import io.airlift.slice.Slice;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

// This specialized SliceOutput has direct access buffered output slices to
// report buffer sizes and to get the final output.  Additionally, a new
// CompressedSliceOutput can be created that reuses the underlying output
// buffer
public final class MemoryCompressedSliceOutput
        extends BufferedOutputStreamSliceOutput
{
    private final ChunkedSliceOutput bufferedOutput;
    private final Supplier<MemoryCompressedSliceOutput> resetFactory;
    private final Runnable onDestroy;
    private boolean closed;
    private boolean destroyed;

    /**
     * @param compressionStream the compressed output stream to delegate to
     * @param bufferedOutput the output for the compressionStream
     * @param resetFactory the function to create a new CompressedSliceOutput that reuses the bufferedOutput
     * @param onDestroy used to cleanup the compression when done
     */
    public MemoryCompressedSliceOutput(
            OutputStream compressionStream,
            ChunkedSliceOutput bufferedOutput,
            Supplier<MemoryCompressedSliceOutput> resetFactory,
            Runnable onDestroy)
    {
        super(compressionStream);
        this.bufferedOutput = requireNonNull(bufferedOutput, "bufferedOutput is null");
        this.resetFactory = requireNonNull(resetFactory, "resetFactory is null");
        this.onDestroy = requireNonNull(onDestroy, "onDestroy is null");
    }

    @Override
    public long getRetainedSize()
    {
        return super.getRetainedSize() + bufferedOutput.getRetainedSize();
    }

    public int getCompressedSize()
    {
        checkState(closed, "Stream has not been closed");
        checkState(!destroyed, "Stream has been destroyed");
        return bufferedOutput.size();
    }

    public List<Slice> getCompressedSlices()
    {
        checkState(closed, "Stream has not been closed");
        checkState(!destroyed, "Stream has been destroyed");
        return bufferedOutput.getSlices();
    }

    public MemoryCompressedSliceOutput createRecycledCompressedSliceOutput()
    {
        checkState(closed, "Stream has not been closed");
        checkState(!destroyed, "Stream has been destroyed");
        destroyed = true;
        return resetFactory.get();
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            super.close();
        }
    }

    public void destroy()
            throws IOException
    {
        if (!destroyed) {
            destroyed = true;
            try {
                close();
            }
            finally {
                onDestroy.run();
            }
        }
    }

    public static MemoryCompressedSliceOutput createUncompressedMemorySliceOutput(int minChunkSize, int maxChunkSize)
    {
        return new UncompressedSliceOutputSupplier(minChunkSize, maxChunkSize).get();
    }

    private static class UncompressedSliceOutputSupplier
            implements Supplier<MemoryCompressedSliceOutput>
    {
        private final ChunkedSliceOutput chunkedSliceOutput;

        private UncompressedSliceOutputSupplier(int minChunkSize, int maxChunkSize)
        {
            chunkedSliceOutput = new ChunkedSliceOutput(minChunkSize, maxChunkSize);
        }

        @Override
        public MemoryCompressedSliceOutput get()
        {
            chunkedSliceOutput.reset();
            return new MemoryCompressedSliceOutput(chunkedSliceOutput, chunkedSliceOutput, this, () -> {});
        }
    }
}
