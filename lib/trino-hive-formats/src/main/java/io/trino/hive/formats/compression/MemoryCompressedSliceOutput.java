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

import io.airlift.compress.hadoop.HadoopStreams;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

// This specialized SliceOutput has direct access buffered output slices to
// report buffer sizes and to get the final output.  Additionally, a new
// CompressedSliceOutput can be created that reuses the underlying output
// buffer
public final class MemoryCompressedSliceOutput
        extends BufferedOutputStreamSliceOutput
{
    private final HadoopStreams hadoopStreams;
    private final ChunkedSliceOutput bufferedOutput;
    private boolean closed;
    private boolean destroyed;

    MemoryCompressedSliceOutput(HadoopStreams hadoopStreams, int minChunkSize, int maxChunkSize)
            throws IOException
    {
        this(hadoopStreams, new ChunkedSliceOutput(minChunkSize, maxChunkSize));
    }

    private MemoryCompressedSliceOutput(HadoopStreams hadoopStreams, ChunkedSliceOutput bufferedOutput)
            throws IOException
    {
        super(hadoopStreams == null ? bufferedOutput : hadoopStreams.createOutputStream(bufferedOutput));
        this.hadoopStreams = hadoopStreams;
        this.bufferedOutput = requireNonNull(bufferedOutput, "bufferedOutput is null");
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
            throws IOException
    {
        checkState(closed, "Stream has not been closed");
        checkState(!destroyed, "Stream has been destroyed");
        destroyed = true;

        bufferedOutput.reset();
        return new MemoryCompressedSliceOutput(hadoopStreams, bufferedOutput);
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
            close();
        }
    }

    public static MemoryCompressedSliceOutput createUncompressedMemorySliceOutput(int minChunkSize, int maxChunkSize)
            throws IOException
    {
        return new MemoryCompressedSliceOutput(null, new ChunkedSliceOutput(minChunkSize, maxChunkSize));
    }
}
