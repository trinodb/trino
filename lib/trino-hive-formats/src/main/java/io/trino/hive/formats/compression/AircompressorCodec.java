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
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class AircompressorCodec
        implements Codec
{
    private final HadoopStreams hadoopStreams;

    public AircompressorCodec(HadoopStreams hadoopStreams)
    {
        this.hadoopStreams = requireNonNull(hadoopStreams, "hadoopStreams is null");
    }

    @Override
    public OutputStream createStreamCompressor(OutputStream outputStream)
            throws IOException
    {
        return hadoopStreams.createOutputStream(outputStream);
    }

    @Override
    public ValueCompressor createValueCompressor()
    {
        return new AircompressorValueCompressor(hadoopStreams);
    }

    private static class AircompressorValueCompressor
            implements ValueCompressor
    {
        private final HadoopStreams hadoopStreams;
        private final DynamicSliceOutput buffer;

        private AircompressorValueCompressor(HadoopStreams hadoopStreams)
        {
            this.hadoopStreams = requireNonNull(hadoopStreams, "hadoopStreams is null");
            this.buffer = new DynamicSliceOutput(1024);
        }

        @Override
        public Slice compress(Slice slice)
                throws IOException
        {
            buffer.reset();
            try (OutputStream compressionStream = hadoopStreams.createOutputStream(buffer)) {
                slice.getInput().transferTo(compressionStream);
            }
            return buffer.slice();
        }
    }

    @Override
    public MemoryCompressedSliceOutput createMemoryCompressedSliceOutput(int minChunkSize, int maxChunkSize)
    {
        return new AircompressorCompressedSliceOutputSupplier(hadoopStreams, minChunkSize, maxChunkSize).get();
    }

    // this can be dramatically simplified when actual hadoop codecs are dropped
    private static class AircompressorCompressedSliceOutputSupplier
            implements Supplier<MemoryCompressedSliceOutput>
    {
        private final HadoopStreams hadoopStreams;
        private final ChunkedSliceOutput compressedOutput;

        public AircompressorCompressedSliceOutputSupplier(HadoopStreams hadoopStreams, int minChunkSize, int maxChunkSize)
        {
            this.hadoopStreams = requireNonNull(hadoopStreams, "hadoopStreams is null");
            this.compressedOutput = new ChunkedSliceOutput(minChunkSize, maxChunkSize);
        }

        @Override
        public MemoryCompressedSliceOutput get()
        {
            try {
                compressedOutput.reset();
                OutputStream compressionStream = hadoopStreams.createOutputStream(compressedOutput);
                return new MemoryCompressedSliceOutput(compressionStream, compressedOutput, this, () -> {});
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public InputStream createStreamDecompressor(InputStream inputStream)
            throws IOException
    {
        return hadoopStreams.createInputStream(inputStream);
    }

    @Override
    public ValueDecompressor createValueDecompressor()
    {
        return new AircompressorValueDecompressor(hadoopStreams);
    }

    private record AircompressorValueDecompressor(HadoopStreams hadoopStreams)
            implements ValueDecompressor
    {
        @Override
        public void decompress(Slice compressed, OutputStream uncompressed)
                throws IOException
        {
            try (InputStream decompressorStream = hadoopStreams.createInputStream(compressed.getInput())) {
                decompressorStream.transferTo(uncompressed);
            }
            catch (IndexOutOfBoundsException | IOException e) {
                throw new IOException("Compressed stream is truncated", e);
            }
        }

        @Override
        public void decompress(Slice compressed, Slice uncompressed)
                throws IOException
        {
            try (InputStream decompressorStream = hadoopStreams.createInputStream(compressed.getInput())) {
                uncompressed.setBytes(0, decompressorStream, uncompressed.length());
            }
            catch (IndexOutOfBoundsException | IOException e) {
                throw new IOException("Compressed stream is truncated", e);
            }
        }
    }
}
