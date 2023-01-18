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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class AircompressorCodec
        implements Codec
{
    // Airlift Codecs are assumed to not retain memory and are assumed to not be pooled
    private final CompressionCodec codec;

    public AircompressorCodec(CompressionCodec codec)
    {
        this.codec = requireNonNull(codec, "codec is null");
    }

    @Override
    public OutputStream createStreamCompressor(OutputStream outputStream)
            throws IOException
    {
        return codec.createOutputStream(outputStream);
    }

    @Override
    public ValueCompressor createValueCompressor()
    {
        return new AircompressorValueCompressor(codec);
    }

    private static class AircompressorValueCompressor
            implements ValueCompressor
    {
        private final CompressionCodec codec;
        private final DynamicSliceOutput buffer;

        private AircompressorValueCompressor(CompressionCodec codec)
        {
            this.codec = requireNonNull(codec, "codec is null");
            this.buffer = new DynamicSliceOutput(1024);
        }

        @Override
        public Slice compress(Slice slice)
                throws IOException
        {
            buffer.reset();
            try (CompressionOutputStream compressionStream = codec.createOutputStream(buffer, codec.createCompressor())) {
                slice.getInput().transferTo(compressionStream);
            }
            return buffer.slice();
        }
    }

    @Override
    public MemoryCompressedSliceOutput createMemoryCompressedSliceOutput(int minChunkSize, int maxChunkSize)
    {
        return new AircompressorCompressedSliceOutputSupplier(codec, minChunkSize, maxChunkSize).get();
    }

    // this can be dramatically simplified when actual hadoop codecs are dropped
    private static class AircompressorCompressedSliceOutputSupplier
            implements Supplier<MemoryCompressedSliceOutput>
    {
        private final CompressionCodec codec;
        private final ChunkedSliceOutput compressedOutput;

        public AircompressorCompressedSliceOutputSupplier(CompressionCodec codec, int minChunkSize, int maxChunkSize)
        {
            this.codec = requireNonNull(codec, "codec is null");
            this.compressedOutput = new ChunkedSliceOutput(minChunkSize, maxChunkSize);
        }

        @Override
        public MemoryCompressedSliceOutput get()
        {
            try {
                compressedOutput.reset();
                CompressionOutputStream compressionStream = codec.createOutputStream(compressedOutput);
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
        return codec.createInputStream(inputStream);
    }

    @Override
    public ValueDecompressor createValueDecompressor()
    {
        return new AircompressorValueDecompressor(codec);
    }

    private static class AircompressorValueDecompressor
            implements ValueDecompressor
    {
        private final CompressionCodec codec;

        private AircompressorValueDecompressor(CompressionCodec codec)
        {
            this.codec = requireNonNull(codec, "codec is null");
        }

        @Override
        public void decompress(Slice compressed, Slice uncompressed)
                throws IOException
        {
            try (CompressionInputStream decompressorStream = codec.createInputStream(compressed.getInput())) {
                uncompressed.setBytes(0, decompressorStream, uncompressed.length());
            }
            catch (IndexOutOfBoundsException | IOException e) {
                throw new IOException("Compressed stream is truncated", e);
            }
        }
    }
}
