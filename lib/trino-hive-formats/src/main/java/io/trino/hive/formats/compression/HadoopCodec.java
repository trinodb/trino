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
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HadoopCodec
        implements Codec
{
    private final CompressionCodec codec;

    public HadoopCodec(CompressionCodec codec)
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
        return new HadoopValueCompressor(codec);
    }

    private static class HadoopValueCompressor
            implements ValueCompressor
    {
        private final CompressionCodec codec;
        private final Compressor compressor;
        private final DynamicSliceOutput buffer;

        private HadoopValueCompressor(CompressionCodec codec)
        {
            this.codec = requireNonNull(codec, "codec is null");
            this.compressor = CodecPool.getCompressor(requireNonNull(codec, "codec is null"));
            this.buffer = new DynamicSliceOutput(1024);
        }

        @Override
        public Slice compress(Slice slice)
                throws IOException
        {
            compressor.reset();
            buffer.reset();
            try (CompressionOutputStream compressionStream = codec.createOutputStream(buffer, compressor)) {
                slice.getInput().transferTo(compressionStream);
            }
            return buffer.slice();
        }

        @Override
        public void close()
        {
            CodecPool.returnCompressor(compressor);
        }
    }

    @Override
    public MemoryCompressedSliceOutput createMemoryCompressedSliceOutput(int minChunkSize, int maxChunkSize)
    {
        return new HadoopCompressedSliceOutputSupplier(codec, minChunkSize, maxChunkSize).get();
    }

    private static class HadoopCompressedSliceOutputSupplier
            implements Supplier<MemoryCompressedSliceOutput>
    {
        private final CompressionCodec codec;
        private final Compressor compressor;
        private final ChunkedSliceOutput bufferedOutput;

        public HadoopCompressedSliceOutputSupplier(CompressionCodec codec, int minChunkSize, int maxChunkSize)
        {
            this.codec = requireNonNull(codec, "codec is null");
            this.compressor = CodecPool.getCompressor(requireNonNull(codec, "codec is null"));
            this.bufferedOutput = new ChunkedSliceOutput(minChunkSize, maxChunkSize);
        }

        @Override
        public MemoryCompressedSliceOutput get()
        {
            try {
                compressor.reset();
                bufferedOutput.reset();
                CompressionOutputStream compressionStream = codec.createOutputStream(bufferedOutput, compressor);
                return new MemoryCompressedSliceOutput(compressionStream, bufferedOutput, this, () -> CodecPool.returnCompressor(compressor));
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
        return new HadoopValueDecompressor(codec);
    }

    private static class HadoopValueDecompressor
            implements ValueDecompressor
    {
        private final CompressionCodec codec;
        private final Decompressor decompressor;
        private boolean closed;

        private HadoopValueDecompressor(CompressionCodec codec)
        {
            this.codec = requireNonNull(codec, "codec is null");
            decompressor = CodecPool.getDecompressor(codec);
        }

        @Override
        public void decompress(Slice compressed, Slice uncompressed)
                throws IOException
        {
            checkState(!closed, "Value decompressor has been closed");
            decompressor.reset();
            try (CompressionInputStream decompressorStream = codec.createInputStream(compressed.getInput(), decompressor)) {
                uncompressed.setBytes(0, decompressorStream, uncompressed.length());
            }
            catch (IndexOutOfBoundsException | IOException e) {
                throw new IOException("Compressed stream is truncated", e);
            }
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            CodecPool.returnDecompressor(decompressor);
        }
    }
}
