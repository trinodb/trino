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
package io.trino.parquet.writer;

import io.airlift.compress.v3.Compressor;
import io.airlift.compress.v3.lz4.Lz4Compressor;
import io.airlift.compress.v3.snappy.SnappyCompressor;
import io.airlift.compress.v3.zstd.ZstdCompressor;
import io.airlift.slice.Slices;
import org.apache.parquet.format.CompressionCodec;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

import static io.trino.parquet.writer.ParquetDataOutput.createDataOutput;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

interface ParquetCompressor
{
    /**
     * Compresses the input into the output buffer of {@code buffers}; the result is valid until the next call with the same buffers.
     */
    ParquetDataOutput compress(ByteBuffer input, CompressionBuffers buffers)
            throws IOException;

    static ParquetCompressor getCompressor(CompressionCodec codec)
    {
        return switch (codec) {
            case GZIP -> new GzipCompressor();
            case SNAPPY -> new AirLiftCompressor(SnappyCompressor.create());
            case ZSTD -> new AirLiftCompressor(ZstdCompressor.create());
            case LZ4 -> new AirLiftCompressor(Lz4Compressor.create());
            case UNCOMPRESSED -> null;
            case LZO, LZ4_RAW, BROTLI -> {
                // TODO Support LZO and LZ4_RAW compression
                // Note: LZ4 compression scheme has been deprecated by parquet-format in favor of LZ4_RAW
                // When using airlift LZO or LZ4 compressor, decompressing page in reader throws exception.
                throw new RuntimeException("Unsupported codec: " + codec);
            }
        };
    }

    class GzipCompressor
            implements ParquetCompressor
    {
        // the header written by GZIPOutputStream: magic, deflate, no flags, no modification time, default level, unknown OS
        private static final byte[] HEADER = {0x1F, (byte) 0x8B, 8, 0, 0, 0, 0, 0, 0, (byte) 0xFF};
        private static final int TRAILER_SIZE = 8;

        @Override
        public ParquetDataOutput compress(ByteBuffer input, CompressionBuffers buffers)
                throws IOException
        {
            int inputSize = input.remaining();
            CRC32 crc = new CRC32();
            crc.update(input.duplicate());

            // zlib's deflateBound plus slack for the raw stream
            byte[] output = buffers.output(toIntExact(HEADER.length + inputSize + (inputSize >> 12) + (inputSize >> 14) + (inputSize >> 25) + 64L + TRAILER_SIZE));
            System.arraycopy(HEADER, 0, output, 0, HEADER.length);
            int size = HEADER.length;

            Deflater deflater = buffers.deflater();
            deflater.setInput(input);
            deflater.finish();
            while (!deflater.finished()) {
                int written = deflater.deflate(output, size, output.length - TRAILER_SIZE - size);
                if (written == 0) {
                    throw new IOException("Insufficient output buffer for GZIP compression");
                }
                size += written;
            }
            writeLittleEndianInt(output, size, (int) crc.getValue());
            writeLittleEndianInt(output, size + 4, inputSize);
            return createDataOutput(Slices.wrappedBuffer(output, 0, size + TRAILER_SIZE));
        }

        private static void writeLittleEndianInt(byte[] output, int offset, int value)
        {
            output[offset] = (byte) value;
            output[offset + 1] = (byte) (value >> 8);
            output[offset + 2] = (byte) (value >> 16);
            output[offset + 3] = (byte) (value >> 24);
        }
    }

    class AirLiftCompressor
            implements ParquetCompressor
    {
        private final Compressor compressor;

        AirLiftCompressor(Compressor compressor)
        {
            this.compressor = requireNonNull(compressor, "compressor is null");
        }

        @Override
        public ParquetDataOutput compress(ByteBuffer input, CompressionBuffers buffers)
                throws IOException
        {
            byte[] compressionBuffer = buffers.output(compressor.maxCompressedLength(input.remaining()));
            int compressedDataSize = compressor.compress(MemorySegment.ofBuffer(input), MemorySegment.ofArray(compressionBuffer));
            return createDataOutput(Slices.wrappedBuffer(compressionBuffer, 0, compressedDataSize));
        }
    }
}
