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

import io.airlift.compress.Compressor;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.format.CompressionCodec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import static io.trino.parquet.writer.ParquetDataOutput.createDataOutput;
import static java.util.Objects.requireNonNull;

interface ParquetCompressor
{
    ParquetDataOutput compress(byte[] input)
            throws IOException;

    static ParquetCompressor getCompressor(CompressionCodec codec)
    {
        switch (codec) {
            case GZIP:
                return new GzipCompressor();
            case SNAPPY:
                return new AirLiftCompressor(new SnappyCompressor());
            case ZSTD:
                return new AirLiftCompressor(new ZstdCompressor());
            case UNCOMPRESSED:
                return null;
            case LZ4:
                // Note: LZ4 compression scheme has been deprecated by parquet-format in favor of LZ4_RAW.
                // However, LZ4 is the more widely supported encoding in other engines.
                return new Lz4FramedCompressor();
            // When using airlift LZO compressor, decompressing page in reader throws exception.
            case LZO:
            case LZ4_RAW:
            case BROTLI:
                // unsupported
                break;
        }
        throw new RuntimeException("Unsupported codec: " + codec);
    }

    class GzipCompressor
            implements ParquetCompressor
    {
        @Override
        public ParquetDataOutput compress(byte[] input)
                throws IOException
        {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            try (GZIPOutputStream outputStream = new GZIPOutputStream(byteArrayOutputStream)) {
                outputStream.write(input, 0, input.length);
            }
            return createDataOutput(BytesInput.from(byteArrayOutputStream));
        }
    }

    class Lz4FramedCompressor
            implements ParquetCompressor
    {
        private final Compressor compressor = new Lz4Compressor();

        @Override
        public ParquetDataOutput compress(byte[] input)
                throws IOException
        {
            int minCompressionBufferSize = compressor.maxCompressedLength(input.length);
            byte[] compressionBuffer = new byte[8 + minCompressionBufferSize];
            int compressedDataSize = compressor.compress(input, 0, input.length, compressionBuffer, 8, minCompressionBufferSize);
            Slice output = Slices.wrappedBuffer(compressionBuffer, 0, 8 + compressedDataSize);
            output.setInt(0, Integer.reverseBytes(input.length));
            output.setInt(4, Integer.reverseBytes(compressedDataSize));
            return createDataOutput(output);
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
        public ParquetDataOutput compress(byte[] input)
                throws IOException
        {
            int minCompressionBufferSize = compressor.maxCompressedLength(input.length);
            byte[] compressionBuffer = new byte[minCompressionBufferSize];
            // TODO compressedDataSize > bytes.length?
            int compressedDataSize = compressor.compress(input, 0, input.length, compressionBuffer, 0, compressionBuffer.length);
            return createDataOutput(Slices.wrappedBuffer(compressionBuffer, 0, compressedDataSize));
        }
    }
}
