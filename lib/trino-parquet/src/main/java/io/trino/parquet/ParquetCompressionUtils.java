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
package io.trino.parquet;

import com.github.luben.zstd.Zstd;
import com.google.common.io.ByteStreams;
import io.airlift.compress.Decompressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.lzo.LzoDecompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.apache.parquet.format.CompressionCodec;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ParquetCompressionUtils
{
    private static final int GZIP_BUFFER_SIZE = 8 * 1024;
    // Parquet pages written by parquet-mr and trino writers should be typically around 1MB or smaller in uncompressed size.
    // However, it's possible to encounter much larger pages than that from other writers or unusual configurations.
    // We avoid using native decompressor for large pages to reduce chances of GCLocker being held for a long time by JNI code
    private static final long JNI_DECOMPRESSION_SIZE_LIMIT = DataSize.of(1536, KILOBYTE).toBytes(); // 1MB is typical page size + 0.5MB tolerance as page sizing in writer is not exact

    private ParquetCompressionUtils() {}

    public static Slice decompress(ParquetDataSourceId dataSourceId, CompressionCodec codec, Slice input, int uncompressedSize, boolean isNativeZstdDecompressorEnabled, boolean isNativeSnappyDecompressorEnabled)
            throws IOException
    {
        requireNonNull(input, "input is null");

        if (input.length() == 0) {
            return EMPTY_SLICE;
        }

        return switch (codec) {
            case UNCOMPRESSED -> input;
            case GZIP -> decompressGzip(input, uncompressedSize);
            case SNAPPY -> isNativeSnappyDecompressorEnabled && !isLargeUncompressedPage(uncompressedSize)
                    ? decompressJniSnappy(input, uncompressedSize)
                    : decompressSnappy(input, uncompressedSize);
            case LZO -> decompressLZO(input, uncompressedSize);
            case LZ4 -> decompressLz4(input, uncompressedSize);
            case ZSTD -> isNativeZstdDecompressorEnabled && !isLargeUncompressedPage(uncompressedSize)
                    ? decompressJniZstd(input, uncompressedSize)
                    : decompressZstd(input, uncompressedSize);
            case BROTLI, LZ4_RAW -> throw new ParquetCorruptionException(dataSourceId, "Codec not supported in Parquet: %s", codec);
        };
    }

    private static Slice decompressJniSnappy(Slice input, int uncompressedSize)
            throws IOException
    {
        if (uncompressedSize == 0) {
            return EMPTY_SLICE;
        }

        verifyRange(input.byteArray(), input.byteArrayOffset(), input.length());
        byte[] buffer = new byte[uncompressedSize];
        int bytesRead = Snappy.uncompress(input.byteArray(), input.byteArrayOffset(), input.length(), buffer, 0);
        if (bytesRead != uncompressedSize) {
            throw new IllegalArgumentException(format("Invalid uncompressedSize for ZSTD input. Expected %s, actual: %s", uncompressedSize, bytesRead));
        }
        return wrappedBuffer(buffer, 0, bytesRead);
    }

    private static Slice decompressSnappy(Slice input, int uncompressedSize)
    {
        // Snappy decompressor is more efficient if there's at least a long's worth of extra space
        // in the output buffer
        byte[] buffer = new byte[uncompressedSize + SIZE_OF_LONG];
        int actualUncompressedSize = decompress(new SnappyDecompressor(), input, 0, input.length(), buffer, 0);
        if (actualUncompressedSize != uncompressedSize) {
            throw new IllegalArgumentException(format("Invalid uncompressedSize for SNAPPY input. Expected %s, actual: %s", uncompressedSize, actualUncompressedSize));
        }
        return wrappedBuffer(buffer, 0, uncompressedSize);
    }

    private static Slice decompressJniZstd(Slice input, int uncompressedSize)
    {
        if (uncompressedSize == 0) {
            return EMPTY_SLICE;
        }

        byte[] buffer = new byte[uncompressedSize];
        int bytesRead = toIntExact(Zstd.decompressByteArray(buffer, 0, buffer.length, input.byteArray(), input.byteArrayOffset(), input.length()));
        if (bytesRead != uncompressedSize) {
            throw new IllegalArgumentException(format("Invalid uncompressedSize for ZSTD input. Expected %s, actual: %s", uncompressedSize, bytesRead));
        }
        return wrappedBuffer(buffer, 0, bytesRead);
    }

    private static Slice decompressZstd(Slice input, int uncompressedSize)
    {
        byte[] buffer = new byte[uncompressedSize];
        decompress(new ZstdDecompressor(), input, 0, input.length(), buffer, 0);
        return wrappedBuffer(buffer);
    }

    private static Slice decompressGzip(Slice input, int uncompressedSize)
            throws IOException
    {
        if (uncompressedSize == 0) {
            return EMPTY_SLICE;
        }

        try (GZIPInputStream gzipInputStream = new GZIPInputStream(input.getInput(), min(GZIP_BUFFER_SIZE, input.length()))) {
            byte[] buffer = new byte[uncompressedSize];
            int bytesRead = ByteStreams.read(gzipInputStream, buffer, 0, buffer.length);
            if (bytesRead != uncompressedSize) {
                throw new IllegalArgumentException(format("Invalid uncompressedSize for GZIP input. Expected %s, actual: %s", uncompressedSize, bytesRead));
            }
            // Verify we're at EOF and aren't truncating the input
            checkArgument(gzipInputStream.read() == -1, "Invalid uncompressedSize for GZIP input. Actual size exceeds %s bytes", uncompressedSize);
            return wrappedBuffer(buffer, 0, bytesRead);
        }
    }

    private static Slice decompressLz4(Slice input, int uncompressedSize)
    {
        return decompressFramed(new Lz4Decompressor(), input, uncompressedSize);
    }

    private static Slice decompressLZO(Slice input, int uncompressedSize)
    {
        return decompressFramed(new LzoDecompressor(), input, uncompressedSize);
    }

    private static Slice decompressFramed(Decompressor decompressor, Slice input, int uncompressedSize)
    {
        long totalDecompressedCount = 0;
        // over allocate buffer which makes decompression easier
        byte[] output = new byte[uncompressedSize + SIZE_OF_LONG];
        int outputOffset = 0;
        int inputOffset = 0;
        int cumulativeUncompressedBlockLength = 0;

        while (totalDecompressedCount < uncompressedSize) {
            if (totalDecompressedCount == cumulativeUncompressedBlockLength) {
                cumulativeUncompressedBlockLength += Integer.reverseBytes(input.getInt(inputOffset));
                inputOffset += SIZE_OF_INT;
            }
            int compressedChunkLength = Integer.reverseBytes(input.getInt(inputOffset));
            inputOffset += SIZE_OF_INT;
            int decompressionSize = decompress(decompressor, input, inputOffset, compressedChunkLength, output, outputOffset);
            totalDecompressedCount += decompressionSize;
            outputOffset += decompressionSize;
            inputOffset += compressedChunkLength;
        }
        checkArgument(outputOffset == uncompressedSize);
        return wrappedBuffer(output, 0, uncompressedSize);
    }

    private static int decompress(Decompressor decompressor, Slice input, int inputOffset, int inputLength, byte[] output, int outputOffset)
    {
        byte[] byteArray = input.byteArray();
        int byteArrayOffset = inputOffset + input.byteArrayOffset();
        return decompressor.decompress(byteArray, byteArrayOffset, inputLength, output, outputOffset, output.length - outputOffset);
    }

    private static boolean isLargeUncompressedPage(int uncompressedSize)
    {
        return uncompressedSize > JNI_DECOMPRESSION_SIZE_LIMIT;
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }
}
