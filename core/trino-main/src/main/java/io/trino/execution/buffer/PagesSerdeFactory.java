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
package io.trino.execution.buffer;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.compress.v3.Compressor;
import io.airlift.compress.v3.Decompressor;
import io.airlift.compress.v3.lz4.Lz4Compressor;
import io.airlift.compress.v3.lz4.Lz4Decompressor;
import io.airlift.compress.v3.zstd.ZstdCompressor;
import io.airlift.compress.v3.zstd.ZstdDecompressor;
import io.trino.spi.block.BlockEncodingSerde;

import javax.crypto.SecretKey;

import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.execution.buffer.CompressionCodec.LZ4;
import static io.trino.execution.buffer.CompressionCodec.ZSTD;
import static java.util.Objects.requireNonNull;

public class PagesSerdeFactory
{
    private static final int SERIALIZED_PAGE_DEFAULT_BLOCK_SIZE_IN_BYTES = 64 * 1024;
    private final BlockEncodingSerde blockEncodingSerde;
    private final CompressionCodec compressionCodec;
    private final int blockSizeInBytes;

    // created via PagesSerdes.create*
    PagesSerdeFactory(BlockEncodingSerde blockEncodingSerde, CompressionCodec compressionCodec)
    {
        this(blockEncodingSerde, compressionCodec, SERIALIZED_PAGE_DEFAULT_BLOCK_SIZE_IN_BYTES);
    }

    @VisibleForTesting
    PagesSerdeFactory(BlockEncodingSerde blockEncodingSerde, CompressionCodec compressionCodec, int blockSizeInBytes)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.compressionCodec = requireNonNull(compressionCodec, "compressionCodec is null");
        this.blockSizeInBytes = blockSizeInBytes;
    }

    public PageSerializer createSerializer(Optional<SecretKey> encryptionKey)
    {
        return new CompressingEncryptingPageSerializer(
                blockEncodingSerde,
                createCompressor(compressionCodec),
                encryptionKey,
                blockSizeInBytes,
                maxCompressedSize(blockSizeInBytes, compressionCodec));
    }

    public PageDeserializer createDeserializer(Optional<SecretKey> encryptionKey)
    {
        return new CompressingDecryptingPageDeserializer(
                blockEncodingSerde,
                createDecompressor(compressionCodec),
                decompressorRetainedSize(compressionCodec),
                encryptionKey,
                blockSizeInBytes,
                maxCompressedSize(blockSizeInBytes, compressionCodec));
    }

    public static Optional<Compressor> createCompressor(CompressionCodec compressionCodec)
    {
        return switch (compressionCodec) {
            case NONE -> Optional.empty();
            case LZ4 -> Optional.of(Lz4Compressor.create());
            case ZSTD -> Optional.of(ZstdCompressor.create());
        };
    }

    public static Optional<Decompressor> createDecompressor(CompressionCodec compressionCodec)
    {
        return switch (compressionCodec) {
            case NONE -> Optional.empty();
            case LZ4 -> Optional.of(Lz4Decompressor.create());
            case ZSTD -> Optional.of(ZstdDecompressor.create());
        };
    }

    private static OptionalInt maxCompressedSize(int uncompressedSize, CompressionCodec compressionCodec)
    {
        return switch (compressionCodec) {
            case NONE -> OptionalInt.of(uncompressedSize);
            case LZ4 -> LZ4.maxCompressedLength(uncompressedSize);
            case ZSTD -> ZSTD.maxCompressedLength(uncompressedSize);
        };
    }

    private static int decompressorRetainedSize(CompressionCodec compressionCodec)
    {
        // TODO: implement getRetainedSizeInBytes in Lz4Decompressor and ZstdDecompressor
        return switch (compressionCodec) {
            case NONE -> 0;
            case LZ4 -> instanceSize(Lz4Decompressor.class);
            case ZSTD -> instanceSize(ZstdDecompressor.class);
        };
    }
}
