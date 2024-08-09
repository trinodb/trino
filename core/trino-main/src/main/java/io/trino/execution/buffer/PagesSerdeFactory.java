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

import io.airlift.compress.v2.Compressor;
import io.airlift.compress.v2.Decompressor;
import io.airlift.compress.v2.lz4.Lz4JavaCompressor;
import io.airlift.compress.v2.lz4.Lz4JavaDecompressor;
import io.airlift.compress.v2.lz4.Lz4NativeCompressor;
import io.airlift.compress.v2.lz4.Lz4NativeDecompressor;
import io.airlift.compress.v2.zstd.ZstdJavaCompressor;
import io.airlift.compress.v2.zstd.ZstdJavaDecompressor;
import io.airlift.compress.v2.zstd.ZstdNativeCompressor;
import io.airlift.compress.v2.zstd.ZstdNativeDecompressor;
import io.trino.spi.block.BlockEncodingSerde;

import javax.crypto.SecretKey;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PagesSerdeFactory
{
    private static final int SERIALIZED_PAGE_DEFAULT_BLOCK_SIZE_IN_BYTES = 64 * 1024;

    private final BlockEncodingSerde blockEncodingSerde;
    private final CompressionCodec compressionCodec;

    public PagesSerdeFactory(BlockEncodingSerde blockEncodingSerde, CompressionCodec compressionCodec)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.compressionCodec = requireNonNull(compressionCodec, "compressionCodec is null");
    }

    public PageSerializer createSerializer(Optional<SecretKey> encryptionKey)
    {
        return new PageSerializer(blockEncodingSerde, createCompressor(compressionCodec), encryptionKey, SERIALIZED_PAGE_DEFAULT_BLOCK_SIZE_IN_BYTES);
    }

    public PageDeserializer createDeserializer(Optional<SecretKey> encryptionKey)
    {
        return new PageDeserializer(blockEncodingSerde, createDecompressor(compressionCodec), encryptionKey, SERIALIZED_PAGE_DEFAULT_BLOCK_SIZE_IN_BYTES);
    }

    public static Optional<Compressor> createCompressor(CompressionCodec compressionCodec)
    {
        return switch (compressionCodec) {
            case NONE -> Optional.empty();
            case LZ4 -> Optional.of(Lz4NativeCompressor.isEnabled() ? new Lz4NativeCompressor() : new Lz4JavaCompressor());
            case ZSTD -> Optional.of(ZstdNativeCompressor.isEnabled() ? new ZstdNativeCompressor() : new ZstdJavaCompressor());
        };
    }

    public static Optional<Decompressor> createDecompressor(CompressionCodec compressionCodec)
    {
        return switch (compressionCodec) {
            case NONE -> Optional.empty();
            case LZ4 -> Optional.of(Lz4NativeCompressor.isEnabled() ? new Lz4NativeDecompressor() : new Lz4JavaDecompressor());
            case ZSTD -> Optional.of(ZstdNativeDecompressor.isEnabled() ? new ZstdNativeDecompressor() : new ZstdJavaDecompressor());
        };
    }
}
