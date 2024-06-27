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

import io.airlift.compress.v3.lz4.Lz4Compressor;
import io.airlift.compress.v3.zstd.ZstdCompressor;

import java.util.OptionalInt;

public enum CompressionCodec
{
    NONE, LZ4, ZSTD;

    public OptionalInt maxCompressedLength(int inputSize)
    {
        return switch (this) {
            case NONE -> OptionalInt.empty();
            case LZ4 -> OptionalInt.of(Lz4Compressor.create().maxCompressedLength(inputSize));
            case ZSTD -> OptionalInt.of(ZstdCompressor.create().maxCompressedLength(inputSize));
        };
    }
}
