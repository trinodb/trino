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
package io.trino.client.spooling.encoding;

import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

import static java.util.Objects.requireNonNull;

public class AirliftCompressionCodecFactory
        implements CompressionCodec.Factory
{
    @Override
    public CompressionCodec createCodec(CompressionUtil.CodecType codecType)
    {
        if (requireNonNull(codecType) == CompressionUtil.CodecType.ZSTD) {
            return new AirliftZstdCompressionCodec();
        }
        throw new IllegalStateException("Unsupported codec type: " + codecType);
    }

    @Override
    public CompressionCodec createCodec(CompressionUtil.CodecType codecType, int compressionLevel)
    {
        if (requireNonNull(codecType) == CompressionUtil.CodecType.ZSTD) {
            return new AirliftZstdCompressionCodec();
        }
        throw new IllegalStateException("Unsupported codec type: " + codecType);
    }
}
