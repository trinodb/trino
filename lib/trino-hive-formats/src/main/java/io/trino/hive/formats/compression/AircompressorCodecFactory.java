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

import io.airlift.compress.gzip.JdkGzipCodec;
import io.airlift.compress.lz4.Lz4Codec;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.snappy.SnappyCodec;

import static java.util.Objects.requireNonNull;

public class AircompressorCodecFactory
        implements CodecFactory
{
    private static final String SNAPPY_CODEC_NAME = "org.apache.hadoop.io.compress.SnappyCodec";
    private static final String LZO_CODEC_NAME = "com.hadoop.compression.lzo.LzoCodec";
    private static final String LZO_CODEC_NAME_DEPRECATED = "org.apache.hadoop.io.compress.LzoCodec";
    private static final String LZ4_CODEC_NAME = "org.apache.hadoop.io.compress.Lz4Codec";
    private static final String GZIP_CODEC_NAME = "org.apache.hadoop.io.compress.GzipCodec";

    private final CodecFactory delegate;

    public AircompressorCodecFactory(CodecFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Compressor createCompressor(String codecName)
    {
        return switch (codecName) {
            case SNAPPY_CODEC_NAME -> new AircompressorCompressor(new SnappyCodec());
            case LZO_CODEC_NAME, LZO_CODEC_NAME_DEPRECATED -> new AircompressorCompressor(new LzoCodec());
            case LZ4_CODEC_NAME -> new AircompressorCompressor(new Lz4Codec());
            case GZIP_CODEC_NAME -> new AircompressorCompressor(new JdkGzipCodec());
            default -> delegate.createCompressor(codecName);
        };
    }

    @Override
    public Decompressor createDecompressor(String codecName)
    {
        return switch (codecName) {
            case SNAPPY_CODEC_NAME -> new AircompressorDecompressor(new SnappyCodec());
            case LZO_CODEC_NAME, LZO_CODEC_NAME_DEPRECATED -> new AircompressorDecompressor(new LzoCodec());
            case LZ4_CODEC_NAME -> new AircompressorDecompressor(new Lz4Codec());
            case GZIP_CODEC_NAME -> new AircompressorDecompressor(new JdkGzipCodec());
            default -> delegate.createDecompressor(codecName);
        };
    }
}
