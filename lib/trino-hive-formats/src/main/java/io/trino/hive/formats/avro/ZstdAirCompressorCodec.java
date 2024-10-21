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
package io.trino.hive.formats.avro;

import io.airlift.compress.v3.zstd.ZstdCompressor;
import io.airlift.compress.v3.zstd.ZstdDecompressor;
import io.airlift.compress.v3.zstd.ZstdInputStream;
import org.apache.avro.file.Codec;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static java.lang.Math.toIntExact;

public class ZstdAirCompressorCodec
        extends Codec
{
    public static final CodecFactory ZSTD_CODEC_FACTORY = new CodecFactory()
    {
        @Override
        protected Codec createInstance()
        {
            return new ZstdAirCompressorCodec();
        }
    };

    private final ZstdCompressor compressor = ZstdCompressor.create();
    private final ZstdDecompressor decompressor = ZstdDecompressor.create();

    @Override
    public String getName()
    {
        return DataFileConstants.ZSTANDARD_CODEC;
    }

    @Override
    public ByteBuffer compress(ByteBuffer uncompressedData)
            throws IOException
    {
        if (uncompressedData.isDirect()) {
            throw new IllegalArgumentException("Direct byte buffer not supported");
        }
        ByteBuffer output = ByteBuffer.allocate(compressor.maxCompressedLength(uncompressedData.remaining()));
        int compressedSize = compressor.compress(
                uncompressedData.array(), uncompressedData.arrayOffset() + uncompressedData.position(), uncompressedData.remaining(),
                output.array(), 0, output.capacity());
        output.position(compressedSize);
        output.flip();
        return output;
    }

    @Override
    public ByteBuffer decompress(ByteBuffer compressedData)
            throws IOException
    {
        if (compressedData.isDirect()) {
            throw new IllegalArgumentException("Direct byte buffer not supported");
        }
        int decompressedSize = toIntExact(decompressor.getDecompressedSize(compressedData.array(), compressedData.arrayOffset() + compressedData.position(), compressedData.remaining()));
        if (decompressedSize < 0) {
            return ByteBuffer.wrap(new ZstdInputStream(new ByteArrayInputStream(compressedData.array(), compressedData.arrayOffset() + compressedData.position(), compressedData.remaining())).readAllBytes());
        }
        else {
            ByteBuffer output = ByteBuffer.allocate(decompressedSize);
            decompressor.decompress(compressedData.array(), compressedData.arrayOffset() + compressedData.position(), compressedData.remaining(),
                    output.array(), 0, output.capacity());
            output.position(decompressedSize);
            output.flip();
            return output;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ZstdAirCompressorCodec that)) {
            return false;
        }
        return compressor.equals(that.compressor) && decompressor.equals(that.decompressor);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(compressor, decompressor);
    }
}
