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

import io.airlift.compress.v3.snappy.SnappyCompressor;
import io.airlift.compress.v3.snappy.SnappyDecompressor;
import org.apache.avro.file.Codec;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.zip.CRC32;

public class SnappyAirCompressorCodec
        extends Codec
{
    public static final CodecFactory SNAPPY_CODEC_FACTORY = new CodecFactory()
    {
        @Override
        protected Codec createInstance()
        {
            return new SnappyAirCompressorCodec();
        }
    };

    private final SnappyCompressor compressor = SnappyCompressor.create();
    private final SnappyDecompressor decompressor = SnappyDecompressor.create();
    private final CRC32 crc32 = new CRC32();

    @Override
    public String getName()
    {
        return DataFileConstants.SNAPPY_CODEC;
    }

    @Override
    public ByteBuffer compress(ByteBuffer uncompressedData)
            throws IOException
    {
        if (uncompressedData.isDirect()) {
            throw new IllegalArgumentException("Direct byte buffer not supported");
        }
        crc32.reset();
        crc32.update(uncompressedData.array(), uncompressedData.arrayOffset() + uncompressedData.position(), uncompressedData.remaining());
        ByteBuffer output = ByteBuffer.allocate(compressor.maxCompressedLength(uncompressedData.remaining()) + 4);
        int compressedSize = compressor.compress(
                uncompressedData.array(), uncompressedData.arrayOffset() + uncompressedData.position(), uncompressedData.remaining(),
                output.array(), 0, output.capacity());
        output.position(compressedSize);
        output.putInt((int) crc32.getValue());
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
        int remaining = compressedData.remaining() - 4;
        ByteBuffer output;
        if (compressedData.limit() == compressedData.capacity() && compressedData.arrayOffset() == 0) {
            output = ByteBuffer.allocate(decompressor.getUncompressedLength(compressedData.array(), compressedData.position()));
        }
        else {
            byte[] input = new byte[remaining];
            System.arraycopy(compressedData.array(), compressedData.arrayOffset() + compressedData.position(), input, 0, remaining);
            output = ByteBuffer.allocate(decompressor.getUncompressedLength(input, 0));
        }

        compressedData.limit(compressedData.limit() - 4);
        int decompressedSize = decompressor.decompress(
                compressedData.array(), compressedData.arrayOffset() + compressedData.position(), compressedData.remaining(),
                output.array(), 0, output.capacity());
        output.position(decompressedSize);
        output.flip();
        compressedData.limit(compressedData.limit() + 4);
        int storedCrc32 = compressedData.getInt(compressedData.limit() - 4);
        crc32.reset();
        crc32.update(output.array(), output.arrayOffset() + output.position(), output.remaining());
        if (storedCrc32 != (int) crc32.getValue()) {
            throw new IOException("Checksum failure " + storedCrc32 + " " + crc32.getValue());
        }

        return output;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SnappyAirCompressorCodec that)) {
            return false;
        }
        return compressor.equals(that.compressor) && decompressor.equals(that.decompressor) && crc32.equals(that.crc32);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(compressor, decompressor, crc32);
    }
}
