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

import com.google.common.io.ByteStreams;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.trino.client.QueryDataDecoder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static java.lang.String.format;

public class Lz4QueryDataDecoder
        extends CompressedQueryDataDecoder
{
    public Lz4QueryDataDecoder(QueryDataDecoder delegate)
    {
        super(delegate);
    }

    @Override
    InputStream decompress(InputStream stream, int uncompressedSize)
            throws IOException
    {
        Lz4Decompressor decompressor = new Lz4Decompressor();
        byte[] bytes = ByteStreams.toByteArray(stream);
        byte[] output = new byte[uncompressedSize];

        int decompressedSize = decompressor.decompress(bytes, 0, bytes.length, output, 0, output.length);
        if (decompressedSize != uncompressedSize) {
            throw new IOException(format("Decompressed size does not match expected segment size, expected %d, got %d", decompressedSize, uncompressedSize));
        }
        return new ByteArrayInputStream(output);
    }

    @Override
    public String encoding()
    {
        return delegate.encoding() + "+lz4";
    }
}
