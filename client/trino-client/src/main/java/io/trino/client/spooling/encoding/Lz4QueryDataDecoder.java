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

import io.trino.client.QueryDataDecoder;

import java.io.IOException;

import static java.lang.String.format;

public class Lz4QueryDataDecoder
        extends CompressedQueryDataDecoder
{
    public Lz4QueryDataDecoder(QueryDataDecoder delegate)
    {
        super(delegate);
    }

    @Override
    void decompress(byte[] input, byte[] output)
            throws IOException
    {
        int decompressedSize = DecompressionUtils.decompressLZ4(input, output);
        if (decompressedSize != output.length) {
            throw new IOException(format("Decompressed size does not match expected segment size, expected %d, got %d", decompressedSize, output.length));
        }
    }

    @Override
    public String encoding()
    {
        return delegate.encoding() + "+lz4";
    }
}
