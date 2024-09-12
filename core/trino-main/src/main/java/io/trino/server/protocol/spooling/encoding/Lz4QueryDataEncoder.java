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
package io.trino.server.protocol.spooling.encoding;

import io.airlift.compress.v3.lz4.Lz4Compressor;
import io.trino.server.protocol.spooling.QueryDataEncoder;

import java.io.IOException;
import java.io.OutputStream;

public class Lz4QueryDataEncoder
        extends CompressedQueryDataEncoder
{
    private static final int COMPRESSION_THRESHOLD = 2048;

    public Lz4QueryDataEncoder(QueryDataEncoder delegate)
    {
        super(delegate, COMPRESSION_THRESHOLD);
    }

    @Override
    protected int compress(byte[] buffer, int uncompressedSize, OutputStream output)
            throws IOException
    {
        Lz4Compressor compressor = Lz4Compressor.create();
        byte[] compressed = new byte[compressor.maxCompressedLength(uncompressedSize)];
        int compressedSize = compressor.compress(buffer, 0, uncompressedSize, compressed, 0, compressed.length);
        output.write(compressed, 0, compressedSize);
        return compressedSize;
    }

    @Override
    public String encoding()
    {
        return delegate.encoding() + "+lz4";
    }
}
