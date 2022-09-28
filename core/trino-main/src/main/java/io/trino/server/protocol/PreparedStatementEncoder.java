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
package io.trino.server.protocol;

import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import io.trino.server.ProtocolConfig;

import javax.inject.Inject;

import static com.google.common.io.BaseEncoding.base64Url;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;

public class PreparedStatementEncoder
{
    // No valid SQL statement starts with $
    private static final String PREFIX = "$zstd:";

    private final int compressionThreshold;
    private final int compressionMinGain;

    @Inject
    public PreparedStatementEncoder(ProtocolConfig protocolConfig)
    {
        this.compressionThreshold = protocolConfig.getPreparedStatementCompressionThreshold();
        this.compressionMinGain = protocolConfig.getPreparedStatementCompressionMinimalGain();
    }

    public String encodePreparedStatementForHeader(String preparedStatement)
    {
        if (preparedStatement.length() < compressionThreshold) {
            return preparedStatement;
        }

        ZstdCompressor compressor = new ZstdCompressor();
        byte[] inputBytes = preparedStatement.getBytes(UTF_8);
        byte[] compressed = new byte[compressor.maxCompressedLength(inputBytes.length)];
        int outputSize = compressor.compress(inputBytes, 0, inputBytes.length, compressed, 0, compressed.length);
        String encoded = base64Url().encode(compressed, 0, outputSize);

        if (encoded.length() + PREFIX.length() + compressionMinGain > preparedStatement.length()) {
            return preparedStatement;
        }
        return PREFIX + encoded;
    }

    public String decodePreparedStatementFromHeader(String headerValue)
    {
        if (!headerValue.startsWith(PREFIX)) {
            return headerValue;
        }

        String encoded = headerValue.substring(PREFIX.length());
        byte[] compressed = base64Url().decode(encoded);

        byte[] preparedStatement = new byte[toIntExact(ZstdDecompressor.getDecompressedSize(compressed, 0, compressed.length))];
        new ZstdDecompressor().decompress(compressed, 0, compressed.length, preparedStatement, 0, preparedStatement.length);
        return new String(preparedStatement, UTF_8);
    }
}
