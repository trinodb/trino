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
package io.trino.server.security.oauth2;

import io.airlift.compress.v3.zstd.ZstdInputStream;
import io.airlift.compress.v3.zstd.ZstdOutputStream;
import io.jsonwebtoken.io.CompressionAlgorithm;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;

public class ZstdCodec
        implements CompressionAlgorithm
{
    public static final String CODEC_NAME = "ZSTD";

    @Override
    public String getId()
    {
        return CODEC_NAME;
    }

    @Override
    public OutputStream compress(OutputStream out)
    {
        try {
            return new ZstdOutputStream(out);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public InputStream decompress(InputStream in)
    {
        return new ZstdInputStream(in);
    }
}
