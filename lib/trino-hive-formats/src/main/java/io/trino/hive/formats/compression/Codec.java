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

import io.airlift.compress.hadoop.HadoopStreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

public final class Codec
{
    private final HadoopStreams hadoopStreams;

    Codec(HadoopStreams hadoopStreams)
    {
        this.hadoopStreams = requireNonNull(hadoopStreams, "hadoopStreams is null");
    }

    public OutputStream createStreamCompressor(OutputStream outputStream)
            throws IOException
    {
        return hadoopStreams.createOutputStream(outputStream);
    }

    public ValueCompressor createValueCompressor()
    {
        return new ValueCompressor(hadoopStreams);
    }

    public MemoryCompressedSliceOutput createMemoryCompressedSliceOutput(int minChunkSize, int maxChunkSize)
            throws IOException
    {
        return new MemoryCompressedSliceOutput(hadoopStreams, minChunkSize, maxChunkSize);
    }

    public InputStream createStreamDecompressor(InputStream inputStream)
            throws IOException
    {
        return hadoopStreams.createInputStream(inputStream);
    }

    public ValueDecompressor createValueDecompressor()
    {
        return new ValueDecompressor(hadoopStreams);
    }
}
