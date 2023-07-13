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
import io.airlift.slice.Slice;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

public final class ValueDecompressor
{
    private final HadoopStreams hadoopStreams;

    ValueDecompressor(HadoopStreams hadoopStreams)
    {
        this.hadoopStreams = requireNonNull(hadoopStreams, "hadoopStreams is null");
    }

    public void decompress(Slice compressed, OutputStream uncompressed)
            throws IOException
    {
        try (InputStream decompressorStream = hadoopStreams.createInputStream(compressed.getInput())) {
            decompressorStream.transferTo(uncompressed);
        }
        catch (IndexOutOfBoundsException | IOException e) {
            throw new IOException("Compressed stream is truncated", e);
        }
    }

    public void decompress(Slice compressed, Slice uncompressed)
            throws IOException
    {
        try (InputStream decompressorStream = hadoopStreams.createInputStream(compressed.getInput())) {
            uncompressed.setBytes(0, decompressorStream, uncompressed.length());
        }
        catch (IndexOutOfBoundsException | IOException e) {
            throw new IOException("Compressed stream is truncated", e);
        }
    }
}
