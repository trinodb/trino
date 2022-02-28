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
package io.trino.util;

import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class ThreadLocalCompressorDecompressor
        implements Compressor, Decompressor
{
    private final ThreadLocal<Compressor> compressor;
    private final ThreadLocal<Decompressor> decompressor;

    public ThreadLocalCompressorDecompressor(Supplier<Compressor> compressorFactory, Supplier<Decompressor> decompressorFactory)
    {
        this.compressor = ThreadLocal.withInitial(requireNonNull(compressorFactory, "compressorFactory is null"));
        this.decompressor = ThreadLocal.withInitial((requireNonNull(decompressorFactory, "decompressorFactory is null")));
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return compressor.get().maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        return compressor.get().compress(input, inputOffset, inputLength, output, outputOffset, maxOutputLength);
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        compressor.get().compress(input, output);
    }

    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        return decompressor.get().decompress(input, inputOffset, inputLength, output, outputOffset, maxOutputLength);
    }

    @Override
    public void decompress(ByteBuffer input, ByteBuffer output)
            throws MalformedInputException
    {
        decompressor.get().decompress(input, output);
    }
}
