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
package io.airlift.compress.zstd;

import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;
import io.airlift.compress.v2.zstd.ZstdJavaDecompressor;
import io.airlift.compress.v2.zstd.ZstdNativeDecompressor;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import static java.lang.Math.toIntExact;

public class ZstdDecompressor
        implements Decompressor
{
    private static final boolean NATIVE_ENABLED = ZstdNativeDecompressor.isEnabled();
    private final io.airlift.compress.v2.zstd.ZstdDecompressor decompressor;

    public ZstdDecompressor()
    {
        this.decompressor = NATIVE_ENABLED ? new ZstdNativeDecompressor() : new ZstdJavaDecompressor();
    }

    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(inputOffset, inputLength);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);

        return toIntExact(decompressor.decompress(inputSegment, outputSegment));
    }

    @Override
    public void decompress(ByteBuffer input, ByteBuffer output)
            throws MalformedInputException
    {
        MemorySegment inputSegment = MemorySegment.ofBuffer(input);
        MemorySegment outputSegment = MemorySegment.ofBuffer(output);

        int written = decompressor.decompress(inputSegment, outputSegment);
        output.position(output.position() + written);
    }

    public static long getDecompressedSize(byte[] input, int offset, int length)
    {
        if (NATIVE_ENABLED) {
            return new ZstdNativeDecompressor().getDecompressedSize(input, offset, length);
        }
        return new ZstdJavaDecompressor().getDecompressedSize(input, offset, length);
    }
}
