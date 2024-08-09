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
package io.airlift.compress.snappy;

import io.airlift.compress.Compressor;
import io.airlift.compress.v2.snappy.SnappyJavaCompressor;
import io.airlift.compress.v2.snappy.SnappyNativeCompressor;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import static java.lang.Math.toIntExact;

public class SnappyCompressor
        implements Compressor
{
    private static final boolean NATIVE_ENABLED = SnappyNativeCompressor.isEnabled();
    private final io.airlift.compress.v2.snappy.SnappyCompressor compressor;

    public SnappyCompressor()
    {
        this.compressor = NATIVE_ENABLED ? new SnappyNativeCompressor() : new SnappyJavaCompressor();
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return compressor.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(inputOffset, inputLength);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);

        return toIntExact(compressor.compress(inputSegment, outputSegment));
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        MemorySegment inputSegment = MemorySegment.ofBuffer(input);
        MemorySegment outputSegment = MemorySegment.ofBuffer(output);

        int written = compressor.compress(inputSegment, outputSegment);
        output.position(output.position() + written);
    }
}
