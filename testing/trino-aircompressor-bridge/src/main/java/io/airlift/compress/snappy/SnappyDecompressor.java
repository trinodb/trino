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

import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;
import io.airlift.compress.v2.snappy.SnappyJavaDecompressor;
import io.airlift.compress.v2.snappy.SnappyNativeDecompressor;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import static java.lang.Math.toIntExact;

public class SnappyDecompressor
        implements Decompressor
{
    private static final boolean NATIVE_ENABLED = SnappyNativeDecompressor.isEnabled();
    private final io.airlift.compress.v2.snappy.SnappyDecompressor decompressor;

    public SnappyDecompressor()
    {
        this.decompressor = NATIVE_ENABLED ? new SnappyNativeDecompressor() : new SnappyJavaDecompressor();
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

    public static int getUncompressedLength(byte[] input, int offset)
    {
        if (NATIVE_ENABLED) {
            return new SnappyNativeDecompressor().getUncompressedLength(input, offset);
        }
        return new SnappyJavaDecompressor().getUncompressedLength(input, offset);
    }
}
