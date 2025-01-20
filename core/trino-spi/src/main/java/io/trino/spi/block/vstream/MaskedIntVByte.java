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
package io.trino.spi.block.vstream;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class MaskedIntVByte
{
    private MaskedIntVByte() {}

    public static int writeInts(SliceOutput output, int[] values)
    {
        return writeInts(output, values, 0, values.length);
    }

    public static int writeInts(SliceOutput output, int[] values, int sourceIndex, int length)
    {
        int bytes = 0;
        for (int i = sourceIndex; i < sourceIndex + length; i++) {
            int value = values[i];
            while ((value & ~0x7F) != 0) {
                output.writeByte((byte) ((value & 0x7F) | 0x80));
                bytes++;
                value >>>= 7;
            }
            bytes++;
            output.writeByte((byte) value);
        }
        return bytes;
    }

    public static int maxEncodedSize(int size)
    {
        return size * (SIZE_OF_INT + SIZE_OF_BYTE);
    }

    public static int[] readInts(SliceInput input, int size)
    {
        int[] values = new int[size];
        readInts(input, values, 0, size);
        return values;
    }

    public static void readInts(SliceInput input, int[] destination)
    {
        readInts(input, destination, 0, destination.length);
    }

    public static void readInts(SliceInput input, int[] destination, int destinationIndex, int length)
    {
        for (int i = destinationIndex; i < destinationIndex + length; i++) {
            byte b = input.readByte();
            int value = b & 0x7F;
            for (int shift = 7; (b & 0x80) != 0; shift += 7) {
                b = input.readByte();
                value |= (b & 0x7F) << shift;
            }
            destination[i] = value;
        }
    }
}
