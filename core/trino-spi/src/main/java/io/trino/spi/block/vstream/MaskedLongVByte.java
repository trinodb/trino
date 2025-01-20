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
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.spi.block.vstream.BitMath.zagZig;
import static io.trino.spi.block.vstream.BitMath.zigZag;

public class MaskedLongVByte
{
    private MaskedLongVByte() {}

    public static int writeLongs(SliceOutput output, long[] values)
    {
        return writeLongs(output, values, 0, values.length);
    }

    public static int writeLongs(SliceOutput output, long[] values, int sourceIndex, int length)
    {
        int written = 0;
        for (int i = sourceIndex; i < sourceIndex + length; i++) {
            long value = zigZag(values[i]);
            while ((value & ~0x7FL) != 0) {
                output.writeByte((byte) ((value & 0x7F) | 0x80));
                written++;
                value >>>= 7;
            }
            written++;
            output.writeByte((byte) value);
        }
        return written;
    }

    public static long[] readLongs(SliceInput input, int size)
    {
        long[] values = new long[size];
        readLongs(input, values, 0, size);
        return values;
    }

    public static void readLongs(SliceInput input, long[] destination)
    {
        readLongs(input, destination, 0, destination.length);
    }

    public static void readLongs(SliceInput input, long[] destination, int destinationIndex, int length)
    {
        for (int i = destinationIndex; i < destinationIndex + length; i++) {
            byte b = input.readByte();
            long value = b & 0x7F;
            for (int shift = 7; (b & 0x80) != 0; shift += 7) {
                b = input.readByte();
                value |= (b & 0x7FL) << shift;
            }
            destination[i] = zagZig(value);
        }
    }

    public static int maxEncodedSize(int size)
    {
        return size * (SIZE_OF_LONG + SIZE_OF_BYTE * 2);
    }
}
