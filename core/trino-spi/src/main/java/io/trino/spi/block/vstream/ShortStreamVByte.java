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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static java.lang.Math.min;

public class ShortStreamVByte
{
    private static final int GROUP_SIZE = 8; // 1 bit to encode the length of the long (1 - 2 bytes)

    private ShortStreamVByte() {}

    public static int writeShorts(SliceOutput output, short[] values)
    {
        return writeShorts(output, values, 0, values.length);
    }

    public static int writeShorts(SliceOutput output, short[] values, int sourceIndex, int length)
    {
        Slice buffer = Slices.allocate(length * SIZE_OF_SHORT); // TODO: this is needed due to the encryption/compression working across write boundaries
        SliceOutput bufferOutput = buffer.getOutput();
        byte[] controlBytes = new byte[controlBytesTableSize(length)];

        int outputIndex = 0;
        int writtenSize = 0;
        while (outputIndex < length) {
            byte controlByte = 0;

            for (int i = 0; i < min(GROUP_SIZE, length - outputIndex); i++) {
                byte byteSize = byteWidth(values[sourceIndex + outputIndex + i]);
                writtenSize += byteSize;
                controlByte |= (byte) ((byteSize - 1) << i);
                putValue(bufferOutput, values[sourceIndex + outputIndex + i], byteSize);
            }

            controlBytes[outputIndex / GROUP_SIZE] = controlByte;
            outputIndex += GROUP_SIZE;
        }

        output.writeBytes(controlBytes);
        output.writeBytes(buffer, 0, writtenSize);
        return output.size();
    }

    public static int maxEncodedSize(int size)
    {
        return size * SIZE_OF_SHORT + controlBytesTableSize(size);
    }

    static int controlBytesTableSize(int size)
    {
        return (size + GROUP_SIZE - 1) / GROUP_SIZE;
    }

    public static short[] readShorts(SliceInput input, int size)
    {
        short[] values = new short[size];
        readShorts(input, values, 0, size);
        return values;
    }

    public static void readShorts(SliceInput input, short[] destination)
    {
        readShorts(input, destination, 0, destination.length);
    }

    public static void readShorts(SliceInput input, short[] destination, int destinationIndex, int length)
    {
        byte[] controlBytes = new byte[controlBytesTableSize(length)];
        input.readBytes(controlBytes);
        int index = 0;
        for (byte controlByte : controlBytes) {
            for (int i = 0; i < GROUP_SIZE; i++) {
                byte valueSize = (byte) (((controlByte >> i) & 0x01) + 1);
                destination[destinationIndex + index++] = (short) getValue(input, valueSize);
                if (index >= length) {
                    break;
                }
            }
        }
    }

    static byte byteWidth(short value)
    {
        if ((value & 0xFFFFFF80) == 0) {
            return 1;
        }
        return 2;
    }

    private static void putValue(SliceOutput buffer, int value, byte byteSize)
    {
        switch (byteSize) {
            case 1:
                buffer.writeByte(value); // 1 bytes
                return;
            case 2:
                buffer.writeShort(value); // 2 bytes
                return;
        }
        throw new IllegalArgumentException("Invalid byte size: " + byteSize);
    }

    private static int getValue(SliceInput input, byte byteSize)
    {
        return switch (byteSize) {
            case 1 -> input.readByte();
            case 2 -> input.readShort();
            default -> throw new IllegalArgumentException("Invalid byte size: " + byteSize);
        };
    }
}
