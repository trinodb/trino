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

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Math.min;

public class IntStreamVByte
{
    private static final int GROUP_SIZE = 4; // 2 bits to encode the length of the int (1 - 4 bytes)

    private IntStreamVByte() {}

    public static int writeInts(SliceOutput output, int[] values)
    {
        return writeInts(output, values, 0, values.length);
    }

    public static int writeInts(SliceOutput output, int[] values, int sourceIndex, int length)
    {
        Slice buffer = Slices.allocate(length * SIZE_OF_INT); // TODO: this is needed due to the encryption/compression working across write boundaries
        SliceOutput bufferOutput = buffer.getOutput();
        byte[] controlBytes = new byte[controlBytesTableSize(length)];
        int dataIndex = 0;
        int writtenSize = 0;
        while (dataIndex < length) {
            byte controlByte = 0;

            for (int i = 0; i < min(GROUP_SIZE, length - dataIndex); i++) {
                byte byteSize = byteWidth(values[sourceIndex + dataIndex + i]);
                writtenSize += byteSize;
                controlByte |= (byte) ((byteSize - 1) << (i * 2));
                putValue(bufferOutput, values[sourceIndex + dataIndex + i], byteSize);
            }

            controlBytes[dataIndex / GROUP_SIZE] = controlByte;
            dataIndex += GROUP_SIZE;
        }

        output.writeBytes(controlBytes);
        output.writeBytes(buffer, 0, writtenSize);
        return output.size();
    }

    public static int maxEncodedSize(int size)
    {
        return size * SIZE_OF_INT + controlBytesTableSize(size);
    }

    static int controlBytesTableSize(int size)
    {
        return (size + GROUP_SIZE - 1) / GROUP_SIZE;
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
        byte[] controlBytes = new byte[controlBytesTableSize(length)];
        input.readBytes(controlBytes);
        int index = 0;
        for (byte controlByte : controlBytes) {
            for (int i = 0; i < GROUP_SIZE; i++) {
                byte valueSize = (byte) (((controlByte >> (i * 2)) & 0x03) + 1);
                destination[destinationIndex + index++] = getValue(input, valueSize);
                if (index >= length) {
                    break;
                }
            }
        }
    }

    static byte byteWidth(int value)
    {
        if ((value & 0xFFFFFF80) == 0) {
            return 1;
        }
        if ((value & 0xFFFF8000) == 0) {
            return 2;
        }
        if ((value & 0xFF800000) == 0) {
            return 3;
        }
        return 4;
    }

    private static void putValue(SliceOutput buffer, int value, byte byteSize)
    {
        switch (byteSize) {
            case 1:
                buffer.writeByte(value); // 1 byte
                return;
            case 2:
                buffer.writeShort(value); // 2 bytes
                return;
            case 3:
                buffer.writeShort(value & 0xFFFF); // 2 bytes
                buffer.writeByte((value >> 16) & 0xFF); // 1 byte +
                return;
            case 4:
                buffer.writeInt(value); // 4 bytes
                return;
        }
        throw new IllegalArgumentException("Invalid byte size: " + byteSize);
    }

    private static int getValue(SliceInput input, byte byteSize)
    {
        return switch (byteSize) {
            case 1 -> input.readByte();
            case 2 -> input.readShort();
            case 3 -> input.readShort() & 0xFFFF | (input.readByte() << 16);
            case 4 -> input.readInt();
            default -> throw new IllegalArgumentException("Invalid byte size: " + byteSize);
        };
    }
}
