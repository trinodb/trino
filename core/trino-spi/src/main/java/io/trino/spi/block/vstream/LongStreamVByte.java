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

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.Math.min;

public class LongStreamVByte
{
    private static final int GROUP_SIZE = 2; // 4 bits to encode the length of the long (1 - 8 bytes)

    private LongStreamVByte() {}

    public static int writeLongs(SliceOutput output, long[] values)
    {
        return writeLongs(output, values, 0, values.length);
    }

    public static int writeLongs(SliceOutput output, long[] values, int sourceIndex, int length)
    {
        Slice buffer = Slices.allocate(length * SIZE_OF_LONG); // TODO: this is needed due to the encryption/compression working across write boundaries
        SliceOutput bufferOutput = buffer.getOutput();
        byte[] controlBytes = new byte[controlBytesTableSize(length)];
        int groupIndex = 0;
        int writtenSize = 0;
        while (groupIndex < length) {
            byte controlByte = 0;

            for (int i = 0; i < min(GROUP_SIZE, length - groupIndex); i++) {
                int byteSize = byteWidth(values[sourceIndex + groupIndex + i]);
                writtenSize += byteSize;
                controlByte |= (byte) ((byteSize - 1) << (i * 4));
                putValue(bufferOutput, values[sourceIndex + groupIndex + i], byteSize);
            }
            controlBytes[groupIndex / GROUP_SIZE] = controlByte;
            groupIndex += GROUP_SIZE;
        }

        output.writeBytes(controlBytes);
        output.writeBytes(buffer, 0, writtenSize);
        return output.size();
    }

    public static int maxEncodedSize(int size)
    {
        return (size * SIZE_OF_LONG) + controlBytesTableSize(size);
    }

    static int controlBytesTableSize(int size)
    {
        return (size + GROUP_SIZE - 1) / GROUP_SIZE;
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
        byte[] controlBytes = new byte[controlBytesTableSize(length)];
        input.readBytes(controlBytes);
        int index = 0;
        for (byte controlByte : controlBytes) {
            for (int i = 0; i < GROUP_SIZE; i++) {
                int valueSize = ((controlByte >> (i * 4)) & 0x0F) + 1;
                destination[destinationIndex + index++] = getValue(input, valueSize);
                if (index >= length) {
                    break;
                }
            }
        }
    }

    static int byteWidth(long value)
    {
        if ((value & 0xFFFFFFFFFFFFFF80L) == 0) {
            return 1;
        }
        if ((value & 0xFFFFFFFFFFFF8000L) == 0) {
            return 2;
        }
        if ((value & 0xFFFFFFFFFF800000L) == 0) {
            return 3;
        }
        if ((value & 0xFFFFFFFF80000000L) == 0) {
            return 4;
        }
        if ((value & 0xFFFFFF8000000000L) == 0) {
            return 5;
        }
        if ((value & 0xFFFF800000000000L) == 0) {
            return 6;
        }
        if ((value & 0xFF80000000000000L) == 0) {
            return 7;
        }
        return 8;
    }

    private static void putValue(SliceOutput buffer, long value, int byteSize)
    {
        switch (byteSize) {
            case 1:
                buffer.writeByte((byte) value); // 1 byte
                return;
            case 2:
                buffer.writeShort((short) value); // 2 bytes
                return;
            case 3:
                buffer.writeShort((int) (value & 0xFFFF)); // 2 bytes
                buffer.writeByte((int) (value >> 16) & 0xFF); // 1 byte +
                return;
            case 4:
                buffer.writeInt((int) value); // 4 bytes
                return;
            case 5:
                buffer.writeInt((int) (value & 0xFFFFFFFFL)); // 4 bytes
                buffer.writeByte((byte) (value >> 32) & 0xFFFF); // 1 byte +
                return;
            case 6:
                buffer.writeInt((int) (value & 0xFFFFFFFFL));
                buffer.writeShort((int) ((value >> 32) & 0xFFFF)); // 2 bytes +
                return;
            case 7:
                buffer.writeInt((int) (value & 0xFFFFFFFFL)); // 4 bytes +
                buffer.writeShort((int) ((value >> 32) & 0xFFFF)); // 2 bytes +
                buffer.writeByte((int) (value >> 48)); // 1 byte
                return;
            case 8:
                buffer.writeLong(value); // 8 bytes
                return;
        }

        throw new IllegalArgumentException("Invalid byte size: " + byteSize);
    }

    private static long getValue(SliceInput input, int byteSize)
    {
        return switch (byteSize) {
            case 1 -> input.readByte();
            case 2 -> input.readShort();
            case 3 -> input.readShort() & 0xFFFF | (input.readByte() << 16);
            case 4 -> input.readInt();
            case 5 -> input.readInt() & 0xFFFFFFFFL | ((input.readByte() & 0xFFL) << 32);
            case 6 -> input.readInt() & 0xFFFFFFFFL | (input.readShort() & 0xFFFFL) << 32;
            case 7 -> input.readInt() & 0xFFFFFFFFL | (input.readShort() & 0xFFFFL) << 32 | (input.readByte() & 0xFFL) << 48;
            case 8 -> input.readLong();
            default -> throw new IllegalArgumentException("Invalid byte size: " + byteSize);
        };
    }
}
