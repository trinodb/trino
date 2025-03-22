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
package io.trino.hive.formats;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// faster versions of org.apache.hadoop.io.WritableUtils methods adapted for Slice
public final class ReadWriteUtils
{
    // 0xFFFF_FFFF + syncFirst(long) + syncSecond(long)
    private static final int SYNC_SEQUENCE_LENGTH = SIZE_OF_INT + SIZE_OF_LONG + SIZE_OF_LONG;

    private ReadWriteUtils() {}

    public static int decodeVIntSize(Slice slice, int offset)
    {
        return decodeVIntSize(slice.getByte(offset));
    }

    public static int decodeVIntSize(byte value)
    {
        if (value >= -112) {
            return 1;
        }
        if (value < -120) {
            return -119 - value;
        }
        return -111 - value;
    }

    public static boolean isNegativeVInt(Slice slice, int offset)
    {
        return isNegativeVInt(slice.getByte(offset));
    }

    public static boolean isNegativeVInt(byte value)
    {
        return value < -120 || (value >= -112 && value < 0);
    }

    public static long readVInt(TrinoDataInputStream in)
            throws IOException
    {
        byte firstByte = in.readByte();
        int length = decodeVIntSize(firstByte);
        if (length == 1) {
            return firstByte;
        }

        long value = 0;
        for (int i = 1; i < length; i++) {
            value <<= 8;
            value |= (in.readByte() & 0xFF);
        }
        return isNegativeVInt(firstByte) ? ~value : value;
    }

    public static long readVInt(SliceInput in)
    {
        byte firstByte = in.readByte();
        int length = decodeVIntSize(firstByte);
        if (length == 1) {
            return firstByte;
        }

        long value = 0;
        for (int i = 1; i < length; i++) {
            value <<= 8;
            value |= (in.readByte() & 0xFF);
        }
        return isNegativeVInt(firstByte) ? ~value : value;
    }

    public static long readVInt(Slice slice, int start)
    {
        byte firstByte = slice.getByte(start);
        int length = decodeVIntSize(firstByte);
        if (length == 1) {
            return firstByte;
        }

        return readVIntInternal(slice, start, length);
    }

    public static long readVInt(Slice slice, int start, int length)
    {
        if (length == 1) {
            return slice.getByte(start);
        }

        return readVIntInternal(slice, start, length);
    }

    private static long readVIntInternal(Slice slice, int start, int length)
    {
        long value = 0;
        for (int i = 1; i < length; i++) {
            value <<= 8;
            value |= (slice.getByte(start + i) & 0xFF);
        }
        return isNegativeVInt(slice.getByte(start)) ? ~value : value;
    }

    /**
     * Find the beginning of the first full sync sequence that starts within the specified range.
     */
    public static long findFirstSyncPosition(TrinoInputFile inputFile, long offset, long length, long syncFirst, long syncSecond)
            throws IOException
    {
        requireNonNull(inputFile, "inputFile is null");
        checkArgument(offset >= 0, "offset is negative");
        checkArgument(length >= 1, "length must be at least 1");
        checkArgument(offset + length <= inputFile.length(), "offset plus length is greater than data size");

        // The full sync sequence is "0xFFFFFFFF syncFirst syncSecond".  If
        // this sequence begins the file range, the start position is returned
        // even if the sequence finishes after length.
        // NOTE: this decision must agree with RcFileReader.nextBlock

        Slice sync = Slices.allocate(SIZE_OF_INT + SIZE_OF_LONG + SIZE_OF_LONG);
        sync.setInt(0, 0xFFFF_FFFF);
        sync.setLong(SIZE_OF_INT, syncFirst);
        sync.setLong(SIZE_OF_INT + SIZE_OF_LONG, syncSecond);

        // read 4 MB chunks at a time, but only skip ahead 4 MB - SYNC_SEQUENCE_LENGTH bytes
        // this causes a re-read of SYNC_SEQUENCE_LENGTH bytes each time, but is much simpler code
        byte[] buffer = new byte[toIntExact(min(1 << 22, length + (SYNC_SEQUENCE_LENGTH - 1)))];
        Slice bufferSlice = Slices.wrappedBuffer(buffer);
        try (TrinoInput input = inputFile.newInput()) {
            for (long position = 0; position < length; position += bufferSlice.length() - (SYNC_SEQUENCE_LENGTH - 1)) {
                // either fill the buffer entirely, or read enough to allow all bytes in offset + length to be a start sequence
                int bufferSize = toIntExact(min(buffer.length, length + (SYNC_SEQUENCE_LENGTH - 1) - position));
                // don't read off the end of the file
                bufferSize = toIntExact(min(bufferSize, inputFile.length() - offset - position));

                input.readFully(offset + position, buffer, 0, bufferSize);

                // find the starting index position of the sync sequence
                int index = bufferSlice.indexOf(sync);
                if (index >= 0) {
                    // If the starting position is before the end of the search region, return the
                    // absolute start position of the sequence.
                    if (position + index < length) {
                        long startOfSyncSequence = offset + position + index;
                        return startOfSyncSequence;
                    }
                    // Otherwise, this is not a match for this region
                    // Note: this case isn't strictly needed as the loop will exit, but it is
                    // simpler to explicitly call it out.
                    return -1;
                }
            }
        }
        return -1;
    }

    public static void writeLengthPrefixedString(DataOutputStream out, Slice slice)
            throws IOException
    {
        writeVInt(out, slice.length());
        out.write(slice);
    }

    public static void writeVInt(DataOutputStream out, int value)
            throws IOException
    {
        if (value >= -112 && value <= 127) {
            out.writeByte(value);
            return;
        }

        int length = -112;
        if (value < 0) {
            value ^= -1; // take one's complement'
            length = -120;
        }

        int tmp = value;
        while (tmp != 0) {
            tmp = tmp >> 8;
            length--;
        }

        out.writeByte(length);

        length = (length < -120) ? -(length + 120) : -(length + 112);

        for (int idx = length; idx != 0; idx--) {
            int shiftBits = (idx - 1) * 8;
            out.writeByte((value >> shiftBits) & 0xFF);
        }
    }

    public static int computeVIntLength(int value)
    {
        if (value >= -112 && value <= 127) {
            return 1;
        }

        if (value < 0) {
            // one's complement
            value ^= -1;
        }

        return ((31 - Integer.numberOfLeadingZeros(value)) / 8) + 2;
    }

    public static void writeVInt(SliceOutput out, int value)
    {
        if (value >= -112 && value <= 127) {
            out.writeByte(value);
            return;
        }

        int length = -112;
        if (value < 0) {
            value ^= -1; // take one's complement'
            length = -120;
        }

        int tmp = value;
        while (tmp != 0) {
            tmp = tmp >> 8;
            length--;
        }

        out.writeByte(length);

        length = (length < -120) ? -(length + 120) : -(length + 112);

        for (int idx = length; idx != 0; idx--) {
            int shiftBits = (idx - 1) * 8;
            out.writeByte((value >> shiftBits) & 0xFF);
        }
    }

    public static void writeVLong(SliceOutput out, long value)
    {
        if (value >= -112 && value <= 127) {
            out.writeByte((byte) value);
            return;
        }

        int length = -112;
        if (value < 0) {
            value ^= -1; // take one's complement'
            length = -120;
        }

        long tmp = value;
        while (tmp != 0) {
            tmp = tmp >> 8;
            length--;
        }

        out.writeByte(length);

        length = (length < -120) ? -(length + 120) : -(length + 112);

        for (int idx = length; idx != 0; idx--) {
            int shiftBits = (idx - 1) * 8;
            long mask = 0xFFL << shiftBits;
            out.writeByte((byte) ((value & mask) >> shiftBits));
        }
    }

    public static int calculateTruncationLength(Type type, Slice slice, int offset, int length)
    {
        requireNonNull(type, "type is null");
        if (type instanceof VarcharType varcharType) {
            if (varcharType.isUnbounded()) {
                return length;
            }
            return calculateTruncationLength(varcharType.getBoundedLength(), slice, offset, length);
        }
        if (type instanceof CharType charType) {
            int truncationLength = calculateTruncationLength(charType.getLength(), slice, offset, length);
            // At run-time char(x) is represented without trailing spaces
            while (truncationLength > 0 && slice.getByte(offset + truncationLength - 1) == ' ') {
                truncationLength--;
            }
            return truncationLength;
        }
        return length;
    }

    private static int calculateTruncationLength(int maxCharacterCount, Slice slice, int offset, int length)
    {
        requireNonNull(slice, "slice is null");
        if (maxCharacterCount < 0) {
            throw new IllegalArgumentException("Max length must be greater or equal than zero");
        }
        if (length <= maxCharacterCount) {
            return length;
        }

        int indexEnd = offsetOfCodePoint(slice, offset, maxCharacterCount);
        if (indexEnd < 0) {
            return length;
        }
        // end index could run over length because of large code points (e.g., 4-byte code points)
        // or within length because of small code points (e.g., 1-byte code points)
        return min(indexEnd - offset, length);
    }
}
