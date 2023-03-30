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
package io.trino.hive.formats.encodings.text;

import com.google.common.primitives.UnsignedBytes;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.hive.formats.encodings.ColumnData;
import io.trino.hive.formats.encodings.EncodeOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.Type;

import static io.trino.hive.formats.ReadWriteUtils.calculateTruncationLength;

public class StringEncoding
        implements TextColumnEncoding
{
    private final Type type;
    private final Slice nullSequence;
    private final byte escapeByte;
    private final boolean[] needsEscape;
    private final DynamicSliceOutput escapeBuffer;

    public StringEncoding(Type type, Slice nullSequence, Byte escapeChar, Slice separators)
    {
        this.type = type;
        this.nullSequence = nullSequence;
        if (escapeChar == null) {
            escapeByte = 0;
            escapeBuffer = null;
            needsEscape = null;
        }
        else {
            escapeByte = escapeChar;
            escapeBuffer = new DynamicSliceOutput(1024);
            needsEscape = new boolean[256];
            needsEscape[UnsignedBytes.toInt(escapeByte)] = true;
            for (int i = 0; i < separators.length(); i++) {
                needsEscape[UnsignedBytes.toInt(separators.getByte(i))] = true;
            }
        }
    }

    @Override
    public void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                output.writeBytes(nullSequence);
            }
            else {
                encodeValue(block, output, position);
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public void encodeValueInto(Block block, int position, SliceOutput output)
    {
        encodeValue(block, output, position);
    }

    private void encodeValue(Block block, SliceOutput output, int position)
    {
        Slice slice = type.getSlice(block, position);
        if (type instanceof CharType charType) {
            slice = Chars.padSpaces(charType.getSlice(block, position), charType);
        }
        if (needsEscape == null) {
            output.writeBytes(slice);
            return;
        }

        escapeBuffer.reset();
        for (int i = 0; i < slice.length(); i++) {
            int b = slice.getUnsignedByte(i);
            if (needsEscape[b]) {
                output.appendByte(escapeByte);
            }
            output.appendByte(b);
        }
    }

    @Override
    public Block decodeColumn(ColumnData columnData)
    {
        if (needsEscape != null) {
            columnData = unescape(columnData, escapeByte);
        }

        int size = columnData.rowCount();
        BlockBuilder builder = type.createBlockBuilder(null, size);

        Slice slice = columnData.getSlice();
        for (int i = 0; i < size; i++) {
            int offset = columnData.getOffset(i);
            int length = columnData.getLength(i);
            if (nullSequence.equals(0, nullSequence.length(), slice, offset, length)) {
                builder.appendNull();
            }
            else {
                length = calculateTruncationLength(type, slice, offset, length);
                type.writeSlice(builder, slice, offset, length);
            }
        }
        return builder.build();
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
    {
        if (needsEscape != null) {
            Slice newSlice = Slices.allocate(slice.length());
            SliceOutput output = newSlice.getOutput();
            unescape(escapeByte, output, slice, offset, length);
            slice = newSlice;
            offset = 0;
            length = output.size();
        }

        length = calculateTruncationLength(type, slice, offset, length);
        type.writeSlice(builder, slice, offset, length);
    }

    private static ColumnData unescape(ColumnData columnData, byte escapeByte)
    {
        Slice slice = columnData.getSlice();
        // does slice contain escape byte
        if (slice.indexOfByte(escapeByte) < 0) {
            return columnData;
        }

        Slice newSlice = Slices.allocate(slice.length());
        SliceOutput output = newSlice.getOutput();
        int[] newOffsets = new int[columnData.rowCount() + 1];
        for (int row = 0; row < columnData.rowCount(); row++) {
            int offset = columnData.getOffset(row);
            int length = columnData.getLength(row);

            unescape(escapeByte, output, slice, offset, length);
            newOffsets[row + 1] = output.size();
        }
        return new ColumnData(newOffsets, output.slice());
    }

    private static void unescape(byte escapeByte, SliceOutput output, Slice slice, int offset, int length)
    {
        for (int i = 0; i < length; i++) {
            byte value = slice.getByte(offset + i);
            if (value == escapeByte && i + 1 < length) {
                // skip the escape byte
                continue;
            }
            output.write(value);
        }
    }
}
