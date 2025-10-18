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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.hive.formats.encodings.ColumnData;
import io.trino.hive.formats.encodings.EncodeOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

public class BooleanEncoding
        implements TextColumnEncoding
{
    private static final Slice TRUE = Slices.utf8Slice("true");
    private static final Slice FALSE = Slices.utf8Slice("false");

    private final Type type;
    private final boolean isExtendedBooleanLiteral;
    private final Slice nullSequence;

    public BooleanEncoding(Type type, Slice nullSequence, boolean isExtendedBooleanLiteral)
    {
        this.type = type;
        this.nullSequence = nullSequence;
        this.isExtendedBooleanLiteral = isExtendedBooleanLiteral;
    }

    @Override
    public void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                output.writeBytes(nullSequence);
            }
            else {
                encodeValue(block, position, output);
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public void encodeValueInto(Block block, int position, SliceOutput output)
    {
        encodeValue(block, position, output);
    }

    private void encodeValue(Block block, int position, SliceOutput output)
    {
        if (type.getBoolean(block, position)) {
            output.writeBytes(TRUE);
        }
        else {
            output.writeBytes(FALSE);
        }
    }

    @Override
    public Block decodeColumn(ColumnData columnData)
    {
        int size = columnData.rowCount();
        BlockBuilder builder = type.createBlockBuilder(null, size);

        Slice slice = columnData.getSlice();
        for (int i = 0; i < size; i++) {
            int offset = columnData.getOffset(i);
            int length = columnData.getLength(i);
            decodeValue(builder, slice, offset, length);
        }
        return builder.build();
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
    {
        decodeValue(builder, slice, offset, length);
    }

    private void decodeValue(BlockBuilder builder, Slice slice, int offset, int length)
    {
        if (isTrue(slice, offset, length)) {
            type.writeBoolean(builder, true);
        }
        else if (isFalse(slice, offset, length)) {
            type.writeBoolean(builder, false);
        }
        else {
            builder.appendNull();
        }
    }

    @SuppressWarnings("PointlessArithmeticExpression")
    private boolean isFalse(Slice slice, int start, int length)
    {
        if (length == 5) {
            return (toUpperCase(slice.getByte(start + 0)) == 'F') &&
                    (toUpperCase(slice.getByte(start + 1)) == 'A') &&
                    (toUpperCase(slice.getByte(start + 2)) == 'L') &&
                    (toUpperCase(slice.getByte(start + 3)) == 'S') &&
                    (toUpperCase(slice.getByte(start + 4)) == 'E');
        }

        if (length == 1 && isExtendedBooleanLiteral) {
            byte byteValue = slice.getByte(start);
            return toUpperCase(byteValue) == 'F' ||
                    byteValue == '0';
        }
        return false;
    }

    @SuppressWarnings("PointlessArithmeticExpression")
    private boolean isTrue(Slice slice, int start, int length)
    {
        if (length == 4) {
            return (toUpperCase(slice.getByte(start + 0)) == 'T') &&
                    (toUpperCase(slice.getByte(start + 1)) == 'R') &&
                    (toUpperCase(slice.getByte(start + 2)) == 'U') &&
                    (toUpperCase(slice.getByte(start + 3)) == 'E');
        }
        if (length == 1 && isExtendedBooleanLiteral) {
            byte byteValue = slice.getByte(start);
            return toUpperCase(byteValue) == 'T' ||
                    byteValue == '1';
        }
        return false;
    }

    private static byte toUpperCase(byte b)
    {
        return isLowerCase(b) ? ((byte) (b - 32)) : b;
    }

    private static boolean isLowerCase(byte b)
    {
        return (b >= 'a') && (b <= 'z');
    }
}
