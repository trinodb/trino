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
import io.trino.hive.formats.encodings.ColumnData;
import io.trino.hive.formats.encodings.EncodeOutput;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class LongEncoding
        implements TextColumnEncoding
{
    private final Type type;
    private final Slice nullSequence;
    private final StringBuilder buffer = new StringBuilder();

    public LongEncoding(Type type, Slice nullSequence)
    {
        this.type = type;
        this.nullSequence = nullSequence;
    }

    @Override
    public void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                output.writeBytes(nullSequence);
            }
            else {
                long value = type.getLong(block, position);
                buffer.setLength(0);
                buffer.append(value);
                for (int index = 0; index < buffer.length(); index++) {
                    output.writeByte(buffer.charAt(index));
                }
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public void encodeValueInto(Block block, int position, SliceOutput output)
    {
        long value = type.getLong(block, position);
        buffer.setLength(0);
        buffer.append(value);
        for (int index = 0; index < buffer.length(); index++) {
            output.writeByte(buffer.charAt(index));
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
            if (length == 0 || nullSequence.equals(0, nullSequence.length(), slice, offset, length)) {
                builder.appendNull();
            }
            else {
                decodeValue(builder, slice, offset, length);
            }
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
        if (length == 0) {
            builder.appendNull();
            return;
        }

        try {
            type.writeLong(builder, parseLong(slice, offset, length));
        }
        catch (TrinoException e) {
            // writeLong for INTEGER, SMALLINT, and TINYINT will throw
            // GENERIC_INTERNAL_ERROR when the value is out of range, and
            // in that text format writes null
            if (e.getErrorCode() != GENERIC_INTERNAL_ERROR.toErrorCode()) {
                throw e;
            }
            builder.appendNull();
        }
        catch (NumberFormatException ignored) {
            builder.appendNull();
        }
    }

    private static long parseLong(Slice slice, int start, int length)
    {
        int limit = start + length;

        // process optional sign
        boolean negative = false;
        if (slice.getByte(start) == '-') {
            negative = true;
            start++;
        }
        else if (slice.getByte(start) == '+') {
            start++;
        }

        // an empty string or a sign just by itself is not allowed
        if (start == limit) {
            throw new NumberFormatException();
        }

        // skip leading zeros
        while (start < limit && slice.getByte(start) == '0') {
            start++;
        }

        // decimal parts of number must be valid, but is otherwise ignored
        limit = truncateDecimal(slice, start, limit);

        // if the number is longer than 19 characters is it out of range for a long
        // we check length here as it avoids many special cases in the code below
        if (limit - start > 19) {
            throw new NumberFormatException();
        }

        long value = 0;
        while (start < limit) {
            byte b = slice.getByte(start);
            start++;

            value = value * 10 + toDigit(b);
            if (value < 0 && value != Long.MIN_VALUE) {
                throw new NumberFormatException();
            }
        }

        if (negative) {
            value = -value;
            if (value > 0) {
                throw new NumberFormatException();
            }
        }
        else if (value < 0) {
            throw new NumberFormatException();
        }
        return value;
    }

    private static int truncateDecimal(Slice slice, int start, int limit)
    {
        for (int position = start; position < limit; position++) {
            byte b = slice.getByte(position);
            if (b == '.') {
                // verify the decimal part only contains valid digits
                for (int decimalPosition = position + 1; decimalPosition < limit; decimalPosition++) {
                    toDigit(slice.getByte(decimalPosition));
                }
                // ignore the decimal part
                return position;
            }
        }
        return limit;
    }

    private static int toDigit(byte b)
    {
        if (b > '9' || b < '0') {
            throw new NumberFormatException();
        }
        return b - ((int) '0');
    }
}
