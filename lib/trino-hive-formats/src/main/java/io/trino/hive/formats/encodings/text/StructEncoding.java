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
import io.trino.hive.formats.FileCorruptionException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import java.util.List;

public class StructEncoding
        extends BlockEncoding
{
    private final byte separator;
    private final boolean lastColumnTakesRest;
    private final List<TextColumnEncoding> structFields;

    public StructEncoding(
            Type type,
            Slice nullSequence,
            byte separator,
            Byte escapeByte,
            boolean lastColumnTakesRest,
            List<TextColumnEncoding> structFields)
    {
        super(type, nullSequence, escapeByte);
        this.separator = separator;
        this.lastColumnTakesRest = lastColumnTakesRest;
        this.structFields = structFields;
    }

    @Override
    public void encodeValueInto(Block block, int position, SliceOutput output)
            throws FileCorruptionException
    {
        Block row = block.getObject(position, Block.class);
        for (int fieldIndex = 0; fieldIndex < structFields.size(); fieldIndex++) {
            if (fieldIndex > 0) {
                output.writeByte(separator);
            }

            if (row.isNull(fieldIndex)) {
                output.writeBytes(nullSequence);
            }
            else {
                structFields.get(fieldIndex).encodeValueInto(row, fieldIndex, output);
            }
        }
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
            throws FileCorruptionException
    {
        int end = offset + length;

        BlockBuilder structBuilder = builder.beginBlockEntry();
        int elementOffset = offset;
        int fieldIndex = 0;
        while (offset < end) {
            byte currentByte = slice.getByte(offset);
            if (currentByte == separator) {
                decodeElementValueInto(fieldIndex, structBuilder, slice, elementOffset, offset - elementOffset);
                elementOffset = offset + 1;
                fieldIndex++;
                if (lastColumnTakesRest && fieldIndex == structFields.size() - 1) {
                    // no need to process the remaining bytes as they are all assigned to the last column
                    break;
                }
                if (fieldIndex == structFields.size()) {
                    // this was the last field, so there is no more data to process
                    builder.closeEntry();
                    return;
                }
            }
            else if (isEscapeByte(currentByte)) {
                // ignore the char after escape_char
                offset++;
            }
            offset++;
        }
        decodeElementValueInto(fieldIndex, structBuilder, slice, elementOffset, end - elementOffset);
        fieldIndex++;

        // missing fields are null
        while (fieldIndex < structFields.size()) {
            structBuilder.appendNull();
            fieldIndex++;
        }

        builder.closeEntry();
    }

    private void decodeElementValueInto(int fieldIndex, BlockBuilder builder, Slice slice, int offset, int length)
            throws FileCorruptionException
    {
        // ignore extra fields
        if (fieldIndex >= structFields.size()) {
            return;
        }

        if (isNullSequence(slice, offset, length)) {
            builder.appendNull();
        }
        else {
            structFields.get(fieldIndex).decodeValueInto(builder, slice, offset, length);
        }
    }
}
