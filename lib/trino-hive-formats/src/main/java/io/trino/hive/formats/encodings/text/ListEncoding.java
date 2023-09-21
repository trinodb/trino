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
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

public class ListEncoding
        extends BlockEncoding
{
    private final byte separator;
    private final TextColumnEncoding elementEncoding;

    public ListEncoding(Type type, Slice nullSequence, byte separator, Byte escapeByte, TextColumnEncoding elementEncoding)
    {
        super(type, nullSequence, escapeByte);
        this.separator = separator;
        this.elementEncoding = elementEncoding;
    }

    @Override
    public void encodeValueInto(Block block, int position, SliceOutput output)
            throws FileCorruptionException
    {
        Block list = block.getObject(position, Block.class);
        for (int elementIndex = 0; elementIndex < list.getPositionCount(); elementIndex++) {
            if (elementIndex > 0) {
                output.writeByte(separator);
            }
            if (list.isNull(elementIndex)) {
                output.writeBytes(nullSequence);
            }
            else {
                elementEncoding.encodeValueInto(list, elementIndex, output);
            }
        }
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
            throws FileCorruptionException
    {
        ((ArrayBlockBuilder) builder).buildEntry(elementBuilder -> decodeArrayInto(elementBuilder, slice, offset, length));
    }

    private void decodeArrayInto(BlockBuilder elementBuilder, Slice slice, int offset, int length)
            throws FileCorruptionException
    {
        if (length <= 0) {
            return;
        }

        int end = offset + length;
        int elementOffset = offset;
        while (offset < end) {
            byte currentByte = slice.getByte(offset);
            if (currentByte == separator) {
                decodeElementValueInto(elementBuilder, slice, elementOffset, offset - elementOffset);
                elementOffset = offset + 1;
            }
            else if (isEscapeByte(currentByte) && offset + 1 < length) {
                // ignore the char after escape_char
                offset++;
            }
            offset++;
        }
        decodeElementValueInto(elementBuilder, slice, elementOffset, offset - elementOffset);
    }

    private void decodeElementValueInto(BlockBuilder blockBuilder, Slice slice, int offset, int length)
            throws FileCorruptionException
    {
        if (nullSequence.equals(0, nullSequence.length(), slice, offset, length)) {
            blockBuilder.appendNull();
        }
        else {
            elementEncoding.decodeValueInto(blockBuilder, slice, offset, length);
        }
    }
}
