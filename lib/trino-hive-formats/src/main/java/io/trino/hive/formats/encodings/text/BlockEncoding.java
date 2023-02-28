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
import io.trino.hive.formats.encodings.ColumnData;
import io.trino.hive.formats.encodings.EncodeOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

public abstract class BlockEncoding
        implements TextColumnEncoding
{
    private final Type type;
    protected final Slice nullSequence;
    private final Byte escapeByte;

    public BlockEncoding(Type type, Slice nullSequence, Byte escapeByte)
    {
        this.type = type;
        this.nullSequence = nullSequence;
        this.escapeByte = escapeByte;
    }

    @Override
    public final void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
            throws FileCorruptionException
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                output.writeBytes(nullSequence);
            }
            else {
                encodeValueInto(block, position, output);
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public final Block decodeColumn(ColumnData columnData)
            throws FileCorruptionException
    {
        int size = columnData.rowCount();

        Slice slice = columnData.getSlice();
        BlockBuilder builder = type.createBlockBuilder(null, size);
        for (int i = 0; i < size; i++) {
            int length = columnData.getLength(i);
            int offset = columnData.getOffset(i);
            if (!isNullSequence(slice, offset, length)) {
                decodeValueInto(builder, slice, offset, length);
            }
            else {
                builder.appendNull();
            }
        }

        return builder.build();
    }

    protected final boolean isNullSequence(Slice slice, int offset, int length)
    {
        return nullSequence.equals(0, nullSequence.length(), slice, offset, length);
    }

    protected final boolean isEscapeByte(byte currentByte)
    {
        return escapeByte != null && currentByte == escapeByte;
    }
}
