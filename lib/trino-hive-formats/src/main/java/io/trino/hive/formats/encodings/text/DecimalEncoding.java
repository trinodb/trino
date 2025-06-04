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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.hive.formats.HiveFormatUtils.writeDecimal;

public class DecimalEncoding
        implements TextColumnEncoding
{
    private final DecimalType type;
    private final Slice nullSequence;

    public DecimalEncoding(Type type, Slice nullSequence)
    {
        this.type = (DecimalType) type;
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
        if (type.isShort()) {
            output.writeBytes(utf8Slice(Decimals.toString(type.getLong(block, position), type.getScale())));
        }
        else {
            output.writeBytes(utf8Slice(Decimals.toString((Int128) type.getObject(block, position), type.getScale())));
        }
    }

    @Override
    public Block decodeColumn(ColumnData columnData)
    {
        int size = columnData.rowCount();
        BlockBuilder builder = type.createFixedSizeBlockBuilder(size);

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
        try {
            writeDecimal(slice.toStringAscii(offset, length), type, builder);
        }
        catch (NumberFormatException e) {
            builder.appendNull();
        }
    }
}
