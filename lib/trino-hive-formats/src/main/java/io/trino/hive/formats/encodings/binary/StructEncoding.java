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
package io.trino.hive.formats.encodings.binary;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.RowType;

import java.util.List;

public class StructEncoding
        extends BlockEncoding
{
    private final List<BinaryColumnEncoding> structFields;
    private final RowType rowType;

    public StructEncoding(RowType rowType, List<BinaryColumnEncoding> structFields)
    {
        super(rowType);
        this.rowType = rowType;
        this.structFields = ImmutableList.copyOf(structFields);
    }

    @Override
    public void encodeValue(Block block, int position, SliceOutput output)
    {
        SqlRow row = rowType.getObject(block, position);
        int rawIndex = row.getRawIndex();

        // write values
        for (int batchStart = 0; batchStart < row.getFieldCount(); batchStart += 8) {
            int batchEnd = Math.min(batchStart + 8, structFields.size());

            int nullByte = 0;
            for (int fieldId = batchStart; fieldId < batchEnd; fieldId++) {
                if (!row.getRawFieldBlock(fieldId).isNull(rawIndex)) {
                    nullByte |= (1 << (fieldId % 8));
                }
            }
            output.writeByte(nullByte);
            for (int fieldId = batchStart; fieldId < batchEnd; fieldId++) {
                Block fieldBlock = row.getRawFieldBlock(fieldId);
                if (!fieldBlock.isNull(rawIndex)) {
                    BinaryColumnEncoding field = structFields.get(fieldId);
                    field.encodeValueInto(fieldBlock, rawIndex, output);
                }
            }
        }
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
    {
        ((RowBlockBuilder) builder).buildEntry(fieldBuilders -> {
            int fieldId = 0;
            int nullByte = 0;
            int elementOffset = offset;
            while (fieldId < structFields.size() && elementOffset < offset + length) {
                BinaryColumnEncoding field = structFields.get(fieldId);

                // null byte prefixes every 8 fields
                if ((fieldId % 8) == 0) {
                    nullByte = slice.getByte(elementOffset);
                    elementOffset++;
                }

                // read field
                if ((nullByte & (1 << (fieldId % 8))) != 0) {
                    int valueOffset = field.getValueOffset(slice, elementOffset);
                    int valueLength = field.getValueLength(slice, elementOffset);

                    field.decodeValueInto(fieldBuilders.get(fieldId), slice, elementOffset + valueOffset, valueLength);

                    elementOffset = elementOffset + valueOffset + valueLength;
                }
                else {
                    fieldBuilders.get(fieldId).appendNull();
                }
                fieldId++;
            }
            // Sometimes a struct does not have all fields written, so we fill with nulls
            while (fieldId < structFields.size()) {
                fieldBuilders.get(fieldId).appendNull();
                fieldId++;
            }
        });
    }
}
