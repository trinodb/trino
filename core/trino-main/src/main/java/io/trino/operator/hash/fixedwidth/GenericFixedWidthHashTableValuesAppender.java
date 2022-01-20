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
package io.trino.operator.hash.fixedwidth;

import io.trino.operator.hash.AbstractHashTableValuesAppender;
import io.trino.operator.hash.ColumnValueExtractor;
import io.trino.operator.hash.HashTableData;
import io.trino.operator.hash.fastbb.FastByteBuffer;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;

import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class GenericFixedWidthHashTableValuesAppender
        extends AbstractHashTableValuesAppender
{
    private final FixedWidthEntryStructure structure;

    public GenericFixedWidthHashTableValuesAppender(FixedWidthEntryStructure structure)
    {
        this.structure = requireNonNull(structure, "structure is null");
    }

    @Override
    public void appendValuesTo(HashTableData data, int groupId, PageBuilder pageBuilder, int outputChannelOffset, boolean outputHash)
    {
        FixedWidthGroupByHashTableEntries entries = (FixedWidthGroupByHashTableEntries) data.entries();
        int position = data.getPosition(groupId);
        FastByteBuffer buffer = entries.getBuffer();
        int valuesOffset = entries.getValuesOffset(position);
        ColumnValueExtractor[] columnValueExtractors = structure.getColumnValueExtractors();
        int[] valuesOffsets = structure.getValuesOffsets();

        for (int i = 0; i < structure.getHashChannelsCount(); i++, outputChannelOffset++) {
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
            if (entries.isNull(position, i) == 1) {
                blockBuilder.appendNull();
            }
            else {
                columnValueExtractors[i].appendValue(buffer, valuesOffset + valuesOffsets[i], blockBuilder);
            }
        }
        if (outputHash) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
            BIGINT.writeLong(hashBlockBuilder, entries.getHash(position));
        }
    }
}
