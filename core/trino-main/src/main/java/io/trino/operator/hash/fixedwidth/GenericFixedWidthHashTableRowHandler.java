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

import io.trino.operator.hash.ColumnValueExtractor;
import io.trino.operator.hash.GroupByHashTableEntries;
import io.trino.operator.hash.HashTableRowHandler;
import io.trino.operator.hash.fastbb.FastByteBuffer;
import io.trino.spi.Page;
import io.trino.spi.block.Block;

import static java.util.Objects.requireNonNull;

public class GenericFixedWidthHashTableRowHandler
        implements HashTableRowHandler
{
    private final FixedWidthEntryStructure structure;

    public GenericFixedWidthHashTableRowHandler(FixedWidthEntryStructure structure)
    {
        this.structure = requireNonNull(structure, "structure is null");
    }

    @Override
    public void putEntry(GroupByHashTableEntries data, int entriesPosition, int groupId, Page page, int position, long rawHash)
    {
        FixedWidthGroupByHashTableEntries entries = (FixedWidthGroupByHashTableEntries) data;
        entries.putGroupId(entriesPosition, groupId);
        entries.putHash(entriesPosition, rawHash);

        int valuesOffset = entries.getValuesOffset(entriesPosition);
        FastByteBuffer buffer = entries.getBuffer();

        ColumnValueExtractor[] columnValueExtractors = structure.getColumnValueExtractors();
        int[] valuesOffsets = structure.getValuesOffsets();
        for (int i = 0; i < structure.getHashChannelsCount(); i++) {
            Block block = page.getBlock(i);
            byte valueIsNull = (byte) (block.isNull(position) ? 1 : 0);
            entries.putIsNull(entriesPosition, i, valueIsNull);
            columnValueExtractors[i].putValue(buffer, valuesOffset + valuesOffsets[i], block, position);
        }
    }

    @Override
    public boolean keyEquals(GroupByHashTableEntries entries, int entriesPosition, Page page, int position, long rawHash)
    {
//        if (rawHash != entries.getHash(entriesPosition)) {
//            return false;
//        }

        FixedWidthGroupByHashTableEntries table = (FixedWidthGroupByHashTableEntries) entries;

        int valuesOffset = table.getValuesOffset(entriesPosition);
        FastByteBuffer buffer = table.getBuffer();
        ColumnValueExtractor[] columnValueExtractors = structure.getColumnValueExtractors();
        int[] valuesOffsets = structure.getValuesOffsets();
        for (int i = 0; i < structure.getHashChannelsCount(); i++) {
            Block block = page.getBlock(i);

            boolean blockValueNull = block.isNull(position);
            boolean tableValueIsNull = table.isNull(entriesPosition, i) == 1;

            if (!(blockValueNull == tableValueIsNull &&
                    columnValueExtractors[i].valueEquals(buffer, valuesOffset + valuesOffsets[i], block, position))) {
                return false;
            }
        }
        return true;
    }
}
