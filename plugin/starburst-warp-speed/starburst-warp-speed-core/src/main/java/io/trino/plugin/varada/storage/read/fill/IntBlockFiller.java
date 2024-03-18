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
package io.trino.plugin.varada.storage.read.fill;

import io.trino.plugin.varada.dictionary.ReadDictionary;
import io.trino.plugin.varada.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.type.IntegerType;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.Optional;

public class IntBlockFiller
        extends BlockFiller<Long>
{
    public IntBlockFiller()
    {
        super(IntegerType.INTEGER, BlockFillerType.INTEGER);
    }

    @Override
    protected Long getSingleValue(ReadJuffersWarmUpElement juffersWE, int recLength, int currPos)
    {
        return (long) ((IntBuffer) juffersWE.getRecordBuffer()).get();
    }

    @Override
    protected Block createSingleWithNullBlock(ReadJuffersWarmUpElement juffersWE, RecTypeCode recTypeCode, int recLength, int rowsToFill)
    {
        int singleValue = getSingleValue(juffersWE, recLength, 0).intValue();
        Block mappingBlock = createSingleMappingBlock(singleValue);
        return wrapSingleWithNulls(juffersWE, rowsToFill, mappingBlock, 1);
    }

    @Override
    protected Block fillRawBlock(ReadJuffersWarmUpElement juffersWE,
            RecTypeCode recTypeCode,
            int recLength,
            int rowsToFill,
            boolean collectNulls)
    {
        IntBuffer buff = (IntBuffer) juffersWE.getRecordBuffer();
        int[] values = new int[rowsToFill];
        Optional<boolean[]> nullsOptional;
        if (collectNulls) {
            ByteBuffer nullBuff = juffersWE.getNullBuffer();
            boolean[] nulls = new boolean[rowsToFill];
            for (int i = 0; i < rowsToFill; i++) {
                if (isNull(nullBuff, i)) {
                    nulls[i] = true;
                }
            }
            nullsOptional = Optional.of(nulls);
        }
        else {
            nullsOptional = Optional.empty();
        }
        buff.get(values);
        return new IntArrayBlock(rowsToFill, nullsOptional, values);
    }

    @Override
    protected Block createSingleBlockWithMapping(ReadJuffersWarmUpElement juffersWE, int mapKey, int rowsToFill, Block mapBlock, boolean collectNulls)
    {
        int singleVal = ((IntArrayBlock) mapBlock).getInt(mapKey);
        Block retBlock;

        if (!collectNulls) {
            retBlock = createSingleValueBlock(spiBuilderType, (long) singleVal, rowsToFill);
        }
        else {
            Block singleMapBlock = createSingleMappingBlock(singleVal);
            retBlock = wrapSingleWithNulls(juffersWE, rowsToFill, singleMapBlock, 1);
        }

        return retBlock;
    }

    @Override
    public Block fillRawBlockWithDictionary(ReadJuffersWarmUpElement juffersWE,
            int rowsToFill,
            RecTypeCode recTypeCode,
            int recTypeLength,
            boolean collectNulls,
            ReadDictionary readDictionary)
    {
        ShortBuffer buff = (ShortBuffer) juffersWE.getRecordBuffer();
        int[] ids = new int[rowsToFill];
        Block resultBlock;

        Block dictionaryAsBlock = readDictionary.getPreBlockDictionaryIfExists(rowsToFill);
        if (dictionaryAsBlock != null) {
            if (collectNulls) {
                int nullPosition = dictionaryAsBlock.getPositionCount() - 1;
                ByteBuffer nullBuff = juffersWE.getNullBuffer();
                for (int currRow = 0; currRow < rowsToFill; currRow++) {
                    if (isNull(nullBuff, currRow)) {
                        ids[currRow] = nullPosition;
                    }
                    else {
                        ids[currRow] = Short.toUnsignedInt(buff.get(currRow));
                    }
                }
            }
            else {
                for (int currRow = 0; currRow < rowsToFill; currRow++) {
                    ids[currRow] = Short.toUnsignedInt(buff.get(currRow));
                }
            }
            resultBlock = DictionaryBlock.create(ids.length, dictionaryAsBlock, ids);
        }
        else {
            int[] values = new int[rowsToFill];
            Optional<boolean[]> valueIsNullOptional;
            if (collectNulls) {
                ByteBuffer nullBuff = juffersWE.getNullBuffer();
                boolean[] valueIsNull = new boolean[rowsToFill];
                for (int currentRow = 0; currentRow < rowsToFill; currentRow++) {
                    if (isNull(nullBuff, currentRow)) {
                        valueIsNull[currentRow] = true;
                    }
                    else {
                        values[currentRow] = (int) readDictionary.get(Short.toUnsignedInt(buff.get(currentRow)));
                    }
                }
                valueIsNullOptional = Optional.of(valueIsNull);
            }
            else {
                for (int currentRow = 0; currentRow < rowsToFill; currentRow++) {
                    values[currentRow] = (int) readDictionary.get(Short.toUnsignedInt(buff.get(currentRow)));
                }
                valueIsNullOptional = Optional.empty();
            }
            resultBlock = new IntArrayBlock(rowsToFill, valueIsNullOptional, values);
        }
        return resultBlock;
    }

    @Override
    protected Long getSingleValueWithDictionary(int mappingKey, ReadDictionary readDictionary)
    {
        return ((Integer) readDictionary.get(mappingKey)).longValue();
    }

    @Override
    protected Block createSingleWithNullBlockWithDictionary(ReadJuffersWarmUpElement juffersWE, int mappingKey, int rowsToFill, ReadDictionary readDictionary)
    {
        int singleValue = getSingleValueWithDictionary(mappingKey, readDictionary).intValue();
        Block mappingBlock = createSingleMappingBlock(singleValue);

        return wrapSingleWithNulls(juffersWE, rowsToFill, mappingBlock, 1);
    }

    private Block createSingleMappingBlock(int singleValue)
    {
        int[] mappingValues = new int[2]; // including null
        boolean[] nulls = new boolean[2];
        mappingValues[0] = singleValue;
        nulls[1] = true;
        return new IntArrayBlock(2, Optional.of(nulls), mappingValues);
    }
}
