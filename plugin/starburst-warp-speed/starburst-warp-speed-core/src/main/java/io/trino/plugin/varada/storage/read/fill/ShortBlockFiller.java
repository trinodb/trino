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

import io.trino.plugin.varada.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.spi.block.Block;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.type.SmallintType;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.Optional;

public class ShortBlockFiller
        extends BlockFiller<Long>
{
    public ShortBlockFiller()
    {
        super(SmallintType.SMALLINT, BlockFillerType.SHORT);
    }

    @Override
    protected Long getSingleValue(ReadJuffersWarmUpElement juffersWE, int recLength, int currPos)
    {
        ShortBuffer buff = (ShortBuffer) juffersWE.getRecordBuffer();
        return (long) buff.get();
    }

    @Override
    protected Block createSingleWithNullBlock(ReadJuffersWarmUpElement juffersWE, RecTypeCode recTypeCode, int recLength, int rowsToFill)
    {
        short[] mappingValues = new short[2]; // including null
        boolean[] nulls = new boolean[2];
        ShortBuffer buff = (ShortBuffer) juffersWE.getRecordBuffer();
        mappingValues[0] = buff.get();
        nulls[1] = true;
        Block dictionaryBlock = new ShortArrayBlock(2, Optional.of(nulls), mappingValues);

        return wrapSingleWithNulls(juffersWE, rowsToFill, dictionaryBlock, 1);
    }

    @Override
    protected Block fillRawBlock(ReadJuffersWarmUpElement juffersWE,
            RecTypeCode recTypeCode,
            int recLength,
            int rowsToFill,
            boolean collectNulls)
    {
        Optional<boolean[]> valueIsNullOptional;
        short[] values = new short[rowsToFill];
        ShortBuffer buff = (ShortBuffer) juffersWE.getRecordBuffer();
        if (collectNulls) {
            ByteBuffer nullBuff = juffersWE.getNullBuffer();
            boolean[] valueIsNull = new boolean[rowsToFill];
            for (int i = 0; i < rowsToFill; i++) {
                if (isNull(nullBuff, i)) {
                    valueIsNull[i] = true;
                }
            }
            valueIsNullOptional = Optional.of(valueIsNull);
        }
        else {
            valueIsNullOptional = Optional.empty();
        }
        buff.get(values);
        return new ShortArrayBlock(rowsToFill, valueIsNullOptional, values);
    }

    @Override
    protected Block createSingleBlockWithMapping(ReadJuffersWarmUpElement juffersWE, int mapKey, int rowsToFill, Block mapBlock, boolean collectNulls)
    {
        short singleVal = ((ShortArrayBlock) mapBlock).getShort(mapKey);
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

    private Block createSingleMappingBlock(short singleValue)
    {
        short[] mappingValues = new short[2]; // including null
        boolean[] nulls = new boolean[2];
        mappingValues[0] = singleValue;
        nulls[1] = true;
        return new ShortArrayBlock(2, Optional.of(nulls), mappingValues);
    }
}
