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
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Optional;

public class LongDecimalBlockFiller
        extends BlockFiller<Int128>
{
    public LongDecimalBlockFiller()
    {
        super(DecimalType.createDecimalType(), BlockFillerType.LONG);
    }

    @Override
    protected Int128 getSingleValue(ReadJuffersWarmUpElement juffersWE, int recLength, int currPos)
    {
        long[] value = new long[2];
        LongBuffer buff = (LongBuffer) juffersWE.getRecordBuffer();

        buff.get(value);
        return Int128.valueOf(value[0], value[1]);
    }

    @Override
    protected Block createSingleWithNullBlock(ReadJuffersWarmUpElement juffersWE, RecTypeCode recTypeCode, int recLength, int rowsToFill)
    {
        long[] mappingValues = new long[4]; // including null
        boolean[] nulls = new boolean[2];
        long[] value = new long[2];
        LongBuffer buff = (LongBuffer) juffersWE.getRecordBuffer();

        buff.get(value);
        mappingValues[0] = value[0];
        mappingValues[1] = value[1];
        nulls[1] = true;
        Block dictionaryBlock = new Int128ArrayBlock(2, Optional.of(nulls), mappingValues);

        return wrapSingleWithNulls(juffersWE, rowsToFill, dictionaryBlock, 1);
    }

    @Override
    protected Block fillRawBlock(ReadJuffersWarmUpElement juffersWE,
            RecTypeCode recTypeCode,
            int recLength,
            int rowsToFill,
            boolean collectNulls)
    {
        LongBuffer buff = (LongBuffer) juffersWE.getRecordBuffer();
        long[] values = new long[rowsToFill * 2];
        Optional<boolean[]> valueIsNullOptional;

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
        return new Int128ArrayBlock(rowsToFill, valueIsNullOptional, values);
    }

    @Override
    protected Block createSingleBlockWithMapping(ReadJuffersWarmUpElement juffersWE, int mapKey, int rowsToFill, Block mapBlock, boolean collectNulls)
    {
        if (!collectNulls) {
            Int128 value = ((Int128ArrayBlock) mapBlock).getInt128(0);
            return createSingleValueBlock(spiBuilderType, value, rowsToFill);
        }

        Block singleMapBlock = createSingleMapWithNullFromMapBlock(mapBlock);

        return wrapSingleWithNulls(juffersWE, rowsToFill, singleMapBlock, 1);
    }

    private Block createSingleMapWithNullFromMapBlock(Block mapBlock)
    {
        long[] mappingValues = new long[4]; // including null
        mappingValues[0] = ((Int128ArrayBlock) mapBlock).getInt128Low(0);
        mappingValues[1] = ((Int128ArrayBlock) mapBlock).getInt128High(0);
        boolean[] nulls = new boolean[2];
        nulls[1] = true;

        return new Int128ArrayBlock(2, Optional.of(nulls), mappingValues);
    }
}
