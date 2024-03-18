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

import io.airlift.log.Logger;
import io.trino.plugin.varada.dictionary.ReadDictionary;
import io.trino.plugin.varada.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.warp.gen.constants.QueryResultType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;

public abstract class BlockFiller<V>
{
    private static final Logger logger = Logger.get(BlockFiller.class);
    private static final byte NULL_VALUE_BYTE = -1;
    private final BlockFillerType blockFillerType;
    protected final Type spiBuilderType;

    public BlockFiller(Type spiBuilderType, BlockFillerType blockFillerType)
    {
        this.spiBuilderType = spiBuilderType;
        this.blockFillerType = blockFillerType;
    }

    protected boolean isNull(ByteBuffer nullBuff, int currRow)
    {
        return (nullBuff.get(currRow) == NULL_VALUE_BYTE);
    }

    // for value size that might be larger than one
    protected boolean isNull(ByteBuffer nullBuff, int nullValuesSize, int currRow)
    {
        int base = currRow * nullValuesSize;
        for (int i = 0; i < nullValuesSize; i++) {
            if (nullBuff.get(base + i) != NULL_VALUE_BYTE) {
                return false;
            }
        }
        return true;
    }

    public Block fillBlock(ReadJuffersWarmUpElement juffersWE,
            QueryResultType queryResultType,
            int rowsToFill,
            RecTypeCode recTypeCode,
            int recLength,
            boolean collectNulls)
            throws IOException
    {
        return switch (queryResultType) {
            case QUERY_RESULT_TYPE_RAW -> fillRawBlock(juffersWE, recTypeCode, recLength, rowsToFill, collectNulls);
            case QUERY_RESULT_TYPE_RAW_NO_NULL -> fillRawBlock(juffersWE, recTypeCode, recLength, rowsToFill, false);
            case QUERY_RESULT_TYPE_ALL_NULL -> createSingleValueBlock(spiBuilderType, null, rowsToFill);
            case QUERY_RESULT_TYPE_SINGLE -> createSingleWithNullBlock(juffersWE, recTypeCode, recLength, rowsToFill);
            case QUERY_RESULT_TYPE_SINGLE_NO_NULL -> createSingleValueBlock(spiBuilderType, getSingleValue(juffersWE, recLength, 0), rowsToFill);
            default -> throw new UnsupportedOperationException();
        };
    }

    protected abstract V getSingleValue(ReadJuffersWarmUpElement juffersWE, int recLength, int currPos)
            throws IOException;

    protected abstract Block createSingleWithNullBlock(ReadJuffersWarmUpElement juffersWE, RecTypeCode recTypeCode, int recLength, int rowsToFill)
            throws IOException;

    protected abstract Block fillRawBlock(ReadJuffersWarmUpElement juffersWE,
            RecTypeCode recTypeCode,
            int recLength,
            int rowsToFill,
            boolean collectNulls)
            throws IOException;

    public Block fillBlockWithDictionary(ReadJuffersWarmUpElement juffersWE,
            QueryResultType queryResultType,
            int rowsToFill,
            RecTypeCode recTypeCode,
            int recLength,
            boolean collectNulls,
            ReadDictionary readDictionary)
    {
        logger.debug("got result of type=%s, row to fill=%d", queryResultType, rowsToFill);
        ShortBuffer shortBuff;
        int mappingKey;
        return switch (queryResultType) {
            case QUERY_RESULT_TYPE_RAW -> fillRawBlockWithDictionary(juffersWE, rowsToFill, recTypeCode, recLength, collectNulls, readDictionary);
            case QUERY_RESULT_TYPE_RAW_NO_NULL -> fillRawBlockWithDictionary(juffersWE, rowsToFill, recTypeCode, recLength, false, readDictionary);
            case QUERY_RESULT_TYPE_ALL_NULL -> createSingleValueBlock(spiBuilderType, null, rowsToFill);
            case QUERY_RESULT_TYPE_SINGLE -> {
                shortBuff = (ShortBuffer) juffersWE.getRecordBuffer();
                mappingKey = Short.toUnsignedInt(shortBuff.get());
                yield createSingleWithNullBlockWithDictionary(juffersWE, mappingKey, rowsToFill, readDictionary);
            }
            case QUERY_RESULT_TYPE_SINGLE_NO_NULL -> {
                shortBuff = (ShortBuffer) juffersWE.getRecordBuffer();
                mappingKey = Short.toUnsignedInt(shortBuff.get());
                yield createSingleValueBlock(spiBuilderType, getSingleValueWithDictionary(mappingKey, readDictionary), rowsToFill);
            }
            default -> throw new UnsupportedOperationException();
        };
    }

    public Block fillBlockWithMapping(ReadJuffersWarmUpElement juffersWE,
                                      QueryResultType queryResultType,
                                      int rowsToFill,
                                      boolean collectNulls,
                                      Block mapBlock)
    {
        ByteBuffer byteBuff;
        int mapKey;

        return switch (queryResultType) {
            case QUERY_RESULT_TYPE_RAW -> fillRawBlockWithMapping(juffersWE, rowsToFill, collectNulls, mapBlock);
            case QUERY_RESULT_TYPE_RAW_NO_NULL -> fillRawBlockWithMapping(juffersWE, rowsToFill, false, mapBlock);
            case QUERY_RESULT_TYPE_ALL_NULL -> createSingleValueBlock(spiBuilderType, null, rowsToFill);
            case QUERY_RESULT_TYPE_SINGLE -> {
                byteBuff = (ByteBuffer) juffersWE.getRecordBuffer();
                mapKey = Short.toUnsignedInt(byteBuff.get());
                yield createSingleBlockWithMapping(juffersWE, mapKey, rowsToFill, mapBlock, collectNulls);
            }
            case QUERY_RESULT_TYPE_SINGLE_NO_NULL -> {
                byteBuff = (ByteBuffer) juffersWE.getRecordBuffer();
                mapKey = Short.toUnsignedInt(byteBuff.get());
                yield createSingleBlockWithMapping(juffersWE, mapKey, rowsToFill, mapBlock, false);
            }
            default -> throw new UnsupportedOperationException();
        };
    }

    protected Block createSingleBlockWithMapping(ReadJuffersWarmUpElement juffersWE, int mapKey, int rowsToFill, Block mapBlock, boolean collectNulls)
    {
        throw new UnsupportedOperationException();
    }

    protected Block fillRawBlockWithMapping(ReadJuffersWarmUpElement juffersWE,
                                            int rowsToFill,
                                            boolean collectNulls,
                                            Block mapBlock)
    {
        ByteBuffer buff = (ByteBuffer) juffersWE.getRecordBuffer();
        int[] ids = new int[rowsToFill];

        if (collectNulls) {
            int nullPosition = mapBlock.getPositionCount() - 1;
            ByteBuffer nullBuff = juffersWE.getNullBuffer();
            for (int currRow = 0; currRow < rowsToFill; currRow++) {
                if (isNull(nullBuff, currRow)) {
                    ids[currRow] = nullPosition;
                }
                else {
                    ids[currRow] = Byte.toUnsignedInt(buff.get(currRow));
                }
            }
        }
        else {
            for (int currRow = 0; currRow < rowsToFill; currRow++) {
                ids[currRow] = Byte.toUnsignedInt(buff.get(currRow));
            }
        }
        return DictionaryBlock.create(ids.length, mapBlock, ids);
    }

    protected Block fillRawBlockWithDictionary(ReadJuffersWarmUpElement juffersWE,
            int rowsToFill,
            RecTypeCode recTypeCode,
            int recTypeLength,
            boolean collectNulls,
            ReadDictionary readDictionary)
    {
        throw new UnsupportedOperationException();
    }

    protected V getSingleValueWithDictionary(int mappingKey, ReadDictionary readDictionary)
    {
        throw new UnsupportedOperationException();
    }

    protected Block createSingleWithNullBlockWithDictionary(ReadJuffersWarmUpElement juffersWE, int mappingKey, int rowsToFill, ReadDictionary readDictionary)
    {
        throw new UnsupportedOperationException();
    }

    protected Block createSingleValueBlock(Type spiType, V value, int rowsToFill)
    {
        return RunLengthEncodedBlock.create(spiType, value, rowsToFill);
    }

    protected Block wrapSingleWithNulls(ReadJuffersWarmUpElement juffersWE, int rowsToFill, Block mappingBlock, int nullValuesSize)
    {
        ByteBuffer nullBuff = juffersWE.getNullBuffer();
        int[] values = new int[rowsToFill];
        if (nullValuesSize > 1) {
            for (int i = 0; i < rowsToFill; i++) {
                if (isNull(nullBuff, nullValuesSize, i)) {
                    values[i] = 1;
                }
            }
        }
        else {
            for (int i = 0; i < rowsToFill; i++) {
                if (isNull(nullBuff, i)) {
                    values[i] = 1;
                }
            }
        }
        return DictionaryBlock.create(rowsToFill, mappingBlock, values);
    }

    @Override
    public String toString()
    {
        return blockFillerType.toString();
    }
}
