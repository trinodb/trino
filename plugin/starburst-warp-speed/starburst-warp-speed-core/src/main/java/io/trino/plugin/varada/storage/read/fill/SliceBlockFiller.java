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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.dictionary.ReadDictionary;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.Optional;

public abstract class SliceBlockFiller
        extends BlockFiller<Slice>
{
    protected final StorageEngineConstants storageEngineConstants;
    protected final NativeConfiguration nativeConfiguration;
    protected final int queryStringNullValueSize;

    public SliceBlockFiller(
            Type spiBuilderType,
            StorageEngineConstants storageEngineConstants,
            NativeConfiguration nativeConfiguration)
    {
        super(spiBuilderType, BlockFillerType.SLICE);
        this.storageEngineConstants = storageEngineConstants;
        this.nativeConfiguration = nativeConfiguration;
        this.queryStringNullValueSize = storageEngineConstants.getQueryStringNullValueSize();
    }

    @Override
    protected Slice getSingleValue(ReadJuffersWarmUpElement juffersWE, int recLength, int currPos)
            throws IOException
    {
        ByteBuffer recordBuff = (ByteBuffer) juffersWE.getRecordBuffer();
        ByteBuffer nullBuff = juffersWE.getNullBuffer();
        ShortBuffer lenBuff = nullBuff.asShortBuffer();

        byte[] valuesByteArray = allocateValuesByteArray(1, recLength);
        Slice outputSlice = Slices.wrappedBuffer(valuesByteArray);

        int actualSize = copyFromJufferOneRecord(recordBuff,
                                lenBuff,
                                currPos,
                                recLength,
                                0,
                                outputSlice,
                                0);
        outputSlice = outputSlice.slice(0, actualSize);
        return outputSlice;
    }

    // in case the query was with a limit, we might have only nulls. in that case we return -1
    private int getSinlgeValuePos(ReadJuffersWarmUpElement juffersWE, int rowsToFill, int nullValuesSize)
    {
        ByteBuffer nullBuff = juffersWE.getNullBuffer();
        for (int i = 0; i < rowsToFill; i++) {
            if (!isNull(nullBuff, nullValuesSize, i)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    protected Block createSingleWithNullBlock(ReadJuffersWarmUpElement juffersWE, RecTypeCode recTypeCode, int recLength, int rowsToFill)
            throws IOException
    {
        int currPos = 0;
        int nullValueSize = 1;
        if (TypeUtils.isVarlenStr(recTypeCode)) {
            nullValueSize = queryStringNullValueSize;
            currPos = getSinlgeValuePos(juffersWE, rowsToFill, nullValueSize);
        }

        if (currPos == -1) {
            return createSingleValueBlock(spiBuilderType, null, rowsToFill);
        }

        Slice singleValue = getSingleValue(juffersWE, recLength, currPos);
        Block mappingBlock = createSingleMappingBlock(singleValue);
        return wrapSingleWithNulls(juffersWE, rowsToFill, mappingBlock, nullValueSize);
    }

    @Override
    protected Block fillRawBlock(ReadJuffersWarmUpElement juffersWE,
            RecTypeCode recTypeCode,
            int recLength,
            int rowsToFill,
            boolean collectNulls)
            throws IOException
    {
        // buffers
        ByteBuffer recordBuff = (ByteBuffer) juffersWE.getRecordBuffer();
        ByteBuffer nullBuff = juffersWE.getNullBuffer();
        ShortBuffer lenBuff = nullBuff.asShortBuffer();
        // block parameters
        int jufferOffset = 0;
        int[] offsets = allocateOffsetsArray(rowsToFill);
        byte[] values = allocateValuesByteArray(rowsToFill, recLength);
        Slice outputSlice = Slices.wrappedBuffer(values);

        Optional<boolean[]> valueIsNullOptional;

        if (collectNulls) {
            int nullValueSize = TypeUtils.isVarlenStr(recTypeCode) ? queryStringNullValueSize : 1;
            boolean[] valueIsNull = new boolean[rowsToFill];
            for (int currRow = 0; currRow < rowsToFill; currRow++) {
                if (isNull(nullBuff, nullValueSize, currRow)) {
                    valueIsNull[currRow] = true;
                    jufferOffset++;
                    offsets[currRow + 1] = offsets[currRow];
                }
                else {
                    int size = copyFromJufferOneRecord(recordBuff,
                                                       lenBuff,
                                                       currRow,
                                                       recLength,
                                                       jufferOffset,
                                                       outputSlice,
                                                       offsets[currRow]);
                    jufferOffset += size;
                    offsets[currRow + 1] = offsets[currRow] + size;
                }
            }
            valueIsNullOptional = Optional.of(valueIsNull);
        }
        else {
            copyFromJufferMultipleRecords(recordBuff,
                                          lenBuff,
                                          recLength,
                                          rowsToFill,
                                          outputSlice,
                                          offsets);
            valueIsNullOptional = Optional.empty();
        }
        return new VariableWidthBlock(rowsToFill, outputSlice, offsets, valueIsNullOptional);
    }

    protected abstract int copyFromJufferOneRecord(ByteBuffer recordBuff,
            ShortBuffer lenBuff,
            int currPos,
            int recLength,
            int jufferOffset,
            Slice outputSlice,
            int outputSliceOffset)
            throws IOException;

    protected abstract void copyFromJufferMultipleRecords(ByteBuffer recordBuff,
            ShortBuffer lenBuff,
            int recLength,
            int numRecords,
            Slice outputSlice,
            int[] offsets)
            throws IOException;

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
            int[] offsets = allocateOffsetsArray(rowsToFill);
            byte[] values = allocateValuesByteArray(rowsToFill, recTypeLength);
            Slice outputSlice = Slices.wrappedBuffer(values);
            Optional<boolean[]> valueIsNullOptional;
            if (collectNulls) {
                ByteBuffer nullBuff = juffersWE.getNullBuffer();
                boolean[] valueIsNull = new boolean[rowsToFill];
                for (int currRow = 0; currRow < rowsToFill; currRow++) {
                    if (isNull(nullBuff, currRow)) {
                        valueIsNull[currRow] = true;
                        offsets[currRow + 1] = offsets[currRow];
                    }
                    else {
                        copyFromDictionary(buff, readDictionary, currRow, outputSlice, offsets);
                    }
                }
                valueIsNullOptional = Optional.of(valueIsNull);
            }
            else {
                for (int currRow = 0; currRow < rowsToFill; currRow++) {
                    copyFromDictionary(buff, readDictionary, currRow, outputSlice, offsets);
                }
                valueIsNullOptional = Optional.empty();
            }
            resultBlock = new VariableWidthBlock(rowsToFill, outputSlice, offsets, valueIsNullOptional);
        }
        return resultBlock;
    }

    protected abstract void copyFromDictionary(ShortBuffer buff,
            ReadDictionary readDictionary,
            int currPos,
            Slice outputSlice,
            int[] offsets);

    @Override
    protected Slice getSingleValueWithDictionary(int mappingKey, ReadDictionary readDictionary)
    {
        return (Slice) readDictionary.get(mappingKey);
    }

    @Override
    protected Block createSingleWithNullBlockWithDictionary(ReadJuffersWarmUpElement juffersWE, int mappingKey, int rowsToFill, ReadDictionary readDictionary)
    {
        Slice singleValue = getSingleValueWithDictionary(mappingKey, readDictionary);
        Block mappingBlock = createSingleMappingBlock(singleValue);
        return wrapSingleWithNulls(juffersWE, rowsToFill, mappingBlock, 1);
    }

    private Block createSingleMappingBlock(Slice singleValue)
    {
        Slice[] mappingValues = new Slice[2]; // including null
        boolean[] nulls = new boolean[2];
        int[] offsets = new int[3]; // +1 contains the total size

        mappingValues[0] = singleValue;
        nulls[1] = true;
        offsets[1] = mappingValues[0].length();
        offsets[2] = mappingValues[0].length(); // last offset should contain the total length
        return new VariableWidthBlock(2, mappingValues[0], offsets, Optional.of(nulls));
    }

    private int[] allocateOffsetsArray(int rowsToFill)
    {
        return new int[rowsToFill + 1]; // +1 contains the total size
    }

    private byte[] allocateValuesByteArray(int rowsToFill, int recTypeLength)
    {
        final int maxValuesArraySize = nativeConfiguration.getMaxRecJufferSize();
        long maxNeededBuffLong = (long) rowsToFill * (long) recTypeLength;
        int maxNeededBuff = (int) maxNeededBuffLong;
        if (maxNeededBuff != maxNeededBuffLong) { // rowsToFill * recTypeLength might cause an overflow so we need to handle it as max buffer size
            maxNeededBuff = maxValuesArraySize;
        }
        return new byte[Math.min(maxNeededBuff, maxValuesArraySize)];
    }
}
