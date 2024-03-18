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
package io.trino.plugin.varada.dispatcher.warmup.transform;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.storage.common.StorageConstants;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.varada.warmup.exceptions.WarmupException;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;
import java.util.function.Function;

import static io.trino.plugin.varada.storage.common.StorageConstants.NULL_EXISTS_MARKER_SIZE;
import static io.trino.plugin.varada.storage.common.StorageConstants.NULL_VALUE_MARKER_SIZE;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class ArrayBlockToVarcharBlockTransformer
        implements BlockTransformer
{
    @Override
    public BlockPosHolder transformBlock(BlockPosHolder blockPos, Type blockType)
    {
        Type bufferType = ((ArrayType) blockType).getElementType();
        Block convertedBlock;

        if (TypeUtils.isIntType(bufferType) || TypeUtils.isRealType(bufferType) || TypeUtils.isDateType(bufferType)) {
            convertedBlock = convertArray(blockPos, this::getIntArraySlice);
        }
        else if (TypeUtils.isLongType(bufferType) || TypeUtils.isTimestampType(bufferType) || TypeUtils.isDoubleType(bufferType)) {
            convertedBlock = convertArray(blockPos, this::getLongArraySlice);
        }
        else if (TypeUtils.isVarcharType(bufferType)) {
            convertedBlock = convertArray(blockPos, this::getVarcharArraySlice);
        }
        else if (TypeUtils.isCharType(bufferType)) {
            convertedBlock = convertArray(blockPos, this::getVarcharArraySlice);
        }
        else if (TypeUtils.isBooleanType(bufferType)) {
            convertedBlock = convertArray(blockPos, this::getBooleanArraySlice);
        }
        else {
            throw new WarmupException(
                    String.format("unsupported buffer type, received type %s", bufferType),
                    WarmUpElementState.State.FAILED_PERMANENTLY);
        }
        return new BlockPosHolder(convertedBlock, VarcharType.VARCHAR, 0, convertedBlock.getPositionCount());
    }

    private Block convertArray(BlockPosHolder blockPos, Function<Block, Slice> getSlice)
    {
        Slice[] dataSlices = new Slice[blockPos.getNumEntries()];
        int currPos = 0;
        int totalLength = 0;
        int numEntries = blockPos.getNumEntries();
        int[] outputOffsets = new int[numEntries + 1];
        boolean[] valueIsNull = null;

        if (blockPos.mayHaveNull()) {
            valueIsNull = new boolean[numEntries];
            while (blockPos.inRange()) {
                if (blockPos.isNull()) {
                    valueIsNull[currPos] = true;
                }
                else {
                    Block dataBlock = (Block) blockPos.getObject();
                    dataSlices[currPos] = getSlice.apply(dataBlock);
                    totalLength += dataSlices[currPos].length();
                }
                blockPos.advance();
                currPos++;
                outputOffsets[currPos] = totalLength;
            }
        }
        else {
            while (blockPos.inRange()) {
                Block dataBlock = (Block) blockPos.getObject();
                dataSlices[currPos] = getSlice.apply(dataBlock);
                totalLength += dataSlices[currPos].length();
                blockPos.advance();
                currPos++;
                outputOffsets[currPos] = totalLength;
            }
        }
        byte[] values = new byte[totalLength];
        Slice outputSlice = Slices.wrappedBuffer(values);

        if (valueIsNull != null) {
            for (currPos = 0; currPos < numEntries; currPos++) {
                if (!valueIsNull[currPos]) {
                    outputSlice.setBytes(outputOffsets[currPos], dataSlices[currPos]);
                }
            }
        }
        else {
            for (currPos = 0; currPos < numEntries; currPos++) {
                outputSlice.setBytes(outputOffsets[currPos], dataSlices[currPos]);
            }
        }
        return new VariableWidthBlock(blockPos.getNumEntries(), outputSlice, outputOffsets, valueIsNull != null ? Optional.of(valueIsNull) : Optional.empty());
    }

    private Slice getVarcharArraySlice(Block dataBlock)
    {
        int numberOfElements = dataBlock.getPositionCount();

        int calculatedSizeInBytes = 0;
        for (int currInt = 0; currInt < numberOfElements; currInt++) {
            Slice slice = VARCHAR.getSlice(dataBlock, currInt);
            calculatedSizeInBytes += slice.length();
        }

        Slice dataSlice = Slices.allocate(Integer.BYTES + calculatedSizeInBytes + ((Byte.BYTES + Integer.BYTES) * numberOfElements)); //elementsCount + actualBlockSize +  (nullSignalByte + elementLength) * numberOfElementsInRow

        int position = 0;
        dataSlice.setInt(position, numberOfElements);
        position += Integer.BYTES;
        for (int currInt = 0; currInt < numberOfElements; currInt++) {
            if (dataBlock.isNull(currInt)) {
                dataSlice.setByte(position, StorageConstants.NULL_MARKER_VALUE);
                position += Byte.BYTES; //update position
            }
            else {
                Slice slice = VARCHAR.getSlice(dataBlock, currInt);
                int sliceLength = slice.length();
                dataSlice.setByte(position, 0);
                position += Byte.BYTES; //update position
                dataSlice.setInt(position, sliceLength); //set string length
                position += Integer.BYTES; //update the position
                if (sliceLength > 0) {
                    dataSlice.setBytes(position, slice); //set the  string to a slice
                }
                position += sliceLength;
            }
        }
        return dataSlice;
    }

    private Slice getIntArraySlice(Block block)
    {
        Slice dataSlice;
        if (block.mayHaveNull()) {
            dataSlice = Slices.allocate(block.getPositionCount() * (Integer.BYTES + NULL_VALUE_MARKER_SIZE) + NULL_EXISTS_MARKER_SIZE);
            dataSlice.setByte(0, StorageConstants.MAY_HAVE_NULL);
            for (int currInt = 0; currInt < block.getPositionCount(); currInt++) {
                int pos = getSliceOffsetFromIndex(currInt, (Integer.BYTES + NULL_VALUE_MARKER_SIZE));
                boolean isNull = block.isNull(currInt);
                dataSlice.setByte(pos, isNull ? StorageConstants.NULL_MARKER_VALUE : StorageConstants.NOT_NULL_MARKER_VALUE);
                if (!isNull) {
                    dataSlice.setInt(pos + NULL_VALUE_MARKER_SIZE, IntegerType.INTEGER.getInt(block, currInt));
                }
            }
        }
        else {
            dataSlice = Slices.allocate(block.getPositionCount() * Integer.BYTES + NULL_VALUE_MARKER_SIZE);
            dataSlice.setByte(0, StorageConstants.DONT_HAVE_NULL);
            for (int currInt = 0; currInt < block.getPositionCount(); currInt++) {
                dataSlice.setInt(getSliceOffsetFromIndex(currInt, Integer.BYTES), IntegerType.INTEGER.getInt(block, currInt));
            }
        }

        return dataSlice;
    }

    private Slice getLongArraySlice(Block block)
    {
        Slice dataSlice;
        if (block.mayHaveNull()) {
            dataSlice = Slices.allocate(block.getPositionCount() * (Long.BYTES + NULL_VALUE_MARKER_SIZE) + NULL_EXISTS_MARKER_SIZE);
            dataSlice.setByte(0, StorageConstants.MAY_HAVE_NULL);
            for (int currInt = 0; currInt < block.getPositionCount(); currInt++) {
                int pos = getSliceOffsetFromIndex(currInt, (Long.BYTES + NULL_VALUE_MARKER_SIZE));
                boolean isNull = block.isNull(currInt);
                dataSlice.setByte(pos, isNull ? StorageConstants.NULL_MARKER_VALUE : StorageConstants.NOT_NULL_MARKER_VALUE);
                if (!isNull) {
                    dataSlice.setLong(pos + NULL_VALUE_MARKER_SIZE, BigintType.BIGINT.getLong(block, currInt));
                }
            }
            return dataSlice;
        }
        else {
            dataSlice = Slices.allocate(block.getPositionCount() * Long.BYTES + NULL_EXISTS_MARKER_SIZE);
            dataSlice.setByte(0, StorageConstants.DONT_HAVE_NULL);
            for (int currInt = 0; currInt < block.getPositionCount(); currInt++) {
                dataSlice.setLong(getSliceOffsetFromIndex(currInt, Long.BYTES), BigintType.BIGINT.getLong(block, currInt));
            }
        }
        return dataSlice;
    }

    private Slice getBooleanArraySlice(Block block)
    {
        Slice dataSlice;
        if (block.mayHaveNull()) {
            dataSlice = Slices.allocate(block.getPositionCount() * (Byte.BYTES + NULL_VALUE_MARKER_SIZE) + NULL_EXISTS_MARKER_SIZE);
            dataSlice.setByte(0, StorageConstants.MAY_HAVE_NULL);

            for (int currInt = 0; currInt < block.getPositionCount(); currInt++) {
                int pos = getSliceOffsetFromIndex(currInt, (Byte.BYTES + NULL_VALUE_MARKER_SIZE));
                boolean isNull = block.isNull(currInt);
                dataSlice.setByte(pos, isNull ? StorageConstants.NULL_MARKER_VALUE : StorageConstants.NOT_NULL_MARKER_VALUE);
                if (!isNull) {
                    dataSlice.setByte(pos + NULL_VALUE_MARKER_SIZE, TinyintType.TINYINT.getByte(block, currInt));
                }
            }
        }
        else {
            dataSlice = Slices.allocate(block.getPositionCount() + NULL_EXISTS_MARKER_SIZE);
            dataSlice.setByte(0, StorageConstants.DONT_HAVE_NULL);
            for (int currInt = 0; currInt < block.getPositionCount(); currInt++) {
                dataSlice.setByte(getSliceOffsetFromIndex(currInt, Byte.BYTES), TinyintType.TINYINT.getByte(block, currInt));
            }
        }

        return dataSlice;
    }

    private int getSliceOffsetFromIndex(int currIndex, int typeLength)
    {
        return NULL_EXISTS_MARKER_SIZE + currIndex * typeLength;
    }
}
