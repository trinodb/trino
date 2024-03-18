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
import io.trino.plugin.varada.dictionary.ReadDictionary;
import io.trino.plugin.varada.juffer.ByteBufferInputStream;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;

public abstract class ArraySliceFiller
        extends BlockFiller<Block>
{
    protected final int queryStringNullValueSize;
    protected final Type elementType;

    protected ArraySliceFiller(StorageEngineConstants storageEngineConstants,
            BlockFillerType blockFillerType,
            Type elementType)
    {
        super(elementType, blockFillerType);
        this.elementType = elementType;
        this.queryStringNullValueSize = storageEngineConstants.getQueryStringNullValueSize();
    }

    @Override
    protected Block getSingleValue(ReadJuffersWarmUpElement juffersWE,
            int recLength, // not used
            int currPos)
    {
        Type type = new ArrayType(elementType);
        ByteBuffer recordBuff = (ByteBuffer) juffersWE.getRecordBuffer();
        ByteBuffer nullBuff = juffersWE.getNullBuffer();
        ShortBuffer lenBuff = nullBuff.asShortBuffer();

        ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(recordBuff);
        BlockBuilder output = type.createBlockBuilder(null, 1);
        writeSlice(lenBuff, byteBufferInputStream, output, type, currPos, 0);
        return output.build();
    }

    @Override
    protected Block createSingleValueBlock(Type spiType, Block value, int rowsToFill)
    {
        if (value == null) {
            return super.createSingleValueBlock(spiType, value, rowsToFill);
        }
        return RunLengthEncodedBlock.create(value, rowsToFill);
    }

    @Override
    protected Block createSingleWithNullBlock(ReadJuffersWarmUpElement juffersWE,
            RecTypeCode recTypeCode,
            int recLength, // not used
            int rowsToFill)
    {
        ByteBuffer recordBuff = (ByteBuffer) juffersWE.getRecordBuffer();
        ByteBuffer nullBuff = juffersWE.getNullBuffer();
        ShortBuffer lenBuff = nullBuff.asShortBuffer();

        ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(recordBuff);
        Block block = getBlock(byteBufferInputStream, 0, Short.toUnsignedInt(lenBuff.get(0)));

        Type type = new ArrayType(elementType);
        ArrayBlockBuilder output = (ArrayBlockBuilder) type.createBlockBuilder(null, rowsToFill);

        int nullValueSize = TypeUtils.isVarlenStr(recTypeCode) ? queryStringNullValueSize : 1;
        for (int i = 0; i < rowsToFill; i++) {
            if (isNull(nullBuff, nullValueSize, i)) {
                output.appendNull();
            }
            else {
                type.writeObject(output, block);
            }
        }

        return output.build();
    }

    @Override
    protected Block fillRawBlock(ReadJuffersWarmUpElement juffersWE,
            RecTypeCode recTypeCode,
            int recLength, // not used
            int rowsToFill,
            boolean collectNulls)
    {
        Type type = new ArrayType(elementType);
        BlockBuilder output = type.createBlockBuilder(null, rowsToFill);
        ByteBuffer recordBuff = (ByteBuffer) juffersWE.getRecordBuffer();
        ByteBuffer nullBuff = juffersWE.getNullBuffer();
        ShortBuffer lenBuff = nullBuff.asShortBuffer();
        ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(recordBuff);
        int offset = 0;

        if (collectNulls) {
            int nullValueSize = TypeUtils.isVarlenStr(recTypeCode) ? queryStringNullValueSize : 1;
            for (int currRow = 0; currRow < rowsToFill; currRow++) {
                if (isNull(nullBuff, nullValueSize, currRow)) {
                    output.appendNull();
                    offset++; // null value takes a byte
                }
                else {
                    byteBufferInputStream.position(offset);
                    offset += writeSlice(lenBuff, byteBufferInputStream, output, type, currRow, offset);
                }
            }
        }
        else {
            for (int currRow = 0; currRow < rowsToFill; currRow++) {
                byteBufferInputStream.position(offset);
                offset += writeSlice(lenBuff, byteBufferInputStream, output, type, currRow, offset);
            }
        }

        return output.build();
    }

    protected abstract Block getBlock(ByteBufferInputStream byteBufferInputStream, int offset, int length);

    protected abstract Block getBlock(Slice slice, int offset, int length);

    private int writeSlice(ShortBuffer lenBuff, ByteBufferInputStream byteBufferInputStream, BlockBuilder output, Type type, int currPos, int offset)
    {
        int len = Short.toUnsignedInt(lenBuff.get(currPos));
        type.writeObject(output, getBlock(byteBufferInputStream, offset, len));
        return len;
    }

    @Override
    public Block fillRawBlockWithDictionary(ReadJuffersWarmUpElement juffersWE,
            int rowsToFill,
            RecTypeCode recTypeCode,
            int recTypeLength,
            boolean collectNulls,
            ReadDictionary readDictionary)
    {
        Type type = new ArrayType(elementType);
        BlockBuilder output = type.createBlockBuilder(null, rowsToFill);
        ShortBuffer shortBuffer = (ShortBuffer) juffersWE.getRecordBuffer();
        if (collectNulls) {
            ByteBuffer nullBuff = juffersWE.getNullBuffer();
            int nullValueSize = TypeUtils.isVarlenStr(recTypeCode) ? queryStringNullValueSize : 1;
            for (int currRow = 0; currRow < rowsToFill; currRow++) {
                if (isNull(nullBuff, nullValueSize, currRow)) {
                    output.appendNull();
                }
                else {
                    Slice slice = (Slice) readDictionary.get(Short.toUnsignedInt(shortBuffer.get(currRow)));
                    Block block = getBlock(slice, 0, slice.length());
                    type.writeObject(output, block);
                }
            }
        }
        else {
            for (int currRow = 0; currRow < rowsToFill; currRow++) {
                Slice slice = (Slice) readDictionary.get(Short.toUnsignedInt(shortBuffer.get(currRow)));
                Block block = getBlock(slice, 0, slice.length());
                type.writeObject(output, block);
            }
        }
        return output.build();
    }

    @Override
    protected Block getSingleValueWithDictionary(int mappingKey, ReadDictionary readDictionary)
    {
        Slice slice = (Slice) readDictionary.get(mappingKey);
        return getBlock(slice, 0, slice.length());
    }

    @Override
    protected Block createSingleWithNullBlockWithDictionary(ReadJuffersWarmUpElement juffersWE, int mappingKey, int rowsToFill, ReadDictionary readDictionary)
    {
        Block block = getSingleValueWithDictionary(mappingKey, readDictionary);
        Type type = new ArrayType(elementType);
        BlockBuilder output = type.createBlockBuilder(null, rowsToFill);
        ByteBuffer nullBuff = juffersWE.getNullBuffer();
        for (int i = 0; i < rowsToFill; i++) {
            if (isNull(nullBuff, i)) {
                output.appendNull();
            }
            else {
                type.writeObject(output, block);
            }
        }

        return output.build();
    }
}
