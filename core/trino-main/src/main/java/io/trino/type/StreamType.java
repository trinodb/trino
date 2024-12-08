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
package io.trino.type;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.AbstractType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperatorDeclaration;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.singletonList;

public class StreamType
        extends AbstractType
{
    public static final StreamType STREAM_INTEGER = new StreamType(new ArrayType(INTEGER));
    public static final StreamType STREAM_BIGINT = new StreamType(new ArrayType(BIGINT));
    public static final StreamType STREAM_VARCHAR = new StreamType(new ArrayType(VARCHAR));

    private static final String STREAM = "stream";
    private volatile TypeOperatorDeclaration operatorDeclaration;

    private final Type elementType;
    private final ArrayType arrayType;

    public StreamType(Type elementType)
    {
        super(new TypeSignature(STREAM, TypeSignatureParameter.typeParameter(elementType.getTypeSignature())), Block.class, ArrayBlock.class);
        checkArgument(elementType instanceof ArrayType || elementType instanceof StreamType, "elementType must be an array type or a stream type");
        this.elementType = elementType;
        this.arrayType = underlyingArrayType(elementType);
    }

    private static ArrayType underlyingArrayType(Type type)
    {
        Type arrayType = type;
        while (arrayType instanceof StreamType streamType) {
            arrayType = streamType.elementType;
        }
        return (ArrayType) arrayType;
    }

    public ArrayType getArrayType()
    {
        return arrayType;
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return singletonList(elementType);
    }

    @Override
    public String getDisplayName()
    {
        return STREAM + "(" + elementType.getDisplayName() + ")";
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        if (operatorDeclaration == null) {
            operatorDeclaration = arrayType.getTypeOperatorDeclaration(typeOperators);
        }
        return operatorDeclaration;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        return arrayType.getObjectValue(session, block, position);
    }

    @Override
    public Object getObject(Block block, int position)
    {
        return arrayType.getObject(block, position);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        arrayType.writeObject(blockBuilder, value);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        arrayType.appendTo(block, position, blockBuilder);
    }

    @Override
    public int getFlatFixedSize()
    {
        return 8;
    }

    @Override
    public boolean isFlatVariableWidth()
    {
        return true;
    }

    @Override
    public int getFlatVariableWidthSize(Block block, int position)
    {
        return arrayType.getFlatVariableWidthSize(block, position);
    }

    @Override
    public int relocateFlatVariableWidthOffsets(byte[] fixedSizeSlice, int fixedSizeOffset, byte[] variableSizeSlice, int variableSizeOffset)
    {
        return arrayType.relocateFlatVariableWidthOffsets(fixedSizeSlice, fixedSizeOffset, variableSizeSlice, variableSizeOffset);
    }

    @Override
    public boolean isComparable()
    {
        return elementType.isComparable();
    }

    @Override
    public boolean isOrderable()
    {
        return elementType.isOrderable();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return arrayType.createBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, 100);
    }

    public Iterable<Object> valueIterable(Block block)
    {
        ValueBlock valueBlock = getValueBlock(block, 0);
        return () -> IntStream.range(0, valueBlock.getPositionCount()).boxed()
                .map(i -> getValueObject(valueBlock, i))
                .iterator();
    }

    private static Object getValueObject(ValueBlock valueBlock, int position)
    {
        if (valueBlock instanceof IntArrayBlock intArrayBlock) {
            return (long) intArrayBlock.getInt(position);
        }
        if (valueBlock instanceof LongArrayBlock longArrayBlock) {
            return longArrayBlock.getLong(position);
        }
        if (valueBlock instanceof VariableWidthBlock variableWidthBlock) {
            return variableWidthBlock.getSlice(position);
        }
        throw new UnsupportedOperationException("unsupported block type: " + valueBlock.getClass().getName());
    }

    private static ValueBlock getValueBlock(Block block, int position)
    {
        if (block instanceof ArrayBlock arrayBlock) {
            return arrayBlock.getUnderlyingValueBlock().getArray(arrayBlock.getUnderlyingValuePosition(position)).getUnderlyingValueBlock();
        }
        if (block instanceof ValueBlock valueBlock) {
            return valueBlock.getUnderlyingValueBlock();
        }
        if (block instanceof RunLengthEncodedBlock rleBlock) {
            return rleBlock.getValue();
        }
        if (block instanceof DictionaryBlock dictionaryBlock) {
            return dictionaryBlock.getDictionary();
        }

        throw new UnsupportedOperationException("unsupported block type: " + block.getClass().getName());
    }

    private Iterable<ValueBlock> streamValueBlocks(ValueBlock streamBlock, int position)
    {
        ValueBlock block = streamBlock.getSingleValueBlock(position);
        if (elementType instanceof StreamType subStreamType) {
            return subStreamType.arrayIterable(getValueBlock(block, position), 0);
        }
        return ImmutableList.of(block);
    }

    public Iterable<ValueBlock> blockValueIterable(Block block, int position)
    {
        ValueBlock streamBlock = getValueBlock(block, position);

        return () -> IntStream.range(0, streamBlock.getPositionCount())
                .boxed()
                .flatMap(i -> Streams.stream(streamValueBlocks(streamBlock, i)))
                .iterator();
    }

    public Iterable<ValueBlock> arrayIterable(Block block, int position)
    {
        return Iterables.transform(blockValueIterable(block, position),
                b -> ArrayBlock.fromElementBlock(1, Optional.of(new boolean[] {false}), new int[] {0, 1}, b));
    }
}
