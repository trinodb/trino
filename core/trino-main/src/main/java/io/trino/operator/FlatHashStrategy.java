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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.trino.operator.scalar.CombineHashFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.operator.VariableWidthData.EMPTY_CHUNK;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static io.trino.sql.planner.optimizations.HashGenerationOptimizer.INITIAL_HASH_VALUE;
import static java.lang.Math.toIntExact;
import static java.lang.invoke.MethodHandles.arrayElementGetter;
import static java.lang.invoke.MethodHandles.collectArguments;
import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.guardWithTest;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;

public class FlatHashStrategy
{
    private static final MethodHandle READ_FLAT_FIELD_IS_NULL;
    private static final MethodHandle READ_FLAT_NULL_FIELD;
    private static final MethodHandle WRITE_FLAT_NULL_FIELD;
    private static final MethodHandle FLAT_IS_NULL;
    private static final MethodHandle BLOCK_IS_NULL;
    private static final MethodHandle LOGICAL_OR;
    private static final MethodHandle BOOLEAN_NOT_EQUALS;
    private static final MethodHandle INTEGER_ADD;

    static {
        try {
            MethodHandles.Lookup lookup = lookup();
            READ_FLAT_FIELD_IS_NULL = lookup.findStatic(FlatHashStrategy.class, "readFlatFieldIsNull", methodType(boolean.class, int.class, byte[].class, int.class));
            READ_FLAT_NULL_FIELD = lookup.findVirtual(BlockBuilder.class, "appendNull", methodType(BlockBuilder.class)).asType(methodType(void.class, BlockBuilder.class));
            WRITE_FLAT_NULL_FIELD = dropArguments(
                    dropArguments(
                            lookup.findStatic(FlatHashStrategy.class, "writeFieldNull", methodType(void.class, int.class, byte[].class, int.class)),
                            3,
                            byte[].class,
                            int.class),
                    1,
                    Block.class,
                    int.class);
            FLAT_IS_NULL = lookup.findStatic(FlatHashStrategy.class, "flatIsNull", methodType(boolean.class, int.class, byte[].class, int.class));
            BLOCK_IS_NULL = lookup.findVirtual(Block.class, "isNull", methodType(boolean.class, int.class));
            LOGICAL_OR = lookup.findStatic(Boolean.class, "logicalOr", methodType(boolean.class, boolean.class, boolean.class));
            BOOLEAN_NOT_EQUALS = lookup.findStatic(FlatHashStrategy.class, "booleanNotEquals", methodType(boolean.class, boolean.class, boolean.class));
            INTEGER_ADD = lookup.findStatic(FlatHashStrategy.class, "integerAdd", methodType(int.class, int.class, int.class));
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private final List<Type> types;
    private final boolean anyVariableWidth;
    private final int totalFlatFixedLength;
    private final List<MethodHandle> readFlatMethods;
    private final List<MethodHandle> writeFlatMethods;
    private final List<MethodHandle> hashFlatMethods;
    private final List<MethodHandle> hashBlockMethods;
    private final List<MethodHandle> distinctFlatBlockMethods;

    public FlatHashStrategy(List<Type> types, TypeOperators typeOperators)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        ImmutableList.Builder<MethodHandle> readFlatMethods = ImmutableList.builder();
        ImmutableList.Builder<MethodHandle> writeFlatMethods = ImmutableList.builder();
        ImmutableList.Builder<MethodHandle> distinctFlatBlockMethods = ImmutableList.builder();
        ImmutableList.Builder<MethodHandle> hashFlatMethods = ImmutableList.builder();
        ImmutableList.Builder<MethodHandle> hashBlockMethods = ImmutableList.builder();

        try {
            MethodHandle readFlatNullField = dropArguments(READ_FLAT_NULL_FIELD, 0, byte[].class, int.class, byte[].class);

            int[] fieldIsNullOffsets = new int[types.size()];
            int[] fieldFixedOffsets = new int[types.size()];

            int variableWidthCount = (int) types.stream().filter(Type::isFlatVariableWidth).count();

            int fixedOffset = 0;
            for (int i = 0; i < types.size(); i++) {
                Type type = types.get(i);
                fieldIsNullOffsets[i] = fixedOffset;
                fixedOffset++;
                fieldFixedOffsets[i] = fixedOffset;
                fixedOffset += type.getFlatFixedSize();
            }
            totalFlatFixedLength = fixedOffset;
            anyVariableWidth = variableWidthCount > 0;

            for (int i = 0; i < types.size(); i++) {
                Type type = types.get(i);

                MethodHandle readFlat = typeOperators.getReadValueOperator(type, simpleConvention(BLOCK_BUILDER, FLAT));
                readFlat = toAbsoluteFlatArgument(type, readFlat, 0, fieldFixedOffsets[i]);
                readFlat = guardWithTest(
                        insertArguments(READ_FLAT_FIELD_IS_NULL, 0, fieldIsNullOffsets[i]),
                        readFlatNullField,
                        readFlat);
                readFlat = collectArguments(readFlat, 3, insertArguments(arrayElementGetter(BlockBuilder[].class), 1, i));
                readFlatMethods.add(readFlat);

                MethodHandle writeFlat = typeOperators.getReadValueOperator(type, simpleConvention(FLAT_RETURN, BLOCK_POSITION_NOT_NULL));
                // add the field fixed offset to the base fixed offset
                writeFlat = collectArguments(writeFlat, 3, insertArguments(INTEGER_ADD, 1, fieldFixedOffsets[i]));
                writeFlat = guardWithTest(
                        BLOCK_IS_NULL,
                        insertArguments(WRITE_FLAT_NULL_FIELD, 0, fieldIsNullOffsets[i]),
                        writeFlat);
                writeFlat = collectArguments(writeFlat, 0, insertArguments(arrayElementGetter(Block[].class), 1, i));
                writeFlatMethods.add(writeFlat);

                MethodHandle distinctFlatBlock = typeOperators.getDistinctFromOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, BLOCK_POSITION_NOT_NULL));
                distinctFlatBlock = toAbsoluteFlatArgument(type, distinctFlatBlock, 0, fieldFixedOffsets[i]);
                distinctFlatBlock = guardWithTest(
                        LOGICAL_OR,
                        dropArguments(BOOLEAN_NOT_EQUALS, 2, byte[].class, int.class, byte[].class, Block.class, int.class),
                        dropArguments(distinctFlatBlock, 0, boolean.class, boolean.class));
                distinctFlatBlock = collectArguments(distinctFlatBlock, 1, BLOCK_IS_NULL);
                distinctFlatBlock = collectArguments(distinctFlatBlock, 0, insertArguments(FLAT_IS_NULL, 0, fieldIsNullOffsets[i]));
                distinctFlatBlock = permuteArguments(
                        distinctFlatBlock,
                        methodType(boolean.class, byte[].class, int.class, byte[].class, Block.class, int.class),
                        0, 1, 3, 4, 0, 1, 2, 3, 4);
                distinctFlatBlock = collectArguments(distinctFlatBlock, 3, insertArguments(arrayElementGetter(Block[].class), 1, i));
                distinctFlatBlockMethods.add(distinctFlatBlock);

                MethodHandle hashFlat = typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, FLAT));
                hashFlat = toAbsoluteFlatArgument(type, hashFlat, 0, fieldFixedOffsets[i]);
                hashFlat = guardWithTest(
                        insertArguments(FLAT_IS_NULL, 0, fieldIsNullOffsets[i]),
                        dropArguments(constant(long.class, NULL_HASH_CODE), 0, byte[].class, int.class, byte[].class),
                        hashFlat);
                hashFlatMethods.add(hashFlat);

                MethodHandle hashBlock = typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));
                hashBlock = guardWithTest(
                        BLOCK_IS_NULL,
                        dropArguments(constant(long.class, NULL_HASH_CODE), 0, Block.class, int.class),
                        hashBlock);
                hashBlock = collectArguments(hashBlock, 0, insertArguments(arrayElementGetter(Block[].class), 1, i));
                hashBlockMethods.add(hashBlock);
            }
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }

        this.readFlatMethods = readFlatMethods.build();
        this.writeFlatMethods = writeFlatMethods.build();
        this.distinctFlatBlockMethods = distinctFlatBlockMethods.build();
        this.hashFlatMethods = hashFlatMethods.build();
        this.hashBlockMethods = hashBlockMethods.build();
    }

    public boolean isAnyVariableWidth()
    {
        return anyVariableWidth;
    }

    public int getTotalFlatFixedLength()
    {
        return totalFlatFixedLength;
    }

    public int getTotalVariableWidth(Block[] blocks, int position)
    {
        if (!anyVariableWidth) {
            return 0;
        }

        long variableWidth = 0;
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            Block block = blocks[i];

            if (type.isFlatVariableWidth()) {
                variableWidth += type.getFlatVariableWidthSize(block, position);
            }
        }
        return toIntExact(variableWidth);
    }

    public void readFlat(byte[] fixedChunk, int fixedOffset, byte[] variableChunk, int variableOffset, BlockBuilder[] blockBuilders)
    {
        try {
            for (MethodHandle readFlatMethod : readFlatMethods) {
                readFlatMethod.invokeExact(fixedChunk, fixedOffset, variableChunk, blockBuilders);
            }
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    public void writeFlat(Block[] blocks, int position, byte[] fixedChunk, int fixedOffset, byte[] variableChunk, int variableOffset)
    {
        try {
            for (int i = 0, writeFlatMethodsSize = writeFlatMethods.size(); i < writeFlatMethodsSize; i++) {
                writeFlatMethods.get(i).invokeExact(blocks, position, fixedChunk, fixedOffset, variableChunk, variableOffset);
                Type type = types.get(i);
                if (type.isFlatVariableWidth() && !blocks[i].isNull(position)) {
                    variableOffset += type.getFlatVariableWidthSize(blocks[i], position);
                }
            }
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    public boolean valueNotDistinctFrom(
            byte[] leftFixedChunk,
            int leftFixedOffset,
            byte[] leftVariableChunk,
            Block[] rightBlocks,
            int rightPosition)
    {
        try {
            for (MethodHandle distinctFlatBlockMethod : distinctFlatBlockMethods) {
                if ((boolean) distinctFlatBlockMethod.invokeExact(leftFixedChunk, leftFixedOffset, leftVariableChunk, rightBlocks, rightPosition)) {
                    return false;
                }
            }
            return true;
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    public long hash(Block[] blocks, int position)
    {
        try {
            long result = INITIAL_HASH_VALUE;
            for (MethodHandle hashBlockMethod : hashBlockMethods) {
                result = CombineHashFunction.getHash(result, (long) hashBlockMethod.invokeExact(blocks, position));
            }
            return result;
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    public long hash(byte[] fixedChunk, int fixedOffset, byte[] variableChunk)
    {
        try {
            long result = INITIAL_HASH_VALUE;
            for (MethodHandle hashFlat : hashFlatMethods) {
                result = CombineHashFunction.getHash(result, (long) hashFlat.invokeExact(fixedChunk, fixedOffset, variableChunk));
            }
            return result;
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private static MethodHandle toAbsoluteFlatArgument(Type type, MethodHandle methodHandle, int argument, int fixedPosition)
            throws ReflectiveOperationException
    {
        // offset the fixed position by the field offset
        methodHandle = collectArguments(methodHandle, argument + 1, insertArguments(INTEGER_ADD, 1, fixedPosition));

        // for fixed types, hard code a reference to the empty slice
        if (!type.isFlatVariableWidth()) {
            methodHandle = insertArguments(methodHandle, argument + 2, (Object) EMPTY_CHUNK);
            methodHandle = dropArguments(methodHandle, argument + 2, byte[].class);
        }
        return methodHandle;
    }

    private static boolean readFlatFieldIsNull(int fieldNullOffset, byte[] fixedChunk, int fixedOffset)
    {
        return fixedChunk[fixedOffset + fieldNullOffset] != 0;
    }

    private static void writeFieldNull(int fieldNullOffset, byte[] fixedChunk, int fixedOffset)
    {
        fixedChunk[fixedOffset + fieldNullOffset] = 1;
    }

    private static boolean flatIsNull(
            int fieldNullOffset,
            byte[] fixedChunk,
            int fixedOffset)
    {
        return fixedChunk[fixedOffset + fieldNullOffset] != 0;
    }

    private static boolean booleanNotEquals(boolean left, boolean right)
    {
        return left != right;
    }

    private static int integerAdd(int left, int right)
    {
        return left + right;
    }
}
