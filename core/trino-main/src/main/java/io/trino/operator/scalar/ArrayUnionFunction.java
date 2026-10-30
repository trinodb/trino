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
package io.trino.operator.scalar;

import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsIdentical;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.lang.invoke.MethodHandle;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.operator.scalar.BlockSet.MAX_FUNCTION_MEMORY;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeTemplates.arrayType;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public final class ArrayUnionFunction
        extends SqlScalarFunction
{
    private static final String FUNCTION_NAME = "array_union";
    private static final String DESCRIPTION = "Union elements of the given arrays";

    private static final MethodHandle METHOD_HANDLE = methodHandle(
            ArrayUnionFunction.class,
            "union",
            Type.class,
            BlockPositionIsIdentical.class,
            BlockPositionHashCode.class,
            Block[].class);

    private final BlockTypeOperators blockTypeOperators;

    public ArrayUnionFunction(BlockTypeOperators blockTypeOperators)
    {
        super(FunctionMetadata.scalarBuilder(FUNCTION_NAME)
                .signature(Signature.builder()
                        .typeVariable("E")
                        .returnType(arrayType(typeVariable("E")))
                        .argumentType(arrayType(typeVariable("E")))
                        .variableArity()
                        .build())
                .description(DESCRIPTION)
                .build());
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        if (boundSignature.getArity() < 2) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more arguments to %s".formatted(FUNCTION_NAME));
        }
        Type elementType = ((ArrayType)boundSignature.getReturnType()).getElementType();
        BlockPositionIsIdentical elementIdentical = blockTypeOperators.getIdenticalOperator(elementType);
        BlockPositionHashCode elementHashCode = blockTypeOperators.getHashCodeOperator(elementType);

        MethodHandle methodHandle = METHOD_HANDLE
                .bindTo(elementType)
                .bindTo(elementIdentical)
                .bindTo(elementHashCode)
                .asVarargsCollector(Block[].class)
                .asType(methodType(Block.class, nCopies(boundSignature.getArity(), Block.class)));

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                nCopies(boundSignature.getArity(), NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Block union(
            Type type,
            BlockPositionIsIdentical elementIdentical,
            BlockPositionHashCode elementHashCode,
            Block... arrays)
    {
        if (type == BIGINT) {
            return bigintUnion(arrays);
        }

        int totalPositionCount = totalPositionCount(arrays);

        BlockSet set = new BlockSet(elementIdentical, elementHashCode, totalPositionCount);
        for (Block array : arrays) {
            for (int i = 0; i < array.getPositionCount(); i++) {
                set.add(array, i);
            }
        }

        BlockBuilder blockBuilder = type.createBlockBuilder(null, set.size());
        set.getAllWithSizeLimit(blockBuilder, FUNCTION_NAME, MAX_FUNCTION_MEMORY);
        return blockBuilder.build();
    }

    private static Block bigintUnion(Block[] arrays)
    {
        int expectedEntries = Math.min(totalPositionCount(arrays), 16);
        LongSet set = new LongOpenHashSet(expectedEntries);
        BlockBuilder distinctElementBlockBuilder = BIGINT.createBlockBuilder(null, expectedEntries);
        AtomicBoolean containsNull = new AtomicBoolean(false);
        for (Block array : arrays) {
            appendBigintArray(array, containsNull, set, distinctElementBlockBuilder);
        }
        return distinctElementBlockBuilder.build();
    }

    private static int totalPositionCount(Block[] arrays)
    {
        long totalPositionCount = 0;
        for (Block array : arrays) {
            totalPositionCount += array.getPositionCount();
        }
        if (totalPositionCount > Integer.MAX_VALUE) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Total number of array elements exceeds " + Integer.MAX_VALUE);
        }
        return (int) totalPositionCount;
    }

    private static void appendBigintArray(Block array, AtomicBoolean containsNull, LongSet set, BlockBuilder blockBuilder)
    {
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (array.isNull(i)) {
                if (!containsNull.get()) {
                    containsNull.set(true);
                    blockBuilder.appendNull();
                }
                continue;
            }
            long value = BIGINT.getLong(array, i);
            if (set.add(value)) {
                BIGINT.writeLong(blockBuilder, value);
            }
        }
    }
}
