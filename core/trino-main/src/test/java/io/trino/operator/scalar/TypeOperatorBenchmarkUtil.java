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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.lang.invoke.MethodHandle;
import java.util.Base64;
import java.util.Random;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;

final class TypeOperatorBenchmarkUtil
{
    private TypeOperatorBenchmarkUtil() {}

    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final MethodHandle EQUAL_BLOCK;
    private static final MethodHandle HASH_CODE_BLOCK;

    static {
        try {
            HASH_CODE_BLOCK = lookup().findStatic(TypeOperatorBenchmarkUtil.class, "hashBlock", methodType(long.class, MethodHandle.class, Block.class));
            EQUAL_BLOCK = lookup().findStatic(TypeOperatorBenchmarkUtil.class, "equalBlock", methodType(long.class, MethodHandle.class, Block.class, Block.class));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Type toType(String type)
    {
        switch (type) {
            case "BIGINT":
                return BIGINT;
            case "VARCHAR":
                return VARCHAR;
            case "DOUBLE":
                return DOUBLE;
            case "BOOLEAN":
                return BOOLEAN;
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static MethodHandle getEqualBlockMethod(Type type)
    {
        MethodHandle equalOperator = TYPE_OPERATORS.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL));
        return EQUAL_BLOCK.bindTo(equalOperator);
    }

    public static MethodHandle getHashCodeBlockMethod(Type type)
    {
        MethodHandle hashCodeOperator = TYPE_OPERATORS.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));
        return HASH_CODE_BLOCK.bindTo(hashCodeOperator);
    }

    private static long equalBlock(MethodHandle equalOperator, Block left, Block right)
            throws Throwable
    {
        int positionCount = left.getPositionCount();
        long count = 0;
        for (int position = 0; position < positionCount; position++) {
            if ((Boolean) equalOperator.invokeExact(left, position, right, position) == Boolean.TRUE) {
                count++;
            }
        }
        return count;
    }

    private static long hashBlock(MethodHandle hashOperator, Block block)
            throws Throwable
    {
        int positionCount = block.getPositionCount();
        long hash = 0;
        for (int position = 0; position < positionCount; position++) {
            hash += (long) hashOperator.invokeExact(block, position);
        }
        return hash;
    }

    public static void addElement(Type type, Random random, BlockBuilder builder)
    {
        if (type.getJavaType() == long.class) {
            type.writeLong(builder, random.nextLong());
        }
        else if (type.getJavaType() == double.class) {
            type.writeDouble(builder, random.nextDouble());
        }
        else if (type.getJavaType() == boolean.class) {
            type.writeBoolean(builder, random.nextBoolean());
        }
        else if (type.equals(VARCHAR)) {
            type.writeSlice(builder, randomSlice(random));
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    public static void addElement(Type type, Random random, BlockBuilder leftBuilder, BlockBuilder rightBuilder, boolean equal)
    {
        if (type.getJavaType() == long.class) {
            long value = random.nextLong();
            type.writeLong(leftBuilder, value);
            if (equal) {
                type.writeLong(rightBuilder, value);
            }
            else {
                type.writeLong(rightBuilder, random.nextLong());
            }
        }
        else if (type.getJavaType() == double.class) {
            double value = random.nextDouble();
            type.writeDouble(leftBuilder, value);
            if (equal) {
                type.writeDouble(rightBuilder, value);
            }
            else {
                type.writeDouble(rightBuilder, random.nextDouble());
            }
        }
        else if (type.getJavaType() == boolean.class) {
            boolean value = random.nextBoolean();
            type.writeBoolean(leftBuilder, value);
            if (equal) {
                type.writeBoolean(rightBuilder, value);
            }
            else {
                type.writeBoolean(rightBuilder, random.nextBoolean());
            }
        }
        else if (type.equals(VARCHAR)) {
            Slice testString = randomSlice(random);
            type.writeSlice(leftBuilder, testString);
            if (equal) {
                type.writeSlice(rightBuilder, testString);
            }
            else {
                type.writeSlice(rightBuilder, randomSlice(random));
            }
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    private static Slice randomSlice(Random random)
    {
        // random bytes with length [6, 12) and average length of 8.5
        byte[] bytes = new byte[6 + random.nextInt(6)];
        random.nextBytes(bytes);
        // random base 64 encode so length is [8, 16]
        byte[] base64 = Base64.getEncoder().encode(bytes);
        return Slices.wrappedBuffer(base64);
    }
}
