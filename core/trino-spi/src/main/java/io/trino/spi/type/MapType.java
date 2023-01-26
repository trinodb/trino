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
package io.trino.spi.type;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.SingleMapBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorMethodHandle;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeOperatorDeclaration.NO_TYPE_OPERATOR_DECLARATION;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static java.lang.String.format;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Arrays.asList;

public class MapType
        extends AbstractType
{
    private static final InvocationConvention EQUAL_CONVENTION = simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL);
    private static final InvocationConvention HASH_CODE_CONVENTION = simpleConvention(FAIL_ON_NULL, NEVER_NULL);
    private static final InvocationConvention DISTINCT_FROM_CONVENTION = simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE, BOXED_NULLABLE);
    private static final InvocationConvention INDETERMINATE_CONVENTION = simpleConvention(FAIL_ON_NULL, NULL_FLAG);

    private static final MethodHandle EQUAL;
    private static final MethodHandle HASH_CODE;

    private static final MethodHandle SEEK_KEY;
    private static final MethodHandle DISTINCT_FROM;
    private static final MethodHandle INDETERMINATE;

    static {
        try {
            Lookup lookup = MethodHandles.lookup();
            EQUAL = lookup.findStatic(MapType.class, "equalOperator", methodType(Boolean.class, MethodHandle.class, MethodHandle.class, Block.class, Block.class));
            HASH_CODE = lookup.findStatic(MapType.class, "hashOperator", methodType(long.class, MethodHandle.class, MethodHandle.class, Block.class));
            DISTINCT_FROM = lookup.findStatic(MapType.class, "distinctFromOperator", methodType(boolean.class, MethodHandle.class, MethodHandle.class, Block.class, Block.class));
            INDETERMINATE = lookup.findStatic(MapType.class, "indeterminate", methodType(boolean.class, MethodHandle.class, Block.class, boolean.class));
            SEEK_KEY = lookup.findVirtual(
                    SingleMapBlock.class,
                    "seekKey",
                    methodType(int.class, MethodHandle.class, MethodHandle.class, Block.class, int.class));
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private final Type keyType;
    private final Type valueType;
    private static final int EXPECTED_BYTES_PER_ENTRY = 32;

    private final MethodHandle keyNativeHashCode;
    private final MethodHandle keyBlockHashCode;
    private final MethodHandle keyBlockNativeEqual;
    private final MethodHandle keyBlockEqual;

    // this field is used in double checked locking
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private volatile TypeOperatorDeclaration typeOperatorDeclaration;

    public MapType(Type keyType, Type valueType, TypeOperators typeOperators)
    {
        super(
                new TypeSignature(
                        StandardTypes.MAP,
                        TypeSignatureParameter.typeParameter(keyType.getTypeSignature()),
                        TypeSignatureParameter.typeParameter(valueType.getTypeSignature())),
                Block.class);
        if (!keyType.isComparable()) {
            throw new IllegalArgumentException(format("key type must be comparable, got %s", keyType));
        }
        this.keyType = keyType;
        this.valueType = valueType;

        keyBlockNativeEqual = typeOperators.getEqualOperator(keyType, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, NEVER_NULL))
                .asType(methodType(Boolean.class, Block.class, int.class, keyType.getJavaType().isPrimitive() ? keyType.getJavaType() : Object.class));
        keyBlockEqual = typeOperators.getEqualOperator(keyType, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION));
        keyNativeHashCode = typeOperators.getHashCodeOperator(keyType, HASH_CODE_CONVENTION)
                .asType(methodType(long.class, keyType.getJavaType().isPrimitive() ? keyType.getJavaType() : Object.class));
        keyBlockHashCode = typeOperators.getHashCodeOperator(keyType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        if (typeOperatorDeclaration == null) {
            generateTypeOperators(typeOperators);
        }
        return typeOperatorDeclaration;
    }

    private synchronized void generateTypeOperators(TypeOperators typeOperators)
    {
        if (typeOperatorDeclaration != null) {
            return;
        }
        if (!valueType.isComparable()) {
            typeOperatorDeclaration = NO_TYPE_OPERATOR_DECLARATION;
        }
        typeOperatorDeclaration = TypeOperatorDeclaration.builder(getJavaType())
                .addEqualOperator(getEqualOperatorMethodHandle(typeOperators, keyType, valueType))
                .addHashCodeOperator(getHashCodeOperatorMethodHandle(typeOperators, keyType, valueType))
                .addXxHash64Operator(getXxHash64OperatorMethodHandle(typeOperators, keyType, valueType))
                .addDistinctFromOperator(getDistinctFromOperatorInvoker(typeOperators, keyType, valueType))
                .addIndeterminateOperator(getIndeterminateOperatorInvoker(typeOperators, valueType))
                .build();
    }

    private static OperatorMethodHandle getHashCodeOperatorMethodHandle(TypeOperators typeOperators, Type keyType, Type valueType)
    {
        MethodHandle keyHashCodeOperator = typeOperators.getHashCodeOperator(keyType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));
        MethodHandle valueHashCodeOperator = typeOperators.getHashCodeOperator(valueType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));
        return new OperatorMethodHandle(HASH_CODE_CONVENTION, HASH_CODE.bindTo(keyHashCodeOperator).bindTo(valueHashCodeOperator));
    }

    private static OperatorMethodHandle getXxHash64OperatorMethodHandle(TypeOperators typeOperators, Type keyType, Type valueType)
    {
        MethodHandle keyHashCodeOperator = typeOperators.getXxHash64Operator(keyType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));
        MethodHandle valueHashCodeOperator = typeOperators.getXxHash64Operator(valueType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));
        return new OperatorMethodHandle(HASH_CODE_CONVENTION, HASH_CODE.bindTo(keyHashCodeOperator).bindTo(valueHashCodeOperator));
    }

    private static OperatorMethodHandle getEqualOperatorMethodHandle(TypeOperators typeOperators, Type keyType, Type valueType)
    {
        MethodHandle seekKey = MethodHandles.insertArguments(
                SEEK_KEY,
                1,
                typeOperators.getEqualOperator(keyType, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION)),
                typeOperators.getHashCodeOperator(keyType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION)));
        MethodHandle valueEqualOperator = typeOperators.getEqualOperator(valueType, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION));
        return new OperatorMethodHandle(EQUAL_CONVENTION, EQUAL.bindTo(seekKey).bindTo(valueEqualOperator));
    }

    private static OperatorMethodHandle getDistinctFromOperatorInvoker(TypeOperators typeOperators, Type keyType, Type valueType)
    {
        MethodHandle seekKey = MethodHandles.insertArguments(
                SEEK_KEY,
                1,
                typeOperators.getEqualOperator(keyType, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION)),
                typeOperators.getHashCodeOperator(keyType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION)));

        MethodHandle valueDistinctFromOperator = typeOperators.getDistinctFromOperator(valueType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));
        return new OperatorMethodHandle(DISTINCT_FROM_CONVENTION, DISTINCT_FROM.bindTo(seekKey).bindTo(valueDistinctFromOperator));
    }

    private static OperatorMethodHandle getIndeterminateOperatorInvoker(TypeOperators typeOperators, Type valueType)
    {
        MethodHandle valueIndeterminateOperator = typeOperators.getIndeterminateOperator(valueType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));
        return new OperatorMethodHandle(INDETERMINATE_CONVENTION, INDETERMINATE.bindTo(valueIndeterminateOperator));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return new MapBlockBuilder(this, blockBuilderStatus, expectedEntries);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, EXPECTED_BYTES_PER_ENTRY);
    }

    public Type getKeyType()
    {
        return keyType;
    }

    public Type getValueType()
    {
        return valueType;
    }

    @Override
    public boolean isComparable()
    {
        return valueType.isComparable();
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        Block singleMapBlock = block.getObject(position, Block.class);
        if (!(singleMapBlock instanceof SingleMapBlock)) {
            throw new UnsupportedOperationException("Map is encoded with legacy block representation");
        }
        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < singleMapBlock.getPositionCount(); i += 2) {
            map.put(keyType.getObjectValue(session, singleMapBlock, i), valueType.getObjectValue(session, singleMapBlock, i + 1));
        }

        return Collections.unmodifiableMap(map);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            writeObject(blockBuilder, getObject(block, position));
        }
    }

    @Override
    public Block getObject(Block block, int position)
    {
        return block.getObject(position, Block.class);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        if (!(value instanceof SingleMapBlock singleMapBlock)) {
            throw new IllegalArgumentException("Maps must be represented with SingleMapBlock");
        }

        BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();

        for (int i = 0; i < singleMapBlock.getPositionCount(); i += 2) {
            keyType.appendTo(singleMapBlock, i, entryBuilder);
            valueType.appendTo(singleMapBlock, i + 1, entryBuilder);
        }

        blockBuilder.closeEntry();
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return asList(getKeyType(), getValueType());
    }

    @Override
    public String getDisplayName()
    {
        return "map(" + keyType.getDisplayName() + ", " + valueType.getDisplayName() + ")";
    }

    public Block createBlockFromKeyValue(Optional<boolean[]> mapIsNull, int[] offsets, Block keyBlock, Block valueBlock)
    {
        return MapBlock.fromKeyValueBlock(
                mapIsNull,
                offsets,
                keyBlock,
                valueBlock,
                this);
    }

    /**
     * Internal use by this package and io.trino.spi.block only.
     */
    public MethodHandle getKeyNativeHashCode()
    {
        return keyNativeHashCode;
    }

    /**
     * Internal use by this package and io.trino.spi.block only.
     */
    public MethodHandle getKeyBlockHashCode()
    {
        return keyBlockHashCode;
    }

    /**
     * Internal use by this package and io.trino.spi.block only.
     */
    public MethodHandle getKeyBlockNativeEqual()
    {
        return keyBlockNativeEqual;
    }

    /**
     * Internal use by this package and io.trino.spi.block only.
     */
    public MethodHandle getKeyBlockEqual()
    {
        return keyBlockEqual;
    }

    private static long hashOperator(MethodHandle keyOperator, MethodHandle valueOperator, Block block)
            throws Throwable
    {
        long result = 0;
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            result += invokeHashOperator(keyOperator, block, i) ^ invokeHashOperator(valueOperator, block, i + 1);
        }
        return result;
    }

    private static long invokeHashOperator(MethodHandle keyOperator, Block block, int position)
            throws Throwable
    {
        if (block.isNull(position)) {
            return NULL_HASH_CODE;
        }
        return (long) keyOperator.invokeExact(block, position);
    }

    private static Boolean equalOperator(
            MethodHandle seekKey,
            MethodHandle valueEqualOperator,
            Block leftBlock,
            Block rightBlock)
            throws Throwable
    {
        if (leftBlock.getPositionCount() != rightBlock.getPositionCount()) {
            return false;
        }

        SingleMapBlock leftSingleMapLeftBlock = (SingleMapBlock) leftBlock;
        SingleMapBlock rightSingleMapBlock = (SingleMapBlock) rightBlock;

        boolean unknown = false;
        for (int position = 0; position < leftSingleMapLeftBlock.getPositionCount(); position += 2) {
            int leftPosition = position + 1;
            int rightPosition = (int) seekKey.invokeExact(rightSingleMapBlock, leftBlock, position);
            if (rightPosition == -1) {
                return false;
            }

            if (leftBlock.isNull(leftPosition) || rightBlock.isNull(rightPosition)) {
                unknown = true;
            }
            else {
                Boolean result = (Boolean) valueEqualOperator.invokeExact((Block) leftSingleMapLeftBlock, leftPosition, (Block) rightSingleMapBlock, rightPosition);
                if (result == null) {
                    unknown = true;
                }
                else if (!result) {
                    return false;
                }
            }
        }

        if (unknown) {
            return null;
        }
        return true;
    }

    private static boolean distinctFromOperator(
            MethodHandle seekKey,
            MethodHandle valueDistinctFromOperator,
            Block leftBlock,
            Block rightBlock)
            throws Throwable
    {
        boolean leftIsNull = leftBlock == null;
        boolean rightIsNull = rightBlock == null;
        if (leftIsNull || rightIsNull) {
            return leftIsNull != rightIsNull;
        }

        if (leftBlock.getPositionCount() != rightBlock.getPositionCount()) {
            return true;
        }

        SingleMapBlock leftSingleMapLeftBlock = (SingleMapBlock) leftBlock;
        SingleMapBlock rightSingleMapBlock = (SingleMapBlock) rightBlock;

        for (int position = 0; position < leftSingleMapLeftBlock.getPositionCount(); position += 2) {
            int leftPosition = position + 1;
            int rightPosition = (int) seekKey.invokeExact(rightSingleMapBlock, leftBlock, position);
            if (rightPosition == -1) {
                return true;
            }

            boolean result = (boolean) valueDistinctFromOperator.invokeExact((Block) leftSingleMapLeftBlock, leftPosition, (Block) rightSingleMapBlock, rightPosition);
            if (result) {
                return true;
            }
        }

        return false;
    }

    private static boolean indeterminate(MethodHandle valueIndeterminateFunction, Block block, boolean isNull)
            throws Throwable
    {
        if (isNull) {
            return true;
        }
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            // since maps are not allowed to have indeterminate keys we only check values here
            if (block.isNull(i + 1)) {
                return true;
            }
            if ((boolean) valueIndeterminateFunction.invokeExact(block, i + 1)) {
                return true;
            }
        }
        return false;
    }
}
