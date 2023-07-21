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
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorMethodHandle;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeOperatorDeclaration.NO_TYPE_OPERATOR_DECLARATION;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.filterReturnValue;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Arrays.asList;

public class MapType
        extends AbstractType
{
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private static final MethodHandle NOT;
    private static final InvocationConvention READ_FLAT_CONVENTION = simpleConvention(FAIL_ON_NULL, FLAT);
    private static final InvocationConvention READ_FLAT_TO_BLOCK_CONVENTION = simpleConvention(BLOCK_BUILDER, FLAT);
    private static final InvocationConvention WRITE_FLAT_CONVENTION = simpleConvention(FLAT_RETURN, NEVER_NULL);
    private static final InvocationConvention EQUAL_CONVENTION = simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL);
    private static final InvocationConvention HASH_CODE_CONVENTION = simpleConvention(FAIL_ON_NULL, NEVER_NULL);
    private static final InvocationConvention DISTINCT_FROM_CONVENTION = simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE, BOXED_NULLABLE);
    private static final InvocationConvention INDETERMINATE_CONVENTION = simpleConvention(FAIL_ON_NULL, NULL_FLAG);

    private static final MethodHandle READ_FLAT;
    private static final MethodHandle READ_FLAT_TO_BLOCK;
    private static final MethodHandle WRITE_FLAT;
    private static final MethodHandle EQUAL;
    private static final MethodHandle HASH_CODE;

    private static final MethodHandle SEEK_KEY;
    private static final MethodHandle DISTINCT_FROM;
    private static final MethodHandle INDETERMINATE;

    static {
        try {
            Lookup lookup = MethodHandles.lookup();
            NOT = lookup.findStatic(MapType.class, "not", methodType(boolean.class, boolean.class));
            READ_FLAT = lookup.findStatic(MapType.class, "readFlat", methodType(SqlMap.class, MapType.class, MethodHandle.class, MethodHandle.class, int.class, int.class, byte[].class, int.class, byte[].class));
            READ_FLAT_TO_BLOCK = lookup.findStatic(MapType.class, "readFlatToBlock", methodType(void.class, MethodHandle.class, MethodHandle.class, int.class, int.class, byte[].class, int.class, byte[].class, BlockBuilder.class));
            WRITE_FLAT = lookup.findStatic(MapType.class, "writeFlat", methodType(void.class, Type.class, Type.class, MethodHandle.class, MethodHandle.class, int.class, int.class, boolean.class, boolean.class, SqlMap.class, byte[].class, int.class, byte[].class, int.class));
            EQUAL = lookup.findStatic(MapType.class, "equalOperator", methodType(Boolean.class, MethodHandle.class, MethodHandle.class, SqlMap.class, SqlMap.class));
            HASH_CODE = lookup.findStatic(MapType.class, "hashOperator", methodType(long.class, MethodHandle.class, MethodHandle.class, SqlMap.class));
            DISTINCT_FROM = lookup.findStatic(MapType.class, "distinctFromOperator", methodType(boolean.class, MethodHandle.class, MethodHandle.class, SqlMap.class, SqlMap.class));
            INDETERMINATE = lookup.findStatic(MapType.class, "indeterminate", methodType(boolean.class, MethodHandle.class, SqlMap.class, boolean.class));
            SEEK_KEY = lookup.findVirtual(
                    SqlMap.class,
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

    private final MethodHandle keyBlockNativeNotDistinctFrom;
    private final MethodHandle keyBlockNotDistinctFrom;
    private final MethodHandle keyNativeHashCode;
    private final MethodHandle keyBlockHashCode;
    private final MethodHandle keyBlockNativeEqual;
    private final MethodHandle keyBlockEqual;

    // this field is used in double-checked locking
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private volatile TypeOperatorDeclaration typeOperatorDeclaration;

    public MapType(Type keyType, Type valueType, TypeOperators typeOperators)
    {
        super(
                new TypeSignature(
                        StandardTypes.MAP,
                        TypeSignatureParameter.typeParameter(keyType.getTypeSignature()),
                        TypeSignatureParameter.typeParameter(valueType.getTypeSignature())),
                SqlMap.class,
                MapBlock.class);
        if (!keyType.isComparable()) {
            throw new IllegalArgumentException(format("key type must be comparable, got %s", keyType));
        }
        this.keyType = keyType;
        this.valueType = valueType;

        keyBlockNativeEqual = typeOperators.getEqualOperator(keyType, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION_NOT_NULL, NEVER_NULL))
                .asType(methodType(Boolean.class, Block.class, int.class, keyType.getJavaType().isPrimitive() ? keyType.getJavaType() : Object.class));
        keyBlockEqual = typeOperators.getEqualOperator(keyType, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL));

        keyBlockNativeNotDistinctFrom = filterReturnValue(typeOperators.getDistinctFromOperator(keyType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, NEVER_NULL)), NOT)
                .asType(methodType(boolean.class, Block.class, int.class, keyType.getJavaType().isPrimitive() ? keyType.getJavaType() : Object.class));
        keyBlockNotDistinctFrom = filterReturnValue(typeOperators.getDistinctFromOperator(keyType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)), NOT);

        keyNativeHashCode = typeOperators.getHashCodeOperator(keyType, HASH_CODE_CONVENTION)
                .asType(methodType(long.class, keyType.getJavaType().isPrimitive() ? keyType.getJavaType() : Object.class));
        keyBlockHashCode = typeOperators.getHashCodeOperator(keyType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));
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
                .addReadValueOperators(getReadValueOperatorMethodHandles(typeOperators, this))
                .addEqualOperator(getEqualOperatorMethodHandle(typeOperators, keyType, valueType))
                .addHashCodeOperator(getHashCodeOperatorMethodHandle(typeOperators, keyType, valueType))
                .addXxHash64Operator(getXxHash64OperatorMethodHandle(typeOperators, keyType, valueType))
                .addDistinctFromOperator(getDistinctFromOperatorInvoker(typeOperators, keyType, valueType))
                .addIndeterminateOperator(getIndeterminateOperatorInvoker(typeOperators, valueType))
                .build();
    }

    private static List<OperatorMethodHandle> getReadValueOperatorMethodHandles(TypeOperators typeOperators, MapType mapType)
    {
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();

        MethodHandle keyReadOperator = typeOperators.getReadValueOperator(keyType, simpleConvention(BLOCK_BUILDER, FLAT));
        MethodHandle valueReadOperator = typeOperators.getReadValueOperator(valueType, simpleConvention(BLOCK_BUILDER, FLAT));
        MethodHandle readFlat = insertArguments(
                READ_FLAT,
                0,
                mapType,
                keyReadOperator,
                valueReadOperator,
                keyType.getFlatFixedSize(),
                valueType.getFlatFixedSize());
        MethodHandle readFlatToBlock = insertArguments(
                READ_FLAT_TO_BLOCK,
                0,
                keyReadOperator,
                valueReadOperator,
                keyType.getFlatFixedSize(),
                valueType.getFlatFixedSize());

        MethodHandle keyWriteOperator = typeOperators.getReadValueOperator(keyType, simpleConvention(FLAT_RETURN, BLOCK_POSITION));
        MethodHandle valueWriteOperator = typeOperators.getReadValueOperator(valueType, simpleConvention(FLAT_RETURN, BLOCK_POSITION));
        MethodHandle writeFlat = insertArguments(
                WRITE_FLAT,
                0,
                mapType.getKeyType(),
                mapType.getValueType(),
                keyWriteOperator,
                valueWriteOperator,
                keyType.getFlatFixedSize(),
                valueType.getFlatFixedSize(),
                keyType.isFlatVariableWidth(),
                valueType.isFlatVariableWidth());

        return List.of(
                new OperatorMethodHandle(READ_FLAT_CONVENTION, readFlat),
                new OperatorMethodHandle(READ_FLAT_TO_BLOCK_CONVENTION, readFlatToBlock),
                new OperatorMethodHandle(WRITE_FLAT_CONVENTION, writeFlat));
    }

    private static OperatorMethodHandle getHashCodeOperatorMethodHandle(TypeOperators typeOperators, Type keyType, Type valueType)
    {
        MethodHandle keyHashCodeOperator = typeOperators.getHashCodeOperator(keyType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));
        MethodHandle valueHashCodeOperator = typeOperators.getHashCodeOperator(valueType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));
        return new OperatorMethodHandle(HASH_CODE_CONVENTION, HASH_CODE.bindTo(keyHashCodeOperator).bindTo(valueHashCodeOperator));
    }

    private static OperatorMethodHandle getXxHash64OperatorMethodHandle(TypeOperators typeOperators, Type keyType, Type valueType)
    {
        MethodHandle keyHashCodeOperator = typeOperators.getXxHash64Operator(keyType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));
        MethodHandle valueHashCodeOperator = typeOperators.getXxHash64Operator(valueType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));
        return new OperatorMethodHandle(HASH_CODE_CONVENTION, HASH_CODE.bindTo(keyHashCodeOperator).bindTo(valueHashCodeOperator));
    }

    private static OperatorMethodHandle getEqualOperatorMethodHandle(TypeOperators typeOperators, Type keyType, Type valueType)
    {
        MethodHandle seekKey = insertArguments(
                SEEK_KEY,
                1,
                typeOperators.getEqualOperator(keyType, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL)),
                typeOperators.getHashCodeOperator(keyType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)));
        MethodHandle valueEqualOperator = typeOperators.getEqualOperator(valueType, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL));
        return new OperatorMethodHandle(EQUAL_CONVENTION, EQUAL.bindTo(seekKey).bindTo(valueEqualOperator));
    }

    private static OperatorMethodHandle getDistinctFromOperatorInvoker(TypeOperators typeOperators, Type keyType, Type valueType)
    {
        MethodHandle seekKey = insertArguments(
                SEEK_KEY,
                1,
                typeOperators.getEqualOperator(keyType, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL)),
                typeOperators.getHashCodeOperator(keyType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)));

        MethodHandle valueDistinctFromOperator = typeOperators.getDistinctFromOperator(valueType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));
        MethodHandle methodHandle = DISTINCT_FROM.bindTo(seekKey).bindTo(valueDistinctFromOperator);
        return new OperatorMethodHandle(DISTINCT_FROM_CONVENTION, methodHandle);
    }

    private static OperatorMethodHandle getIndeterminateOperatorInvoker(TypeOperators typeOperators, Type valueType)
    {
        MethodHandle valueIndeterminateOperator = typeOperators.getIndeterminateOperator(valueType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));
        return new OperatorMethodHandle(INDETERMINATE_CONVENTION, INDETERMINATE.bindTo(valueIndeterminateOperator));
    }

    @Override
    public MapBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return new MapBlockBuilder(this, blockBuilderStatus, expectedEntries);
    }

    @Override
    public MapBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
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

        SqlMap sqlMap = block.getObject(position, SqlMap.class);
        int rawOffset = sqlMap.getRawOffset();
        Block rawKeyBlock = sqlMap.getRawKeyBlock();
        Block rawValueBlock = sqlMap.getRawValueBlock();

        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < sqlMap.getSize(); i++) {
            map.put(keyType.getObjectValue(session, rawKeyBlock, rawOffset + i), valueType.getObjectValue(session, rawValueBlock, rawOffset + i));
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
    public SqlMap getObject(Block block, int position)
    {
        return block.getObject(position, SqlMap.class);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        if (!(value instanceof SqlMap sqlMap)) {
            throw new IllegalArgumentException("Maps must be represented with SqlMap");
        }

        int rawOffset = sqlMap.getRawOffset();
        Block rawKeyBlock = sqlMap.getRawKeyBlock();
        Block rawValueBlock = sqlMap.getRawValueBlock();

        ((MapBlockBuilder) blockBuilder).buildEntry((keyBuilder, valueBuilder) -> {
            for (int i = 0; i < sqlMap.getSize(); i++) {
                keyType.appendTo(rawKeyBlock, rawOffset + i, keyBuilder);
                valueType.appendTo(rawValueBlock, rawOffset + i, valueBuilder);
            }
        });
    }

    // FLAT MEMORY LAYOUT
    //
    // All data of the map is stored in the variable width section. Within the variable width section,
    // fixed data for all keys and values are stored first, followed by variable length data for all keys
    // and values. This simplifies the read implementation as we can simply step through the fixed
    // section without knowing the variable length of each value, since each value stores the offset
    // to its variable length data inside its fixed length data.
    //
    // In the current implementation, the keys and values are stored in an interleaved flat record along
    // with null flags. This layout is not required by the format, and could be changed to a columnar
    // if it is determined to be more efficient. Additionally, this layout allows for a null key, since
    // non-null keys is not always enforced, and null keys may be allowed in the future.
    //
    // Fixed:
    //   int positionCount, int variableSizeOffset
    // Variable:
    //   byte key1Null, keyFixedSize key1FixedData, byte value1Null, valueFixedSize value1FixedData
    //   byte key2Null, keyFixedSize key2FixedData, byte value2Null, valueFixedSize value2FixedData
    //   ...
    //   key1VariableSize key1VariableData, value1VariableSize value1VariableData
    //   key2VariableSize key2VariableData, value2VariableSize value2VariableData
    //   ...

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
        SqlMap sqlMap = getObject(block, position);
        int rawOffset = sqlMap.getRawOffset();
        Block rawKeyBlock = sqlMap.getRawKeyBlock();
        Block rawValueBlock = sqlMap.getRawValueBlock();

        long flatSize = sqlMap.getSize() * (keyType.getFlatFixedSize() + valueType.getFlatFixedSize() + 2L);

        if (keyType.isFlatVariableWidth()) {
            for (int index = 0; index < sqlMap.getSize(); index++) {
                if (!rawKeyBlock.isNull(rawOffset + index)) {
                    flatSize += keyType.getFlatVariableWidthSize(rawKeyBlock, rawOffset + index);
                }
            }
        }
        if (valueType.isFlatVariableWidth()) {
            for (int index = 0; index < sqlMap.getSize(); index++) {
                if (!rawValueBlock.isNull(rawOffset + index)) {
                    flatSize += valueType.getFlatVariableWidthSize(rawValueBlock, rawOffset + index);
                }
            }
        }
        return toIntExact(flatSize);
    }

    @Override
    public int relocateFlatVariableWidthOffsets(byte[] fixedSizeSlice, int fixedSizeOffset, byte[] variableSizeSlice, int variableSizeOffset)
    {
        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset + Integer.BYTES, variableSizeOffset);

        int size = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
        int keyFixedSize = keyType.getFlatFixedSize();
        int valueFixedSize = valueType.getFlatFixedSize();
        if (!keyType.isFlatVariableWidth() && !valueType.isFlatVariableWidth()) {
            return size * (2 + keyFixedSize + valueFixedSize);
        }

        return relocateVariableWidthData(size, keyFixedSize, valueFixedSize, variableSizeSlice, variableSizeOffset);
    }

    private int relocateVariableWidthData(int size, int keyFixedSize, int valueFixedSize, byte[] slice, int offset)
    {
        int writeFixedOffset = offset;
        // variable width data starts after fixed width data for the keys and values
        // there is one extra byte per key and value for a null flag
        int writeVariableWidthOffset = offset + (size * (2 + keyFixedSize + valueFixedSize));
        for (int index = 0; index < size; index++) {
            if (!keyType.isFlatVariableWidth() || slice[writeFixedOffset] != 0) {
                writeFixedOffset++;
            }
            else {
                // skip null byte
                writeFixedOffset++;

                int keyVariableSize = keyType.relocateFlatVariableWidthOffsets(slice, writeFixedOffset, slice, writeVariableWidthOffset);
                writeVariableWidthOffset += keyVariableSize;
            }
            writeFixedOffset += keyFixedSize;

            if (!valueType.isFlatVariableWidth() || slice[writeFixedOffset] != 0) {
                writeFixedOffset++;
            }
            else {
                // skip null byte
                writeFixedOffset++;

                int valueVariableSize = valueType.relocateFlatVariableWidthOffsets(slice, writeFixedOffset, slice, writeVariableWidthOffset);
                writeVariableWidthOffset += valueVariableSize;
            }
            writeFixedOffset += valueFixedSize;
        }
        return writeVariableWidthOffset - offset;
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

    public MapBlock createBlockFromKeyValue(Optional<boolean[]> mapIsNull, int[] offsets, Block keyBlock, Block valueBlock)
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

    /**
     * Internal use by this package and io.trino.spi.block only.
     */
    public MethodHandle getKeyBlockNativeNotDistinctFrom()
    {
        return keyBlockNativeNotDistinctFrom;
    }

    /**
     * Internal use by this package and io.trino.spi.block only.
     */
    public MethodHandle getKeyBlockNotDistinctFrom()
    {
        return keyBlockNotDistinctFrom;
    }

    private static long hashOperator(MethodHandle keyOperator, MethodHandle valueOperator, SqlMap sqlMap)
            throws Throwable
    {
        int rawOffset = sqlMap.getRawOffset();
        Block rawKeyBlock = sqlMap.getRawKeyBlock();
        Block rawValueBlock = sqlMap.getRawValueBlock();

        long result = 0;
        for (int i = 0; i < sqlMap.getSize(); i++) {
            result += invokeHashOperator(keyOperator, rawKeyBlock, rawOffset + i) ^ invokeHashOperator(valueOperator, rawValueBlock, rawOffset + i);
        }
        return result;
    }

    private static long invokeHashOperator(MethodHandle hashOperator, Block block, int position)
            throws Throwable
    {
        if (block.isNull(position)) {
            return NULL_HASH_CODE;
        }
        return (long) hashOperator.invokeExact((Block) block, position);
    }

    private static SqlMap readFlat(
            MapType mapType,
            MethodHandle keyReadOperator,
            MethodHandle valueReadOperator,
            int keyFixedSize,
            int valueFixedSize,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableWidthSlice)
            throws Throwable
    {
        int size = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
        int variableWidthOffset = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset + Integer.BYTES);
        return buildMapValue(mapType, size, (keyBuilder, valueBuilder) ->
                readFlatEntries(
                        keyReadOperator,
                        valueReadOperator,
                        keyFixedSize,
                        valueFixedSize,
                        size,
                        variableWidthSlice,
                        variableWidthOffset,
                        keyBuilder,
                        valueBuilder));
    }

    private static void readFlatToBlock(
            MethodHandle keyReadOperator,
            MethodHandle valueReadOperator,
            int keyFixedSize,
            int valueFixedSize,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableWidthSlice,
            BlockBuilder blockBuilder)
            throws Throwable
    {
        int size = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
        int variableWidthOffset = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset + Integer.BYTES);
        ((MapBlockBuilder) blockBuilder).buildEntry((keyBuilder, valueBuilder) ->
                readFlatEntries(
                        keyReadOperator,
                        valueReadOperator,
                        keyFixedSize,
                        valueFixedSize,
                        size,
                        variableWidthSlice,
                        variableWidthOffset,
                        keyBuilder,
                        valueBuilder));
    }

    private static void readFlatEntries(
            MethodHandle keyReadFlat,
            MethodHandle valueReadFlat,
            int keyFixedSize,
            int valueFixedSize,
            int size,
            byte[] slice,
            int offset,
            BlockBuilder keyBuilder,
            BlockBuilder valueBuilder)
            throws Throwable
    {
        for (int index = 0; index < size; index++) {
            boolean keyIsNull = slice[offset] != 0;
            offset++;
            if (keyIsNull) {
                keyBuilder.appendNull();
            }
            else {
                keyReadFlat.invokeExact(
                        slice,
                        offset,
                        slice,
                        keyBuilder);
            }
            offset += keyFixedSize;

            boolean valueIsNull = slice[offset] != 0;
            offset++;
            if (valueIsNull) {
                valueBuilder.appendNull();
            }
            else {
                valueReadFlat.invokeExact(
                        slice,
                        offset,
                        slice,
                        valueBuilder);
            }
            offset += valueFixedSize;
        }
    }

    private static void writeFlat(
            Type keyType,
            Type valueType,
            MethodHandle keyWriteFlat,
            MethodHandle valueWriteFlat,
            int keyFixedSize,
            int valueFixedSize,
            boolean keyVariableWidth,
            boolean valueVariableWidth,
            SqlMap map,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableSizeSlice,
            int variableSizeOffset)
            throws Throwable
    {
        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset, map.getSize());
        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset + Integer.BYTES, variableSizeOffset);

        writeFlatEntries(keyType, valueType, keyWriteFlat, valueWriteFlat, keyFixedSize, valueFixedSize, keyVariableWidth, valueVariableWidth, map, variableSizeSlice, variableSizeOffset);
    }

    private static void writeFlatEntries(
            Type keyType,
            Type valueType,
            MethodHandle keyWriteFlat,
            MethodHandle valueWriteFlat,
            int keyFixedSize,
            int valueFixedSize,
            boolean keyVariableWidth,
            boolean valueVariableWidth,
            SqlMap sqlMap,
            byte[] slice,
            int offset)
            throws Throwable
    {
        int size = sqlMap.getSize();
        int rawOffset = sqlMap.getRawOffset();
        Block rawKeyBlock = sqlMap.getRawKeyBlock();
        Block rawValueBlock = sqlMap.getRawValueBlock();

        // variable width data starts after fixed width data for the keys and values
        // there is one extra byte per key and value for a null flag
        int writeVariableWidthOffset = offset + (size * (2 + keyFixedSize + valueFixedSize));
        for (int index = 0; index < size; index++) {
            if (rawKeyBlock.isNull(rawOffset + index)) {
                slice[offset] = 1;
                offset++;
            }
            else {
                // skip null byte
                offset++;

                int keyVariableSize = 0;
                if (keyVariableWidth) {
                    keyVariableSize = keyType.getFlatVariableWidthSize(rawKeyBlock, rawOffset + index);
                }
                keyWriteFlat.invokeExact(
                        rawKeyBlock,
                        rawOffset + index,
                        slice,
                        offset,
                        slice,
                        writeVariableWidthOffset);
                writeVariableWidthOffset += keyVariableSize;
            }
            offset += keyFixedSize;

            if (rawValueBlock.isNull(rawOffset + index)) {
                slice[offset] = 1;
                offset++;
            }
            else {
                // skip null byte
                offset++;

                int valueVariableSize = 0;
                if (valueVariableWidth) {
                    valueVariableSize = valueType.getFlatVariableWidthSize(rawValueBlock, rawOffset + index);
                }
                valueWriteFlat.invokeExact(
                        rawValueBlock,
                        rawOffset + index,
                        slice,
                        offset,
                        slice,
                        writeVariableWidthOffset);
                writeVariableWidthOffset += valueVariableSize;
            }
            offset += valueFixedSize;
        }
    }

    private static Boolean equalOperator(
            MethodHandle seekKey,
            MethodHandle valueEqualOperator,
            SqlMap leftMap,
            SqlMap rightMap)
            throws Throwable
    {
        if (leftMap.getSize() != rightMap.getSize()) {
            return false;
        }

        int leftRawOffset = leftMap.getRawOffset();
        Block leftRawKeyBlock = leftMap.getRawKeyBlock();
        Block leftRawValueBlock = leftMap.getRawValueBlock();
        int rightRawOffset = rightMap.getRawOffset();
        Block rightRawValueBlock = rightMap.getRawValueBlock();

        boolean unknown = false;
        for (int leftIndex = 0; leftIndex < leftMap.getSize(); leftIndex++) {
            int rightIndex = (int) seekKey.invokeExact(rightMap, leftRawKeyBlock, leftRawOffset + leftIndex);
            if (rightIndex == -1) {
                return false;
            }

            if (leftRawValueBlock.isNull(leftRawOffset + leftIndex) || rightRawValueBlock.isNull(rightRawOffset + rightIndex)) {
                unknown = true;
            }
            else {
                Boolean result = (Boolean) valueEqualOperator.invokeExact(leftRawValueBlock, leftRawOffset + leftIndex, rightRawValueBlock, rightRawOffset + rightIndex);
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
            SqlMap leftMap,
            SqlMap rightMap)
            throws Throwable
    {
        boolean leftIsNull = leftMap == null;
        boolean rightIsNull = rightMap == null;
        if (leftIsNull || rightIsNull) {
            return leftIsNull != rightIsNull;
        }

        if (leftMap.getSize() != rightMap.getSize()) {
            return true;
        }

        int leftRawOffset = leftMap.getRawOffset();
        Block leftRawKeyBlock = leftMap.getRawKeyBlock();
        Block leftRawValueBlock = leftMap.getRawValueBlock();
        int rightRawOffset = rightMap.getRawOffset();
        Block rightRawValueBlock = rightMap.getRawValueBlock();

        for (int leftIndex = 0; leftIndex < leftMap.getSize(); leftIndex++) {
            int rightIndex = (int) seekKey.invokeExact(rightMap, leftRawKeyBlock, leftRawOffset + leftIndex);
            if (rightIndex == -1) {
                return true;
            }

            boolean result = (boolean) valueDistinctFromOperator.invokeExact(leftRawValueBlock, leftRawOffset + leftIndex, rightRawValueBlock, rightRawOffset + rightIndex);
            if (result) {
                return true;
            }
        }

        return false;
    }

    private static boolean indeterminate(MethodHandle valueIndeterminateFunction, SqlMap sqlMap, boolean isNull)
            throws Throwable
    {
        if (isNull) {
            return true;
        }

        int rawOffset = sqlMap.getRawOffset();
        Block rawValueBlock = sqlMap.getRawValueBlock();

        for (int i = 0; i < sqlMap.getSize(); i++) {
            // since maps are not allowed to have indeterminate keys, we only check values here
            if (rawValueBlock.isNull(rawOffset + i)) {
                return true;
            }
            if ((boolean) valueIndeterminateFunction.invokeExact(rawValueBlock, rawOffset + i)) {
                return true;
            }
        }
        return false;
    }

    private static boolean not(boolean value)
    {
        return !value;
    }
}
