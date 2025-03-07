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

import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorMethodHandle;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.StandardTypes.ARRAY;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static io.trino.spi.type.TypeUtils.checkElementNotNull;
import static java.lang.Math.toIntExact;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class ArrayType
        extends AbstractType
{
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private static final InvocationConvention READ_FLAT_CONVENTION = simpleConvention(FAIL_ON_NULL, FLAT);
    private static final InvocationConvention READ_FLAT_TO_BLOCK_CONVENTION = simpleConvention(BLOCK_BUILDER, FLAT);
    private static final InvocationConvention WRITE_FLAT_CONVENTION = simpleConvention(FLAT_RETURN, NEVER_NULL);
    private static final InvocationConvention EQUAL_CONVENTION = simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL);
    private static final InvocationConvention HASH_CODE_CONVENTION = simpleConvention(FAIL_ON_NULL, NEVER_NULL);
    private static final InvocationConvention IDENTICAL_CONVENTION = simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE, BOXED_NULLABLE);
    private static final InvocationConvention INDETERMINATE_CONVENTION = simpleConvention(FAIL_ON_NULL, NULL_FLAG);
    private static final InvocationConvention COMPARISON_CONVENTION = simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL);

    private static final MethodHandle READ_FLAT;
    private static final MethodHandle READ_FLAT_TO_BLOCK;
    private static final MethodHandle WRITE_FLAT;
    private static final MethodHandle EQUAL;
    private static final MethodHandle HASH_CODE;
    private static final MethodHandle IDENTICAL;
    private static final MethodHandle INDETERMINATE;
    private static final MethodHandle COMPARISON;

    static {
        try {
            Lookup lookup = MethodHandles.lookup();
            READ_FLAT = lookup.findStatic(ArrayType.class, "readFlat", MethodType.methodType(Block.class, Type.class, MethodHandle.class, int.class, byte[].class, int.class, byte[].class, int.class));
            READ_FLAT_TO_BLOCK = lookup.findStatic(ArrayType.class, "readFlatToBlock", MethodType.methodType(void.class, Type.class, MethodHandle.class, int.class, byte[].class, int.class, byte[].class, int.class, BlockBuilder.class));
            WRITE_FLAT = lookup.findStatic(ArrayType.class, "writeFlat", MethodType.methodType(void.class, Type.class, MethodHandle.class, int.class, boolean.class, Block.class, byte[].class, int.class, byte[].class, int.class));
            EQUAL = lookup.findStatic(ArrayType.class, "equalOperator", MethodType.methodType(Boolean.class, MethodHandle.class, Block.class, Block.class));
            HASH_CODE = lookup.findStatic(ArrayType.class, "hashOperator", MethodType.methodType(long.class, MethodHandle.class, Block.class));
            IDENTICAL = lookup.findStatic(ArrayType.class, "identicalOperator", MethodType.methodType(boolean.class, MethodHandle.class, Block.class, Block.class));
            INDETERMINATE = lookup.findStatic(ArrayType.class, "indeterminateOperator", MethodType.methodType(boolean.class, MethodHandle.class, Block.class, boolean.class));
            COMPARISON = lookup.findStatic(ArrayType.class, "comparisonOperator", MethodType.methodType(long.class, MethodHandle.class, Block.class, Block.class));
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static final String ARRAY_NULL_ELEMENT_MSG = "ARRAY comparison not supported for arrays with null elements";

    private final Type elementType;

    // this field is used in double-checked locking
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private volatile TypeOperatorDeclaration operatorDeclaration;

    public ArrayType(Type elementType)
    {
        super(new TypeSignature(ARRAY, TypeSignatureParameter.typeParameter(elementType.getTypeSignature())), Block.class, ArrayBlock.class);
        this.elementType = requireNonNull(elementType, "elementType is null");
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        if (operatorDeclaration == null) {
            generateTypeOperators(typeOperators);
        }
        return operatorDeclaration;
    }

    private synchronized void generateTypeOperators(TypeOperators typeOperators)
    {
        if (operatorDeclaration != null) {
            return;
        }
        operatorDeclaration = TypeOperatorDeclaration.builder(getJavaType())
                .addReadValueOperators(getReadValueOperatorMethodHandles(typeOperators, elementType))
                .addEqualOperators(getEqualOperatorMethodHandles(typeOperators, elementType))
                .addHashCodeOperators(getHashCodeOperatorMethodHandles(typeOperators, elementType))
                .addXxHash64Operators(getXxHash64OperatorMethodHandles(typeOperators, elementType))
                .addIdenticalOperators(getIdenticalOperatorInvokers(typeOperators, elementType))
                .addIndeterminateOperators(getIndeterminateOperatorInvokers(typeOperators, elementType))
                .addComparisonUnorderedLastOperators(getComparisonOperatorInvokers(typeOperators::getComparisonUnorderedLastOperator, elementType))
                .addComparisonUnorderedFirstOperators(getComparisonOperatorInvokers(typeOperators::getComparisonUnorderedFirstOperator, elementType))
                .build();
    }

    private static List<OperatorMethodHandle> getReadValueOperatorMethodHandles(TypeOperators typeOperators, Type elementType)
    {
        MethodHandle elementReadOperator = typeOperators.getReadValueOperator(elementType, simpleConvention(BLOCK_BUILDER, FLAT));
        MethodHandle readFlat = insertArguments(READ_FLAT, 0, elementType, elementReadOperator, elementType.getFlatFixedSize());
        MethodHandle readFlatToBlock = insertArguments(READ_FLAT_TO_BLOCK, 0, elementType, elementReadOperator, elementType.getFlatFixedSize());

        MethodHandle elementWriteOperator = typeOperators.getReadValueOperator(elementType, simpleConvention(FLAT_RETURN, VALUE_BLOCK_POSITION_NOT_NULL));
        MethodHandle writeFlatToBlock = insertArguments(WRITE_FLAT, 0, elementType, elementWriteOperator, elementType.getFlatFixedSize(), elementType.isFlatVariableWidth());
        return List.of(
                new OperatorMethodHandle(READ_FLAT_CONVENTION, readFlat),
                new OperatorMethodHandle(READ_FLAT_TO_BLOCK_CONVENTION, readFlatToBlock),
                new OperatorMethodHandle(WRITE_FLAT_CONVENTION, writeFlatToBlock));
    }

    private static List<OperatorMethodHandle> getEqualOperatorMethodHandles(TypeOperators typeOperators, Type elementType)
    {
        if (!elementType.isComparable()) {
            return emptyList();
        }
        MethodHandle equalOperator = typeOperators.getEqualOperator(elementType, simpleConvention(NULLABLE_RETURN, VALUE_BLOCK_POSITION_NOT_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        return singletonList(new OperatorMethodHandle(EQUAL_CONVENTION, EQUAL.bindTo(equalOperator)));
    }

    private static List<OperatorMethodHandle> getHashCodeOperatorMethodHandles(TypeOperators typeOperators, Type elementType)
    {
        if (!elementType.isComparable()) {
            return emptyList();
        }
        MethodHandle elementHashCodeOperator = typeOperators.getHashCodeOperator(elementType, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        return singletonList(new OperatorMethodHandle(HASH_CODE_CONVENTION, HASH_CODE.bindTo(elementHashCodeOperator)));
    }

    private static List<OperatorMethodHandle> getXxHash64OperatorMethodHandles(TypeOperators typeOperators, Type elementType)
    {
        if (!elementType.isComparable()) {
            return emptyList();
        }
        MethodHandle elementHashCodeOperator = typeOperators.getXxHash64Operator(elementType, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        return singletonList(new OperatorMethodHandle(HASH_CODE_CONVENTION, HASH_CODE.bindTo(elementHashCodeOperator)));
    }

    private static List<OperatorMethodHandle> getIdenticalOperatorInvokers(TypeOperators typeOperators, Type elementType)
    {
        if (!elementType.isComparable()) {
            return emptyList();
        }
        MethodHandle elementIdenticalOperator = typeOperators.getIdenticalOperator(elementType, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        return singletonList(new OperatorMethodHandle(IDENTICAL_CONVENTION, IDENTICAL.bindTo(elementIdenticalOperator)));
    }

    private static List<OperatorMethodHandle> getIndeterminateOperatorInvokers(TypeOperators typeOperators, Type elementType)
    {
        if (!elementType.isComparable()) {
            return emptyList();
        }
        MethodHandle elementIndeterminateOperator = typeOperators.getIndeterminateOperator(elementType, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        return singletonList(new OperatorMethodHandle(INDETERMINATE_CONVENTION, INDETERMINATE.bindTo(elementIndeterminateOperator)));
    }

    private static List<OperatorMethodHandle> getComparisonOperatorInvokers(BiFunction<Type, InvocationConvention, MethodHandle> comparisonOperatorFactory, Type elementType)
    {
        if (!elementType.isOrderable()) {
            return emptyList();
        }
        MethodHandle elementComparisonOperator = comparisonOperatorFactory.apply(elementType, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        return singletonList(new OperatorMethodHandle(COMPARISON_CONVENTION, COMPARISON.bindTo(elementComparisonOperator)));
    }

    public Type getElementType()
    {
        return elementType;
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
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        if (block instanceof ArrayBlock) {
            return ((ArrayBlock) block).apply((valuesBlock, start, length) -> arrayBlockToObjectValues(session, valuesBlock, start, length), position);
        }
        Block arrayBlock = getObject(block, position);
        return arrayBlockToObjectValues(session, arrayBlock, 0, arrayBlock.getPositionCount());
    }

    private List<Object> arrayBlockToObjectValues(ConnectorSession session, Block block, int start, int length)
    {
        List<Object> values = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            values.add(elementType.getObjectValue(session, block, i + start));
        }

        return Collections.unmodifiableList(values);
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
        return read((ArrayBlock) block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        Block arrayBlock = (Block) value;
        ((ArrayBlockBuilder) blockBuilder).buildEntry(elementBuilder -> {
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                elementType.appendTo(arrayBlock, i, elementBuilder);
            }
        });
    }

    // FLAT MEMORY LAYOUT
    //
    // All data of the array is stored in the variable width section. Within the variable width section,
    // fixed data for all elements is stored first, followed by variable length data for all elements
    // This simplifies the read implementation as we can simply step through the fixed section without
    // knowing the variable length of each element, since each element stores the offset to its variable
    // length data inside its fixed length data.
    //
    // In the current implementation, the element and null flag are stored in an interleaved flat record.
    // This layout is not required by the format, and could be changed to a columnar if it is determined
    // to be more efficient.
    //
    // Fixed:
    //   int positionCount, int variableLength
    // Variable:
    //   byte element1Null, elementFixedSize element1FixedData
    //   byte element2Null, elementFixedSize element2FixedData
    //   ...
    //   element1VariableSize element1VariableData
    //   element2VariableSize element2VariableData
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
        Block array = getObject(block, position);
        int arrayLength = array.getPositionCount();

        int flatFixedSize = elementType.getFlatFixedSize();
        boolean variableWidth = elementType.isFlatVariableWidth();

        // one byte for null flag
        long size = arrayLength * (flatFixedSize + 1L);
        if (variableWidth) {
            for (int index = 0; index < arrayLength; index++) {
                if (!array.isNull(index)) {
                    size += elementType.getFlatVariableWidthSize(array, index);
                }
            }
        }
        return toIntExact(size);
    }

    @Override
    public ArrayBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return new ArrayBlockBuilder(elementType, blockBuilderStatus, expectedEntries, expectedBytesPerEntry);
    }

    @Override
    public ArrayBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, 100);
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return singletonList(getElementType());
    }

    @Override
    public String getDisplayName()
    {
        return ARRAY + "(" + elementType.getDisplayName() + ")";
    }

    private static Block read(ArrayBlock block, int position)
    {
        return block.getArray(position);
    }

    @Override
    public int getFlatVariableWidthLength(byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        return (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset + Integer.BYTES);
    }

    private static Block readFlat(
            Type elementType,
            MethodHandle elementReadFlat,
            int elementFixedSize,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableSizeSlice,
            int variableSizeOffset)
            throws Throwable
    {
        int positionCount = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
        BlockBuilder elementBuilder = elementType.createBlockBuilder(null, positionCount);
        readFlatElements(elementType, elementReadFlat, elementFixedSize, variableSizeSlice, variableSizeOffset, positionCount, elementBuilder);
        return elementBuilder.build();
    }

    private static void readFlatToBlock(
            Type elementType,
            MethodHandle elementReadFlat,
            int elementFixedSize,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableSizeSlice,
            int variableSizeOffset,
            BlockBuilder blockBuilder)
            throws Throwable
    {
        int positionCount = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
        ((ArrayBlockBuilder) blockBuilder).buildEntry(elementBuilder ->
                readFlatElements(elementType, elementReadFlat, elementFixedSize, variableSizeSlice, variableSizeOffset, positionCount, elementBuilder));
    }

    private static void readFlatElements(Type elementType, MethodHandle elementReadFlat, int elementFixedSize, byte[] slice, int sliceOffset, int positionCount, BlockBuilder elementBuilder)
            throws Throwable
    {
        boolean elementVariableWidth = elementType.isFlatVariableWidth();
        int elementVariableOffset = sliceOffset + (positionCount * (1 + elementFixedSize));
        for (int i = 0; i < positionCount; i++) {
            boolean elementIsNull = slice[sliceOffset] != 0;
            if (elementIsNull) {
                elementBuilder.appendNull();
            }
            else {
                elementReadFlat.invokeExact(
                        slice,
                        sliceOffset + 1,
                        slice,
                        elementVariableOffset,
                        elementBuilder);
                // advance variable offset
                if (elementVariableWidth) {
                    elementVariableOffset += elementType.getFlatVariableWidthLength(slice, sliceOffset + 1);
                }
            }
            sliceOffset += 1 + elementFixedSize;
        }
    }

    private static void writeFlat(
            Type elementType,
            MethodHandle elementWriteFlat,
            int elementFixedSize,
            boolean elementVariableWidth,
            Block array,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableSizeSlice,
            int variableSizeOffset)
            throws Throwable
    {
        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset, array.getPositionCount());
        int endingOffset = writeFlatElements(elementType, elementWriteFlat, elementFixedSize, elementVariableWidth, array, variableSizeSlice, variableSizeOffset);
        int variableLength = endingOffset - variableSizeOffset;
        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset + Integer.BYTES, variableLength);
    }

    private static int writeFlatElements(Type elementType, MethodHandle elementWriteFlat, int elementFixedSize, boolean elementVariableWidth, Block array, byte[] slice, int offset)
            throws Throwable
    {
        array = array.getLoadedBlock();

        int positionCount = array.getPositionCount();
        // variable width data starts after fixed width data
        // there is one extra byte per position for the null flag
        int writeVariableWidthOffset = offset + positionCount * (1 + elementFixedSize);
        switch (array) {
            case ValueBlock valuesBlock -> {
                for (int index = 0; index < positionCount; index++) {
                    writeVariableWidthOffset = writeFlatElement(elementType, elementWriteFlat, elementVariableWidth, valuesBlock, index, slice, offset, writeVariableWidthOffset);
                    offset += 1 + elementFixedSize;
                }
            }
            case RunLengthEncodedBlock rleBlock -> {
                ValueBlock valuesBlock = rleBlock.getValue();
                for (int index = 0; index < positionCount; index++) {
                    writeVariableWidthOffset = writeFlatElement(elementType, elementWriteFlat, elementVariableWidth, valuesBlock, 0, slice, offset, writeVariableWidthOffset);
                    offset += 1 + elementFixedSize;
                }
            }
            case DictionaryBlock dictionaryBlock -> {
                ValueBlock valuesBlock = dictionaryBlock.getDictionary();
                for (int position = 0; position < positionCount; position++) {
                    int index = dictionaryBlock.getId(position);
                    writeVariableWidthOffset = writeFlatElement(elementType, elementWriteFlat, elementVariableWidth, valuesBlock, index, slice, offset, writeVariableWidthOffset);
                    offset += 1 + elementFixedSize;
                }
            }
            case LazyBlock _ -> throw new IllegalStateException("Did not expect LazyBlock after loading " + array.getClass().getSimpleName());
        }
        return writeVariableWidthOffset;
    }

    private static int writeFlatElement(Type elementType, MethodHandle elementWriteFlat, boolean elementVariableWidth, ValueBlock array, int index, byte[] slice, int offset, int writeVariableWidthOffset)
            throws Throwable
    {
        if (array.isNull(index)) {
            slice[offset] = 1;
        }
        else {
            elementWriteFlat.invokeExact(
                    array,
                    index,
                    slice,
                    offset + 1, // skip null byte
                    slice,
                    writeVariableWidthOffset);
            if (elementVariableWidth) {
                writeVariableWidthOffset += elementType.getFlatVariableWidthLength(slice, offset + 1);
            }
        }
        return writeVariableWidthOffset;
    }

    private static Boolean equalOperator(MethodHandle equalOperator, Block leftArray, Block rightArray)
            throws Throwable
    {
        if (leftArray.getPositionCount() != rightArray.getPositionCount()) {
            return false;
        }

        leftArray = leftArray.getLoadedBlock();
        rightArray = rightArray.getLoadedBlock();

        ValueBlock leftValues = leftArray.getUnderlyingValueBlock();
        ValueBlock rightValues = rightArray.getUnderlyingValueBlock();

        boolean unknown = false;
        for (int position = 0; position < leftArray.getPositionCount(); position++) {
            int leftIndex = leftArray.getUnderlyingValuePosition(position);
            int rightIndex = rightArray.getUnderlyingValuePosition(position);
            if (leftValues.isNull(leftIndex) || rightValues.isNull(rightIndex)) {
                unknown = true;
                continue;
            }
            Boolean result = (Boolean) equalOperator.invokeExact(leftValues, leftIndex, rightValues, rightIndex);
            if (result == null) {
                unknown = true;
            }
            else if (!result) {
                return false;
            }
        }

        if (unknown) {
            return null;
        }
        return true;
    }

    private static long hashOperator(MethodHandle hashOperator, Block array)
            throws Throwable
    {
        array = array.getLoadedBlock();

        if (array instanceof ValueBlock valuesBlock) {
            long hash = 0;
            for (int index = 0; index < valuesBlock.getPositionCount(); index++) {
                long elementHash = valuesBlock.isNull(index) ? NULL_HASH_CODE : (long) hashOperator.invokeExact(valuesBlock, index);
                hash = 31 * hash + elementHash;
            }
            return hash;
        }

        if (array instanceof RunLengthEncodedBlock rleBlock) {
            ValueBlock valuesBlock = rleBlock.getValue();
            long elementHash = valuesBlock.isNull(0) ? NULL_HASH_CODE : (long) hashOperator.invokeExact(valuesBlock, 0);

            long hash = 0;
            for (int position = 0; position < valuesBlock.getPositionCount(); position++) {
                hash = 31 * hash + elementHash;
            }
            return hash;
        }

        if (array instanceof DictionaryBlock dictionaryBlock) {
            ValueBlock valuesBlock = dictionaryBlock.getDictionary();
            long hash = 0;
            for (int position = 0; position < dictionaryBlock.getPositionCount(); position++) {
                int index = dictionaryBlock.getId(position);
                long elementHash = valuesBlock.isNull(index) ? NULL_HASH_CODE : (long) hashOperator.invokeExact(valuesBlock, index);
                hash = 31 * hash + elementHash;
            }
            return hash;
        }

        throw new IllegalArgumentException("Unsupported block type: " + array.getClass().getName());
    }

    private static boolean identicalOperator(MethodHandle identicalOperator, Block leftArray, Block rightArray)
            throws Throwable
    {
        boolean leftIsNull = leftArray == null;
        boolean rightIsNull = rightArray == null;
        if (leftIsNull || rightIsNull) {
            return leftIsNull == rightIsNull;
        }

        if (leftArray.getPositionCount() != rightArray.getPositionCount()) {
            return false;
        }

        leftArray = leftArray.getLoadedBlock();
        rightArray = rightArray.getLoadedBlock();

        ValueBlock leftValues = leftArray.getUnderlyingValueBlock();
        ValueBlock rightValues = rightArray.getUnderlyingValueBlock();

        for (int position = 0; position < leftArray.getPositionCount(); position++) {
            int leftIndex = leftArray.getUnderlyingValuePosition(position);
            int rightIndex = rightArray.getUnderlyingValuePosition(position);

            boolean leftValueIsNull = leftValues.isNull(leftIndex);
            boolean rightValueIsNull = rightValues.isNull(rightIndex);
            if (leftValueIsNull != rightValueIsNull) {
                return false;
            }
            if (leftValueIsNull) {
                continue;
            }

            boolean result = (boolean) identicalOperator.invokeExact(leftValues, leftIndex, rightValues, rightIndex);
            if (!result) {
                return false;
            }
        }

        return true;
    }

    private static boolean indeterminateOperator(MethodHandle elementIndeterminateFunction, Block array, boolean isNull)
            throws Throwable
    {
        if (isNull) {
            return true;
        }

        array = array.getLoadedBlock();

        if (array instanceof ValueBlock valuesBlock) {
            for (int index = 0; index < valuesBlock.getPositionCount(); index++) {
                if (valuesBlock.isNull(index)) {
                    return true;
                }
                if ((boolean) elementIndeterminateFunction.invoke(valuesBlock, index)) {
                    return true;
                }
            }
            return false;
        }

        if (array instanceof RunLengthEncodedBlock rleBlock) {
            ValueBlock valuesBlock = rleBlock.getValue();
            if (valuesBlock.isNull(0)) {
                return true;
            }
            if ((boolean) elementIndeterminateFunction.invoke(valuesBlock, 0)) {
                return true;
            }
            return false;
        }

        if (array instanceof DictionaryBlock dictionaryBlock) {
            ValueBlock valuesBlock = dictionaryBlock.getDictionary();
            for (int position = 0; position < dictionaryBlock.getPositionCount(); position++) {
                int index = dictionaryBlock.getId(position);
                if (valuesBlock.isNull(index)) {
                    return true;
                }
                if ((boolean) elementIndeterminateFunction.invoke(valuesBlock, index)) {
                    return true;
                }
            }
            return false;
        }

        throw new IllegalArgumentException("Unsupported block type: " + array.getClass().getName());
    }

    private static long comparisonOperator(MethodHandle comparisonOperator, Block leftArray, Block rightArray)
            throws Throwable
    {
        leftArray = leftArray.getLoadedBlock();
        rightArray = rightArray.getLoadedBlock();

        ValueBlock leftValues = leftArray.getUnderlyingValueBlock();
        ValueBlock rightValues = rightArray.getUnderlyingValueBlock();

        int len = Math.min(leftArray.getPositionCount(), rightArray.getPositionCount());
        for (int position = 0; position < len; position++) {
            checkElementNotNull(leftArray.isNull(position), ARRAY_NULL_ELEMENT_MSG);
            checkElementNotNull(rightArray.isNull(position), ARRAY_NULL_ELEMENT_MSG);

            int leftIndex = leftArray.getUnderlyingValuePosition(position);
            int rightIndex = rightArray.getUnderlyingValuePosition(position);

            long result = (long) comparisonOperator.invokeExact(leftValues, leftIndex, rightValues, rightIndex);
            if (result != 0) {
                return result;
            }
        }

        return Integer.compare(leftArray.getPositionCount(), rightArray.getPositionCount());
    }
}
