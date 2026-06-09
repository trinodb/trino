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
import io.trino.spi.block.SqlMultiset;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorMethodHandle;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
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
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

/// The SQL `MULTISET` collection type: an unordered collection that retains duplicates.
///
/// Element storage is an [ArrayBlock] (the flat layout is identical to [ArrayType]), but
/// the native value is a [SqlMultiset] that pairs that storage with a lazily-built hash index of
/// distinct values to multiplicities. The collection kinds differ only in how values are compared:
/// equality, hashing, and the bag operators depend on element values and their multiplicities, never
/// on element order. A multiset is a distinct, non-assignable kind from an array and is intentionally
/// not a subtype of [ArrayType].
public class MultisetType
        extends AbstractType
{
    public static final String NAME = "multiset";
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private static final InvocationConvention READ_FLAT_CONVENTION = simpleConvention(FAIL_ON_NULL, FLAT);
    private static final InvocationConvention READ_FLAT_TO_BLOCK_CONVENTION = simpleConvention(BLOCK_BUILDER, FLAT);
    private static final InvocationConvention WRITE_FLAT_CONVENTION = simpleConvention(FLAT_RETURN, NEVER_NULL);
    private static final InvocationConvention EQUAL_CONVENTION = simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL);
    private static final InvocationConvention HASH_CODE_CONVENTION = simpleConvention(FAIL_ON_NULL, NEVER_NULL);
    private static final InvocationConvention IDENTICAL_CONVENTION = simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE, BOXED_NULLABLE);
    private static final InvocationConvention INDETERMINATE_CONVENTION = simpleConvention(FAIL_ON_NULL, NULL_FLAG);

    private static final MethodHandle READ_FLAT;
    private static final MethodHandle READ_FLAT_TO_BLOCK;
    private static final MethodHandle WRITE_FLAT;
    private static final MethodHandle EQUAL;
    private static final MethodHandle HASH_CODE;
    private static final MethodHandle IDENTICAL;
    private static final MethodHandle INDETERMINATE;

    static {
        try {
            Lookup lookup = MethodHandles.lookup();
            READ_FLAT = lookup.findStatic(MultisetType.class, "readFlat", MethodType.methodType(SqlMultiset.class, MultisetType.class, Type.class, MethodHandle.class, int.class, byte[].class, int.class, byte[].class, int.class));
            READ_FLAT_TO_BLOCK = lookup.findStatic(MultisetType.class, "readFlatToBlock", MethodType.methodType(void.class, Type.class, MethodHandle.class, int.class, byte[].class, int.class, byte[].class, int.class, BlockBuilder.class));
            WRITE_FLAT = lookup.findStatic(MultisetType.class, "writeFlat", MethodType.methodType(void.class, Type.class, MethodHandle.class, int.class, boolean.class, SqlMultiset.class, byte[].class, int.class, byte[].class, int.class));
            EQUAL = lookup.findStatic(MultisetType.class, "equalOperator", MethodType.methodType(Boolean.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, SqlMultiset.class, SqlMultiset.class));
            HASH_CODE = lookup.findStatic(MultisetType.class, "hashOperator", MethodType.methodType(long.class, MethodHandle.class, SqlMultiset.class));
            IDENTICAL = lookup.findStatic(MultisetType.class, "identicalOperator", MethodType.methodType(boolean.class, MethodHandle.class, MethodHandle.class, SqlMultiset.class, SqlMultiset.class));
            INDETERMINATE = lookup.findStatic(MultisetType.class, "indeterminateOperator", MethodType.methodType(boolean.class, MethodHandle.class, SqlMultiset.class, boolean.class));
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private final Type elementType;
    // element operators used to build a SqlMultiset's lazy index: HASH_CODE and IDENTICAL key the
    // index, and INDETERMINATE lets membership take an O(1) path when no element is indeterminate
    // (all FAIL_ON_NULL, BLOCK_POSITION(_NOT_NULL))
    private final MethodHandle elementBlockHashCode;
    private final MethodHandle elementBlockIdentical;
    private final MethodHandle elementBlockIndeterminate;

    // this field is used in double-checked locking
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private volatile TypeOperatorDeclaration operatorDeclaration;

    public MultisetType(Type elementType, TypeOperators typeOperators)
    {
        super(new TypeSignature(NAME, TypeParameter.typeParameter(elementType.getTypeSignature())), SqlMultiset.class, ArrayBlock.class);
        requireNonNull(elementType, "elementType is null");
        // a multiset's whole identity is bag semantics over element equality, so a non-comparable
        // element type has no usable value (mirrors the map key requirement)
        if (!elementType.isComparable()) {
            throw new IllegalArgumentException(format("element type must be comparable, got %s", elementType));
        }
        this.elementType = elementType;
        this.elementBlockHashCode = typeOperators.getHashCodeOperator(elementType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));
        this.elementBlockIdentical = typeOperators.getIdenticalOperator(elementType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));
        this.elementBlockIndeterminate = typeOperators.getIndeterminateOperator(elementType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));
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
                .addReadValueOperators(getReadValueOperatorMethodHandles(typeOperators))
                .addEqualOperators(getEqualOperatorMethodHandles(typeOperators, elementType))
                .addHashCodeOperators(getHashCodeOperatorMethodHandles(typeOperators, elementType))
                .addXxHash64Operators(getXxHash64OperatorMethodHandles(typeOperators, elementType))
                .addIdenticalOperators(getIdenticalOperatorInvokers(typeOperators, elementType))
                .addIndeterminateOperators(getIndeterminateOperatorInvokers(typeOperators, elementType))
                .build();
    }

    private List<OperatorMethodHandle> getReadValueOperatorMethodHandles(TypeOperators typeOperators)
    {
        MethodHandle elementReadOperator = typeOperators.getReadValueOperator(elementType, simpleConvention(BLOCK_BUILDER, FLAT));
        MethodHandle readFlat = insertArguments(READ_FLAT, 0, this, elementType, elementReadOperator, elementType.getFlatFixedSize());
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
        MethodHandle equalOperator = typeOperators.getEqualOperator(elementType, simpleConvention(NULLABLE_RETURN, VALUE_BLOCK_POSITION_NOT_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        MethodHandle hashCodeOperator = typeOperators.getHashCodeOperator(elementType, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        MethodHandle indeterminateOperator = typeOperators.getIndeterminateOperator(elementType, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        return singletonList(new OperatorMethodHandle(EQUAL_CONVENTION, insertArguments(EQUAL, 0, equalOperator, hashCodeOperator, indeterminateOperator)));
    }

    private static List<OperatorMethodHandle> getHashCodeOperatorMethodHandles(TypeOperators typeOperators, Type elementType)
    {
        MethodHandle elementHashCodeOperator = typeOperators.getHashCodeOperator(elementType, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        return singletonList(new OperatorMethodHandle(HASH_CODE_CONVENTION, HASH_CODE.bindTo(elementHashCodeOperator)));
    }

    private static List<OperatorMethodHandle> getXxHash64OperatorMethodHandles(TypeOperators typeOperators, Type elementType)
    {
        MethodHandle elementHashCodeOperator = typeOperators.getXxHash64Operator(elementType, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        return singletonList(new OperatorMethodHandle(HASH_CODE_CONVENTION, HASH_CODE.bindTo(elementHashCodeOperator)));
    }

    private static List<OperatorMethodHandle> getIdenticalOperatorInvokers(TypeOperators typeOperators, Type elementType)
    {
        MethodHandle elementIdenticalOperator = typeOperators.getIdenticalOperator(elementType, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        MethodHandle elementHashCodeOperator = typeOperators.getHashCodeOperator(elementType, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        return singletonList(new OperatorMethodHandle(IDENTICAL_CONVENTION, insertArguments(IDENTICAL, 0, elementIdenticalOperator, elementHashCodeOperator)));
    }

    private static List<OperatorMethodHandle> getIndeterminateOperatorInvokers(TypeOperators typeOperators, Type elementType)
    {
        MethodHandle elementIndeterminateOperator = typeOperators.getIndeterminateOperator(elementType, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        return singletonList(new OperatorMethodHandle(INDETERMINATE_CONVENTION, INDETERMINATE.bindTo(elementIndeterminateOperator)));
    }

    public Type getElementType()
    {
        return elementType;
    }

    /// Wraps an element block as a multiset value (the type's native [SqlMultiset]). The elements
    /// are taken in order with their multiplicities; order carries no meaning for a multiset.
    public SqlMultiset toSqlMultiset(Block elements)
    {
        return new SqlMultiset(this, elements, 0, elements.getPositionCount(), elementBlockHashCode, elementBlockIdentical, elementBlockIndeterminate);
    }

    /// Element `HASH_CODE` operator, convention `(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)`,
    /// used to build a [SqlMultiset]'s index. Null when the element type is not comparable.
    public MethodHandle getElementBlockHashCode()
    {
        return elementBlockHashCode;
    }

    /// Element `IDENTICAL` operator, convention `(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)`,
    /// used to build a [SqlMultiset]'s index. Null when the element type is not comparable.
    public MethodHandle getElementBlockIdentical()
    {
        return elementBlockIdentical;
    }

    /// Element `INDETERMINATE` operator, convention `(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)`,
    /// used to build a [SqlMultiset]'s index. Null when the element type is not comparable.
    public MethodHandle getElementBlockIndeterminate()
    {
        return elementBlockIndeterminate;
    }

    @Override
    public boolean isComparable()
    {
        // the constructor rejects non-comparable element types
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        // SQL:2023 makes multisets equality-comparable only: they may not appear in ordering
        // operations (ORDER BY, <, BETWEEN, MAX/MIN). Equality, hashing and IDENTICAL remain, so
        // GROUP BY, DISTINCT, =, IN and the bag set-operators still work.
        return false;
    }

    @Override
    public Object getObjectValue(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        if (block instanceof ArrayBlock arrayBlock) {
            return arrayBlock.apply((valuesBlock, start, length) -> multisetBlockToObjectValues(valuesBlock, start, length), position);
        }
        SqlMultiset multiset = getObject(block, position);
        return multisetBlockToObjectValues(multiset.getRawElementBlock(), multiset.getRawOffset(), multiset.getSize());
    }

    private List<Object> multisetBlockToObjectValues(Block block, int start, int length)
    {
        List<Object> values = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            values.add(elementType.getObjectValue(block, i + start));
        }

        return Collections.unmodifiableList(values);
    }

    @Override
    public SqlMultiset getObject(Block block, int position)
    {
        Block elements = ((ArrayBlock) block.getUnderlyingValueBlock()).getArray(block.getUnderlyingValuePosition(position));
        return new SqlMultiset(this, elements, 0, elements.getPositionCount(), elementBlockHashCode, elementBlockIdentical, elementBlockIndeterminate);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        SqlMultiset multiset = (SqlMultiset) value;
        Block elements = multiset.getRawElementBlock();
        ((ArrayBlockBuilder) blockBuilder).buildEntry(elementBuilder ->
                elementBuilder.appendBlockRange(elements, multiset.getRawOffset(), multiset.getSize()));
    }

    // FLAT MEMORY LAYOUT
    //
    // The flat layout is identical to the array layout: all data is stored in the variable width
    // section, with fixed data for all elements first, followed by variable length data for all
    // elements. Element order in the flat record is the storage order of the backing array block
    // and carries no semantic meaning for a multiset.
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
        SqlMultiset multiset = getObject(block, position);
        int length = multiset.getSize();
        Block elements = multiset.getRawElementBlock();
        int offset = multiset.getRawOffset();

        int flatFixedSize = elementType.getFlatFixedSize();
        boolean variableWidth = elementType.isFlatVariableWidth();

        // one byte for null flag
        long size = length * (flatFixedSize + 1L);
        if (variableWidth) {
            for (int index = 0; index < length; index++) {
                if (!elements.isNull(offset + index)) {
                    size += elementType.getFlatVariableWidthSize(elements, offset + index);
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
        return NAME + "(" + elementType.getDisplayName() + ")";
    }

    @Override
    public int getFlatVariableWidthLength(byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        return (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset + Integer.BYTES);
    }

    private static SqlMultiset readFlat(
            MultisetType multisetType,
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
        Block elements = elementBuilder.build();
        return new SqlMultiset(multisetType, elements, 0, elements.getPositionCount(), multisetType.elementBlockHashCode, multisetType.elementBlockIdentical, multisetType.elementBlockIndeterminate);
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
            SqlMultiset multiset,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableSizeSlice,
            int variableSizeOffset)
            throws Throwable
    {
        int positionCount = multiset.getSize();
        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset, positionCount);

        ValueBlock elements = multiset.getUnderlyingElementBlock();
        // variable width data starts after fixed width data; one extra byte per position for the null flag
        int writeVariableWidthOffset = variableSizeOffset + positionCount * (1 + elementFixedSize);
        int offset = variableSizeOffset;
        for (int i = 0; i < positionCount; i++) {
            int index = multiset.getUnderlyingElementPosition(i);
            writeVariableWidthOffset = writeFlatElement(elementType, elementWriteFlat, elementVariableWidth, elements, index, variableSizeSlice, offset, writeVariableWidthOffset);
            offset += 1 + elementFixedSize;
        }
        int variableLength = writeVariableWidthOffset - variableSizeOffset;
        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset + Integer.BYTES, variableLength);
    }

    private static int writeFlatElement(Type elementType, MethodHandle elementWriteFlat, boolean elementVariableWidth, ValueBlock elements, int index, byte[] slice, int offset, int writeVariableWidthOffset)
            throws Throwable
    {
        if (elements.isNull(index)) {
            slice[offset] = 1;
        }
        else {
            elementWriteFlat.invokeExact(
                    elements,
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

    private static Boolean equalOperator(MethodHandle equalOperator, MethodHandle hashOperator, MethodHandle indeterminateOperator, SqlMultiset leftMultiset, SqlMultiset rightMultiset)
            throws Throwable
    {
        int count = leftMultiset.getSize();
        if (count != rightMultiset.getSize()) {
            return false;
        }

        ValueBlock leftValues = leftMultiset.getUnderlyingElementBlock();
        ValueBlock rightValues = rightMultiset.getUnderlyingElementBlock();

        // Bag equality per ISO/IEC 9075-2:2023 section 8.2 General Rule 2)b)iii: the result is True
        // if some complete pairing of the elements has every pair equal, Unknown if (failing that)
        // some complete pairing has every pair True or Unknown, and False otherwise. The Unknown rule
        // requires the *existence of a complete enumeration* — it is not enough that each element
        // individually has some unknown pairing (MULTISET[1, 2] = MULTISET[3, NULL] is False: only
        // one element can claim the null, and the other must pair with 3, which is definite False).
        //
        // Note the deliberate split from the multiset bag operators (SUBMULTISET, IS A SET, the
        // MULTISET UNION/INTERSECT/EXCEPT family, SET): those pair elements with IDENTICAL, under
        // which null is not distinct from null, so they are total and give definite results on null
        // elements. Value equality here is three-valued instead, mirroring ARRAY[null] = ARRAY[null]
        // being unknown. Do not "unify" the two paths.
        ElementKinds leftKinds = scanElements(indeterminateOperator, leftMultiset);
        ElementKinds rightKinds = scanElements(indeterminateOperator, rightMultiset);

        // No nulls and no indeterminate elements: equality is total (never unknown), so the result is
        // True or False outright. Equal-true is an equivalence, and the element hash is consistent
        // with it, so bucketing on the hash resolves the pairing in O(n) rather than O(n * m).
        if (!leftKinds.hasNull() && !leftKinds.hasIndeterminate() && !rightKinds.hasNull() && !rightKinds.hasIndeterminate()) {
            return bagMatch(
                    (values1, index1, values2, index2) -> {
                        Boolean equal = (Boolean) equalOperator.invokeExact(values1, index1, values2, index2);
                        return equal != null && equal;
                    },
                    hashOperator,
                    leftMultiset,
                    leftValues,
                    rightMultiset,
                    rightValues);
        }

        // Some element is null or indeterminate. Such an element can never be in a True pair, and the
        // equal cardinalities force every element into any complete enumeration, so the result cannot
        // be True: it is Unknown if a complete True-or-Unknown pairing exists, and False otherwise.
        if (!leftKinds.hasIndeterminate() && !rightKinds.hasIndeterminate()) {
            // Only null elements: a null pairs with anything as Unknown, and a determinate pair is
            // True or False outright, so a complete True-or-Unknown pairing exists exactly when, after
            // pairing equal determinate elements class by class, each side's leftover determinate
            // elements fit within the other side's null count.
            return nullsAbsorbUnmatched(equalOperator, hashOperator, leftMultiset, leftValues, rightMultiset, rightValues) ? null : false;
        }

        // An indeterminate element pairs as Unknown with some values and as definite False with
        // others (ROW(1, NULL) against ROW(1, 2) is unknown but against ROW(2, 2) is false), so
        // pairability is not transitive and no per-class arithmetic can decide whether a complete
        // pairing exists: test for a perfect matching in the bipartite pairability graph.
        return pairableMatchingExists(equalOperator, leftMultiset, leftValues, rightMultiset, rightValues) ? null : false;
    }

    private record ElementKinds(boolean hasNull, boolean hasIndeterminate) {}

    /// Scans the elements once, reporting whether any is null and whether any is indeterminate
    /// (a non-null element containing a nested null, so an element `=` comparison can be unknown).
    private static ElementKinds scanElements(MethodHandle elementIndeterminateFunction, SqlMultiset multiset)
            throws Throwable
    {
        ValueBlock values = multiset.getUnderlyingElementBlock();
        boolean hasNull = false;
        boolean hasIndeterminate = false;
        for (int position = 0; position < multiset.getSize(); position++) {
            int index = multiset.getUnderlyingElementPosition(position);
            if (values.isNull(index)) {
                hasNull = true;
            }
            else if (!hasIndeterminate && (boolean) elementIndeterminateFunction.invoke(values, index)) {
                hasIndeterminate = true;
            }
        }
        return new ElementKinds(hasNull, hasIndeterminate);
    }

    /// Decides whether a complete True-or-Unknown pairing exists when the only source of unknown
    /// element comparisons is null elements. Equal determinate elements are paired class by class
    /// (equal-true is an equivalence and the element hash is consistent with it, so greedy hashed
    /// matching maximizes the determinate pairs); the pairing then completes exactly when each side's
    /// unmatched determinate elements fit within the other side's null count (the remaining nulls
    /// pair with each other — the cardinalities are equal, so those counts balance).
    private static boolean nullsAbsorbUnmatched(MethodHandle equalOperator, MethodHandle hashOperator, SqlMultiset leftMultiset, ValueBlock leftValues, SqlMultiset rightMultiset, ValueBlock rightValues)
            throws Throwable
    {
        int count = leftMultiset.getSize();
        int rightNulls = 0;
        Map<Long, List<Integer>> rightByHash = new HashMap<>();
        for (int position = 0; position < count; position++) {
            int index = rightMultiset.getUnderlyingElementPosition(position);
            if (rightValues.isNull(index)) {
                rightNulls++;
                continue;
            }
            long hash = (long) hashOperator.invokeExact(rightValues, index);
            rightByHash.computeIfAbsent(hash, _ -> new ArrayList<>()).add(index);
        }

        int leftNulls = 0;
        int unmatchedLeft = 0;
        int unmatchedRight = count - rightNulls;
        for (int position = 0; position < count; position++) {
            int leftIndex = leftMultiset.getUnderlyingElementPosition(position);
            if (leftValues.isNull(leftIndex)) {
                leftNulls++;
                continue;
            }
            long hash = (long) hashOperator.invokeExact(leftValues, leftIndex);
            boolean paired = false;
            List<Integer> candidates = rightByHash.get(hash);
            if (candidates != null) {
                for (int candidate = 0; candidate < candidates.size(); candidate++) {
                    Boolean equal = (Boolean) equalOperator.invokeExact(leftValues, leftIndex, rightValues, (int) candidates.get(candidate));
                    if (equal != null && equal) {
                        // consume the matched right element by swapping it with the tail and removing the tail
                        candidates.set(candidate, candidates.get(candidates.size() - 1));
                        candidates.remove(candidates.size() - 1);
                        paired = true;
                        break;
                    }
                }
            }
            if (paired) {
                unmatchedRight--;
            }
            else {
                unmatchedLeft++;
            }
        }
        return unmatchedLeft <= rightNulls && unmatchedRight <= leftNulls;
    }

    /// Decides whether a complete True-or-Unknown pairing exists in the presence of indeterminate
    /// elements by testing for a perfect matching in the bipartite graph whose edges are the pairs
    /// that are not definitely unequal (a null on either side pairs with anything as Unknown).
    /// Kuhn's augmenting-path algorithm over a precomputed adjacency, with the recursion unrolled
    /// onto explicit stacks. This corner is only reachable when an element contains a nested null,
    /// and costs O(n * m) element comparisons plus the O(V * E) matching.
    private static boolean pairableMatchingExists(MethodHandle equalOperator, SqlMultiset leftMultiset, ValueBlock leftValues, SqlMultiset rightMultiset, ValueBlock rightValues)
            throws Throwable
    {
        int count = leftMultiset.getSize();
        BitSet[] pairableRights = new BitSet[count];
        for (int left = 0; left < count; left++) {
            BitSet rights = new BitSet(count);
            int leftIndex = leftMultiset.getUnderlyingElementPosition(left);
            boolean leftNull = leftValues.isNull(leftIndex);
            for (int right = 0; right < count; right++) {
                int rightIndex = rightMultiset.getUnderlyingElementPosition(right);
                if (leftNull || rightValues.isNull(rightIndex)) {
                    rights.set(right);
                    continue;
                }
                Boolean equal = (Boolean) equalOperator.invokeExact(leftValues, leftIndex, rightValues, rightIndex);
                if (equal == null || equal) {
                    rights.set(right);
                }
            }
            if (rights.isEmpty()) {
                return false;
            }
            pairableRights[left] = rights;
        }

        int[] matchedLeftByRight = new int[count];
        Arrays.fill(matchedLeftByRight, -1);
        int[] pathLeft = new int[count + 1];
        int[] pathRight = new int[count + 1];
        int[] pathCursor = new int[count + 1];
        for (int start = 0; start < count; start++) {
            // grow the matching with an augmenting path from this left element: walk unvisited
            // pairable rights, descending through each right's current match; reaching a free right
            // flips every (left, right) edge along the path into the matching
            BitSet visited = new BitSet(count);
            boolean augmented = false;
            int top = 0;
            pathLeft[0] = start;
            pathCursor[0] = 0;
            while (top >= 0) {
                int left = pathLeft[top];
                int right = -1;
                for (int candidate = pairableRights[left].nextSetBit(pathCursor[top]); candidate >= 0; candidate = pairableRights[left].nextSetBit(candidate + 1)) {
                    if (!visited.get(candidate)) {
                        right = candidate;
                        break;
                    }
                }
                if (right == -1) {
                    top--;
                    continue;
                }
                pathCursor[top] = right + 1;
                pathRight[top] = right;
                visited.set(right);
                int occupant = matchedLeftByRight[right];
                if (occupant == -1) {
                    for (int i = top; i >= 0; i--) {
                        matchedLeftByRight[pathRight[i]] = pathLeft[i];
                    }
                    augmented = true;
                    break;
                }
                top++;
                pathLeft[top] = occupant;
                pathCursor[top] = 0;
            }
            if (!augmented) {
                return false;
            }
        }
        return true;
    }

    private static long hashOperator(MethodHandle hashOperator, SqlMultiset multiset)
            throws Throwable
    {
        ValueBlock values = multiset.getUnderlyingElementBlock();
        // Sum is commutative, so the hash is independent of element order while still reflecting
        // element multiplicities (unlike XOR, which would cancel out duplicate elements).
        long hash = 0;
        for (int position = 0; position < multiset.getSize(); position++) {
            int index = multiset.getUnderlyingElementPosition(position);
            long elementHash = values.isNull(index) ? NULL_HASH_CODE : (long) hashOperator.invokeExact(values, index);
            hash += elementHash;
        }
        return hash;
    }

    private static boolean identicalOperator(MethodHandle identicalOperator, MethodHandle hashOperator, SqlMultiset leftMultiset, SqlMultiset rightMultiset)
            throws Throwable
    {
        boolean leftIsNull = leftMultiset == null;
        boolean rightIsNull = rightMultiset == null;
        if (leftIsNull || rightIsNull) {
            return leftIsNull == rightIsNull;
        }

        int count = leftMultiset.getSize();
        if (count != rightMultiset.getSize()) {
            return false;
        }

        ValueBlock leftValues = leftMultiset.getUnderlyingElementBlock();
        ValueBlock rightValues = rightMultiset.getUnderlyingElementBlock();

        // Bag identity: pair each left element with a distinct identical right element. Identity is a
        // total equivalence (a null element is identical only to another null element) and the element
        // hash is consistent with it (null hashes to NULL_HASH_CODE), so bucketing on the hash resolves
        // the pairing in O(n) rather than O(n * m).
        return bagMatch(
                (values1, index1, values2, index2) -> {
                    boolean valueIsNull1 = values1.isNull(index1);
                    boolean valueIsNull2 = values2.isNull(index2);
                    if (valueIsNull1 || valueIsNull2) {
                        return valueIsNull1 && valueIsNull2;
                    }
                    return (boolean) identicalOperator.invokeExact(values1, index1, values2, index2);
                },
                hashOperator,
                leftMultiset,
                leftValues,
                rightMultiset,
                rightValues);
    }

    /// Pairs each left element with a distinct right element under a total equivalence `matcher`,
    /// returning whether a perfect matching exists. Right positions are bucketed by their element hash
    /// (null hashing to `NULL_HASH_CODE`); because the hash is consistent with the matcher, only
    /// the same-hash bucket has to be scanned, so the matching is O(n) rather than O(n * m). The two
    /// multisets must already have equal cardinality.
    private static boolean bagMatch(PositionMatcher matcher, MethodHandle hashOperator, SqlMultiset leftMultiset, ValueBlock leftValues, SqlMultiset rightMultiset, ValueBlock rightValues)
            throws Throwable
    {
        int count = leftMultiset.getSize();
        Map<Long, List<Integer>> rightByHash = new HashMap<>();
        for (int position = 0; position < count; position++) {
            int index = rightMultiset.getUnderlyingElementPosition(position);
            long hash = rightValues.isNull(index) ? NULL_HASH_CODE : (long) hashOperator.invokeExact(rightValues, index);
            rightByHash.computeIfAbsent(hash, _ -> new ArrayList<>()).add(index);
        }

        for (int position = 0; position < count; position++) {
            int leftIndex = leftMultiset.getUnderlyingElementPosition(position);
            long hash = leftValues.isNull(leftIndex) ? NULL_HASH_CODE : (long) hashOperator.invokeExact(leftValues, leftIndex);
            List<Integer> candidates = rightByHash.get(hash);
            if (candidates == null) {
                return false;
            }
            boolean paired = false;
            for (int candidate = 0; candidate < candidates.size(); candidate++) {
                if (matcher.matches(leftValues, leftIndex, rightValues, candidates.get(candidate))) {
                    // consume the matched right element by swapping it with the tail and removing the tail
                    candidates.set(candidate, candidates.get(candidates.size() - 1));
                    candidates.remove(candidates.size() - 1);
                    paired = true;
                    break;
                }
            }
            if (!paired) {
                return false;
            }
        }
        return true;
    }

    @FunctionalInterface
    private interface PositionMatcher
    {
        boolean matches(ValueBlock leftValues, int leftIndex, ValueBlock rightValues, int rightIndex)
                throws Throwable;
    }

    private static boolean hasIndeterminateElement(MethodHandle elementIndeterminateFunction, SqlMultiset multiset)
            throws Throwable
    {
        ValueBlock values = multiset.getUnderlyingElementBlock();
        for (int position = 0; position < multiset.getSize(); position++) {
            int index = multiset.getUnderlyingElementPosition(position);
            if (values.isNull(index)) {
                return true;
            }
            if ((boolean) elementIndeterminateFunction.invoke(values, index)) {
                return true;
            }
        }
        return false;
    }

    private static boolean indeterminateOperator(MethodHandle elementIndeterminateFunction, SqlMultiset multiset, boolean isNull)
            throws Throwable
    {
        if (isNull) {
            return true;
        }
        return hasIndeterminateElement(elementIndeterminateFunction, multiset);
    }
}
