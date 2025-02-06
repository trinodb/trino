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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsIdentical;
import io.trino.type.BlockTypeOperators.BlockPositionXxHash64;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.block.BlockSerdeUtil.writeBlock;
import static io.trino.operator.OperatorAssertion.toRow;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.DEFAULT_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.util.StructuralTestUtil.arrayBlockOf;
import static io.trino.util.StructuralTestUtil.sqlMapOf;
import static java.util.Collections.unmodifiableSortedMap;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class AbstractTestType
{
    private final BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

    private final Class<?> objectValueType;
    private final ValueBlock testBlock;
    protected final Type type;

    private final TypeOperators typeOperators;
    private final MethodHandle readBlockMethod;
    private final MethodHandle writeBlockMethod;
    private final MethodHandle writeFlatToBlockMethod;
    private final MethodHandle readFlatMethod;
    private final MethodHandle writeFlatMethod;
    private final MethodHandle writeBlockToFlatMethod;
    private final MethodHandle stackStackEqualOperator;
    private final MethodHandle flatFlatEqualOperator;
    private final MethodHandle flatBlockPositionEqualOperator;
    private final MethodHandle blockPositionFlatEqualOperator;
    private final MethodHandle flatHashCodeOperator;
    private final MethodHandle flatXxHash64Operator;
    private final MethodHandle flatFlatIdenticalOperator;
    private final MethodHandle flatBlockPositionIdenticalOperator;
    private final MethodHandle blockPositionFlatIdenticalOperator;

    protected final BlockTypeOperators blockTypeOperators;
    private final BlockPositionEqual equalOperator;
    private final BlockPositionHashCode hashCodeOperator;
    private final BlockPositionXxHash64 xxHash64Operator;
    private final BlockPositionIsIdentical identicalOperator;
    private final SortedMap<Integer, Object> expectedStackValues;
    private final SortedMap<Integer, Object> expectedObjectValues;
    private final ValueBlock testBlockWithNulls;

    protected AbstractTestType(Type type, Class<?> objectValueType, ValueBlock testBlock)
    {
        this(type, objectValueType, testBlock, testBlock);
    }

    protected AbstractTestType(Type type, Class<?> objectValueType, ValueBlock testBlock, ValueBlock expectedValues)
    {
        this.type = requireNonNull(type, "type is null");
        typeOperators = new TypeOperators();
        readBlockMethod = typeOperators.getReadValueOperator(type, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION_NOT_NULL));
        writeBlockMethod = typeOperators.getReadValueOperator(type, simpleConvention(BLOCK_BUILDER, NEVER_NULL));
        writeFlatToBlockMethod = typeOperators.getReadValueOperator(type, simpleConvention(BLOCK_BUILDER, FLAT));
        readFlatMethod = typeOperators.getReadValueOperator(type, simpleConvention(FAIL_ON_NULL, FLAT));
        writeFlatMethod = typeOperators.getReadValueOperator(type, simpleConvention(FLAT_RETURN, NEVER_NULL));
        writeBlockToFlatMethod = typeOperators.getReadValueOperator(type, simpleConvention(FLAT_RETURN, VALUE_BLOCK_POSITION));

        blockTypeOperators = new BlockTypeOperators(typeOperators);
        if (type.isComparable()) {
            stackStackEqualOperator = typeOperators.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));
            flatFlatEqualOperator = typeOperators.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, FLAT, FLAT));
            flatBlockPositionEqualOperator = typeOperators.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, FLAT, VALUE_BLOCK_POSITION));
            blockPositionFlatEqualOperator = typeOperators.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, VALUE_BLOCK_POSITION, FLAT));
            flatHashCodeOperator = typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, FLAT));
            flatXxHash64Operator = typeOperators.getXxHash64Operator(type, simpleConvention(FAIL_ON_NULL, FLAT));
            flatFlatIdenticalOperator = typeOperators.getIdenticalOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, FLAT));
            flatBlockPositionIdenticalOperator = typeOperators.getIdenticalOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, VALUE_BLOCK_POSITION));
            blockPositionFlatIdenticalOperator = typeOperators.getIdenticalOperator(type, simpleConvention(FAIL_ON_NULL, VALUE_BLOCK_POSITION, FLAT));

            equalOperator = blockTypeOperators.getEqualOperator(type);
            hashCodeOperator = blockTypeOperators.getHashCodeOperator(type);
            xxHash64Operator = blockTypeOperators.getXxHash64Operator(type);
            identicalOperator = blockTypeOperators.getIdenticalOperator(type);
        }
        else {
            stackStackEqualOperator = null;
            flatFlatEqualOperator = null;
            flatBlockPositionEqualOperator = null;
            blockPositionFlatEqualOperator = null;
            flatHashCodeOperator = null;
            flatXxHash64Operator = null;
            flatFlatIdenticalOperator = null;
            flatBlockPositionIdenticalOperator = null;
            blockPositionFlatIdenticalOperator = null;

            equalOperator = null;
            hashCodeOperator = null;
            xxHash64Operator = null;
            identicalOperator = null;
        }
        this.objectValueType = requireNonNull(objectValueType, "objectValueType is null");
        this.testBlock = requireNonNull(testBlock, "testBlock is null");

        requireNonNull(expectedValues, "expectedValues is null");
        this.expectedStackValues = indexStackValues(type, expectedValues);
        this.expectedObjectValues = indexObjectValues(type, expectedValues);
        this.testBlockWithNulls = createAlternatingNullsBlock(testBlock);
    }

    private ValueBlock createAlternatingNullsBlock(Block testBlock)
    {
        BlockBuilder nullsBlockBuilder = type.createBlockBuilder(null, testBlock.getPositionCount());
        for (int position = 0; position < testBlock.getPositionCount(); position++) {
            if (testBlock.isNull(position)) {
                checkState(type instanceof UnknownType);
                nullsBlockBuilder.appendNull();
            }
            else if (type.getJavaType() == boolean.class) {
                type.writeBoolean(nullsBlockBuilder, type.getBoolean(testBlock, position));
            }
            else if (type.getJavaType() == long.class) {
                type.writeLong(nullsBlockBuilder, type.getLong(testBlock, position));
            }
            else if (type.getJavaType() == double.class) {
                type.writeDouble(nullsBlockBuilder, type.getDouble(testBlock, position));
            }
            else if (type.getJavaType() == Slice.class) {
                Slice slice = type.getSlice(testBlock, position);
                type.writeSlice(nullsBlockBuilder, slice, 0, slice.length());
            }
            else {
                type.writeObject(nullsBlockBuilder, type.getObject(testBlock, position));
            }
            nullsBlockBuilder.appendNull();
        }
        return nullsBlockBuilder.buildValueBlock();
    }

    @Test
    public void testBlock()
            throws Throwable
    {
        for (Entry<Integer, Object> entry : expectedStackValues.entrySet()) {
            assertPositionEquals(testBlock, entry.getKey(), entry.getValue(), expectedObjectValues.get(entry.getKey()));
        }
        for (Entry<Integer, Object> entry : expectedStackValues.entrySet()) {
            assertPositionEquals(testBlockWithNulls, entry.getKey() * 2, entry.getValue(), expectedObjectValues.get(entry.getKey()));
            assertPositionEquals(testBlockWithNulls, (entry.getKey() * 2) + 1, null, null);
        }
    }

    @Test
    public void testFlat()
            throws Throwable
    {
        int flatFixedSize = type.getFlatFixedSize();
        int[] variableLengths = new int[expectedStackValues.size()];
        if (type.isFlatVariableWidth()) {
            for (int i = 0; i < variableLengths.length; i++) {
                variableLengths[i] = type.getFlatVariableWidthSize(testBlock, i);
            }
        }

        byte[] fixed = new byte[expectedStackValues.size() * flatFixedSize];
        byte[] variable = new byte[IntStream.of(variableLengths).sum()];
        int variableOffset = 0;
        for (int i = 0; i < expectedStackValues.size(); i++) {
            writeFlatMethod.invoke(expectedStackValues.get(i), fixed, i * flatFixedSize, variable, variableOffset);
            variableOffset += variableLengths[i];
        }
        assertFlat(fixed, 0, variable);

        Arrays.fill(fixed, (byte) 0);
        Arrays.fill(variable, (byte) 0);
        variableOffset = 0;
        for (int i = 0; i < expectedStackValues.size(); i++) {
            writeBlockToFlatMethod.invokeExact(testBlock, i, fixed, i * flatFixedSize, variable, variableOffset);
            variableOffset += variableLengths[i];
        }
        assertFlat(fixed, 0, variable);

        // test relocation
        byte[] newFixed = new byte[fixed.length + 73];
        System.arraycopy(fixed, 0, newFixed, 73, fixed.length);
        byte[] newVariable = new byte[variable.length + 101];
        System.arraycopy(variable, 0, newVariable, 101, variable.length);
        Arrays.fill(fixed, (byte) 0);
        Arrays.fill(variable, (byte) 0);

        variableOffset = 101;
        for (int i = 0; i < expectedStackValues.size(); i++) {
            int variableSize = type.relocateFlatVariableWidthOffsets(newFixed, 73 + i * flatFixedSize, newVariable, variableOffset);
            variableOffset += variableSize;
            assertThat(variableSize).isEqualTo(variableLengths[i]);
        }
        assertFlat(newFixed, 73, newVariable);
    }

    private void assertFlat(byte[] fixed, int fixedOffset, byte[] variable)
            throws Throwable
    {
        int flatFixedSize = type.getFlatFixedSize();
        for (int i = 0; i < expectedStackValues.size(); i++) {
            Object expectedStackValue = expectedStackValues.get(i);
            int elementFixedOffset = fixedOffset + (i * flatFixedSize);
            if (type.getJavaType() == boolean.class) {
                assertThat((boolean) readFlatMethod.invokeExact(fixed, elementFixedOffset, variable)).isEqualTo(expectedStackValue);
            }
            else if (type.getJavaType() == long.class) {
                assertThat((long) readFlatMethod.invokeExact(fixed, elementFixedOffset, variable)).isEqualTo(expectedStackValue);
            }
            else if (type.getJavaType() == double.class) {
                assertThat((double) readFlatMethod.invokeExact(fixed, elementFixedOffset, variable)).isEqualTo(expectedStackValue);
            }
            else if (type.getJavaType() == Slice.class) {
                assertThat((Slice) readFlatMethod.invokeExact(fixed, elementFixedOffset, variable)).isEqualTo(expectedStackValue);
            }
            else if (type.getJavaType() == Block.class) {
                assertBlockEquals((Block) readFlatMethod.invokeExact(fixed, elementFixedOffset, variable), (Block) expectedStackValue);
            }
            else if (stackStackEqualOperator != null) {
                assertThat((Boolean) stackStackEqualOperator.invoke(readFlatMethod.invoke(fixed, elementFixedOffset, variable), expectedStackValue)).isTrue();
            }
            else {
                assertThat(readFlatMethod.invoke(fixed, elementFixedOffset, variable)).isEqualTo(expectedStackValue);
            }

            BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
            writeFlatToBlockMethod.invokeExact(fixed, elementFixedOffset, variable, blockBuilder);
            assertPositionEquals(testBlock, i, expectedStackValue, expectedObjectValues.get(i));

            if (type.isComparable()) {
                assertThat((Boolean) flatFlatEqualOperator.invokeExact(fixed, elementFixedOffset, variable, fixed, elementFixedOffset, variable)).isTrue();
                assertThat((Boolean) flatBlockPositionEqualOperator.invokeExact(fixed, elementFixedOffset, variable, testBlock, i)).isTrue();
                assertThat((Boolean) blockPositionFlatEqualOperator.invokeExact(testBlock, i, fixed, elementFixedOffset, variable)).isTrue();

                assertThat((long) flatHashCodeOperator.invokeExact(fixed, elementFixedOffset, variable)).isEqualTo(hashCodeOperator.hashCodeNullSafe(testBlock, i));

                assertThat((long) flatXxHash64Operator.invokeExact(fixed, elementFixedOffset, variable)).isEqualTo(xxHash64Operator.xxHash64(testBlock, i));

                assertThat((boolean) flatFlatIdenticalOperator.invokeExact(fixed, elementFixedOffset, variable, fixed, elementFixedOffset, variable)).isTrue();
                assertThat((boolean) flatBlockPositionIdenticalOperator.invokeExact(fixed, elementFixedOffset, variable, testBlock, i)).isTrue();
                assertThat((boolean) blockPositionFlatIdenticalOperator.invokeExact(testBlock, i, fixed, elementFixedOffset, variable)).isTrue();

                ValueBlock nullValue = type.createBlockBuilder(null, 1).appendNull().buildValueBlock();
                assertThat((boolean) flatBlockPositionIdenticalOperator.invokeExact(fixed, elementFixedOffset, variable, nullValue, 0)).isFalse();
                assertThat((boolean) blockPositionFlatIdenticalOperator.invokeExact(nullValue, 0, fixed, elementFixedOffset, variable)).isFalse();
            }
        }
    }

    protected Object getSampleValue()
    {
        return requireNonNull(Iterables.get(expectedStackValues.values(), 0), "sample value is null");
    }

    protected void assertPositionEquals(ValueBlock block, int position, Object expectedStackValue, Object expectedObjectValue)
            throws Throwable
    {
        long hash = 0;
        if (type.isComparable()) {
            hash = hashCodeOperator.hashCodeNullSafe(block, position);
        }
        assertPositionValue(block, position, expectedStackValue, hash, expectedObjectValue);
        assertPositionValue(block.getSingleValueBlock(position), 0, expectedStackValue, hash, expectedObjectValue);
        assertPositionValue(block.getRegion(position, 1), 0, expectedStackValue, hash, expectedObjectValue);
        assertPositionValue(block.getRegion(0, position + 1), position, expectedStackValue, hash, expectedObjectValue);
        assertPositionValue(block.getRegion(position, block.getPositionCount() - position), 0, expectedStackValue, hash, expectedObjectValue);

        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        type.appendTo(block, position, blockBuilder);
        assertPositionValue(blockBuilder.buildValueBlock(), 0, expectedStackValue, hash, expectedObjectValue);

        if (expectedStackValue != null) {
            blockBuilder = type.createBlockBuilder(null, 1);
            writeBlockMethod.invoke(expectedStackValue, blockBuilder);
            assertPositionValue(blockBuilder.buildValueBlock(), 0, expectedStackValue, hash, expectedObjectValue);
        }
    }

    private void assertPositionValue(ValueBlock block, int position, Object expectedStackValue, long expectedHash, Object expectedObjectValue)
            throws Throwable
    {
        assertThat(block.isNull(position)).isEqualTo(expectedStackValue == null);

        Object objectValue = type.getObjectValue(SESSION, block, position);
        assertThat(objectValue).isEqualTo(expectedObjectValue);
        if (objectValue != null) {
            assertThat(objectValue).isInstanceOf(objectValueType);
        }

        Block expectedBlock = createBlock(type, expectedStackValue);
        if (type.isComparable()) {
            assertThat(equalOperator.equalNullSafe(block, position, block, position)).isTrue();
            assertThat(equalOperator.equalNullSafe(block, position, expectedBlock, 0)).isTrue();
            assertThat(equalOperator.equalNullSafe(expectedBlock, 0, block, position)).isTrue();
            assertThat(hashCodeOperator.hashCodeNullSafe(block, position)).isEqualTo(expectedHash);
            assertThat(identicalOperator.isIdentical(block, position, block, position)).isTrue();
            assertThat(identicalOperator.isIdentical(block, position, expectedBlock, 0)).isTrue();
            assertThat(identicalOperator.isIdentical(expectedBlock, 0, block, position)).isTrue();
        }
        else {
            assertThatThrownBy(() -> typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION)))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("is not comparable");

            assertThatThrownBy(() -> typeOperators.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION)))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("is not comparable");

            assertThatThrownBy(() -> typeOperators.getEqualOperator(type, simpleConvention(DEFAULT_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("is not comparable");

            assertThatThrownBy(() -> typeOperators.getIdenticalOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("is not comparable");
        }

        assertThat(block.isNull(position)).isEqualTo(expectedStackValue == null);

        if (type.isOrderable()) {
            assertThat(blockTypeOperators.generateBlockPositionOrdering(type, ASC_NULLS_FIRST).order(block, position, expectedBlock, 0) == 0).isTrue();
            assertThat(blockTypeOperators.generateBlockPositionOrdering(type, ASC_NULLS_LAST).order(block, position, expectedBlock, 0) == 0).isTrue();
            assertThat(blockTypeOperators.generateBlockPositionOrdering(type, DESC_NULLS_FIRST).order(block, position, expectedBlock, 0) == 0).isTrue();
            assertThat(blockTypeOperators.generateBlockPositionOrdering(type, DESC_NULLS_LAST).order(block, position, expectedBlock, 0) == 0).isTrue();
        }
        else {
            assertThatThrownBy(() -> typeOperators.getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL)))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("is not orderable");
            assertThatThrownBy(() -> typeOperators.getComparisonUnorderedFirstOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL)))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("is not orderable");
        }

        verifyInvalidPositionHandling(block);

        if (block.isNull(position)) {
            if (type.isOrderable() && !(type instanceof UnknownType)) {
                Block nonNullValue = toBlock(getNonNullValue());
                assertThat(blockTypeOperators.generateBlockPositionOrdering(type, ASC_NULLS_FIRST).order(block, position, nonNullValue, 0) < 0).isTrue();
                assertThat(blockTypeOperators.generateBlockPositionOrdering(type, ASC_NULLS_LAST).order(block, position, nonNullValue, 0) > 0).isTrue();
                assertThat(blockTypeOperators.generateBlockPositionOrdering(type, DESC_NULLS_FIRST).order(block, position, nonNullValue, 0) < 0).isTrue();
                assertThat(blockTypeOperators.generateBlockPositionOrdering(type, DESC_NULLS_LAST).order(block, position, nonNullValue, 0) > 0).isTrue();
            }
            return;
        }

        if (type.isOrderable() && expectedStackValue != Boolean.TRUE) {
            Block greaterValue = toBlock(getGreaterValue(expectedStackValue));
            assertThat(blockTypeOperators.generateBlockPositionOrdering(type, ASC_NULLS_FIRST).order(block, position, greaterValue, 0) < 0).isTrue();
            assertThat(blockTypeOperators.generateBlockPositionOrdering(type, ASC_NULLS_LAST).order(block, position, greaterValue, 0) < 0).isTrue();
            assertThat(blockTypeOperators.generateBlockPositionOrdering(type, DESC_NULLS_FIRST).order(block, position, greaterValue, 0) > 0).isTrue();
            assertThat(blockTypeOperators.generateBlockPositionOrdering(type, DESC_NULLS_LAST).order(block, position, greaterValue, 0) > 0).isTrue();
        }

        if (type.getJavaType() == boolean.class) {
            assertThat(type.getBoolean(block, position)).isEqualTo(expectedStackValue);
            assertThatThrownBy(() -> type.getLong(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> type.getDouble(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> type.getObject(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThat((boolean) readBlockMethod.invokeExact(block, position)).isEqualTo(expectedStackValue);
        }
        else if (type.getJavaType() == long.class) {
            assertThat(type.getLong(block, position)).isEqualTo(expectedStackValue);
            assertThatThrownBy(() -> type.getBoolean(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> type.getDouble(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> type.getObject(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThat((long) readBlockMethod.invokeExact(block, position)).isEqualTo(expectedStackValue);
        }
        else if (type.getJavaType() == double.class) {
            assertThat(type.getDouble(block, position)).isEqualTo(expectedStackValue);
            assertThatThrownBy(() -> type.getBoolean(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> type.getLong(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> type.getObject(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThat((double) readBlockMethod.invokeExact(block, position)).isEqualTo(expectedStackValue);
        }
        else if (type.getJavaType() == Slice.class) {
            assertThat(type.getSlice(block, position)).isEqualTo(expectedStackValue);
            assertThat(type.getObject(block, position)).isEqualTo(expectedStackValue);
            assertThatThrownBy(() -> type.getBoolean(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> type.getLong(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> type.getDouble(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThat((Slice) readBlockMethod.invokeExact(block, position)).isEqualTo(expectedStackValue);
        }
        else if (type.getJavaType() == Block.class) {
            assertBlockEquals((Block) type.getObject(block, position), (Block) expectedStackValue);
            assertThatThrownBy(() -> type.getBoolean(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> type.getLong(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> type.getDouble(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> type.getSlice(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertBlockEquals((Block) readBlockMethod.invokeExact(block, position), (Block) expectedStackValue);
        }
        else {
            if (stackStackEqualOperator != null) {
                assertThat((Boolean) stackStackEqualOperator.invoke(type.getObject(block, position), expectedStackValue)).isTrue();
            }
            else {
                assertThat(type.getObject(block, position)).isEqualTo(expectedStackValue);
            }
            assertThatThrownBy(() -> type.getBoolean(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> type.getLong(block, position)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> type.getDouble(block, position)).isInstanceOf(UnsupportedOperationException.class);
            if (stackStackEqualOperator != null) {
                assertThat((Boolean) stackStackEqualOperator.invoke(readBlockMethod.invoke(block, position), expectedStackValue)).isTrue();
            }
            else {
                assertThat(readBlockMethod.invoke(block, position)).isEqualTo(expectedStackValue);
            }
        }
    }

    private void assertBlockEquals(Block actualValue, Block expectedValue)
    {
        SliceOutput actualSliceOutput = new DynamicSliceOutput(100);
        writeBlock(blockEncodingSerde, actualSliceOutput, actualValue);
        SliceOutput expectedSliceOutput = new DynamicSliceOutput(actualSliceOutput.size());
        writeBlock(blockEncodingSerde, expectedSliceOutput, expectedValue);
        assertThat(actualSliceOutput.slice()).isEqualTo(expectedSliceOutput.slice());
    }

    private void verifyInvalidPositionHandling(Block block)
    {
        assertThatThrownBy(() -> type.getObjectValue(SESSION, block, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid position -1 in block with %d positions", block.getPositionCount());

        assertThatThrownBy(() -> type.getObjectValue(SESSION, block, block.getPositionCount()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid position %d in block with %d positions", block.getPositionCount(), block.getPositionCount());

        if (type.isComparable()) {
            assertThatThrownBy(() -> hashCodeOperator.hashCode(block, -1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position -1 in block with %d positions", block.getPositionCount());

            assertThatThrownBy(() -> hashCodeOperator.hashCode(block, block.getPositionCount()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position %d in block with %d positions", block.getPositionCount(), block.getPositionCount());

            assertThatThrownBy(() -> xxHash64Operator.xxHash64(block, -1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position -1 in block with %d positions", block.getPositionCount());

            assertThatThrownBy(() -> xxHash64Operator.xxHash64(block, block.getPositionCount()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position %d in block with %d positions", block.getPositionCount(), block.getPositionCount());
        }

        if (type.isComparable() && !(type instanceof UnknownType)) {
            Block other = toBlock(getNonNullValue());
            assertThatThrownBy(() -> equalOperator.equal(block, -1, other, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position -1 in block with %d positions", block.getPositionCount());

            assertThatThrownBy(() -> equalOperator.equal(block, block.getPositionCount(), other, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position %d in block with %d positions", block.getPositionCount(), block.getPositionCount());

            assertThatThrownBy(() -> identicalOperator.isIdentical(block, -1, other, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position -1 in block with %d positions", block.getPositionCount());

            assertThatThrownBy(() -> identicalOperator.isIdentical(block, block.getPositionCount(), other, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position %d in block with %d positions", block.getPositionCount(), block.getPositionCount());
        }

        if (type.isOrderable() && !(type instanceof UnknownType)) {
            Block other = toBlock(getNonNullValue());
            assertThatThrownBy(() -> blockTypeOperators.generateBlockPositionOrdering(type, ASC_NULLS_FIRST).order(block, -1, other, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position -1 in block with %d positions", block.getPositionCount());

            assertThatThrownBy(() -> blockTypeOperators.generateBlockPositionOrdering(type, ASC_NULLS_FIRST).order(block, block.getPositionCount(), other, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position %d in block with %d positions", block.getPositionCount(), block.getPositionCount());
        }

        assertThatThrownBy(() -> readBlockMethod.invoke(block, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid position -1 in block with %d positions", block.getPositionCount());

        assertThatThrownBy(() -> readBlockMethod.invoke(block, block.getPositionCount()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid position %d in block with %d positions", block.getPositionCount(), block.getPositionCount());

        if (type.getJavaType() == boolean.class) {
            assertThatThrownBy(() -> type.getBoolean(block, -1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position -1 in block with %d positions", block.getPositionCount());

            assertThatThrownBy(() -> type.getBoolean(block, block.getPositionCount()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position %d in block with %d positions", block.getPositionCount(), block.getPositionCount());
        }
        else if (type.getJavaType() == long.class) {
            assertThatThrownBy(() -> type.getLong(block, -1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position -1 in block with %d positions", block.getPositionCount());

            assertThatThrownBy(() -> type.getLong(block, block.getPositionCount()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position %d in block with %d positions", block.getPositionCount(), block.getPositionCount());
        }
        else if (type.getJavaType() == double.class) {
            assertThatThrownBy(() -> type.getDouble(block, -1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position -1 in block with %d positions", block.getPositionCount());

            assertThatThrownBy(() -> type.getDouble(block, block.getPositionCount()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position %d in block with %d positions", block.getPositionCount(), block.getPositionCount());
        }
        else if (type.getJavaType() == Slice.class) {
            assertThatThrownBy(() -> type.getSlice(block, -1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position -1 in block with %d positions", block.getPositionCount());

            assertThatThrownBy(() -> type.getSlice(block, block.getPositionCount()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position %d in block with %d positions", block.getPositionCount(), block.getPositionCount());
        }
    }

    private static Block createBlock(Type type, Object value)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);

        Class<?> javaType = type.getJavaType();
        if (value == null) {
            blockBuilder.appendNull();
        }
        else if (javaType == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (javaType == long.class) {
            type.writeLong(blockBuilder, (Long) value);
        }
        else if (javaType == double.class) {
            type.writeDouble(blockBuilder, (Double) value);
        }
        else if (javaType == Slice.class) {
            Slice slice = (Slice) value;
            type.writeSlice(blockBuilder, slice, 0, slice.length());
        }
        else {
            type.writeObject(blockBuilder, value);
        }
        return blockBuilder.buildValueBlock();
    }

    /**
     * @param value value, represented in native container type
     * @return a value that is greater than input, represented in native container type
     */
    protected abstract Object getGreaterValue(Object value);

    /**
     * @return a non-null value, represented in native container type
     */
    protected Object getNonNullValue()
    {
        return getNonNullValueForType(type);
    }

    /**
     * @return a non-null value, represented in native container type
     */
    private static Object getNonNullValueForType(Type type)
    {
        if (type.getJavaType() == boolean.class) {
            return true;
        }
        if (type.getJavaType() == long.class) {
            return 1L;
        }
        if (type.getJavaType() == double.class) {
            return 1.0;
        }
        if (type.getJavaType() == Slice.class) {
            return Slices.utf8Slice("_");
        }
        if (type.getJavaType() == LongTimestamp.class) {
            return new LongTimestamp(1, 0);
        }
        if (type.getJavaType() == LongTimestampWithTimeZone.class) {
            return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(1, 0, UTC_KEY);
        }
        switch (type) {
            case ArrayType arrayType -> {
                Type elementType = arrayType.getElementType();
                Object elementNonNullValue = getNonNullValueForType(elementType);
                return arrayBlockOf(elementType, elementNonNullValue);
            }
            case MapType mapType -> {
                Type keyType = mapType.getKeyType();
                Type valueType = mapType.getValueType();
                Object keyNonNullValue = getNonNullValueForType(keyType);
                Object valueNonNullValue = getNonNullValueForType(valueType);
                Map<?, ?> map = ImmutableMap.of(keyNonNullValue, valueNonNullValue);
                return sqlMapOf(keyType, valueType, map);
            }
            case RowType rowType -> {
                List<Type> elementTypes = rowType.getTypeParameters();
                Object[] elementNonNullValues = elementTypes.stream().map(AbstractTestType::getNonNullValueForType).toArray(Object[]::new);
                return toRow(elementTypes, elementNonNullValues);
            }
            default -> throw new IllegalStateException("Unsupported Java type " + type.getJavaType() + " (for type " + type + ")");
        }
    }

    private Block toBlock(Object value)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        Class<?> javaType = type.getJavaType();
        if (value == null) {
            blockBuilder.appendNull();
        }
        else if (javaType == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (javaType == long.class) {
            type.writeLong(blockBuilder, (Long) value);
        }
        else if (javaType == double.class) {
            type.writeDouble(blockBuilder, (Double) value);
        }
        else if (javaType == Slice.class) {
            Slice slice = (Slice) value;
            type.writeSlice(blockBuilder, slice, 0, slice.length());
        }
        else {
            type.writeObject(blockBuilder, value);
        }
        return blockBuilder.buildValueBlock();
    }

    private static SortedMap<Integer, Object> indexStackValues(Type type, Block block)
    {
        SortedMap<Integer, Object> values = new TreeMap<>();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                values.put(position, null);
            }
            else if (type.getJavaType() == boolean.class) {
                values.put(position, type.getBoolean(block, position));
            }
            else if (type.getJavaType() == long.class) {
                values.put(position, type.getLong(block, position));
            }
            else if (type.getJavaType() == double.class) {
                values.put(position, type.getDouble(block, position));
            }
            else if (type.getJavaType() == Slice.class) {
                values.put(position, type.getSlice(block, position));
            }
            else {
                values.put(position, type.getObject(block, position));
            }
        }
        return unmodifiableSortedMap(values);
    }

    private static SortedMap<Integer, Object> indexObjectValues(Type type, Block block)
    {
        SortedMap<Integer, Object> values = new TreeMap<>();
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.put(position, type.getObjectValue(SESSION, block, position));
        }
        return unmodifiableSortedMap(values);
    }
}
