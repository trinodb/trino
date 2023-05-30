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
package io.trino.spi.function;

import com.google.common.base.Defaults;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.trino.spi.function.InvocationConvention.InvocationReturnConvention;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.block.TestingSession.SESSION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.IN_OUT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestScalarFunctionAdapter
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final ArrayType ARRAY_TYPE = new ArrayType(BIGINT);
    private static final CharType CHAR_TYPE = createCharType(7);
    private static final TimestampType TIMESTAMP_TYPE = createTimestampType(9);
    private static final Type RETURN_TYPE = BOOLEAN;
    private static final List<Type> ARGUMENT_TYPES = ImmutableList.of(DOUBLE, VARCHAR, ARRAY_TYPE);
    private static final List<Type> OBJECTS_ARGUMENT_TYPES = ImmutableList.of(VARCHAR, ARRAY_TYPE, CHAR_TYPE, TIMESTAMP_TYPE);

    @Test
    public void testAdaptFromNeverNull()
            throws Throwable
    {
        InvocationConvention actualConvention = new InvocationConvention(
                nCopies(ARGUMENT_TYPES.size(), NEVER_NULL),
                FAIL_ON_NULL,
                false,
                true);
        verifyAllAdaptations(actualConvention, "neverNull", RETURN_TYPE, ARGUMENT_TYPES);
    }

    @Test
    public void testAdaptFromNeverNullObjects()
            throws Throwable
    {
        InvocationConvention actualConvention = new InvocationConvention(
                nCopies(OBJECTS_ARGUMENT_TYPES.size(), NEVER_NULL),
                FAIL_ON_NULL,
                false,
                true);
        verifyAllAdaptations(actualConvention, "neverNullObjects", RETURN_TYPE, OBJECTS_ARGUMENT_TYPES);
    }

    @Test
    public void testAdaptFromBoxedNull()
            throws Throwable
    {
        InvocationConvention actualConvention = new InvocationConvention(
                nCopies(ARGUMENT_TYPES.size(), BOXED_NULLABLE),
                FAIL_ON_NULL,
                false,
                true);
        verifyAllAdaptations(actualConvention, "boxedNull", RETURN_TYPE, ARGUMENT_TYPES);
    }

    @Test
    public void testAdaptFromBoxedNullObjects()
            throws Throwable
    {
        InvocationConvention actualConvention = new InvocationConvention(
                nCopies(OBJECTS_ARGUMENT_TYPES.size(), BOXED_NULLABLE),
                FAIL_ON_NULL,
                false,
                true);
        verifyAllAdaptations(actualConvention, "boxedNullObjects", RETURN_TYPE, OBJECTS_ARGUMENT_TYPES);
    }

    @Test
    public void testAdaptFromNullFlag()
            throws Throwable
    {
        InvocationConvention actualConvention = new InvocationConvention(
                nCopies(ARGUMENT_TYPES.size(), NULL_FLAG),
                FAIL_ON_NULL,
                false,
                true);
        verifyAllAdaptations(actualConvention, "nullFlag", RETURN_TYPE, ARGUMENT_TYPES);
    }

    @Test
    public void testAdaptFromNullFlagObjects()
            throws Throwable
    {
        InvocationConvention actualConvention = new InvocationConvention(
                nCopies(OBJECTS_ARGUMENT_TYPES.size(), NULL_FLAG),
                FAIL_ON_NULL,
                false,
                true);
        verifyAllAdaptations(actualConvention, "nullFlagObjects", RETURN_TYPE, OBJECTS_ARGUMENT_TYPES);
    }

    @Test
    public void testAdaptFromBlockPosition()
            throws Throwable
    {
        InvocationConvention actualConvention = new InvocationConvention(
                nCopies(ARGUMENT_TYPES.size(), BLOCK_POSITION),
                FAIL_ON_NULL,
                false,
                true);
        verifyAllAdaptations(actualConvention, "blockPosition", RETURN_TYPE, ARGUMENT_TYPES);
    }

    @Test
    public void testAdaptFromBlockPositionObjects()
            throws Throwable
    {
        InvocationConvention actualConvention = new InvocationConvention(
                nCopies(OBJECTS_ARGUMENT_TYPES.size(), BLOCK_POSITION),
                FAIL_ON_NULL,
                false,
                true);
        verifyAllAdaptations(actualConvention, "blockPositionObjects", RETURN_TYPE, OBJECTS_ARGUMENT_TYPES);
    }

    @Test
    public void testAdaptFromBlockPositionNotNull()
            throws Throwable
    {
        InvocationConvention actualConvention = new InvocationConvention(
                nCopies(ARGUMENT_TYPES.size(), BLOCK_POSITION_NOT_NULL),
                FAIL_ON_NULL,
                false,
                true);
        verifyAllAdaptations(actualConvention, "blockPosition", RETURN_TYPE, ARGUMENT_TYPES);
    }

    @Test
    public void testAdaptFromBlockPositionNotNullObjects()
            throws Throwable
    {
        InvocationConvention actualConvention = new InvocationConvention(
                nCopies(OBJECTS_ARGUMENT_TYPES.size(), BLOCK_POSITION_NOT_NULL),
                FAIL_ON_NULL,
                false,
                true);
        verifyAllAdaptations(actualConvention, "blockPositionObjects", RETURN_TYPE, OBJECTS_ARGUMENT_TYPES);
    }

    private static void verifyAllAdaptations(
            InvocationConvention actualConvention,
            String methodName,
            Type returnType,
            List<Type> argumentTypes)
            throws Throwable
    {
        MethodType type = methodType(actualConvention.getReturnConvention() == FAIL_ON_NULL ? boolean.class : Boolean.class, toCallArgumentTypes(actualConvention, argumentTypes));
        MethodHandle methodHandle = lookup().findVirtual(Target.class, methodName, type);
        verifyAllAdaptations(actualConvention, methodHandle, returnType, argumentTypes);
    }

    private static void verifyAllAdaptations(
            InvocationConvention actualConvention,
            MethodHandle methodHandle,
            Type returnType,
            List<Type> argumentTypes)
            throws Throwable
    {
        List<List<InvocationArgumentConvention>> allArgumentConventions = allCombinations(
                ImmutableList.of(NEVER_NULL, BLOCK_POSITION_NOT_NULL, BOXED_NULLABLE, NULL_FLAG, BLOCK_POSITION, FLAT, IN_OUT),
                argumentTypes.size());
        for (List<InvocationArgumentConvention> argumentConventions : allArgumentConventions) {
            for (InvocationReturnConvention returnConvention : InvocationReturnConvention.values()) {
                InvocationConvention expectedConvention = new InvocationConvention(argumentConventions, returnConvention, false, true);
                adaptAndVerify(
                        methodHandle,
                        actualConvention,
                        expectedConvention,
                        returnType,
                        argumentTypes);
            }
        }
    }

    private static void adaptAndVerify(
            MethodHandle methodHandle,
            InvocationConvention actualConvention,
            InvocationConvention expectedConvention,
            Type returnType,
            List<Type> argumentTypes)
            throws Throwable
    {
        MethodHandle adaptedMethodHandle;
        try {
            adaptedMethodHandle = ScalarFunctionAdapter.adapt(
                    methodHandle,
                    returnType,
                    argumentTypes,
                    actualConvention,
                    expectedConvention);
            assertTrue(ScalarFunctionAdapter.canAdapt(actualConvention, expectedConvention));
        }
        catch (IllegalArgumentException e) {
            if (!ScalarFunctionAdapter.canAdapt(actualConvention, expectedConvention)) {
                if (hasNullableToNoNullableAdaptation(actualConvention, expectedConvention)) {
                    assertTrue(expectedConvention.getReturnConvention() == FAIL_ON_NULL || expectedConvention.getReturnConvention() == FLAT_RETURN);
                    return;
                }
                if (actualConvention.getArgumentConventions().stream().anyMatch(convention -> convention == BLOCK_POSITION || convention == BLOCK_POSITION_NOT_NULL)) {
                    return;
                }
            }
            throw new AssertionError("Adaptation failed but no illegal conversions found", e);
        }

        InvocationConvention newCallingConvention = new InvocationConvention(
                expectedConvention.getArgumentConventions(),
                expectedConvention.getReturnConvention(),
                actualConvention.supportsSession(),
                actualConvention.supportsInstanceFactory());

        // crete an exact invoker to the handle, so we can use object invoke interface without type coercion concerns
        MethodHandle exactInvoker = MethodHandles.exactInvoker(adaptedMethodHandle.type())
                .bindTo(adaptedMethodHandle);
        if (expectedConvention.getReturnConvention() != BLOCK_BUILDER) {
            exactInvoker = MethodHandles.explicitCastArguments(exactInvoker, exactInvoker.type().changeReturnType(Boolean.class));
        }

        // try all combinations of null and not null arguments
        for (int notNullMask = 0; notNullMask < (1 << actualConvention.getArgumentConventions().size()); notNullMask++) {
            BitSet nullArguments = BitSet.valueOf(new long[] {notNullMask});
            if (!canCallConventionWithNullArguments(expectedConvention, nullArguments)) {
                continue;
            }
            Target target = new Target();
            List<Object> argumentValues = toCallArgumentValues(newCallingConvention, nullArguments, target, argumentTypes);
            try {
                boolean expectNull = expectNullReturn(actualConvention, nullArguments);
                if (expectedConvention.getReturnConvention() == BLOCK_BUILDER) {
                    BlockBuilder blockBuilder = returnType.createBlockBuilder(null, 1);
                    argumentValues.add(blockBuilder);
                    exactInvoker.invokeWithArguments(argumentValues);
                    Block result = blockBuilder.build();
                    assertThat(result.getPositionCount()).isEqualTo(1);
                    assertThat(result.isNull(0)).isEqualTo(expectNull);
                    if (!expectNull) {
                        assertThat(BOOLEAN.getBoolean(result, 0)).isTrue();
                    }
                    return;
                }

                Boolean result = (Boolean) exactInvoker.invokeWithArguments(argumentValues);
                switch (expectedConvention.getReturnConvention()) {
                    case FAIL_ON_NULL -> assertTrue(result);
                    case DEFAULT_ON_NULL -> assertEquals(result, (Boolean) !expectNull);
                    case NULLABLE_RETURN -> assertEquals(result, !expectNull ? true : null);
                    default -> throw new UnsupportedOperationException();
                }
            }
            catch (TrinoException trinoException) {
                assertEquals(trinoException.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
            }
            target.verify(actualConvention, nullArguments, argumentTypes);
        }
    }

    private static boolean hasNullableToNoNullableAdaptation(InvocationConvention actualConvention, InvocationConvention expectedConvention)
    {
        for (int i = 0; i < actualConvention.getArgumentConventions().size(); i++) {
            InvocationArgumentConvention actualArgumentConvention = actualConvention.getArgumentConvention(i);
            InvocationArgumentConvention expectedArgumentConvention = expectedConvention.getArgumentConvention(i);
            if (actualArgumentConvention == NEVER_NULL && (expectedArgumentConvention == BOXED_NULLABLE || expectedArgumentConvention == NULL_FLAG)) {
                // this conversion is not allowed
                return true;
            }
            if (actualArgumentConvention == IN_OUT) {
                // this conversion is not allowed
                return true;
            }
        }
        if (actualConvention.getReturnConvention() != expectedConvention.getReturnConvention()) {
            if (expectedConvention.getReturnConvention() == FLAT_RETURN) {
                // Flat return can not be adapted
                return true;
            }
        }
        return false;
    }

    private static boolean canCallConventionWithNullArguments(InvocationConvention convention, BitSet nullArguments)
    {
        for (int i = 0; i < convention.getArgumentConventions().size(); i++) {
            InvocationArgumentConvention argumentConvention = convention.getArgumentConvention(i);
            if (nullArguments.get(i) && (argumentConvention == NEVER_NULL || argumentConvention == BLOCK_POSITION_NOT_NULL || argumentConvention == FLAT)) {
                return false;
            }
        }
        return true;
    }

    private static boolean expectNullReturn(InvocationConvention convention, BitSet nullArguments)
    {
        for (int i = 0; i < convention.getArgumentConventions().size(); i++) {
            InvocationArgumentConvention argumentConvention = convention.getArgumentConvention(i);
            if (nullArguments.get(i) && !argumentConvention.isNullable()) {
                return true;
            }
        }
        return false;
    }

    private static List<Class<?>> toCallArgumentTypes(InvocationConvention callingConvention, List<Type> argumentTypes)
    {
        List<Class<?>> expectedArguments = new ArrayList<>();
        for (int i = 0; i < callingConvention.getArgumentConventions().size(); i++) {
            Type argumentType = argumentTypes.get(i);
            InvocationArgumentConvention argumentConvention = callingConvention.getArgumentConvention(i);
            Class<?> javaType = argumentType.getJavaType();
            if (argumentType.equals(CHAR_TYPE) || argumentType.equals(TIMESTAMP_TYPE)) {
                javaType = Object.class;
            }
            switch (argumentConvention) {
                case NEVER_NULL -> expectedArguments.add(javaType);
                case BOXED_NULLABLE -> expectedArguments.add(Primitives.wrap(javaType));
                case NULL_FLAG -> {
                    expectedArguments.add(javaType);
                    expectedArguments.add(boolean.class);
                }
                case BLOCK_POSITION_NOT_NULL, BLOCK_POSITION -> {
                    expectedArguments.add(Block.class);
                    expectedArguments.add(int.class);
                }
                case FLAT -> {
                    expectedArguments.add(Slice.class);
                    expectedArguments.add(int.class);
                    expectedArguments.add(Slice.class);
                    expectedArguments.add(int.class);
                }
                case IN_OUT -> expectedArguments.add(InOut.class);
                default -> throw new IllegalArgumentException("Unsupported argument convention: " + argumentConvention);
            }
        }
        return expectedArguments;
    }

    private static List<Object> toCallArgumentValues(InvocationConvention callingConvention, BitSet nullArguments, Target target, List<Type> argumentTypes)
            throws Throwable
    {
        List<Object> callArguments = new ArrayList<>();
        callArguments.add(target);
        for (int i = 0; i < callingConvention.getArgumentConventions().size(); i++) {
            Type argumentType = argumentTypes.get(i);
            boolean nullArgument = nullArguments.get(i);

            Object testValue;
            if (nullArgument) {
                testValue = null;
            }
            else {
                testValue = getTestValue(argumentType);
            }

            InvocationArgumentConvention argumentConvention = callingConvention.getArgumentConvention(i);
            switch (argumentConvention) {
                case NEVER_NULL -> {
                    verify(testValue != null, "null can not be passed to a never null argument");
                    callArguments.add(testValue);
                }
                case BOXED_NULLABLE -> callArguments.add(testValue);
                case NULL_FLAG -> {
                    callArguments.add(testValue == null ? Defaults.defaultValue(argumentType.getJavaType()) : testValue);
                    callArguments.add(testValue == null);
                }
                case BLOCK_POSITION_NOT_NULL -> {
                    verify(testValue != null, "null cannot be passed to a block positions not null argument");
                    BlockBuilder blockBuilder = argumentType.createBlockBuilder(null, 3);
                    blockBuilder.appendNull();
                    writeNativeValue(argumentType, blockBuilder, testValue);
                    blockBuilder.appendNull();
                    callArguments.add(blockBuilder.build());
                    callArguments.add(1);
                }
                case BLOCK_POSITION -> {
                    BlockBuilder blockBuilder = argumentType.createBlockBuilder(null, 3);
                    blockBuilder.appendNull();
                    writeNativeValue(argumentType, blockBuilder, testValue);
                    blockBuilder.appendNull();
                    callArguments.add(blockBuilder.build());
                    callArguments.add(1);
                }
                case FLAT -> {
                    verify(testValue != null, "null cannot be passed to a flat argument");
                    BlockBuilder blockBuilder = argumentType.createBlockBuilder(null, 3);
                    writeNativeValue(argumentType, blockBuilder, testValue);
                    Block block = blockBuilder.build();

                    byte[] fixedSlice = new byte[argumentType.getFlatFixedSize()];
                    int variableWidthLength = argumentType.getFlatVariableWidthSize(block, 0);
                    byte[] variableSlice = new byte[variableWidthLength];
                    MethodHandle writeFlat = TYPE_OPERATORS.getReadValueOperator(argumentType, simpleConvention(FLAT_RETURN, BLOCK_POSITION));
                    writeFlat.invokeExact(block, 0, fixedSlice, 0, variableSlice, 0);

                    callArguments.add(fixedSlice);
                    callArguments.add(0);
                    callArguments.add(variableSlice);
                }
                case IN_OUT -> callArguments.add(new TestingInOut(argumentType, testValue));
                default -> throw new IllegalArgumentException("Unsupported argument convention: " + argumentConvention);
            }
        }
        return callArguments;
    }

    private static Object getTestValue(Type argumentType)
    {
        // BOOLEAN, BIGINT, DOUBLE, VARCHAR, ARRAY_TYPE, createCharType(7), createTimestampType(9)
        if (argumentType.equals(BOOLEAN)) {
            return true;
        }
        if (argumentType.equals(DOUBLE)) {
            return 33.33;
        }
        if (argumentType.equals(BIGINT)) {
            return 42L;
        }
        if (argumentType.equals(VARCHAR)) {
            return Slices.utf8Slice("test");
        }
        if (argumentType.equals(ARRAY_TYPE)) {
            BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, 4);
            blockBuilder.appendNull();
            BIGINT.writeLong(blockBuilder, 99);
            blockBuilder.appendNull();
            BIGINT.writeLong(blockBuilder, 100);
            return blockBuilder.build();
        }
        if (argumentType.equals(CHAR_TYPE)) {
            return Slices.utf8Slice("1234567");
        }
        if (argumentType.equals(TIMESTAMP_TYPE)) {
            return new LongTimestamp(5678, 123_000);
        }
        throw new IllegalArgumentException("Unsupported argument type: " + argumentType);
    }

    private static <T> List<List<T>> allCombinations(List<T> values, int n)
    {
        ImmutableList.Builder<List<T>> combinations = ImmutableList.builder();
        int[] indexes = new int[n];
        do {
            combinations.add(IntStream.of(indexes)
                    .mapToObj(values::get)
                    .collect(toImmutableList()));

            for (int i = 0; i < indexes.length; i++) {
                indexes[i]++;
                if (indexes[i] < values.size()) {
                    break;
                }
                // roll over and increment next
                indexes[i] = 0;
            }
        }
        while (!IntStream.of(indexes).allMatch(i -> i == 0));
        return combinations.build();
    }

    private static class Target
    {
        private boolean invoked;
        private boolean objectsMethod;
        private Double doubleValue;
        private Slice sliceValue;
        private Block blockValue;
        private Object objectCharValue;
        private Object objectTimestampValue;

        @SuppressWarnings("unused")
        public boolean neverNull(double doubleValue, Slice sliceValue, Block blockValue)
        {
            checkState(!invoked, "Already invoked");
            invoked = true;
            objectsMethod = false;

            this.doubleValue = doubleValue;
            this.sliceValue = sliceValue;
            this.blockValue = blockValue;
            return true;
        }

        @SuppressWarnings("unused")
        public boolean neverNullObjects(Slice sliceValue, Block blockValue, Object objectCharValue, Object objectTimestampValue)
        {
            checkState(!invoked, "Already invoked");
            invoked = true;
            objectsMethod = true;

            this.sliceValue = sliceValue;
            this.blockValue = blockValue;
            this.objectCharValue = objectCharValue;
            this.objectTimestampValue = objectTimestampValue;
            return true;
        }

        @SuppressWarnings("unused")
        public boolean boxedNull(Double doubleValue, Slice sliceValue, Block blockValue)
        {
            checkState(!invoked, "Already invoked");
            invoked = true;
            objectsMethod = false;

            this.doubleValue = doubleValue;
            this.sliceValue = sliceValue;
            this.blockValue = blockValue;
            return true;
        }

        @SuppressWarnings("unused")
        public boolean boxedNullObjects(Slice sliceValue, Block blockValue, Object objectCharValue, Object objectTimestampValue)
        {
            checkState(!invoked, "Already invoked");
            invoked = true;
            objectsMethod = true;

            this.sliceValue = sliceValue;
            this.blockValue = blockValue;
            this.objectCharValue = objectCharValue;
            this.objectTimestampValue = objectTimestampValue;
            return true;
        }

        @SuppressWarnings("unused")
        public boolean nullFlag(
                double doubleValue, boolean doubleNull,
                Slice sliceValue, boolean sliceNull,
                Block blockValue, boolean blockNull)
        {
            checkState(!invoked, "Already invoked");
            invoked = true;
            objectsMethod = false;

            if (doubleNull) {
                assertEquals(doubleValue, 0.0);
                this.doubleValue = null;
            }
            else {
                this.doubleValue = doubleValue;
            }

            if (sliceNull) {
                assertNull(sliceValue);
                this.sliceValue = null;
            }
            else {
                this.sliceValue = sliceValue;
            }

            if (blockNull) {
                assertNull(blockValue);
                this.blockValue = null;
            }
            else {
                this.blockValue = blockValue;
            }
            return true;
        }

        @SuppressWarnings("unused")
        public boolean nullFlagObjects(
                Slice sliceValue, boolean sliceNull,
                Block blockValue, boolean blockNull,
                Object objectCharValue, boolean objectCharNull,
                Object objectTimestampValue, boolean objectTimestampNull)
        {
            checkState(!invoked, "Already invoked");
            invoked = true;
            objectsMethod = true;

            if (sliceNull) {
                assertNull(sliceValue);
                this.sliceValue = null;
            }
            else {
                this.sliceValue = sliceValue;
            }

            if (blockNull) {
                assertNull(blockValue);
                this.blockValue = null;
            }
            else {
                this.blockValue = blockValue;
            }

            if (objectCharNull) {
                assertNull(objectCharValue);
                this.objectCharValue = null;
            }
            else {
                this.objectCharValue = objectCharValue;
            }

            if (objectTimestampNull) {
                assertNull(objectTimestampValue);
                this.objectTimestampValue = null;
            }
            else {
                this.objectTimestampValue = objectTimestampValue;
            }
            return true;
        }

        @SuppressWarnings("unused")
        public boolean blockPosition(
                Block doubleBlock, int doublePosition,
                Block sliceBlock, int slicePosition,
                Block blockBlock, int blockPosition)
        {
            checkState(!invoked, "Already invoked");
            invoked = true;
            objectsMethod = false;

            if (doubleBlock.isNull(doublePosition)) {
                this.doubleValue = null;
            }
            else {
                this.doubleValue = DOUBLE.getDouble(doubleBlock, doublePosition);
            }

            if (sliceBlock.isNull(slicePosition)) {
                this.sliceValue = null;
            }
            else {
                this.sliceValue = VARCHAR.getSlice(sliceBlock, slicePosition);
            }

            if (blockBlock.isNull(blockPosition)) {
                this.blockValue = null;
            }
            else {
                this.blockValue = ARRAY_TYPE.getObject(blockBlock, blockPosition);
            }
            return true;
        }

        @SuppressWarnings("unused")
        public boolean blockPositionObjects(
                Block sliceBlock, int slicePosition,
                Block blockBlock, int blockPosition,
                Block objectCharBlock, int objectCharPosition,
                Block objectTimestampBlock, int objectTimestampPosition)
        {
            checkState(!invoked, "Already invoked");
            invoked = true;
            objectsMethod = true;

            if (sliceBlock.isNull(slicePosition)) {
                this.sliceValue = null;
            }
            else {
                this.sliceValue = VARCHAR.getSlice(sliceBlock, slicePosition);
            }

            if (blockBlock.isNull(blockPosition)) {
                this.blockValue = null;
            }
            else {
                this.blockValue = ARRAY_TYPE.getObject(blockBlock, blockPosition);
            }

            if (objectCharBlock.isNull(objectCharPosition)) {
                this.objectCharValue = null;
            }
            else {
                this.objectCharValue = CHAR_TYPE.getObject(objectCharBlock, objectCharPosition);
            }

            if (objectTimestampBlock.isNull(objectTimestampPosition)) {
                this.objectTimestampValue = null;
            }
            else {
                this.objectTimestampValue = TIMESTAMP_TYPE.getObject(objectTimestampBlock, objectTimestampPosition);
            }
            return true;
        }

        public void verify(
                InvocationConvention actualConvention,
                BitSet nullArguments,
                List<Type> argumentTypes)
        {
            if (shouldFunctionBeInvoked(actualConvention, nullArguments)) {
                assertTrue(invoked, "function not invoked");
                if (!objectsMethod) {
                    assertArgumentValue(this.doubleValue, 0, actualConvention, nullArguments, argumentTypes);
                    assertArgumentValue(this.sliceValue, 1, actualConvention, nullArguments, argumentTypes);
                    assertArgumentValue(this.blockValue, 2, actualConvention, nullArguments, argumentTypes);
                }
                else {
                    assertArgumentValue(this.sliceValue, 0, actualConvention, nullArguments, argumentTypes);
                    assertArgumentValue(this.blockValue, 1, actualConvention, nullArguments, argumentTypes);
                    assertArgumentValue(this.objectCharValue, 2, actualConvention, nullArguments, argumentTypes);
                    assertArgumentValue(this.objectTimestampValue, 3, actualConvention, nullArguments, argumentTypes);
                }
            }
            else {
                assertFalse(invoked, "Function should not be invoked when null is passed to a NEVER_NULL argument");
                assertNull(this.doubleValue);
                assertNull(this.sliceValue);
                assertNull(this.blockValue);
                assertNull(this.objectCharValue);
                assertNull(this.objectTimestampValue);
            }

            this.invoked = false;
            this.objectsMethod = false;
            this.doubleValue = null;
            this.sliceValue = null;
            this.blockValue = null;
            this.objectCharValue = null;
            this.objectTimestampValue = null;
        }

        private static boolean shouldFunctionBeInvoked(InvocationConvention actualConvention, BitSet nullArguments)
        {
            for (int i = 0; i < actualConvention.getArgumentConventions().size(); i++) {
                InvocationArgumentConvention argumentConvention = actualConvention.getArgumentConvention(i);
                if ((argumentConvention == NEVER_NULL || argumentConvention == BLOCK_POSITION_NOT_NULL || argumentConvention == FLAT) && nullArguments.get(i)) {
                    return false;
                }
            }
            return true;
        }

        private static void assertArgumentValue(
                Object actualValue,
                int index,
                InvocationConvention actualConvention,
                BitSet nullArguments,
                List<Type> argumentTypes)
        {
            assertArgumentValue(actualValue, actualConvention.getArgumentConvention(index), argumentTypes.get(index), nullArguments.get(index));
        }

        private static void assertArgumentValue(
                Object actualValue,
                InvocationArgumentConvention argumentConvention,
                Type argumentType,
                boolean isNull)
        {
            if (!isNull) {
                assertArgumentValue(actualValue, getTestValue(argumentType));
                return;
            }

            if (argumentConvention != NEVER_NULL && argumentConvention != FLAT) {
                assertNull(actualValue);
                return;
            }

            // the only way for a never null to be called with a null is for the undefined value null convention
            // Currently, for primitives, the value is the java default, but for all other types it could be any value
            if (argumentType.getJavaType().isPrimitive()) {
                assertArgumentValue(actualValue, Defaults.defaultValue(argumentType.getJavaType()));
            }
        }

        private static void assertArgumentValue(Object actual, Object expected)
        {
            if (actual instanceof Block && expected instanceof Block) {
                assertBlockEquals(BIGINT, (Block) actual, (Block) expected);
            }
            else {
                assertEquals(actual, expected);
            }
        }

        private static void assertBlockEquals(Type type, Block actual, Block expected)
        {
            for (int position = 0; position < actual.getPositionCount(); position++) {
                assertEquals(type.getObjectValue(SESSION, actual, position), type.getObjectValue(SESSION, expected, position));
            }
        }
    }

    private static class TestingInOut
            implements InOut, InternalDataAccessor
    {
        private final Type type;
        private Object value;

        public TestingInOut(Type type, Object value)
        {
            this.type = requireNonNull(type, "type is null");
            this.value = value;

            if (value != null) {
                Class<?> javaType = type.getJavaType();
                if (javaType.equals(boolean.class)) {
                    checkArgument(value instanceof Boolean, "Value must be a Boolean for type %s", type);
                }
                else if (javaType.equals(long.class)) {
                    checkArgument(value instanceof Long, "Value must be a Long for type %s", type);
                }
                else if (javaType.equals(double.class)) {
                    checkArgument(value instanceof Double, "Value must be a Double for type %s", type);
                }
            }
        }

        @Override
        public AccumulatorState copy()
        {
            return new TestingInOut(type, value);
        }

        @Override
        public long getEstimatedSize()
        {
            return 0;
        }

        @Override
        public Type getType()
        {
            return type;
        }

        @Override
        public final boolean isNull()
        {
            return value == null;
        }

        @Override
        public final void get(BlockBuilder blockBuilder)
        {
            Class<?> javaType = type.getJavaType();

            Object value = this.value;
            if (value == null) {
                blockBuilder.appendNull();
            }
            else if (javaType.equals(boolean.class)) {
                type.writeBoolean(blockBuilder, (Boolean) value);
            }
            else if (javaType.equals(long.class)) {
                type.writeLong(blockBuilder, (Long) value);
            }
            else if (javaType.equals(double.class)) {
                type.writeDouble(blockBuilder, (Double) value);
            }
            else if (javaType.equals(Slice.class)) {
                type.writeSlice(blockBuilder, (Slice) value);
            }
            else {
                type.writeObject(blockBuilder, value);
            }
        }

        @Override
        public final void set(Block block, int position)
        {
            Class<?> javaType = type.getJavaType();

            Object value;
            if (block.isNull(position)) {
                value = null;
            }
            else if (javaType.equals(boolean.class)) {
                value = type.getBoolean(block, position);
            }
            else if (javaType.equals(long.class)) {
                value = type.getLong(block, position);
            }
            else if (javaType.equals(double.class)) {
                value = type.getDouble(block, position);
            }
            else if (javaType.equals(Slice.class)) {
                value = type.getSlice(block, position);
            }
            else {
                value = type.getObject(block, position);
            }
            this.value = value;
        }

        @Override
        public final void set(InOut otherState)
        {
            checkArgument(type.equals(otherState.getType()), "Expected other state to be type %s, but is type %s", type, otherState.getType());

            Class<?> javaType = type.getJavaType();
            Object value;
            if (otherState.isNull()) {
                value = null;
            }
            else if (javaType.equals(boolean.class)) {
                value = ((InternalDataAccessor) otherState).getBooleanValue();
            }
            else if (javaType.equals(long.class)) {
                value = ((InternalDataAccessor) otherState).getLongValue();
            }
            else if (javaType.equals(double.class)) {
                value = ((InternalDataAccessor) otherState).getDoubleValue();
            }
            else {
                value = ((InternalDataAccessor) otherState).getObjectValue();
            }
            this.value = value;
        }

        @Override
        public final boolean getBooleanValue()
        {
            checkArgument(type.getJavaType().equals(boolean.class), "Type %s does not have a boolean stack type", type);
            Object value = this.value;
            return value != null && (Boolean) value;
        }

        @Override
        public final double getDoubleValue()
        {
            checkArgument(type.getJavaType().equals(double.class), "Type %s does not have a double stack type", type);
            Object value = this.value;
            return value == null ? 0.0 : (Double) value;
        }

        @Override
        public final long getLongValue()
        {
            checkArgument(type.getJavaType().equals(long.class), "Type %s does not have a long stack type", type);
            Object value = this.value;
            return value == null ? 0L : (Long) value;
        }

        @Override
        public final Object getObjectValue()
        {
            checkArgument(!type.getJavaType().isPrimitive(), "Type %s does not have an Object stack type", type);
            Object value = this.value;
            return value;
        }
    }
}
