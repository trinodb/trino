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
import io.trino.spi.function.ScalarFunctionAdapter.NullAdaptationPolicy;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.block.TestingSession.SESSION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.ScalarFunctionAdapter.NullAdaptationPolicy.RETURN_NULL_ON_NULL;
import static io.trino.spi.function.ScalarFunctionAdapter.NullAdaptationPolicy.THROW_ON_NULL;
import static io.trino.spi.function.ScalarFunctionAdapter.NullAdaptationPolicy.UNDEFINED_VALUE_FOR_NULL;
import static io.trino.spi.function.ScalarFunctionAdapter.NullAdaptationPolicy.UNSUPPORTED;
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
import static org.assertj.core.api.Fail.fail;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestScalarFunctionAdapter
{
    private static final ArrayType ARRAY_TYPE = new ArrayType(BIGINT);
    private static final CharType CHAR_TYPE = createCharType(7);
    private static final TimestampType TIMESTAMP_TYPE = createTimestampType(9);
    private static final List<Type> ARGUMENT_TYPES = ImmutableList.of(BOOLEAN, BIGINT, DOUBLE, VARCHAR, ARRAY_TYPE);
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
        String methodName = "neverNull";
        verifyAllAdaptations(actualConvention, methodName, RETURN_NULL_ON_NULL, ARGUMENT_TYPES);
        verifyAllAdaptations(actualConvention, methodName, UNDEFINED_VALUE_FOR_NULL, ARGUMENT_TYPES);
        verifyAllAdaptations(actualConvention, methodName, THROW_ON_NULL, ARGUMENT_TYPES);
        verifyAllAdaptations(actualConvention, methodName, UNSUPPORTED, ARGUMENT_TYPES);
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
        String methodName = "neverNullObjects";
        verifyAllAdaptations(actualConvention, methodName, RETURN_NULL_ON_NULL, OBJECTS_ARGUMENT_TYPES);
        verifyAllAdaptations(actualConvention, methodName, UNDEFINED_VALUE_FOR_NULL, OBJECTS_ARGUMENT_TYPES);
        verifyAllAdaptations(actualConvention, methodName, THROW_ON_NULL, OBJECTS_ARGUMENT_TYPES);
        verifyAllAdaptations(actualConvention, methodName, UNSUPPORTED, OBJECTS_ARGUMENT_TYPES);
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
        String methodName = "boxedNull";
        verifyAllAdaptations(actualConvention, methodName, UNSUPPORTED, ARGUMENT_TYPES);
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
        String methodName = "boxedNullObjects";
        verifyAllAdaptations(actualConvention, methodName, UNSUPPORTED, OBJECTS_ARGUMENT_TYPES);
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
        String methodName = "nullFlag";
        verifyAllAdaptations(actualConvention, methodName, UNSUPPORTED, ARGUMENT_TYPES);
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
        String methodName = "nullFlagObjects";
        verifyAllAdaptations(actualConvention, methodName, UNSUPPORTED, OBJECTS_ARGUMENT_TYPES);
    }

    private static void verifyAllAdaptations(
            InvocationConvention actualConvention,
            String methodName,
            NullAdaptationPolicy nullAdaptationPolicy,
            List<Type> argumentTypes)
            throws Throwable
    {
        MethodType type = methodType(actualConvention.getReturnConvention() == FAIL_ON_NULL ? boolean.class : Boolean.class, toCallArgumentTypes(actualConvention, argumentTypes));
        MethodHandle methodHandle = lookup().findVirtual(Target.class, methodName, type);
        verifyAllAdaptations(actualConvention, methodHandle, nullAdaptationPolicy, argumentTypes);
    }

    private static void verifyAllAdaptations(
            InvocationConvention actualConvention,
            MethodHandle methodHandle,
            NullAdaptationPolicy nullAdaptationPolicy,
            List<Type> argumentTypes)
            throws Throwable
    {
        List<List<InvocationArgumentConvention>> allArgumentConventions = allCombinations(
                ImmutableList.of(NEVER_NULL, BOXED_NULLABLE, NULL_FLAG, BLOCK_POSITION),
                argumentTypes.size());
        for (List<InvocationArgumentConvention> argumentConventions : allArgumentConventions) {
            for (InvocationReturnConvention returnConvention : InvocationReturnConvention.values()) {
                InvocationConvention expectedConvention = new InvocationConvention(argumentConventions, returnConvention, false, true);
                adaptAndVerify(
                        methodHandle,
                        actualConvention,
                        expectedConvention,
                        nullAdaptationPolicy, argumentTypes);
            }
        }
    }

    private static void adaptAndVerify(
            MethodHandle methodHandle,
            InvocationConvention actualConvention,
            InvocationConvention expectedConvention,
            NullAdaptationPolicy nullAdaptationPolicy,
            List<Type> argumentTypes)
            throws Throwable
    {
        ScalarFunctionAdapter scalarFunctionAdapter = new ScalarFunctionAdapter(nullAdaptationPolicy);
        MethodHandle adaptedMethodHandle = null;
        try {
            adaptedMethodHandle = scalarFunctionAdapter.adapt(
                    methodHandle,
                    argumentTypes,
                    actualConvention,
                    expectedConvention);
            assertTrue(scalarFunctionAdapter.canAdapt(actualConvention, expectedConvention));
        }
        catch (IllegalArgumentException e) {
            assertFalse(scalarFunctionAdapter.canAdapt(actualConvention, expectedConvention));
            assertTrue(nullAdaptationPolicy == UNSUPPORTED || (nullAdaptationPolicy == RETURN_NULL_ON_NULL && expectedConvention.getReturnConvention() == FAIL_ON_NULL));
            if (hasNullableToNoNullableAdaptation(actualConvention, expectedConvention)) {
                return;
            }
            fail("Adaptation failed but no illegal conversions found", e);
        }

        InvocationConvention newCallingConvention = new InvocationConvention(
                expectedConvention.getArgumentConventions(),
                expectedConvention.getReturnConvention(),
                actualConvention.supportsSession(),
                actualConvention.supportsInstanceFactory());

        // crete an exact invoker to the handle, so we can use object invoke interface without type coercion concerns
        MethodHandle exactInvoker = MethodHandles.exactInvoker(adaptedMethodHandle.type())
                .bindTo(adaptedMethodHandle);
        exactInvoker = MethodHandles.explicitCastArguments(exactInvoker, exactInvoker.type().changeReturnType(Boolean.class));

        // try all combinations of null and not null arguments
        for (int notNullMask = 0; notNullMask < (1 << actualConvention.getArgumentConventions().size()); notNullMask++) {
            BitSet nullArguments = BitSet.valueOf(new long[] {notNullMask});
            if (!canCallConventionWithNullArguments(expectedConvention, nullArguments)) {
                continue;
            }
            Target target = new Target();
            List<Object> argumentValues = toCallArgumentValues(newCallingConvention, nullArguments, target, argumentTypes);
            try {
                Boolean result = (Boolean) exactInvoker.invokeWithArguments(argumentValues);
                if (result == null) {
                    assertEquals(nullAdaptationPolicy, RETURN_NULL_ON_NULL);
                }
                else {
                    assertTrue(result);
                }
            }
            catch (TrinoException trinoException) {
                if (nullAdaptationPolicy == UNSUPPORTED) {
                    // never null is allowed to be converted to block and position, but will throw if value is null
                    assertTrue(hasNullBlockAndPositionToNeverNullArgument(actualConvention, expectedConvention, nullArguments));
                }
                else {
                    assertTrue(nullAdaptationPolicy == THROW_ON_NULL || nullAdaptationPolicy == RETURN_NULL_ON_NULL);
                }
                assertEquals(trinoException.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
            }
            target.verify(actualConvention, nullArguments, nullAdaptationPolicy, argumentTypes);
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
        }
        return false;
    }

    private static boolean canCallConventionWithNullArguments(InvocationConvention convention, BitSet nullArguments)
    {
        for (int i = 0; i < convention.getArgumentConventions().size(); i++) {
            if (nullArguments.get(i) && convention.getArgumentConvention(i) == NEVER_NULL) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasNullBlockAndPositionToNeverNullArgument(InvocationConvention actualConvention, InvocationConvention expectedConvention, BitSet nullArguments)
    {
        for (int i = 0; i < actualConvention.getArgumentConventions().size(); i++) {
            if (nullArguments.get(i) && actualConvention.getArgumentConvention(i) == NEVER_NULL && expectedConvention.getArgumentConvention(i) == BLOCK_POSITION) {
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
                case NEVER_NULL:
                    expectedArguments.add(javaType);
                    break;
                case BOXED_NULLABLE:
                    expectedArguments.add(Primitives.wrap(javaType));
                    break;
                case NULL_FLAG:
                    expectedArguments.add(javaType);
                    expectedArguments.add(boolean.class);
                    break;
                case BLOCK_POSITION:
                    expectedArguments.add(Block.class);
                    expectedArguments.add(int.class);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported argument convention: " + argumentConvention);
            }
        }
        return expectedArguments;
    }

    private static List<Object> toCallArgumentValues(InvocationConvention callingConvention, BitSet nullArguments, Target target, List<Type> argumentTypes)
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
                case NEVER_NULL:
                    verify(testValue != null, "null can not be passed to a never null argument");
                    callArguments.add(testValue);
                    break;
                case BOXED_NULLABLE:
                    callArguments.add(testValue);
                    break;
                case NULL_FLAG:
                    callArguments.add(testValue == null ? Defaults.defaultValue(argumentType.getJavaType()) : testValue);
                    callArguments.add(testValue == null);
                    break;
                case BLOCK_POSITION:
                    BlockBuilder blockBuilder = argumentType.createBlockBuilder(null, 3);
                    blockBuilder.appendNull();
                    writeNativeValue(argumentType, blockBuilder, testValue);
                    blockBuilder.appendNull();

                    callArguments.add(blockBuilder.build());
                    callArguments.add(1);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported argument convention: " + argumentConvention);
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
            blockBuilder.writeLong(99);
            blockBuilder.appendNull();
            blockBuilder.writeLong(100);
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
        private Boolean booleanValue;
        private Long longValue;
        private Double doubleValue;
        private Slice sliceValue;
        private Block blockValue;
        private Object objectCharValue;
        private Object objectTimestampValue;

        public boolean neverNull(boolean booleanValue, long longValue, double doubleValue, Slice sliceValue, Block blockValue)
        {
            checkState(!invoked, "Already invoked");
            invoked = true;
            objectsMethod = false;

            this.booleanValue = booleanValue;
            this.longValue = longValue;
            this.doubleValue = doubleValue;
            this.sliceValue = sliceValue;
            this.blockValue = blockValue;
            return true;
        }

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

        public boolean boxedNull(Boolean booleanValue, Long longValue, Double doubleValue, Slice sliceValue, Block blockValue)
        {
            checkState(!invoked, "Already invoked");
            invoked = true;
            objectsMethod = false;

            this.booleanValue = booleanValue;
            this.longValue = longValue;
            this.doubleValue = doubleValue;
            this.sliceValue = sliceValue;
            this.blockValue = blockValue;
            return true;
        }

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

        public boolean nullFlag(
                boolean booleanValue, boolean booleanNull,
                long longValue, boolean longNull,
                double doubleValue, boolean doubleNull,
                Slice sliceValue, boolean sliceNull,
                Block blockValue, boolean blockNull)
        {
            checkState(!invoked, "Already invoked");
            invoked = true;
            objectsMethod = false;

            if (booleanNull) {
                assertFalse(booleanValue);
                this.booleanValue = null;
            }
            else {
                this.booleanValue = booleanValue;
            }

            if (longNull) {
                assertEquals(longValue, 0);
                this.longValue = null;
            }
            else {
                this.longValue = longValue;
            }

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

        public void verify(
                InvocationConvention actualConvention,
                BitSet nullArguments,
                NullAdaptationPolicy nullAdaptationPolicy,
                List<Type> argumentTypes)
        {
            if (shouldFunctionBeInvoked(actualConvention, nullArguments, nullAdaptationPolicy)) {
                assertTrue(invoked, "function not invoked");
                if (!objectsMethod) {
                    assertArgumentValue(this.booleanValue, 0, actualConvention, nullArguments, argumentTypes);
                    assertArgumentValue(this.longValue, 1, actualConvention, nullArguments, argumentTypes);
                    assertArgumentValue(this.doubleValue, 2, actualConvention, nullArguments, argumentTypes);
                    assertArgumentValue(this.sliceValue, 3, actualConvention, nullArguments, argumentTypes);
                    assertArgumentValue(this.blockValue, 4, actualConvention, nullArguments, argumentTypes);
                }
                else {
                    assertArgumentValue(this.sliceValue, 0, actualConvention, nullArguments, argumentTypes);
                    assertArgumentValue(this.blockValue, 1, actualConvention, nullArguments, argumentTypes);
                    assertArgumentValue(this.objectCharValue, 2, actualConvention, nullArguments, argumentTypes);
                    assertArgumentValue(this.objectTimestampValue, 3, actualConvention, nullArguments, argumentTypes);
                }
            }
            else {
                assertFalse(invoked, "Function should not be invoked when null is passed to a NEVER_NULL argument and adaptation is " + nullAdaptationPolicy);
                assertNull(this.booleanValue);
                assertNull(this.longValue);
                assertNull(this.doubleValue);
                assertNull(this.sliceValue);
                assertNull(this.blockValue);
                assertNull(this.objectCharValue);
                assertNull(this.objectTimestampValue);
            }

            this.invoked = false;
            this.objectsMethod = false;
            this.booleanValue = null;
            this.longValue = null;
            this.doubleValue = null;
            this.sliceValue = null;
            this.blockValue = null;
            this.objectCharValue = null;
            this.objectTimestampValue = null;
        }

        private static boolean shouldFunctionBeInvoked(InvocationConvention actualConvention, BitSet nullArguments, NullAdaptationPolicy nullAdaptationPolicy)
        {
            if (nullAdaptationPolicy == UNDEFINED_VALUE_FOR_NULL) {
                return true;
            }

            for (int i = 0; i < actualConvention.getArgumentConventions().size(); i++) {
                if (actualConvention.getArgumentConvention(i) == NEVER_NULL && nullArguments.get(i)) {
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

            if (argumentConvention != NEVER_NULL) {
                assertNull(actualValue);
                return;
            }

            // the only way for a never null to be called with a null is for the undefined value null convention
            // Currently, for primitives, the value is the java default, but for all other types it could be anything
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
}
