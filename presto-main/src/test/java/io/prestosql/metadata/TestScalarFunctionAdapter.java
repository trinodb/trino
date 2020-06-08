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
package io.prestosql.metadata;

import com.google.common.base.Defaults;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.ScalarFunctionAdapter.NullAdaptationPolicy;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.block.BlockAssertions.assertBlockEquals;
import static io.prestosql.metadata.ScalarFunctionAdapter.NullAdaptationPolicy.RETURN_NULL_ON_NULL;
import static io.prestosql.metadata.ScalarFunctionAdapter.NullAdaptationPolicy.THROW_ON_NULL;
import static io.prestosql.metadata.ScalarFunctionAdapter.NullAdaptationPolicy.UNDEFINED_VALUE_FOR_NULL;
import static io.prestosql.metadata.ScalarFunctionAdapter.NullAdaptationPolicy.UNSUPPORTED;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TypeUtils.writeNativeValue;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Fail.fail;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestScalarFunctionAdapter
{
    private static final ArrayType ARRAY_TYPE = new ArrayType(BIGINT);
    private static final List<Type> ARGUMENT_TYPES = ImmutableList.of(BOOLEAN, BIGINT, DOUBLE, VARCHAR, ARRAY_TYPE);

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
        verifyAllAdaptations(actualConvention, methodName, RETURN_NULL_ON_NULL);
        verifyAllAdaptations(actualConvention, methodName, UNDEFINED_VALUE_FOR_NULL);
        verifyAllAdaptations(actualConvention, methodName, THROW_ON_NULL);
        verifyAllAdaptations(actualConvention, methodName, UNSUPPORTED);
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
        verifyAllAdaptations(actualConvention, methodName, UNSUPPORTED);
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
        verifyAllAdaptations(actualConvention, methodName, UNSUPPORTED);
    }

    private static void verifyAllAdaptations(
            InvocationConvention actualConvention,
            String methodName,
            NullAdaptationPolicy nullAdaptationPolicy)
            throws Throwable
    {
        MethodType type = methodType(actualConvention.getReturnConvention() == FAIL_ON_NULL ? boolean.class : Boolean.class, toCallArgumentTypes(actualConvention));
        MethodHandle methodHandle = lookup().findVirtual(Target.class, methodName, type);
        verifyAllAdaptations(actualConvention, methodHandle, nullAdaptationPolicy);
    }

    private static void verifyAllAdaptations(
            InvocationConvention actualConvention,
            MethodHandle methodHandle,
            NullAdaptationPolicy nullAdaptationPolicy)
            throws Throwable
    {
        List<List<InvocationArgumentConvention>> allArgumentConventions = allCombinations(
                ImmutableList.of(NEVER_NULL, BOXED_NULLABLE, NULL_FLAG, BLOCK_POSITION),
                ARGUMENT_TYPES.size());
        for (List<InvocationArgumentConvention> argumentConventions : allArgumentConventions) {
            for (InvocationReturnConvention returnConvention : InvocationReturnConvention.values()) {
                InvocationConvention expectedConvention = new InvocationConvention(argumentConventions, returnConvention, false, true);
                adaptAndVerify(
                        methodHandle,
                        actualConvention,
                        expectedConvention,
                        nullAdaptationPolicy);
            }
        }
    }

    private static void adaptAndVerify(
            MethodHandle methodHandle,
            InvocationConvention actualConvention,
            InvocationConvention expectedConvention,
            NullAdaptationPolicy nullAdaptationPolicy)
            throws Throwable
    {
        ScalarFunctionAdapter scalarFunctionAdapter = new ScalarFunctionAdapter(nullAdaptationPolicy);
        MethodHandle adaptedMethodHandle = null;
        try {
            adaptedMethodHandle = scalarFunctionAdapter.adapt(
                    methodHandle,
                    ARGUMENT_TYPES,
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
                actualConvention.supportsInstanceFactor());

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
            List<Object> argumentValues = toCallArgumentValues(newCallingConvention, nullArguments, target);
            try {
                Boolean result = (Boolean) exactInvoker.invokeWithArguments(argumentValues);
                if (result == null) {
                    assertEquals(nullAdaptationPolicy, RETURN_NULL_ON_NULL);
                }
                else {
                    assertTrue(result);
                }
            }
            catch (PrestoException prestoException) {
                if (nullAdaptationPolicy == UNSUPPORTED) {
                    // never null is allowed to be converted to block and position, but will throw if value is null
                    assertTrue(hasNullBlockAndPositionToNeverNullArgument(actualConvention, expectedConvention, nullArguments));
                }
                else {
                    assertTrue(nullAdaptationPolicy == THROW_ON_NULL || nullAdaptationPolicy == RETURN_NULL_ON_NULL);
                }
                assertEquals(prestoException.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
            }
            target.verify(actualConvention, nullArguments, nullAdaptationPolicy);
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

    private static List<Class<?>> toCallArgumentTypes(InvocationConvention callingConvention)
    {
        List<Class<?>> expectedArguments = new ArrayList<>();
        for (int i = 0; i < callingConvention.getArgumentConventions().size(); i++) {
            Type argumentType = ARGUMENT_TYPES.get(i);
            InvocationArgumentConvention argumentConvention = callingConvention.getArgumentConvention(i);
            switch (argumentConvention) {
                case NEVER_NULL:
                    expectedArguments.add(argumentType.getJavaType());
                    break;
                case BOXED_NULLABLE:
                    expectedArguments.add(Primitives.wrap(argumentType.getJavaType()));
                    break;
                case NULL_FLAG:
                    expectedArguments.add(argumentType.getJavaType());
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

    private static List<Object> toCallArgumentValues(InvocationConvention callingConvention, BitSet nullArguments, Target target)
    {
        List<Object> callArguments = new ArrayList<>();
        callArguments.add(target);
        for (int i = 0; i < callingConvention.getArgumentConventions().size(); i++) {
            Type argumentType = ARGUMENT_TYPES.get(i);
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
                    Verify.verify(testValue != null, "null can not be passed to a never null argument");
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
        if (argumentType.getJavaType() == boolean.class) {
            return true;
        }
        if (argumentType.getJavaType() == double.class) {
            return 33.33;
        }
        if (argumentType.getJavaType() == long.class) {
            return 42L;
        }
        if (argumentType.getJavaType() == Slice.class) {
            return Slices.utf8Slice("test");
        }
        if (argumentType.getJavaType() == Block.class) {
            BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, 4);
            blockBuilder.appendNull();
            blockBuilder.writeLong(99);
            blockBuilder.appendNull();
            blockBuilder.writeLong(100);
            return blockBuilder.build();
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
        private Boolean booleanValue;
        private Long longValue;
        private Double doubleValue;
        private Slice sliceValue;
        private Block blockValue;

        @UsedByGeneratedCode
        public boolean neverNull(boolean booleanValue, long longValue, double doubleValue, Slice sliceValue, Block blockValue)
        {
            checkState(!invoked, "Already invoked");
            invoked = true;

            this.booleanValue = booleanValue;
            this.longValue = longValue;
            this.doubleValue = doubleValue;
            this.sliceValue = sliceValue;
            this.blockValue = blockValue;
            return true;
        }

        @UsedByGeneratedCode
        public boolean boxedNull(Boolean booleanValue, Long longValue, Double doubleValue, Slice sliceValue, Block blockValue)
        {
            checkState(!invoked, "Already invoked");
            invoked = true;

            this.booleanValue = booleanValue;
            this.longValue = longValue;
            this.doubleValue = doubleValue;
            this.sliceValue = sliceValue;
            this.blockValue = blockValue;
            return true;
        }

        @UsedByGeneratedCode
        public boolean nullFlag(
                boolean booleanValue, boolean booleanNull,
                long longValue, boolean longNull,
                double doubleValue, boolean doubleNull,
                Slice sliceValue, boolean sliceNull,
                Block blockValue, boolean blockNull)
        {
            checkState(!invoked, "Already invoked");
            invoked = true;

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

        public void verify(
                InvocationConvention actualConvention,
                BitSet nullArguments,
                NullAdaptationPolicy nullAdaptationPolicy)
        {
            if (shouldFunctionBeInvoked(actualConvention, nullArguments, nullAdaptationPolicy)) {
                assertTrue(invoked, "function not invoked");
                assertArgumentValue(this.booleanValue, 0, actualConvention, nullArguments);
                assertArgumentValue(this.longValue, 1, actualConvention, nullArguments);
                assertArgumentValue(this.doubleValue, 2, actualConvention, nullArguments);
                assertArgumentValue(this.sliceValue, 3, actualConvention, nullArguments);
                assertArgumentValue(this.blockValue, 4, actualConvention, nullArguments);
            }
            else {
                assertFalse(invoked, "Function should not be invoked when null is passed to a NEVER_NULL argument and adaptation is " + nullAdaptationPolicy);
                assertNull(this.booleanValue);
                assertNull(this.longValue);
                assertNull(this.doubleValue);
                assertNull(this.sliceValue);
                assertNull(this.blockValue);
            }

            this.invoked = false;
            this.booleanValue = null;
            this.longValue = null;
            this.doubleValue = null;
            this.sliceValue = null;
            this.blockValue = null;
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
                BitSet nullArguments)
        {
            assertArgumentValue(actualValue, actualConvention.getArgumentConvention(index), ARGUMENT_TYPES.get(index), nullArguments.get(index));
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
    }
}
