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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.operator.aggregation.AggregationLoopBuilder.buildLoop;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAggregationLoopBuilder
{
    private static final MethodHandle INPUT_FUNCTION;
    private static final Object LAMBDA_A = "lambda a";
    private static final Object LAMBDA_B = 1234L;

    static {
        try {
            INPUT_FUNCTION = lookup().findStatic(
                    TestAggregationLoopBuilder.class,
                    "input",
                    methodType(void.class, InvocationList.class, ValueBlock.class, int.class, ValueBlock.class, int.class, Object.class, Object.class));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private MethodHandle loop;
    private List<TestParameter> keyBlocks;
    private List<TestParameter> valueBlocks;

    @BeforeClass
    public void setUp()
            throws ReflectiveOperationException
    {
        loop = buildLoop(INPUT_FUNCTION, 1, 2, false);

        ValueBlock keyBasic = new IntArrayBlock(5, Optional.empty(), new int[] {10, 11, 12, 13, 14});
        ValueBlock keyRleValue = new IntArrayBlock(1, Optional.empty(), new int[] {33});
        ValueBlock keyDictionary = new IntArrayBlock(3, Optional.empty(), new int[] {55, 54, 53});

        keyBlocks = ImmutableList.<TestParameter>builder()
                .add(new TestParameter(keyBasic, keyBasic, new int[] {0, 1, 2, 3, 4}))
                .add(new TestParameter(RunLengthEncodedBlock.create(keyRleValue, 5), keyRleValue, new int[] {0, 0, 0, 0, 0}))
                .add(new TestParameter(DictionaryBlock.create(7, keyDictionary, new int[] {9, 9, 2, 1, 0, 1, 2}).getRegion(2, 5), keyDictionary, new int[] {2, 1, 0, 1, 2}))
                .build();

        ValueBlock valueBasic = new IntArrayBlock(5, Optional.empty(), new int[] {10, 11, 12, 13, 14});
        ValueBlock valueRleValue = new IntArrayBlock(1, Optional.empty(), new int[] {44});
        ValueBlock valueDictionary = new IntArrayBlock(3, Optional.empty(), new int[] {66, 65, 64});

        valueBlocks = ImmutableList.<TestParameter>builder()
                .add(new TestParameter(valueBasic, valueBasic, new int[] {0, 1, 2, 3, 4}))
                .add(new TestParameter(RunLengthEncodedBlock.create(valueRleValue, 5), valueRleValue, new int[] {0, 0, 0, 0, 0}))
                .add(new TestParameter(DictionaryBlock.create(7, valueDictionary, new int[] {9, 9, 0, 1, 2, 1, 0}).getRegion(2, 5), valueDictionary, new int[] {0, 1, 2, 1, 0}))
                .build();
    }

    @Test
    public void testSelectAll()
            throws Throwable
    {
        AggregationMask mask = AggregationMask.createSelectAll(5);
        for (TestParameter keyBlock : keyBlocks) {
            for (TestParameter valueBlock : valueBlocks) {
                InvocationList invocationList = new InvocationList();
                loop.invokeExact(mask, invocationList, keyBlock.inputBlock(), valueBlock.inputBlock(), LAMBDA_A, LAMBDA_B);
                assertThat(invocationList.getInvocations()).isEqualTo(buildExpectedInvocation(keyBlock, valueBlock, mask).getInvocations());
            }
        }
    }

    @Test
    public void testMasked()
            throws Throwable
    {
        AggregationMask mask = AggregationMask.createSelectedPositions(5, new int[] {1, 2, 4}, 3);
        for (TestParameter keyBlock : keyBlocks) {
            for (TestParameter valueBlock : valueBlocks) {
                InvocationList invocationList = new InvocationList();
                loop.invokeExact(mask, invocationList, keyBlock.inputBlock(), valueBlock.inputBlock(), LAMBDA_A, LAMBDA_B);
                assertThat(invocationList.getInvocations()).isEqualTo(buildExpectedInvocation(keyBlock, valueBlock, mask).getInvocations());
            }
        }
    }

    private static InvocationList buildExpectedInvocation(TestParameter keyBlock, TestParameter valueBlock, AggregationMask mask)
    {
        InvocationList invocationList = new InvocationList();
        int[] keyPositions = keyBlock.invokedPositions();
        int[] valuePositions = valueBlock.invokedPositions();
        if (mask.isSelectAll()) {
            for (int position = 0; position < keyPositions.length; position++) {
                invocationList.add(keyBlock.invokedBlock(), keyPositions[position], valueBlock.invokedBlock(), valuePositions[position], LAMBDA_A, LAMBDA_B);
            }
        }
        else {
            int[] selectedPositions = mask.getSelectedPositions();
            for (int i = 0; i < mask.getSelectedPositionCount(); i++) {
                int position = selectedPositions[i];
                invocationList.add(keyBlock.invokedBlock(), keyPositions[position], valueBlock.invokedBlock(), valuePositions[position], LAMBDA_A, LAMBDA_B);
            }
        }
        return invocationList;
    }

    @SuppressWarnings("UnusedVariable")
    private record TestParameter(Block inputBlock, ValueBlock invokedBlock, int[] invokedPositions) {}

    public static void input(
            @AggregationState InvocationList invocationList,
            @BlockPosition @SqlType("K") ValueBlock keyBlock,
            @BlockIndex int keyPosition,
            @SqlNullable @BlockPosition @SqlType("V") ValueBlock valueBlock,
            @BlockIndex int valuePosition,
            Object lambdaA,
            Object lambdaB)
    {
        invocationList.add(keyBlock, keyPosition, valueBlock, valuePosition, lambdaA, lambdaB);
    }

    public static class InvocationList
    {
        private final List<Invocation> invocations = new ArrayList<>();

        public void add(ValueBlock keyBlock, int keyPosition, ValueBlock valueBlock, int valuePosition, Object lambdaA, Object lambdaB)
        {
            invocations.add(new Invocation(keyBlock, keyPosition, valueBlock, valuePosition, lambdaA, lambdaB));
        }

        public List<Invocation> getInvocations()
        {
            return ImmutableList.copyOf(invocations);
        }

        public record Invocation(ValueBlock keyBlock, int keyPosition, ValueBlock valueBlock, int valuePosition, Object lambdaA, Object lambdaB) {}
    }
}
