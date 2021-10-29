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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.TypeOperators;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.stream.IntStream;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.util.MinMaxCompare.getMinMaxCompare;
import static org.testng.Assert.assertEquals;

public class TestTypedHeap
{
    private static final int INPUT_SIZE = 1_000_000; // larger than COMPACT_THRESHOLD_* to guarantee coverage of compact
    private static final int OUTPUT_SIZE = 1_000;

    private static final TypeOperators TYPE_OPERATOR_FACTORY = new TypeOperators();
    private static final MethodHandle MAX_ELEMENTS_COMPARATOR = getMinMaxCompare(TYPE_OPERATOR_FACTORY, BIGINT, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION), false);
    private static final MethodHandle MIN_ELEMENTS_COMPARATOR = getMinMaxCompare(TYPE_OPERATOR_FACTORY, BIGINT, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION), true);

    @Test
    public void testAscending()
    {
        test(IntStream.range(0, INPUT_SIZE),
                MAX_ELEMENTS_COMPARATOR,
                IntStream.range(INPUT_SIZE - OUTPUT_SIZE, INPUT_SIZE).iterator());
        test(IntStream.range(0, INPUT_SIZE),
                MIN_ELEMENTS_COMPARATOR,
                IntStream.range(0, OUTPUT_SIZE).map(x -> OUTPUT_SIZE - 1 - x).iterator());
    }

    @Test
    public void testDescending()
    {
        test(IntStream.range(0, INPUT_SIZE).map(x -> INPUT_SIZE - 1 - x),
                MAX_ELEMENTS_COMPARATOR,
                IntStream.range(INPUT_SIZE - OUTPUT_SIZE, INPUT_SIZE).iterator());
        test(IntStream.range(0, INPUT_SIZE).map(x -> INPUT_SIZE - 1 - x),
                MIN_ELEMENTS_COMPARATOR,
                IntStream.range(0, OUTPUT_SIZE).map(x -> OUTPUT_SIZE - 1 - x).iterator());
    }

    @Test
    public void testShuffled()
    {
        List<Integer> list = IntStream.range(0, INPUT_SIZE).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        Collections.shuffle(list);
        test(list.stream().mapToInt(Integer::intValue),
                MAX_ELEMENTS_COMPARATOR,
                IntStream.range(INPUT_SIZE - OUTPUT_SIZE, INPUT_SIZE).iterator());
        test(list.stream().mapToInt(Integer::intValue),
                MIN_ELEMENTS_COMPARATOR,
                IntStream.range(0, OUTPUT_SIZE).map(x -> OUTPUT_SIZE - 1 - x).iterator());
    }

    private static void test(IntStream inputStream, MethodHandle greaterThanMethod, PrimitiveIterator.OfInt outputIterator)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, INPUT_SIZE);
        inputStream.forEach(x -> BIGINT.writeLong(blockBuilder, x));

        TypedHeap heap = new TypedHeap(greaterThanMethod, BIGINT, OUTPUT_SIZE);
        heap.addAll(blockBuilder);

        BlockBuilder resultBlockBuilder = BIGINT.createBlockBuilder(null, OUTPUT_SIZE);
        heap.popAll(resultBlockBuilder);

        Block resultBlock = resultBlockBuilder.build();
        assertEquals(resultBlock.getPositionCount(), OUTPUT_SIZE);
        for (int i = 0; i < OUTPUT_SIZE; i++) {
            assertEquals(BIGINT.getLong(resultBlock, i), outputIterator.nextInt());
        }
    }
}
