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
package io.trino.operator.aggregation.minmaxn;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
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
import static org.testng.Assert.assertEquals;

public class TestTypedHeap
{
    private static final int INPUT_SIZE = 1_000_000; // larger than COMPACT_THRESHOLD_* to guarantee coverage of compact
    private static final int OUTPUT_SIZE = 1_000;

    private static final TypeOperators TYPE_OPERATOR_FACTORY = new TypeOperators();
    private static final MethodHandle MAX_ELEMENTS_COMPARATOR = TYPE_OPERATOR_FACTORY.getComparisonUnorderedFirstOperator(BIGINT, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));
    private static final MethodHandle MIN_ELEMENTS_COMPARATOR = TYPE_OPERATOR_FACTORY.getComparisonUnorderedLastOperator(BIGINT, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));

    @Test
    public void testAscending()
    {
        test(IntStream.range(0, INPUT_SIZE),
                false,
                MAX_ELEMENTS_COMPARATOR,
                BIGINT,
                IntStream.range(INPUT_SIZE - OUTPUT_SIZE, INPUT_SIZE).iterator());
        test(IntStream.range(0, INPUT_SIZE),
                true,
                MIN_ELEMENTS_COMPARATOR,
                BIGINT,
                IntStream.range(0, OUTPUT_SIZE).map(x -> OUTPUT_SIZE - 1 - x).iterator());
    }

    @Test
    public void testDescending()
    {
        test(IntStream.range(0, INPUT_SIZE).map(x -> INPUT_SIZE - 1 - x),
                false,
                MAX_ELEMENTS_COMPARATOR,
                BIGINT,
                IntStream.range(INPUT_SIZE - OUTPUT_SIZE, INPUT_SIZE).iterator());
        test(IntStream.range(0, INPUT_SIZE).map(x -> INPUT_SIZE - 1 - x),
                true,
                MIN_ELEMENTS_COMPARATOR,
                BIGINT,
                IntStream.range(0, OUTPUT_SIZE).map(x -> OUTPUT_SIZE - 1 - x).iterator());
    }

    @Test
    public void testShuffled()
    {
        List<Integer> list = IntStream.range(0, INPUT_SIZE).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        Collections.shuffle(list);
        test(list.stream().mapToInt(Integer::intValue),
                false,
                MAX_ELEMENTS_COMPARATOR,
                BIGINT,
                IntStream.range(INPUT_SIZE - OUTPUT_SIZE, INPUT_SIZE).iterator());
        test(list.stream().mapToInt(Integer::intValue),
                true,
                MIN_ELEMENTS_COMPARATOR,
                BIGINT,
                IntStream.range(0, OUTPUT_SIZE).map(x -> OUTPUT_SIZE - 1 - x).iterator());
    }

    private static void test(IntStream inputStream, boolean min, MethodHandle comparisonMethod, Type elementType, PrimitiveIterator.OfInt outputIterator)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, INPUT_SIZE);
        inputStream.forEach(value -> BIGINT.writeLong(blockBuilder, value));

        TypedHeap heap = new TypedHeap(min, comparisonMethod, elementType, OUTPUT_SIZE);
        heap.addAll(blockBuilder);

        BlockBuilder resultBlockBuilder = BIGINT.createBlockBuilder(null, OUTPUT_SIZE);
        heap.writeAll(resultBlockBuilder);

        Block resultBlock = resultBlockBuilder.build();
        assertEquals(resultBlock.getPositionCount(), OUTPUT_SIZE);
        for (int i = OUTPUT_SIZE - 1; i >= 0; i--) {
            assertEquals(BIGINT.getLong(resultBlock, i), outputIterator.nextInt());
        }
    }
}
