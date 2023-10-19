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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeUtils;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestTypedHeap
{
    private static final int INPUT_SIZE = 1_000_000;
    private static final int OUTPUT_SIZE = 1_000;

    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Test
    public void testAscending()
    {
        test(IntStream.range(0, INPUT_SIZE).boxed().toList());
    }

    @Test
    public void testDescending()
    {
        test(IntStream.range(0, INPUT_SIZE).map(x -> INPUT_SIZE - 1 - x).boxed().toList());
    }

    @Test
    public void testShuffled()
    {
        List<Integer> list = IntStream.range(0, INPUT_SIZE).boxed().collect(Collectors.toList());
        Collections.shuffle(list);
        test(list);
    }

    private static void test(List<Integer> testData)
    {
        test(BIGINT, testData.stream().map(Long::valueOf).toList(), Comparator.naturalOrder());

        // convert data to text numbers, which will not be sorted numerically
        List<Slice> sliceData = testData.stream()
                .map(String::valueOf)
                .map(value -> value + " ".repeat(22) + "x") // ensure value is longer than 12 bytes
                .map(Slices::utf8Slice)
                .toList();
        test(VARCHAR, sliceData, Comparator.naturalOrder());
    }

    @Test
    public void testEmptyVariableWidth()
    {
        test(VARBINARY, Collections.nCopies(INPUT_SIZE, Slices.EMPTY_SLICE), Comparator.naturalOrder());
    }

    private static <T> void test(Type type, List<T> testData, Comparator<T> comparator)
    {
        test(type, true, testData, comparator);
        test(type, false, testData, comparator);
    }

    private static <T> void test(Type type, boolean min, List<T> testData, Comparator<T> comparator)
    {
        MethodHandle readFlat = TYPE_OPERATORS.getReadValueOperator(type, simpleConvention(BLOCK_BUILDER, FLAT));
        MethodHandle writeFlat = TYPE_OPERATORS.getReadValueOperator(type, simpleConvention(FLAT_RETURN, BLOCK_POSITION_NOT_NULL));

        MethodHandle comparisonFlatFlat;
        MethodHandle comparisonFlatBlock;
        if (min) {
            comparisonFlatFlat = TYPE_OPERATORS.getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, FLAT));
            comparisonFlatBlock = TYPE_OPERATORS.getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, BLOCK_POSITION_NOT_NULL));
        }
        else {
            comparisonFlatFlat = TYPE_OPERATORS.getComparisonUnorderedFirstOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, FLAT));
            comparisonFlatBlock = TYPE_OPERATORS.getComparisonUnorderedFirstOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, BLOCK_POSITION_NOT_NULL));
            comparator = comparator.reversed();
        }

        Block expected = toBlock(type, testData.stream().sorted(comparator).limit(OUTPUT_SIZE).toList());
        Block inputData = toBlock(type, testData);

        // verify basic build
        TypedHeap heap = new TypedHeap(min, readFlat, writeFlat, comparisonFlatFlat, comparisonFlatBlock, type, OUTPUT_SIZE);
        heap.addAll(inputData);
        assertEqual(heap, type, expected);

        // verify copy constructor
        assertEqual(new TypedHeap(heap), type, expected);

        // build in two parts and merge together
        TypedHeap part1 = new TypedHeap(min, readFlat, writeFlat, comparisonFlatFlat, comparisonFlatBlock, type, OUTPUT_SIZE);
        part1.addAll(inputData.getRegion(0, inputData.getPositionCount() / 2));
        BlockBuilder part1BlockBuilder = type.createBlockBuilder(null, part1.getCapacity());
        part1.writeAllUnsorted(part1BlockBuilder);
        Block part1Block = part1BlockBuilder.build();

        TypedHeap part2 = new TypedHeap(min, readFlat, writeFlat, comparisonFlatFlat, comparisonFlatBlock, type, OUTPUT_SIZE);
        part2.addAll(inputData.getRegion(inputData.getPositionCount() / 2, inputData.getPositionCount() - (inputData.getPositionCount() / 2)));
        BlockBuilder part2BlockBuilder = type.createBlockBuilder(null, part2.getCapacity());
        part2.writeAllUnsorted(part2BlockBuilder);
        Block part2Block = part2BlockBuilder.build();

        TypedHeap merged = new TypedHeap(min, readFlat, writeFlat, comparisonFlatFlat, comparisonFlatBlock, type, OUTPUT_SIZE);
        merged.addAll(part1Block);
        merged.addAll(part2Block);
        assertEqual(merged, type, expected);
    }

    private static void assertEqual(TypedHeap heap, Type type, Block expected)
    {
        BlockBuilder resultBlockBuilder = type.createBlockBuilder(null, OUTPUT_SIZE);
        heap.writeAllSorted(resultBlockBuilder);
        Block actual = resultBlockBuilder.build();
        assertBlockEquals(type, actual, expected);
    }

    private static <T> Block toBlock(Type type, List<T> inputStream)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, INPUT_SIZE);
        inputStream.forEach(value -> TypeUtils.writeNativeValue(type, blockBuilder, value));
        return blockBuilder.build();
    }
}
