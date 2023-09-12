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
package io.trino.operator.aggregation.minmaxbyn;

import com.google.common.collect.ImmutableList;
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
import java.util.stream.LongStream;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Comparator.comparing;

public class TestTypedKeyValueHeap
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
        test(BIGINT,
                BIGINT,
                testData.stream()
                        .map(Long::valueOf)
                        .map(value -> new Entry<>(value, value))
                        .toList(),
                Comparator.naturalOrder(), OUTPUT_SIZE);

        test(BIGINT,
                VARCHAR,
                testData.stream()
                        .map(Long::valueOf)
                        .map(value -> new Entry<>(value, utf8Slice(value.toString())))
                        .toList(),
                Comparator.naturalOrder(), OUTPUT_SIZE);

        test(VARCHAR,
                BIGINT,
                testData.stream()
                        .map(Long::valueOf)
                        .map(value -> new Entry<>(utf8Slice(value.toString()), value))
                        .toList(),
                Comparator.naturalOrder(), OUTPUT_SIZE);

        test(VARCHAR,
                VARCHAR,
                testData.stream()
                        .map(String::valueOf)
                        .map(Slices::utf8Slice)
                        .map(value -> new Entry<>(value, value))
                        .toList(), Comparator.naturalOrder(), OUTPUT_SIZE);
    }

    @Test
    public void testEmptyVariableWidth()
    {
        test(VARCHAR,
                VARCHAR,
                Collections.nCopies(INPUT_SIZE, new Entry<>(EMPTY_SLICE, EMPTY_SLICE)),
                Comparator.naturalOrder(), OUTPUT_SIZE);
    }

    @Test
    public void testNulls()
    {
        test(BIGINT,
                BIGINT,
                LongStream.range(0, 10).boxed()
                        .map(value -> new Entry<>(value, value == 5 ? null : value))
                        .toList(),
                Comparator.naturalOrder(), OUTPUT_SIZE);
    }

    @Test
    public void testX()
    {
        test(VARCHAR,
                DOUBLE,
                ImmutableList.<Entry<Slice, Double>>builder()
                        .add(new Entry<>(utf8Slice("z"), 1.0))
                        .add(new Entry<>(utf8Slice("a"), 2.0))
                        .add(new Entry<>(utf8Slice("x"), 2.0))
                        .add(new Entry<>(utf8Slice("b"), 3.0))
                        .build(),
                Comparator.naturalOrder(),
                2);
    }

    private static <K, V> void test(Type keyType, Type valueType, List<Entry<K, V>> testData, Comparator<K> comparator, int capacity)
    {
        test(keyType, valueType, true, testData, comparator, capacity);
        test(keyType, valueType, false, testData, comparator, capacity);
    }

    private static <K, V> void test(Type keyType, Type valueType, boolean min, List<Entry<K, V>> testData, Comparator<K> comparator, int capacity)
    {
        MethodHandle keyReadFlat = TYPE_OPERATORS.getReadValueOperator(keyType, simpleConvention(BLOCK_BUILDER, FLAT));
        MethodHandle keyWriteFlat = TYPE_OPERATORS.getReadValueOperator(keyType, simpleConvention(FLAT_RETURN, BLOCK_POSITION_NOT_NULL));
        MethodHandle valueReadFlat = TYPE_OPERATORS.getReadValueOperator(valueType, simpleConvention(BLOCK_BUILDER, FLAT));
        MethodHandle valueWriteFlat = TYPE_OPERATORS.getReadValueOperator(valueType, simpleConvention(FLAT_RETURN, BLOCK_POSITION_NOT_NULL));
        MethodHandle comparisonFlatFlat;
        MethodHandle comparisonFlatBlock;
        if (min) {
            comparisonFlatFlat = TYPE_OPERATORS.getComparisonUnorderedLastOperator(keyType, simpleConvention(FAIL_ON_NULL, FLAT, FLAT));
            comparisonFlatBlock = TYPE_OPERATORS.getComparisonUnorderedLastOperator(keyType, simpleConvention(FAIL_ON_NULL, FLAT, BLOCK_POSITION_NOT_NULL));
        }
        else {
            comparisonFlatFlat = TYPE_OPERATORS.getComparisonUnorderedFirstOperator(keyType, simpleConvention(FAIL_ON_NULL, FLAT, FLAT));
            comparisonFlatBlock = TYPE_OPERATORS.getComparisonUnorderedFirstOperator(keyType, simpleConvention(FAIL_ON_NULL, FLAT, BLOCK_POSITION_NOT_NULL));
            comparator = comparator.reversed();
        }

        Block expected = toBlock(valueType, testData.stream()
                .sorted(comparing(Entry::key, comparator))
                .map(Entry::value)
                .limit(capacity)
                .toList());
        Block inputKeys = toBlock(keyType, testData.stream().map(Entry::key).toList());
        Block inputValues = toBlock(valueType, testData.stream().map(Entry::value).toList());

        // verify basic build
        TypedKeyValueHeap heap = new TypedKeyValueHeap(min, keyReadFlat, keyWriteFlat, valueReadFlat, valueWriteFlat, comparisonFlatFlat, comparisonFlatBlock, keyType, valueType, capacity);
        heap.addAll(inputKeys, inputValues);
        assertEqual(heap, valueType, expected);

        // verify copy constructor
        assertEqual(new TypedKeyValueHeap(heap), valueType, expected);

        // build in two parts and merge together
        TypedKeyValueHeap part1 = new TypedKeyValueHeap(min, keyReadFlat, keyWriteFlat, valueReadFlat, valueWriteFlat, comparisonFlatFlat, comparisonFlatBlock, keyType, valueType, capacity);
        int splitPoint = inputKeys.getPositionCount() / 2;
        part1.addAll(
                inputKeys.getRegion(0, splitPoint),
                inputValues.getRegion(0, splitPoint));
        BlockBuilder part1KeyBlockBuilder = keyType.createBlockBuilder(null, part1.getCapacity());
        BlockBuilder part1ValueBlockBuilder = valueType.createBlockBuilder(null, part1.getCapacity());
        part1.writeAllUnsorted(part1KeyBlockBuilder, part1ValueBlockBuilder);
        Block part1KeyBlock = part1KeyBlockBuilder.build();
        Block part1ValueBlock = part1ValueBlockBuilder.build();

        TypedKeyValueHeap part2 = new TypedKeyValueHeap(min, keyReadFlat, keyWriteFlat, valueReadFlat, valueWriteFlat, comparisonFlatFlat, comparisonFlatBlock, keyType, valueType, capacity);
        part2.addAll(
                inputKeys.getRegion(splitPoint, inputKeys.getPositionCount() - splitPoint),
                inputValues.getRegion(splitPoint, inputValues.getPositionCount() - splitPoint));
        BlockBuilder part2KeyBlockBuilder = keyType.createBlockBuilder(null, part2.getCapacity());
        BlockBuilder part2ValueBlockBuilder = valueType.createBlockBuilder(null, part2.getCapacity());
        part2.writeAllUnsorted(part2KeyBlockBuilder, part2ValueBlockBuilder);
        Block part2KeyBlock = part2KeyBlockBuilder.build();
        Block part2ValueBlock = part2ValueBlockBuilder.build();

        TypedKeyValueHeap merged = new TypedKeyValueHeap(min, keyReadFlat, keyWriteFlat, valueReadFlat, valueWriteFlat, comparisonFlatFlat, comparisonFlatBlock, keyType, valueType, capacity);
        merged.addAll(part1KeyBlock, part1ValueBlock);
        merged.addAll(part2KeyBlock, part2ValueBlock);
        assertEqual(merged, valueType, expected);
    }

    private static void assertEqual(TypedKeyValueHeap heap, Type valueType, Block expected)
    {
        BlockBuilder resultBlockBuilder = valueType.createBlockBuilder(null, OUTPUT_SIZE);
        heap.writeValuesSorted(resultBlockBuilder);
        Block actual = resultBlockBuilder.build();
        assertBlockEquals(valueType, actual, expected);
    }

    private static <T> Block toBlock(Type type, List<T> inputStream)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, INPUT_SIZE);
        inputStream.forEach(value -> TypeUtils.writeNativeValue(type, blockBuilder, value));
        return blockBuilder.build();
    }

    // TODO remove this suppression when the error prone checker actually supports records correctly
    // NOTE: this record supports null values, which is not supported by other Map.Entry implementations
    @SuppressWarnings("unused")
    private record Entry<K, V>(K key, V value) {}
}
