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
package io.trino.operator.project;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static io.trino.operator.project.SelectedPositions.positionsList;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.toDataProvider;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSelectedPositions
{
    private static final Random RANDOM = new Random(38844897);
    private static final SelectedPositions EMPTY_LIST = positionsList(new int[10], 5, 0);
    private static final SelectedPositions EMPTY_RANGE = positionsRange(13, 0);
    private static final SelectedPositions NON_EMPTY_LIST = positionsList(new int[10], 5, 5);
    private static final SelectedPositions NON_EMPTY_RANGE = positionsRange(13, 7);

    @Test
    public void testEmpty()
    {
        assertThat(EMPTY_LIST.union(EMPTY_LIST)).isEqualTo(EMPTY_LIST);
        assertThat(EMPTY_LIST.union(EMPTY_RANGE)).isEqualTo(EMPTY_LIST);
        assertThat(EMPTY_RANGE.union(EMPTY_LIST)).isEqualTo(EMPTY_RANGE);
        assertThat(EMPTY_RANGE.union(EMPTY_RANGE)).isEqualTo(EMPTY_RANGE);

        assertThat(EMPTY_LIST.union(NON_EMPTY_LIST)).isEqualTo(NON_EMPTY_LIST);
        assertThat(EMPTY_LIST.union(NON_EMPTY_RANGE)).isEqualTo(NON_EMPTY_RANGE);
        assertThat(NON_EMPTY_LIST.union(EMPTY_LIST)).isEqualTo(NON_EMPTY_LIST);
        assertThat(NON_EMPTY_RANGE.union(EMPTY_LIST)).isEqualTo(NON_EMPTY_RANGE);
        assertThat(EMPTY_RANGE.union(NON_EMPTY_RANGE)).isEqualTo(NON_EMPTY_RANGE);
        assertThat(NON_EMPTY_RANGE.union(EMPTY_LIST)).isEqualTo(NON_EMPTY_RANGE);

        assertThat(EMPTY_LIST.difference(NON_EMPTY_LIST)).isEqualTo(EMPTY_LIST);
        assertThat(NON_EMPTY_LIST.difference(EMPTY_LIST)).isEqualTo(NON_EMPTY_LIST);
        assertThat(EMPTY_RANGE.difference(NON_EMPTY_RANGE)).isEqualTo(EMPTY_RANGE);
        assertThat(NON_EMPTY_RANGE.difference(EMPTY_RANGE)).isEqualTo(NON_EMPTY_RANGE);
    }

    @ParameterizedTest
    @MethodSource("inputSizes")
    public void testListsUnionAndDifference(int size, int maxGroupSize)
    {
        int[] positions = generateList(size, maxGroupSize);
        // no overlap
        SelectedPositions listA = positionsList(positions, 0, size / 2);
        SelectedPositions listB = positionsList(positions, size / 2, size - (size / 2));
        assertUnionAndDifference(listA, listB);

        // full overlap
        assertUnionAndDifference(listA, listA);
        assertUnionAndDifference(listB, listB);

        // partial overlap
        listA = positionsList(positions, 0, (3 * size) / 4);
        listB = positionsList(positions, size / 4, size - (size / 4));
        assertUnionAndDifference(listA, listB);

        // subset
        listB = positionsList(positions, size / 4, size / 4);
        assertUnionAndDifference(listA, listB);
    }

    @ParameterizedTest
    @MethodSource("inputSizes")
    public void testListRangeUnionAndDifference(int size, int maxGroupSize)
    {
        int[] positions = generateList(size, maxGroupSize);
        // list fully after range
        SelectedPositions list = positionsList(positions, size / 2, size / 2);
        SelectedPositions range = positionsRange(0, positions[size / 4]);
        assertUnionAndDifference(list, range);

        // list fully before range
        list = positionsList(positions, 0, size);
        range = positionsRange(list.getPositions()[list.getOffset() + list.size()] + 1, size);
        assertUnionAndDifference(list, range);

        // partial overlap
        list = positionsList(positions, size / 4, size / 2);
        range = positionsRange(positions[0], list.getPositions()[list.size() - 1] - positions[0]);
        assertUnionAndDifference(list, range);
        range = positionsRange(list.getPositions()[list.getOffset()], positions[size - 1] - list.getPositions()[list.getOffset()]);
        assertUnionAndDifference(list, range);

        // subset
        list = positionsList(positions, 0, size);
        range = positionsRange(positions[0], positions[size - 1] - positions[0]);
        assertUnionAndDifference(list, range);

        // full overlap
        for (int position = 0; position < size; position++) {
            positions[position] = position;
        }
        list = positionsList(positions, 0, size);
        range = positionsRange(0, size);
        assertUnionAndDifference(list, range);
    }

    @Test
    public void testRangesUnionAndDifference()
    {
        // no overlap
        SelectedPositions rangeA = positionsRange(0, 1024);
        SelectedPositions rangeB = positionsRange(2000, 100);
        assertUnionAndDifference(rangeA, rangeB);

        // full overlap
        assertUnionAndDifference(rangeA, rangeA);
        assertUnionAndDifference(rangeB, rangeB);

        // partial overlap
        rangeA = positionsRange(0, 1024);
        rangeB = positionsRange(1000, 1000);
        assertUnionAndDifference(rangeA, rangeB);

        // subset
        rangeB = positionsRange(300, 700);
        assertUnionAndDifference(rangeA, rangeB);
    }

    private static Object[][] inputSizes()
    {
        return cartesianProduct(
                Stream.of(1024, 8096, 65536).collect(toDataProvider()),
                Stream.of(1, 10, 100, 200, 500).collect(toDataProvider()));
    }

    private static int[] generateList(int size, int maxGroupSize)
    {
        IntArrayList groupedList = new IntArrayList();
        int position = 0;
        while (groupedList.size() < size) {
            boolean skip = RANDOM.nextBoolean();
            int groupSize = Math.min(RANDOM.nextInt(1, maxGroupSize + 1), size - groupedList.size());
            if (!skip) {
                for (int i = 0; i < groupSize; i++) {
                    groupedList.add(position + i);
                }
            }
            position += groupSize;
        }
        return groupedList.elements();
    }

    private static void assertUnionAndDifference(SelectedPositions positionsA, SelectedPositions positionsB)
    {
        assertUnion(positionsA, positionsB);
        assertUnion(positionsB, positionsA);
        assertDifference(positionsA, positionsB);
        assertDifference(positionsB, positionsA);
    }

    private static void assertUnion(SelectedPositions positionsA, SelectedPositions positionsB)
    {
        SelectedPositions result = positionsA.union(positionsB);
        assertThat(toSet(result)).isEqualTo(Sets.union(toSet(positionsA), toSet(positionsB)));
        if (result.isList()) {
            int[] activePositions = new int[result.size()];
            System.arraycopy(result.getPositions(), result.getOffset(), activePositions, 0, result.size());
            assertThat(activePositions).isSorted();
            assertThat(activePositions).doesNotHaveDuplicates();
        }
    }

    private static void assertDifference(SelectedPositions positionsA, SelectedPositions positionsB)
    {
        SelectedPositions result = positionsA.difference(positionsB);
        assertThat(toSet(result)).isEqualTo(Sets.difference(toSet(positionsA), toSet(positionsB)));
        if (result.isList()) {
            int[] activePositions = new int[result.size()];
            System.arraycopy(result.getPositions(), result.getOffset(), activePositions, 0, result.size());
            assertThat(activePositions).isSorted();
            assertThat(activePositions).doesNotHaveDuplicates();
        }
    }

    private static Set<Integer> toSet(SelectedPositions positions)
    {
        ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
        if (positions.isList()) {
            for (int index = positions.getOffset(); index < positions.getOffset() + positions.size(); index++) {
                builder.add(positions.getPositions()[index]);
            }
        }
        else {
            for (int position = positions.getOffset(); position < positions.getOffset() + positions.size(); position++) {
                builder.add(position);
            }
        }
        return builder.build();
    }
}
