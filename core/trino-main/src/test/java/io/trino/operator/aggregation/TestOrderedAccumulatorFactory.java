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
import io.airlift.units.DataSize;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.PagesIndex;
import io.trino.operator.UpdateMemory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LongArrayBlock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static java.lang.Math.min;
import static java.util.Comparator.comparing;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsFirst;
import static java.util.Comparator.nullsLast;
import static java.util.Comparator.reverseOrder;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOrderedAccumulatorFactory
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final int POSITIONS_PER_INPUT_PAGE = 10_000;
    // matches the orderings of createKeyOrderedFactory
    private static final Comparator<Row> KEY_ORDERING = comparing(Row::key, nullsFirst(reverseOrder()))
            .thenComparing(Row::value, nullsLast(naturalOrder()));

    // mixed sort orders, nulls, empty groups and groups spanning several sorted pages evaluate correctly at any batch size
    @Test
    public void testGroupedEvaluatesInBatches()
    {
        TestingAggregationFunction arrayAgg = FUNCTION_RESOLUTION.getAggregateFunction("array_agg", fromTypes(BIGINT));
        int groupCount = 1_000;
        List<Row> rows = new ArrayList<>();
        for (int group = 0; group < groupCount; group++) {
            int valueCount = valueCount(group);
            for (long value = 0; value < valueCount; value++) {
                rows.add(new Row(group, value / 2, value));
            }
            if (valueCount > 0) {
                rows.add(new Row(group, null, (long) valueCount));
                rows.add(new Row(group, 0L, null));
            }
        }
        Map<Integer, List<Long>> expected = rows.stream()
                .sorted(KEY_ORDERING)
                .collect(groupingBy(Row::groupId, mapping(Row::value, toList())));

        List<Row> input = new ArrayList<>(rows);
        input.addAll(rows);
        Collections.shuffle(input, new Random(42));
        int[] groupIds = input.stream().mapToInt(Row::groupId).toArray();
        Block values = createLongsBlock(input.stream().map(Row::value).toList());
        Block keys = createLongsBlock(input.stream().map(Row::key).toList());

        for (DataSize maxBatchSize : ImmutableList.of(DataSize.ofBytes(1), DataSize.of(64, KILOBYTE), DataSize.of(1, GIGABYTE))) {
            GroupedAccumulator accumulator = createKeyOrderedFactory(arrayAgg, maxBatchSize).createGroupedAccumulator(ImmutableList.of());
            accumulator.setGroupCount(groupCount);
            addInput(accumulator, groupIds, values, keys);
            accumulator.prepareFinal(UpdateMemory.NOOP);
            assertGroupValues(arrayAgg, evaluateFinal(arrayAgg, accumulator, groupCount), expected);
        }
    }

    // masked rows of many small groups ordered by a low-cardinality key evaluate correctly at any batch size
    @Test
    public void testGroupedEvaluatesMaskedSmallGroups()
    {
        TestingAggregationFunction arrayAgg = FUNCTION_RESOLUTION.getAggregateFunction("array_agg", fromTypes(BIGINT));
        int groupCount = 20_000;
        List<Row> rows = new ArrayList<>();
        for (int group = 0; group < groupCount; group++) {
            for (long value = 0; value < group % 20; value++) {
                rows.add(new Row(group, value % 3, value));
            }
        }
        Collections.shuffle(rows, new Random(42));
        boolean[] selected = new boolean[rows.size()];
        List<Row> selectedRows = new ArrayList<>();
        for (int index = 0; index < rows.size(); index++) {
            selected[index] = isSelected(index);
            if (selected[index]) {
                selectedRows.add(rows.get(index));
            }
        }
        Map<Integer, List<Long>> expected = selectedRows.stream()
                .sorted(KEY_ORDERING)
                .collect(groupingBy(Row::groupId, mapping(Row::value, toList())));

        int[] groupIds = rows.stream().mapToInt(Row::groupId).toArray();
        Block values = createLongsBlock(rows.stream().map(Row::value).toList());
        Block keys = createLongsBlock(rows.stream().map(Row::key).toList());
        for (DataSize maxBatchSize : ImmutableList.of(DataSize.ofBytes(1), DataSize.of(64, KILOBYTE), DataSize.of(1, GIGABYTE))) {
            GroupedAccumulator accumulator = createKeyOrderedFactory(arrayAgg, maxBatchSize).createGroupedAccumulator(ImmutableList.of());
            accumulator.setGroupCount(groupCount);
            addInput(accumulator, groupIds, selected, values, keys);
            accumulator.prepareFinal(UpdateMemory.NOOP);
            assertGroupValues(arrayAgg, evaluateFinal(arrayAgg, accumulator, groupCount), expected);
        }
    }

    // only the buffered rows and one batch of group state are live while groups are evaluated
    @Test
    public void testGroupedEvaluationHoldsOneBatch()
    {
        TestingAggregationFunction arrayAgg = FUNCTION_RESOLUTION.getAggregateFunction("array_agg", fromTypes(BIGINT));
        int groupCount = 20_000;
        int valuesPerGroup = 20;
        int[] groupIds = new int[2 * groupCount * valuesPerGroup];
        long[] values = new long[groupIds.length];
        int row = 0;
        for (int copy = 0; copy < 2; copy++) {
            for (int group = 0; group < groupCount; group++) {
                for (int value = 0; value < valuesPerGroup; value++) {
                    groupIds[row] = group;
                    values[row] = value;
                    row++;
                }
            }
        }
        Block valueBlock = new LongArrayBlock(values.length, Optional.empty(), values);

        GroupedAccumulator allGroups = arrayAgg.getDistinctFactory().createGroupedAccumulator(ImmutableList.of());
        allGroups.setGroupCount(groupCount);
        addInput(allGroups, groupIds, valueBlock);
        long allGroupsSize = allGroups.getEstimatedSize();

        GroupedAccumulator accumulator = createKeyOrderedFactory(arrayAgg, DataSize.of(1, MEGABYTE)).createGroupedAccumulator(ImmutableList.of());
        accumulator.setGroupCount(groupCount);
        addInput(accumulator, groupIds, valueBlock, valueBlock);

        List<Long> sizeAtUpdate = new ArrayList<>();
        accumulator.prepareFinal(() -> {
            sizeAtUpdate.add(accumulator.getEstimatedSize());
            return true;
        });
        long bufferedRowsSize = accumulator.getEstimatedSize();
        evaluateFinal(arrayAgg, accumulator, groupCount);

        assertThat(accumulator.getEstimatedSize()).isLessThan(bufferedRowsSize);
        assertThat(sizeAtUpdate).hasSizeGreaterThan(1);
        assertThat(sizeAtUpdate.getFirst()).isGreaterThan(bufferedRowsSize);
        assertThat(sizeAtUpdate).allSatisfy(size -> assertThat(size).isLessThan(bufferedRowsSize + allGroupsSize / 2));
    }

    // a group before the current batch is rejected
    @Test
    public void testGroupedEvaluationRejectsEarlierGroup()
    {
        TestingAggregationFunction arrayAgg = FUNCTION_RESOLUTION.getAggregateFunction("array_agg", fromTypes(BIGINT));
        int[] groupIds = new int[100_001];
        groupIds[groupIds.length - 1] = 1;
        Block values = new LongArrayBlock(groupIds.length, Optional.empty(), LongStream.range(0, groupIds.length).toArray());

        GroupedAccumulator accumulator = createKeyOrderedFactory(arrayAgg, DataSize.ofBytes(1)).createGroupedAccumulator(ImmutableList.of());
        accumulator.setGroupCount(2);
        addInput(accumulator, groupIds, values, values);
        accumulator.prepareFinal(UpdateMemory.NOOP);
        BlockBuilder output = arrayAgg.getFinalType().createBlockBuilder(null, 2);
        accumulator.evaluateFinal(1, output);

        assertThatThrownBy(() -> accumulator.evaluateFinal(0, output))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("groupId 0 precedes the current batch starting at 1");
    }

    // prepareFinal reports memory while it replays the buffered rows into the delegate
    @Test
    public void testNonGroupedPrepareFinalReportsMemoryWhileReplaying()
    {
        TestingAggregationFunction arrayAgg = FUNCTION_RESOLUTION.getAggregateFunction("array_agg", fromTypes(BIGINT));
        Accumulator accumulator = createValueOrderedFactory(arrayAgg).createAccumulator(ImmutableList.of());

        int positionsPerPage = 100_000;
        for (int page = 0; page < 4; page++) {
            long[] values = new long[positionsPerPage];
            for (int i = 0; i < positionsPerPage; i++) {
                values[i] = ((long) page * positionsPerPage) + i;
            }
            accumulator.addInput(
                    new Page(new LongArrayBlock(positionsPerPage, Optional.empty(), values)),
                    AggregationMask.createSelectAll(positionsPerPage));
        }
        long bufferedPagesSize = accumulator.getEstimatedSize();

        List<Long> sizeAtUpdate = new ArrayList<>();
        UpdateMemory updateMemory = () -> {
            sizeAtUpdate.add(accumulator.getEstimatedSize());
            return true;
        };
        accumulator.prepareFinal(updateMemory);

        assertThat(sizeAtUpdate).hasSizeGreaterThan(1);
        assertThat(sizeAtUpdate.getFirst()).isGreaterThan(bufferedPagesSize);
        // the last update reports the size after the buffered pages are released
        assertThat(sizeAtUpdate.getLast())
                .isEqualTo(accumulator.getEstimatedSize())
                .isLessThan(bufferedPagesSize);
    }

    // prepareFinal resumes from the next sorted page after each yield
    @Test
    public void testNonGroupedPrepareFinalResumesAfterYield()
    {
        TestingAggregationFunction arrayAgg = FUNCTION_RESOLUTION.getAggregateFunction("array_agg", fromTypes(BIGINT));
        Accumulator accumulator = createValueOrderedFactory(arrayAgg).createAccumulator(ImmutableList.of());

        int positionsPerPage = 100_000;
        int pageCount = 4;
        long rows = (long) pageCount * positionsPerPage;
        for (int page = 0; page < pageCount; page++) {
            long[] values = new long[positionsPerPage];
            for (int i = 0; i < positionsPerPage; i++) {
                values[i] = rows - 1 - ((long) page * positionsPerPage) - i;
            }
            accumulator.addInput(
                    new Page(new LongArrayBlock(positionsPerPage, Optional.empty(), values)),
                    AggregationMask.createSelectAll(positionsPerPage));
        }

        int calls = 1;
        while (!accumulator.prepareFinal(() -> false)) {
            calls++;
        }
        assertThat(calls).isGreaterThan(1);

        BlockBuilder output = arrayAgg.getFinalType().createBlockBuilder(null, 1);
        accumulator.evaluateFinal(output);
        Block values = (Block) arrayAgg.getFinalType().getObject(output.build(), 0);
        assertThat(toValues(values)).isEqualTo(LongStream.range(0, rows).boxed().collect(toImmutableList()));
    }

    private static int valueCount(int group)
    {
        // groups ending in 0 or 9 are empty, and group 501 spans several sorted pages
        if (group % 10 == 0 || group % 10 == 9) {
            return 0;
        }
        if (group == 501) {
            return 100_000;
        }
        return 50;
    }

    private static boolean isSelected(int index)
    {
        // the second input page selects no rows, the third selects every row and the rest select two of every three rows
        int page = index / POSITIONS_PER_INPUT_PAGE;
        return page == 2 || (page != 1 && index % 3 != 0);
    }

    private static void addInput(GroupedAccumulator accumulator, int[] groupIds, Block... columns)
    {
        boolean[] selected = new boolean[groupIds.length];
        Arrays.fill(selected, true);
        addInput(accumulator, groupIds, selected, columns);
    }

    private static void addInput(GroupedAccumulator accumulator, int[] groupIds, boolean[] selected, Block... columns)
    {
        for (int start = 0; start < groupIds.length; start += POSITIONS_PER_INPUT_PAGE) {
            int positionCount = min(POSITIONS_PER_INPUT_PAGE, groupIds.length - start);
            int[] selectedPositions = new int[positionCount];
            int selectedPositionCount = 0;
            for (int position = 0; position < positionCount; position++) {
                if (selected[start + position]) {
                    selectedPositions[selectedPositionCount] = position;
                    selectedPositionCount++;
                }
            }
            // the pages index retains the input, so each page gets its own copy
            Block[] pageColumns = new Block[columns.length];
            for (int channel = 0; channel < columns.length; channel++) {
                pageColumns[channel] = columns[channel].copyRegion(start, positionCount);
            }
            accumulator.addInput(
                    Arrays.copyOfRange(groupIds, start, start + positionCount),
                    new Page(positionCount, pageColumns),
                    AggregationMask.createSelectedPositions(positionCount, selectedPositions, selectedPositionCount));
        }
    }

    private static Block evaluateFinal(TestingAggregationFunction arrayAgg, GroupedAccumulator accumulator, int groupCount)
    {
        BlockBuilder output = arrayAgg.getFinalType().createBlockBuilder(null, groupCount);
        for (int group = 0; group < groupCount; group++) {
            accumulator.evaluateFinal(group, output);
        }
        return output.build();
    }

    private static void assertGroupValues(TestingAggregationFunction arrayAgg, Block result, Map<Integer, List<Long>> expected)
    {
        for (int group = 0; group < result.getPositionCount(); group++) {
            if (expected.containsKey(group)) {
                assertThat(toValues((Block) arrayAgg.getFinalType().getObject(result, group))).isEqualTo(expected.get(group));
            }
            else {
                assertThat(result.isNull(group)).isTrue();
            }
        }
    }

    private static List<Long> toValues(Block block)
    {
        List<Long> values = new ArrayList<>();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                values.add(null);
            }
            else {
                values.add(BIGINT.getLong(block, position));
            }
        }
        return values;
    }

    // array_agg(value ORDER BY value) over a page of (value)
    private static OrderedAccumulatorFactory createValueOrderedFactory(TestingAggregationFunction arrayAgg)
    {
        return new OrderedAccumulatorFactory(
                arrayAgg.getFactory(),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),      // argument channels
                ImmutableList.of(0),      // order-by channels
                ImmutableList.of(ASC_NULLS_LAST),
                new PagesIndex.TestingFactory(false));
    }

    // array_agg(DISTINCT value ORDER BY key DESC NULLS FIRST, value ASC NULLS LAST) over a page of (value, key)
    private static OrderedAccumulatorFactory createKeyOrderedFactory(TestingAggregationFunction arrayAgg, DataSize maxBatchSize)
    {
        return new OrderedAccumulatorFactory(
                arrayAgg.getDistinctFactory(),
                ImmutableList.of(BIGINT, BIGINT),
                ImmutableList.of(0),      // argument channels
                ImmutableList.of(1, 0),   // order-by channels
                ImmutableList.of(DESC_NULLS_FIRST, ASC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                maxBatchSize);
    }

    private record Row(int groupId, Long key, Long value) {}
}
