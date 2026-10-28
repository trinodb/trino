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
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.PagesIndex;
import io.trino.operator.UpdateMemory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LongArrayBlock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOrderedAccumulatorFactory
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    // prepareFinal replays the buffered pages into the delegate (for a distinct aggregation, this is
    // where its hash fills up); the transient peak must be reported while the buffer is still held
    @Test
    public void testPrepareFinalReportsMemoryWhileReplaying()
    {
        OrderedAccumulatorFactory factory = new OrderedAccumulatorFactory(
                FUNCTION_RESOLUTION.getAggregateFunction("array_agg", fromTypes(BIGINT)).getDistinctFactory(),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),      // argument channels
                ImmutableList.of(0),      // order-by channels
                ImmutableList.of(ASC_NULLS_LAST),
                new PagesIndex.TestingFactory(false));

        GroupedAccumulator accumulator = factory.createGroupedAccumulator(ImmutableList.of());
        accumulator.setGroupCount(1);

        // buffer enough distinct rows that getSortedPages yields multiple pages to replay
        int positionsPerPage = 100_000;
        for (int page = 0; page < 4; page++) {
            long[] values = new long[positionsPerPage];
            for (int i = 0; i < positionsPerPage; i++) {
                values[i] = ((long) page * positionsPerPage) + i;
            }
            accumulator.addInput(
                    new int[positionsPerPage], // all rows in group 0
                    new Page(new LongArrayBlock(positionsPerPage, Optional.empty(), values)),
                    AggregationMask.createSelectAll(positionsPerPage));
        }
        // the delegate has seen no input yet, so this is what the buffered pages alone weigh
        long bufferedPagesSize = accumulator.getEstimatedSize();

        List<Long> sizeAtUpdate = new ArrayList<>();
        UpdateMemory updateMemory = () -> {
            sizeAtUpdate.add(accumulator.getEstimatedSize());
            return true;
        };
        accumulator.prepareFinal(updateMemory);

        // memory is reported repeatedly during the replay, not just once after it
        assertThat(sizeAtUpdate).hasSizeGreaterThan(1);
        // the buffered pages and the delegate's growth are counted at the same moment
        assertThat(sizeAtUpdate.getFirst()).isGreaterThan(bufferedPagesSize);
        // the delegate's growth is visible as the replay progresses
        assertThat(sizeAtUpdate.getLast()).isGreaterThan(sizeAtUpdate.getFirst());
        // the buffered pages and the delegate's hash are released once the replay is done
        assertThat(accumulator.getEstimatedSize()).isLessThan(sizeAtUpdate.getLast());
        assertThat(accumulator.getEstimatedSize()).isLessThan(sizeAtUpdate.getFirst());
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
}
