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
import io.trino.spi.block.LongArrayBlock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
        // the buffered pages are released once the replay is done
        assertThat(accumulator.getEstimatedSize()).isLessThan(sizeAtUpdate.getLast());
    }
}
