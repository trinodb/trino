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
package io.trino.benchmark;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.operator.AggregationOperator.AggregationOperatorFactory;
import io.trino.operator.FilterAndProjectOperator;
import io.trino.operator.OperatorFactory;
import io.trino.operator.project.InputChannels;
import io.trino.operator.project.PageFilter;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.LocalQueryRunner;
import io.trino.util.DateTimeUtils;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.relational.Expressions.field;

public class HandTpchQuery6
        extends AbstractSimpleOperatorBenchmark
{
    private final BenchmarkAggregationFunction doubleSum;

    public HandTpchQuery6(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "hand_tpch_query_6", 10, 100);
        doubleSum = createAggregationFunction("sum", DOUBLE);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        // select sum(extendedprice * discount) as revenue
        // from lineitem
        // where shipdate >= '1994-01-01'
        //    and shipdate < '1995-01-01'
        //    and discount >= 0.05
        //    and discount <= 0.07
        //    and quantity < 24;
        OperatorFactory tableScanOperator = createTableScanOperator(0, new PlanNodeId("test"), "lineitem", "extendedprice", "discount", "shipdate", "quantity");

        Supplier<PageProjection> projection = new PageFunctionCompiler(localQueryRunner.getFunctionManager(), 0).compileProjection(field(0, BIGINT), Optional.empty());

        OperatorFactory tpchQuery6Operator = FilterAndProjectOperator.createOperatorFactory(
                1,
                new PlanNodeId("test"),
                () -> new PageProcessor(Optional.of(new TpchQuery6Filter()), ImmutableList.of(projection.get())),
                ImmutableList.of(DOUBLE),
                DataSize.ofBytes(0),
                0);

        AggregationOperatorFactory aggregationOperator = new AggregationOperatorFactory(
                2,
                new PlanNodeId("test"),
                ImmutableList.of(
                        doubleSum.bind(ImmutableList.of(0))));

        return ImmutableList.of(tableScanOperator, tpchQuery6Operator, aggregationOperator);
    }

    public static class TpchQuery6Filter
            implements PageFilter
    {
        private static final int MIN_SHIP_DATE = DateTimeUtils.parseDate("1994-01-01");
        private static final int MAX_SHIP_DATE = DateTimeUtils.parseDate("1995-01-01");
        private static final InputChannels INPUT_CHANNELS = new InputChannels(1, 2, 3);

        private boolean[] selectedPositions = new boolean[0];

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return INPUT_CHANNELS;
        }

        @Override
        public SelectedPositions filter(ConnectorSession session, Page page)
        {
            if (selectedPositions.length < page.getPositionCount()) {
                selectedPositions = new boolean[page.getPositionCount()];
            }

            for (int position = 0; position < page.getPositionCount(); position++) {
                selectedPositions[position] = filter(page, position);
            }

            return PageFilter.positionsArrayToSelectedPositions(selectedPositions, page.getPositionCount());
        }

        private static boolean filter(Page page, int position)
        {
            Block discountBlock = page.getBlock(0);
            Block shipDateBlock = page.getBlock(1);
            Block quantityBlock = page.getBlock(2);
            return !shipDateBlock.isNull(position) && DATE.getInt(shipDateBlock, position) >= MIN_SHIP_DATE &&
                    !shipDateBlock.isNull(position) && DATE.getInt(shipDateBlock, position) < MAX_SHIP_DATE &&
                    !discountBlock.isNull(position) && DOUBLE.getDouble(discountBlock, position) >= 0.05 &&
                    !discountBlock.isNull(position) && DOUBLE.getDouble(discountBlock, position) <= 0.07 &&
                    !quantityBlock.isNull(position) && BIGINT.getLong(quantityBlock, position) < 24;
        }
    }

    public static void main(String[] args)
    {
        new HandTpchQuery6(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
