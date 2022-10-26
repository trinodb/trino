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
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.trino.benchmark.HandTpchQuery1.TpchQuery1Operator.TpchQuery1OperatorFactory;
import io.trino.operator.DriverContext;
import io.trino.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.LocalQueryRunner;
import io.trino.util.DateTimeUtils;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class HandTpchQuery1
        extends AbstractSimpleOperatorBenchmark
{
    private final BenchmarkAggregationFunction longAverage;
    private final BenchmarkAggregationFunction doubleAverage;
    private final BenchmarkAggregationFunction doubleSum;
    private final BenchmarkAggregationFunction countFunction;

    public HandTpchQuery1(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "hand_tpch_query_1", 1, 5);

        longAverage = createAggregationFunction("avg", BIGINT);
        doubleAverage = createAggregationFunction("avg", DOUBLE);
        doubleSum = createAggregationFunction("sum", DOUBLE);
        countFunction = createAggregationFunction("count");
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        // select
        //     returnflag,
        //     linestatus,
        //     sum(quantity) as sum_qty,
        //     sum(extendedprice) as sum_base_price,
        //     sum(extendedprice * (1 - discount)) as sum_disc_price,
        //     sum(extendedprice * (1 - discount) * (1 + tax)) as sum_charge,
        //     avg(quantity) as avg_qty,
        //     avg(extendedprice) as avg_price,
        //     avg(discount) as avg_disc,
        //     count(*) as count_order
        // from
        //     lineitem
        // where
        //     shipdate <= '1998-09-02'
        // group by
        //     returnflag,
        //     linestatus
        // order by
        //     returnflag,
        //     linestatus

        OperatorFactory tableScanOperator = createTableScanOperator(
                0,
                new PlanNodeId("test"),
                "lineitem",
                "returnflag",
                "linestatus",
                "quantity",
                "extendedprice",
                "discount",
                "tax",
                "shipdate");

        TpchQuery1OperatorFactory tpchQuery1Operator = new TpchQuery1OperatorFactory(1);
        HashAggregationOperatorFactory aggregationOperator = new HashAggregationOperatorFactory(
                2,
                new PlanNodeId("test"),
                getColumnTypes("lineitem", "returnflag", "linestatus"),
                Ints.asList(0, 1),
                ImmutableList.of(),
                Step.SINGLE,
                ImmutableList.of(
                        doubleSum.bind(ImmutableList.of(2)),
                        doubleSum.bind(ImmutableList.of(3)),
                        doubleSum.bind(ImmutableList.of(4)),
                        longAverage.bind(ImmutableList.of(2)),
                        doubleAverage.bind(ImmutableList.of(5)),
                        doubleAverage.bind(ImmutableList.of(6)),
                        countFunction.bind(ImmutableList.of(2))),
                Optional.empty(),
                Optional.empty(),
                10_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                new JoinCompiler(localQueryRunner.getTypeOperators()),
                localQueryRunner.getBlockTypeOperators(),
                Optional.empty());

        return ImmutableList.of(tableScanOperator, tpchQuery1Operator, aggregationOperator);
    }

    public static class TpchQuery1Operator
            implements io.trino.operator.Operator // TODO: use import when Java 7 compiler bug is fixed
    {
        private static final ImmutableList<Type> TYPES = ImmutableList.of(
                VARCHAR,
                VARCHAR,
                DOUBLE,
                DOUBLE,
                DOUBLE,
                DOUBLE,
                DOUBLE);

        public static class TpchQuery1OperatorFactory
                implements OperatorFactory
        {
            private final int operatorId;

            public TpchQuery1OperatorFactory(int operatorId)
            {
                this.operatorId = operatorId;
            }

            @Override
            public Operator createOperator(DriverContext driverContext)
            {
                OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, new PlanNodeId("test"), TpchQuery1Operator.class.getSimpleName());
                return new TpchQuery1Operator(operatorContext);
            }

            @Override
            public void noMoreOperators()
            {
            }

            @Override
            public OperatorFactory duplicate()
            {
                throw new UnsupportedOperationException();
            }
        }

        private final OperatorContext operatorContext;
        private final PageBuilder pageBuilder;
        private boolean finishing;

        public TpchQuery1Operator(OperatorContext operatorContext)
        {
            this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
            this.pageBuilder = new PageBuilder(TYPES);
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public void finish()
        {
            finishing = true;
        }

        @Override
        public boolean isFinished()
        {
            return finishing && pageBuilder.isEmpty();
        }

        @Override
        public boolean needsInput()
        {
            return !pageBuilder.isFull();
        }

        @Override
        public void addInput(Page page)
        {
            requireNonNull(page, "page is null");
            checkState(!pageBuilder.isFull(), "Output buffer is full");
            checkState(!finishing, "Operator is finished");

            filterAndProjectRowOriented(pageBuilder,
                    page.getBlock(0),
                    page.getBlock(1),
                    page.getBlock(2),
                    page.getBlock(3),
                    page.getBlock(4),
                    page.getBlock(5),
                    page.getBlock(6));
        }

        @Override
        public Page getOutput()
        {
            // only return a page if the page buffer isFull or we are finishing and the page buffer has data
            if (pageBuilder.isFull() || (finishing && !pageBuilder.isEmpty())) {
                Page page = pageBuilder.build();
                pageBuilder.reset();
                return page;
            }
            return null;
        }

        private static final int MAX_SHIP_DATE = DateTimeUtils.parseDate("1998-09-02");

        private static void filterAndProjectRowOriented(
                PageBuilder pageBuilder,
                Block returnFlagBlock,
                Block lineStatusBlock,
                Block quantityBlock,
                Block extendedPriceBlock,
                Block discountBlock,
                Block taxBlock,
                Block shipDateBlock)
        {
            int rows = returnFlagBlock.getPositionCount();
            for (int position = 0; position < rows; position++) {
                if (shipDateBlock.isNull(position)) {
                    continue;
                }

                int shipDate = DATE.getInt(shipDateBlock, position);

                // where
                //     shipdate <= '1998-09-02'
                if (shipDate <= MAX_SHIP_DATE) {
                    //     returnflag,
                    //     linestatus
                    //     quantity
                    //     extendedprice
                    //     extendedprice * (1 - discount)
                    //     extendedprice * (1 - discount) * (1 + tax)
                    //     discount

                    pageBuilder.declarePosition();
                    if (returnFlagBlock.isNull(position)) {
                        pageBuilder.getBlockBuilder(0).appendNull();
                    }
                    else {
                        VARCHAR.appendTo(returnFlagBlock, position, pageBuilder.getBlockBuilder(0));
                    }
                    if (lineStatusBlock.isNull(position)) {
                        pageBuilder.getBlockBuilder(1).appendNull();
                    }
                    else {
                        VARCHAR.appendTo(lineStatusBlock, position, pageBuilder.getBlockBuilder(1));
                    }

                    double quantity = DOUBLE.getDouble(quantityBlock, position);
                    double extendedPrice = DOUBLE.getDouble(extendedPriceBlock, position);
                    double discount = DOUBLE.getDouble(discountBlock, position);
                    double tax = DOUBLE.getDouble(taxBlock, position);

                    boolean quantityIsNull = quantityBlock.isNull(position);
                    boolean extendedPriceIsNull = extendedPriceBlock.isNull(position);
                    boolean discountIsNull = discountBlock.isNull(position);
                    boolean taxIsNull = taxBlock.isNull(position);

                    if (quantityIsNull) {
                        pageBuilder.getBlockBuilder(2).appendNull();
                    }
                    else {
                        DOUBLE.writeDouble(pageBuilder.getBlockBuilder(2), quantity);
                    }

                    if (extendedPriceIsNull) {
                        pageBuilder.getBlockBuilder(3).appendNull();
                    }
                    else {
                        DOUBLE.writeDouble(pageBuilder.getBlockBuilder(3), extendedPrice);
                    }

                    if (extendedPriceIsNull || discountIsNull) {
                        pageBuilder.getBlockBuilder(4).appendNull();
                    }
                    else {
                        DOUBLE.writeDouble(pageBuilder.getBlockBuilder(4), extendedPrice * (1 - discount));
                    }

                    if (extendedPriceIsNull || discountIsNull || taxIsNull) {
                        pageBuilder.getBlockBuilder(5).appendNull();
                    }
                    else {
                        DOUBLE.writeDouble(pageBuilder.getBlockBuilder(5), extendedPrice * (1 - discount) * (1 + tax));
                    }

                    if (discountIsNull) {
                        pageBuilder.getBlockBuilder(6).appendNull();
                    }
                    else {
                        DOUBLE.writeDouble(pageBuilder.getBlockBuilder(6), discount);
                    }
                }
            }
        }
    }

    public static void main(String[] args)
    {
        new HandTpchQuery1(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
