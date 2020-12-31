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
import io.trino.operator.LimitOperator.LimitOperatorFactory;
import io.trino.operator.OperatorFactory;
import io.trino.operator.OrderByOperator.OrderByOperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.spi.type.Type;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.LocalQueryRunner;

import java.util.List;
import java.util.Optional;

import static io.trino.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;

public class OrderByBenchmark
        extends AbstractSimpleOperatorBenchmark
{
    private static final int ROWS = 1_500_000;

    public OrderByBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "in_memory_orderby_1.5M", 5, 10);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        List<Type> tableScanTypes = getColumnTypes("orders", "totalprice", "clerk");
        OperatorFactory tableScanOperator = createTableScanOperator(0, new PlanNodeId("test"), "orders", "totalprice", "clerk");

        LimitOperatorFactory limitOperator = new LimitOperatorFactory(1, new PlanNodeId("test"), ROWS);

        OrderByOperatorFactory orderByOperator = new OrderByOperatorFactory(
                2,
                new PlanNodeId("test"),
                tableScanTypes,
                ImmutableList.of(1),
                ROWS,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                false,
                Optional.empty(),
                new OrderingCompiler(localQueryRunner.getTypeOperators()));

        return ImmutableList.of(tableScanOperator, limitOperator, orderByOperator);
    }

    public static void main(String[] args)
    {
        new OrderByBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
