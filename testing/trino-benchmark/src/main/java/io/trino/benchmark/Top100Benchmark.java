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
import io.trino.operator.OperatorFactory;
import io.trino.operator.TopNOperator;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.LocalQueryRunner;

import java.util.List;
import java.util.Optional;

import static io.trino.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;

public class Top100Benchmark
        extends AbstractSimpleOperatorBenchmark
{
    public Top100Benchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "top100", 5, 50);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        List<Type> tableScanTypes = getColumnTypes("orders", "totalprice");
        OperatorFactory tableScanOperator = createTableScanOperator(0, new PlanNodeId("test"), "orders", "totalprice");
        OperatorFactory topNOperator = TopNOperator.createOperatorFactory(
                1,
                new PlanNodeId("test"),
                tableScanTypes,
                100,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                localQueryRunner.getTypeOperators(),
                Optional.of(DataSize.of(16, DataSize.Unit.MEGABYTE)));
        return ImmutableList.of(tableScanOperator, topNOperator);
    }

    public static void main(String[] args)
    {
        new Top100Benchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
