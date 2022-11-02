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
import io.trino.operator.AggregationOperator.AggregationOperatorFactory;
import io.trino.operator.OperatorFactory;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.LocalQueryRunner;

import java.util.List;

import static io.trino.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;

public class CountAggregationBenchmark
        extends AbstractSimpleOperatorBenchmark
{
    private final BenchmarkAggregationFunction countFunction;

    public CountAggregationBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "count_agg", 10, 100);
        countFunction = createAggregationFunction("count");
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        OperatorFactory tableScanOperator = createTableScanOperator(0, new PlanNodeId("test"), "orders", "orderkey");
        AggregationOperatorFactory aggregationOperator = new AggregationOperatorFactory(
                1,
                new PlanNodeId("test"),
                ImmutableList.of(countFunction.bind(ImmutableList.of(0))));
        return ImmutableList.of(tableScanOperator, aggregationOperator);
    }

    public static void main(String[] args)
    {
        new CountAggregationBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
