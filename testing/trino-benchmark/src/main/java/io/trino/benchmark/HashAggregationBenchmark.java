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
import io.trino.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import io.trino.operator.OperatorFactory;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.LocalQueryRunner;

import java.util.List;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static io.trino.spi.type.DoubleType.DOUBLE;

public class HashAggregationBenchmark
        extends AbstractSimpleOperatorBenchmark
{
    private final BenchmarkAggregationFunction doubleSum;

    public HashAggregationBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "hash_agg", 5, 25);

        doubleSum = createAggregationFunction("sum", DOUBLE);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        List<Type> tableTypes = getColumnTypes("orders", "orderstatus", "totalprice");
        OperatorFactory tableScanOperator = createTableScanOperator(0, new PlanNodeId("test"), "orders", "orderstatus", "totalprice");
        HashAggregationOperatorFactory aggregationOperator = new HashAggregationOperatorFactory(
                1,
                new PlanNodeId("test"),
                ImmutableList.of(tableTypes.get(0)),
                Ints.asList(0),
                ImmutableList.of(),
                Step.SINGLE,
                ImmutableList.of(doubleSum.bind(ImmutableList.of(1))),
                Optional.empty(),
                Optional.empty(),
                100_000,
                Optional.of(DataSize.of(16, MEGABYTE)),
                new JoinCompiler(localQueryRunner.getTypeOperators()),
                localQueryRunner.getTypeOperators(),
                Optional.empty());
        return ImmutableList.of(tableScanOperator, aggregationOperator);
    }

    public static void main(String[] args)
    {
        new HashAggregationBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
