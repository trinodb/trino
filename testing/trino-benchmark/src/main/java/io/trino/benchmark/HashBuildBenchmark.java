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
import io.trino.operator.Driver;
import io.trino.operator.DriverFactory;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.TaskContext;
import io.trino.operator.ValuesOperator.ValuesOperatorFactory;
import io.trino.operator.join.HashBuilderOperator.HashBuilderOperatorFactory;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.PartitionedLookupSourceFactory;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.SingleStreamSpillerFactory;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.NullOutputOperator.NullOutputOperatorFactory;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static io.trino.operator.HashArraySizeSupplier.incrementalLoadFactorHashArraySizeSupplier;
import static io.trino.operator.JoinOperatorType.innerJoin;
import static io.trino.operator.OperatorFactories.spillingJoin;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spiller.PartitioningSpillerFactory.unsupportedPartitioningSpillerFactory;

public class HashBuildBenchmark
        extends AbstractOperatorBenchmark
{
    public HashBuildBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "hash_build", 4, 5);
    }

    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        // hash build
        List<Type> ordersTypes = getColumnTypes("orders", "orderkey", "totalprice");
        OperatorFactory ordersTableScan = createTableScanOperator(0, new PlanNodeId("test"), "orders", "orderkey", "totalprice");
        TypeOperators typeOperators = new TypeOperators();
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = JoinBridgeManager.lookupAllAtOnce(new PartitionedLookupSourceFactory(
                ordersTypes,
                ImmutableList.of(0, 1).stream()
                        .map(ordersTypes::get)
                        .collect(toImmutableList()),
                Ints.asList(0).stream()
                        .map(ordersTypes::get)
                        .collect(toImmutableList()),
                1,
                false,
                typeOperators));
        HashBuilderOperatorFactory hashBuilder = new HashBuilderOperatorFactory(
                1,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                ImmutableList.of(0, 1),
                Ints.asList(0),
                OptionalInt.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                1_500_000,
                new PagesIndex.TestingFactory(false),
                false,
                SingleStreamSpillerFactory.unsupportedSingleStreamSpillerFactory(),
                incrementalLoadFactorHashArraySizeSupplier(session));
        DriverFactory hashBuildDriverFactory = new DriverFactory(0, true, true, ImmutableList.of(ordersTableScan, hashBuilder), OptionalInt.empty());

        // empty join so build finishes
        ImmutableList.Builder<OperatorFactory> joinDriversBuilder = ImmutableList.builder();
        joinDriversBuilder.add(new ValuesOperatorFactory(0, new PlanNodeId("values"), ImmutableList.of()));
        OperatorFactory joinOperator = spillingJoin(
                innerJoin(false, false),
                2,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                ImmutableList.of(BIGINT),
                Ints.asList(0),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                unsupportedPartitioningSpillerFactory(),
                typeOperators);
        joinDriversBuilder.add(joinOperator);
        joinDriversBuilder.add(new NullOutputOperatorFactory(3, new PlanNodeId("test")));
        DriverFactory joinDriverFactory = new DriverFactory(1, true, true, joinDriversBuilder.build(), OptionalInt.empty());

        Driver hashBuildDriver = hashBuildDriverFactory.createDriver(taskContext.addPipelineContext(0, true, true, false).addDriverContext());
        hashBuildDriverFactory.noMoreDrivers();
        Driver joinDriver = joinDriverFactory.createDriver(taskContext.addPipelineContext(1, true, true, false).addDriverContext());
        joinDriverFactory.noMoreDrivers();

        return ImmutableList.of(hashBuildDriver, joinDriver);
    }

    public static void main(String[] args)
    {
        new HashBuildBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
