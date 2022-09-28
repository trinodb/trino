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
import io.trino.operator.DriverContext;
import io.trino.operator.DriverFactory;
import io.trino.operator.OperatorFactories;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.TaskContext;
import io.trino.operator.TrinoOperatorFactories;
import io.trino.operator.join.HashBuilderOperator.HashBuilderOperatorFactory;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.LookupSourceProvider;
import io.trino.operator.join.PartitionedLookupSourceFactory;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.SingleStreamSpillerFactory;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.NullOutputOperator.NullOutputOperatorFactory;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Future;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static io.trino.execution.executor.PrioritizedSplitRunner.SPLIT_RUN_QUANTA;
import static io.trino.operator.HashArraySizeSupplier.incrementalLoadFactorHashArraySizeSupplier;
import static io.trino.operator.OperatorFactories.JoinOperatorType.innerJoin;
import static io.trino.spiller.PartitioningSpillerFactory.unsupportedPartitioningSpillerFactory;
import static java.util.Objects.requireNonNull;

public class HashJoinBenchmark
        extends AbstractOperatorBenchmark
{
    private final OperatorFactories operatorFactories;
    private DriverFactory probeDriverFactory;

    public HashJoinBenchmark(LocalQueryRunner localQueryRunner)
    {
        this(localQueryRunner, new TrinoOperatorFactories());
    }

    public HashJoinBenchmark(LocalQueryRunner localQueryRunner, OperatorFactories operatorFactories)
    {
        super(localQueryRunner, "hash_join", 4, 50);
        this.operatorFactories = requireNonNull(operatorFactories, "operatorFactories is null");
    }

    /*
    select orderkey, quantity, totalprice
    from lineitem join orders using (orderkey)
     */

    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        if (probeDriverFactory == null) {
            List<Type> ordersTypes = getColumnTypes("orders", "orderkey", "totalprice");
            OperatorFactory ordersTableScan = createTableScanOperator(0, new PlanNodeId("test"), "orders", "orderkey", "totalprice");
            BlockTypeOperators blockTypeOperators = new BlockTypeOperators(new TypeOperators());
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
                    blockTypeOperators));
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

            DriverContext driverContext = taskContext.addPipelineContext(0, false, false, false).addDriverContext();
            DriverFactory buildDriverFactory = new DriverFactory(0, false, false, ImmutableList.of(ordersTableScan, hashBuilder), OptionalInt.empty());

            List<Type> lineItemTypes = getColumnTypes("lineitem", "orderkey", "quantity");
            OperatorFactory lineItemTableScan = createTableScanOperator(0, new PlanNodeId("test"), "lineitem", "orderkey", "quantity");
            OperatorFactory joinOperator = operatorFactories.spillingJoin(
                    innerJoin(false, false),
                    1,
                    new PlanNodeId("test"),
                    lookupSourceFactoryManager,
                    false,
                    lineItemTypes,
                    Ints.asList(0),
                    OptionalInt.empty(),
                    Optional.empty(),
                    OptionalInt.empty(),
                    unsupportedPartitioningSpillerFactory(),
                    blockTypeOperators);
            NullOutputOperatorFactory output = new NullOutputOperatorFactory(2, new PlanNodeId("test"));
            this.probeDriverFactory = new DriverFactory(1, true, true, ImmutableList.of(lineItemTableScan, joinOperator, output), OptionalInt.empty());

            Driver driver = buildDriverFactory.createDriver(driverContext);
            Future<LookupSourceProvider> lookupSourceProvider = lookupSourceFactoryManager.getJoinBridge().createLookupSourceProvider();
            while (!lookupSourceProvider.isDone()) {
                driver.processForDuration(SPLIT_RUN_QUANTA);
            }
            getFutureValue(lookupSourceProvider).close();
        }

        DriverContext driverContext = taskContext.addPipelineContext(1, true, true, false).addDriverContext();
        Driver driver = probeDriverFactory.createDriver(driverContext);
        return ImmutableList.of(driver);
    }

    public static void main(String[] args)
    {
        new HashJoinBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
