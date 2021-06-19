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
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.operator.Driver;
import io.trino.operator.DriverFactory;
import io.trino.operator.OperatorFactories;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.TaskContext;
import io.trino.operator.TrinoOperatorFactories;
import io.trino.operator.join.HashBuilderOperator.HashBuilderOperatorFactory;
import io.trino.operator.join.JoinBridgeManager;
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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static io.trino.benchmark.BenchmarkQueryRunner.createLocalQueryRunnerHashEnabled;
import static io.trino.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spiller.PartitioningSpillerFactory.unsupportedPartitioningSpillerFactory;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public class HashBuildAndJoinBenchmark
        extends AbstractOperatorBenchmark
{
    private final boolean hashEnabled;
    private final List<Type> ordersTableTypes = getColumnTypes("orders", "orderkey", "totalprice");
    private final OperatorFactory ordersTableScan = createTableScanOperator(0, new PlanNodeId("test"), "orders", "orderkey", "totalprice");
    private final List<Type> lineItemTableTypes = getColumnTypes("lineitem", "orderkey", "quantity");
    private final OperatorFactory lineItemTableScan = createTableScanOperator(0, new PlanNodeId("test"), "lineitem", "orderkey", "quantity");
    private final OperatorFactories operatorFactories;

    public HashBuildAndJoinBenchmark(Session session, LocalQueryRunner localQueryRunner)
    {
        this(session, localQueryRunner, new TrinoOperatorFactories());
    }

    public HashBuildAndJoinBenchmark(Session session, LocalQueryRunner localQueryRunner, OperatorFactories operatorFactories)
    {
        super(session, localQueryRunner, "hash_build_and_join_hash_enabled_" + isHashEnabled(session), 4, 5);
        this.hashEnabled = isHashEnabled(session);
        this.operatorFactories = requireNonNull(operatorFactories, "operatorFactories is null");
    }

    private static boolean isHashEnabled(Session session)
    {
        return SystemSessionProperties.isOptimizeHashGenerationEnabled(session);
    }

    /*
    select orderkey, quantity, totalprice
    from lineitem join orders using (orderkey)
     */
    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        ImmutableList.Builder<OperatorFactory> driversBuilder = ImmutableList.builder();
        driversBuilder.add(ordersTableScan);
        List<Type> sourceTypes = ordersTableTypes;
        OptionalInt hashChannel = OptionalInt.empty();
        if (hashEnabled) {
            driversBuilder.add(createHashProjectOperator(1, new PlanNodeId("test"), sourceTypes));
            sourceTypes = ImmutableList.<Type>builder()
                    .addAll(sourceTypes)
                    .add(BIGINT)
                    .build();
            hashChannel = OptionalInt.of(sourceTypes.size() - 1);
        }

        // hash build
        BlockTypeOperators blockTypeOperators = new BlockTypeOperators(new TypeOperators());
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = JoinBridgeManager.lookupAllAtOnce(new PartitionedLookupSourceFactory(
                sourceTypes,
                ImmutableList.of(0, 1).stream()
                        .map(sourceTypes::get)
                        .collect(toImmutableList()),
                Ints.asList(0).stream()
                        .map(sourceTypes::get)
                        .collect(toImmutableList()),
                1,
                false,
                blockTypeOperators));
        HashBuilderOperatorFactory hashBuilder = new HashBuilderOperatorFactory(
                2,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                ImmutableList.of(0, 1),
                Ints.asList(0),
                hashChannel,
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                1_500_000,
                new PagesIndex.TestingFactory(false),
                false,
                SingleStreamSpillerFactory.unsupportedSingleStreamSpillerFactory());
        driversBuilder.add(hashBuilder);
        DriverFactory hashBuildDriverFactory = new DriverFactory(0, true, false, driversBuilder.build(), OptionalInt.empty(), UNGROUPED_EXECUTION);

        // join
        ImmutableList.Builder<OperatorFactory> joinDriversBuilder = ImmutableList.builder();
        joinDriversBuilder.add(lineItemTableScan);
        sourceTypes = lineItemTableTypes;
        hashChannel = OptionalInt.empty();
        if (hashEnabled) {
            joinDriversBuilder.add(createHashProjectOperator(1, new PlanNodeId("test"), sourceTypes));
            sourceTypes = ImmutableList.<Type>builder()
                    .addAll(sourceTypes)
                    .add(BIGINT)
                    .build();
            hashChannel = OptionalInt.of(sourceTypes.size() - 1);
        }

        OperatorFactory joinOperator = operatorFactories.innerJoin(
                2,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                false,
                false,
                false,
                sourceTypes,
                Ints.asList(0),
                hashChannel,
                Optional.empty(),
                OptionalInt.empty(),
                unsupportedPartitioningSpillerFactory(),
                blockTypeOperators);
        joinDriversBuilder.add(joinOperator);
        joinDriversBuilder.add(new NullOutputOperatorFactory(3, new PlanNodeId("test")));
        DriverFactory joinDriverFactory = new DriverFactory(1, true, true, joinDriversBuilder.build(), OptionalInt.empty(), UNGROUPED_EXECUTION);

        Driver hashBuildDriver = hashBuildDriverFactory.createDriver(taskContext.addPipelineContext(0, true, false, false).addDriverContext());
        hashBuildDriverFactory.noMoreDrivers();
        Driver joinDriver = joinDriverFactory.createDriver(taskContext.addPipelineContext(1, true, true, false).addDriverContext());
        joinDriverFactory.noMoreDrivers();

        return ImmutableList.of(hashBuildDriver, joinDriver);
    }

    public static void main(String[] args)
    {
        new HashBuildAndJoinBenchmark(testSessionBuilder().build(), createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new HashBuildAndJoinBenchmark(testSessionBuilder().build(), createLocalQueryRunnerHashEnabled()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
