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
package io.trino.operator.join.unspilled;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.RowPagesBuilder;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PipelineContext;
import io.trino.operator.TaskContext;
import io.trino.operator.ValuesOperator;
import io.trino.operator.exchange.LocalExchange;
import io.trino.operator.exchange.LocalExchangeSinkOperator;
import io.trino.operator.exchange.LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory;
import io.trino.operator.join.InternalJoinFilterFunction;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.LookupSource;
import io.trino.operator.join.StandardJoinFilterFunction;
import io.trino.operator.join.unspilled.HashBuilderOperator.HashBuilderOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.operator.HashArraySizeSupplier.incrementalLoadFactorHashArraySizeSupplier;
import static io.trino.operator.JoinOperatorType.innerJoin;
import static io.trino.operator.OperatorFactories.join;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static java.util.Objects.requireNonNull;

public final class JoinTestUtils
{
    private static final int PARTITION_COUNT = 4;
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private JoinTestUtils() {}

    public static OperatorFactory innerJoinOperatorFactory(
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager,
            RowPagesBuilder probePages,
            boolean hasFilter)
    {
        return innerJoinOperatorFactory(lookupSourceFactoryManager, probePages, false, hasFilter);
    }

    public static OperatorFactory innerJoinOperatorFactory(
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager,
            RowPagesBuilder probePages,
            boolean outputSingleMatch,
            boolean hasFilter)
    {
        return join(
                innerJoin(outputSingleMatch, false),
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                hasFilter,
                probePages.getTypes(),
                probePages.getHashChannels().orElseThrow(),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                TYPE_OPERATORS);
    }

    public static void instantiateBuildDrivers(BuildSideSetup buildSideSetup, TaskContext taskContext)
    {
        PipelineContext buildPipeline = taskContext.addPipelineContext(1, true, true, false);
        List<Driver> buildDrivers = new ArrayList<>();
        List<HashBuilderOperator> buildOperators = new ArrayList<>();
        for (int i = 0; i < buildSideSetup.getPartitionCount(); i++) {
            DriverContext buildDriverContext = buildPipeline.addDriverContext();
            HashBuilderOperator buildOperator = buildSideSetup.getBuildOperatorFactory().createOperator(buildDriverContext);
            Driver driver = Driver.createDriver(
                    buildDriverContext,
                    buildSideSetup.getBuildSideSourceOperatorFactory().createOperator(buildDriverContext),
                    buildOperator);
            buildDrivers.add(driver);
            buildOperators.add(buildOperator);
        }

        buildSideSetup.setDriversAndOperators(buildDrivers, buildOperators);
    }

    public static BuildSideSetup setupBuildSide(
            NodePartitioningManager nodePartitioningManager,
            boolean parallelBuild,
            TaskContext taskContext,
            RowPagesBuilder buildPages,
            Optional<InternalJoinFilterFunction> filterFunction)
    {
        return setupBuildSide(nodePartitioningManager, parallelBuild, taskContext, buildPages, filterFunction, true);
    }

    public static BuildSideSetup setupBuildSide(
            NodePartitioningManager nodePartitioningManager,
            boolean parallelBuild,
            TaskContext taskContext,
            RowPagesBuilder buildPages,
            Optional<InternalJoinFilterFunction> filterFunction,
            boolean enableSingleChannelBigintLookupSource)
    {
        Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory = filterFunction
                .map(function -> (session, addresses, pages) -> new StandardJoinFilterFunction(function, addresses, pages));

        int partitionCount = parallelBuild ? PARTITION_COUNT : 1;
        List<Integer> hashChannels = buildPages.getHashChannels().orElseThrow();
        List<Type> types = buildPages.getTypes();
        List<Type> hashChannelTypes = hashChannels.stream()
                .map(types::get)
                .collect(toImmutableList());
        LocalExchange localExchange = new LocalExchange(
                nodePartitioningManager,
                taskContext.getSession(),
                partitionCount,
                FIXED_HASH_DISTRIBUTION,
                hashChannels,
                hashChannelTypes,
                buildPages.getHashChannel(),
                DataSize.of(32, DataSize.Unit.MEGABYTE),
                TYPE_OPERATORS,
                DataSize.of(32, DataSize.Unit.MEGABYTE));

        // collect input data into the partitioned exchange
        DriverContext collectDriverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        ValuesOperator.ValuesOperatorFactory valuesOperatorFactory = new ValuesOperator.ValuesOperatorFactory(0, new PlanNodeId("values"), buildPages.build());
        LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory sinkOperatorFactory = new LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory(localExchange.createSinkFactory(), 1, new PlanNodeId("sink"), Function.identity());
        Driver sourceDriver = Driver.createDriver(collectDriverContext,
                valuesOperatorFactory.createOperator(collectDriverContext),
                sinkOperatorFactory.createOperator(collectDriverContext));
        valuesOperatorFactory.noMoreOperators();
        sinkOperatorFactory.noMoreOperators();
        sinkOperatorFactory.localPlannerComplete();

        while (!sourceDriver.isFinished()) {
            sourceDriver.processUntilBlocked();
        }

        // build side operator factories
        LocalExchangeSourceOperatorFactory sourceOperatorFactory = new LocalExchangeSourceOperatorFactory(0, new PlanNodeId("source"), localExchange);
        PartitionedLookupSourceFactory factory = new PartitionedLookupSourceFactory(
                buildPages.getTypes(),
                rangeList(buildPages.getTypes().size()).stream()
                        .map(buildPages.getTypes()::get)
                        .collect(toImmutableList()),
                hashChannels.stream()
                        .map(buildPages.getTypes()::get)
                        .collect(toImmutableList()),
                partitionCount,
                false,
                TYPE_OPERATORS);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = new JoinBridgeManager<PartitionedLookupSourceFactory>(
                false,
                factory,
                factory.getOutputTypes());

        HashBuilderOperatorFactory buildOperatorFactory = new HashBuilderOperatorFactory(
                1,
                new PlanNodeId("build"),
                lookupSourceFactoryManager,
                rangeList(buildPages.getTypes().size()),
                hashChannels,
                buildPages.getHashChannel()
                        .map(OptionalInt::of).orElse(OptionalInt.empty()),
                filterFunctionFactory,
                Optional.empty(),
                ImmutableList.of(),
                100,
                new PagesIndex.TestingFactory(false, enableSingleChannelBigintLookupSource),
                incrementalLoadFactorHashArraySizeSupplier(taskContext.getSession()));
        return new BuildSideSetup(lookupSourceFactoryManager, buildOperatorFactory, sourceOperatorFactory, partitionCount);
    }

    public static void buildLookupSource(ExecutorService executor, BuildSideSetup buildSideSetup)
    {
        requireNonNull(buildSideSetup, "buildSideSetup is null");

        PartitionedLookupSourceFactory lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager().getJoinBridge();
        Future<LookupSource> lookupSource = lookupSourceFactory.createLookupSource();
        List<Driver> buildDrivers = buildSideSetup.getBuildDrivers();

        while (!lookupSource.isDone()) {
            for (Driver buildDriver : buildDrivers) {
                buildDriver.processForNumberOfIterations(1);
            }
        }
        getFutureValue(lookupSource).close();

        for (Driver buildDriver : buildDrivers) {
            runDriverInThread(executor, buildDriver);
        }
    }

    /**
     * Runs Driver in another thread until it is finished
     */
    public static void runDriverInThread(ExecutorService executor, Driver driver)
    {
        executor.execute(() -> {
            if (!driver.isFinished()) {
                try {
                    driver.processUntilBlocked();
                }
                catch (TrinoException e) {
                    driver.getDriverContext().failed(e);
                    return;
                }
                runDriverInThread(executor, driver);
            }
        });
    }

    public static OptionalInt getHashChannelAsInt(RowPagesBuilder probePages)
    {
        return probePages.getHashChannel()
                .map(OptionalInt::of).orElse(OptionalInt.empty());
    }

    private static List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }

    public static class BuildSideSetup
    {
        private final JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager;
        private final HashBuilderOperatorFactory buildOperatorFactory;
        private final LocalExchangeSourceOperatorFactory buildSideSourceOperatorFactory;
        private final int partitionCount;
        private List<Driver> buildDrivers;
        private List<HashBuilderOperator> buildOperators;

        public BuildSideSetup(JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager, HashBuilderOperatorFactory buildOperatorFactory, LocalExchangeSourceOperatorFactory buildSideSourceOperatorFactory, int partitionCount)
        {
            this.lookupSourceFactoryManager = requireNonNull(lookupSourceFactoryManager, "lookupSourceFactoryManager is null");
            this.buildOperatorFactory = requireNonNull(buildOperatorFactory, "buildOperatorFactory is null");
            this.buildSideSourceOperatorFactory = buildSideSourceOperatorFactory;
            this.partitionCount = partitionCount;
        }

        public void setDriversAndOperators(List<Driver> buildDrivers, List<HashBuilderOperator> buildOperators)
        {
            checkArgument(buildDrivers.size() == buildOperators.size());
            this.buildDrivers = ImmutableList.copyOf(buildDrivers);
            this.buildOperators = ImmutableList.copyOf(buildOperators);
        }

        public JoinBridgeManager<PartitionedLookupSourceFactory> getLookupSourceFactoryManager()
        {
            return lookupSourceFactoryManager;
        }

        public HashBuilderOperatorFactory getBuildOperatorFactory()
        {
            return buildOperatorFactory;
        }

        public LocalExchangeSourceOperatorFactory getBuildSideSourceOperatorFactory()
        {
            return buildSideSourceOperatorFactory;
        }

        public int getPartitionCount()
        {
            return partitionCount;
        }

        public List<Driver> getBuildDrivers()
        {
            checkState(buildDrivers != null, "buildDrivers is not initialized yet");
            return buildDrivers;
        }

        public List<HashBuilderOperator> getBuildOperators()
        {
            checkState(buildOperators != null, "buildDrivers is not initialized yet");
            return buildOperators;
        }
    }

    public static class TestInternalJoinFilterFunction
            implements InternalJoinFilterFunction
    {
        public interface Lambda
        {
            boolean filter(int leftPosition, Page leftPage, int rightPosition, Page rightPage);
        }

        private final Lambda lambda;

        public TestInternalJoinFilterFunction(Lambda lambda)
        {
            this.lambda = lambda;
        }

        @Override
        public boolean filter(int leftPosition, Page leftPage, int rightPosition, Page rightPage)
        {
            return lambda.filter(leftPosition, leftPage, rightPosition, rightPage);
        }
    }
}
