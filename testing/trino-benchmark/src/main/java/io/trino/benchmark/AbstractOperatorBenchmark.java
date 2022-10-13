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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.stats.CpuTimer;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStateMachine;
import io.trino.memory.MemoryPool;
import io.trino.memory.QueryContext;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.FilterAndProjectOperator;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PageSourceOperator;
import io.trino.operator.TaskContext;
import io.trino.operator.TaskStats;
import io.trino.operator.project.InputPageProjection;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProjection;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.type.Type;
import io.trino.spiller.SpillSpaceTracker;
import io.trino.split.SplitSource;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.optimizations.HashGenerationOptimizer;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.testing.LocalQueryRunner;
import io.trino.transaction.TransactionId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.stats.CpuTimer.CpuDuration;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SystemSessionProperties.getFilterAndProjectMinOutputPageRowCount;
import static io.trino.SystemSessionProperties.getFilterAndProjectMinOutputPageSize;
import static io.trino.execution.executor.PrioritizedSplitRunner.SPLIT_RUN_QUANTA;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.sql.relational.SqlToRowExpressionTranslator.translate;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Abstract template for benchmarks that want to test the performance of an Operator.
 */
public abstract class AbstractOperatorBenchmark
        extends AbstractBenchmark
{
    protected final LocalQueryRunner localQueryRunner;
    protected final Session session;

    protected AbstractOperatorBenchmark(
            LocalQueryRunner localQueryRunner,
            String benchmarkName,
            int warmupIterations,
            int measuredIterations)
    {
        this(localQueryRunner.getDefaultSession(), localQueryRunner, benchmarkName, warmupIterations, measuredIterations);
    }

    protected AbstractOperatorBenchmark(
            Session session,
            LocalQueryRunner localQueryRunner,
            String benchmarkName,
            int warmupIterations,
            int measuredIterations)
    {
        super(benchmarkName, warmupIterations, measuredIterations);
        this.localQueryRunner = requireNonNull(localQueryRunner, "localQueryRunner is null");

        TransactionId transactionId = localQueryRunner.getTransactionManager().beginTransaction(false);
        this.session = session.beginTransactionId(
                transactionId,
                localQueryRunner.getTransactionManager(),
                new AllowAllAccessControl());
    }

    @Override
    protected void tearDown()
    {
        localQueryRunner.getTransactionManager().asyncAbort(session.getRequiredTransactionId());
        super.tearDown();
    }

    protected final List<Type> getColumnTypes(String tableName, String... columnNames)
    {
        checkState(session.getCatalog().isPresent(), "catalog not set");
        checkState(session.getSchema().isPresent(), "schema not set");

        // look up the table
        Metadata metadata = localQueryRunner.getMetadata();
        QualifiedObjectName qualifiedTableName = new QualifiedObjectName(session.getCatalog().get(), session.getSchema().get(), tableName);
        TableHandle tableHandle = metadata.getTableHandle(session, qualifiedTableName)
                .orElseThrow(() -> new IllegalArgumentException(format("Table '%s' does not exist", qualifiedTableName)));

        Map<String, ColumnHandle> allColumnHandles = metadata.getColumnHandles(session, tableHandle);
        return Arrays.stream(columnNames)
                .map(allColumnHandles::get)
                .map(columnHandle -> metadata.getColumnMetadata(session, tableHandle, columnHandle).getType())
                .collect(toImmutableList());
    }

    protected final BenchmarkAggregationFunction createAggregationFunction(String name, Type... argumentTypes)
    {
        ResolvedFunction resolvedFunction = localQueryRunner.getMetadata().resolveFunction(session, QualifiedName.of(name), fromTypes(argumentTypes));
        AggregationImplementation aggregationImplementation = localQueryRunner.getFunctionManager().getAggregationImplementation(resolvedFunction);
        return new BenchmarkAggregationFunction(resolvedFunction, aggregationImplementation);
    }

    protected final OperatorFactory createTableScanOperator(int operatorId, PlanNodeId planNodeId, String tableName, String... columnNames)
    {
        checkArgument(session.getCatalog().isPresent(), "catalog not set");
        checkArgument(session.getSchema().isPresent(), "schema not set");

        // look up the table
        Metadata metadata = localQueryRunner.getMetadata();
        QualifiedObjectName qualifiedTableName = new QualifiedObjectName(session.getCatalog().get(), session.getSchema().get(), tableName);
        TableHandle tableHandle = metadata.getTableHandle(session, qualifiedTableName).orElse(null);
        checkArgument(tableHandle != null, "Table '%s' does not exist", qualifiedTableName);

        // lookup the columns
        Map<String, ColumnHandle> allColumnHandles = metadata.getColumnHandles(session, tableHandle);
        ImmutableList.Builder<ColumnHandle> columnHandlesBuilder = ImmutableList.builder();
        for (String columnName : columnNames) {
            ColumnHandle columnHandle = allColumnHandles.get(columnName);
            checkArgument(columnHandle != null, "Table '%s' does not have a column '%s'", tableName, columnName);
            columnHandlesBuilder.add(columnHandle);
        }
        List<ColumnHandle> columnHandles = columnHandlesBuilder.build();

        // get the split for this table
        Split split = getLocalQuerySplit(session, tableHandle);

        return new OperatorFactory()
        {
            @Override
            public Operator createOperator(DriverContext driverContext)
            {
                OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, "BenchmarkSource");
                ConnectorPageSource pageSource = localQueryRunner.getPageSourceManager().createPageSource(session, split, tableHandle, columnHandles, DynamicFilter.EMPTY);
                return new PageSourceOperator(pageSource, operatorContext);
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
        };
    }

    private Split getLocalQuerySplit(Session session, TableHandle handle)
    {
        SplitSource splitSource = localQueryRunner.getSplitManager().getSplits(session, handle, EMPTY, alwaysTrue());
        List<Split> splits = new ArrayList<>();
        while (!splitSource.isFinished()) {
            splits.addAll(getNextBatch(splitSource));
        }
        checkArgument(splits.size() == 1, "Expected only one split for a local query, but got %s splits", splits.size());
        return splits.get(0);
    }

    private static List<Split> getNextBatch(SplitSource splitSource)
    {
        return getFutureValue(splitSource.getNextBatch(1000)).getSplits();
    }

    protected final OperatorFactory createHashProjectOperator(int operatorId, PlanNodeId planNodeId, List<Type> types)
    {
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        ImmutableMap.Builder<Symbol, Integer> symbolToInputMapping = ImmutableMap.builder();
        ImmutableList.Builder<PageProjection> projections = ImmutableList.builder();
        for (int channel = 0; channel < types.size(); channel++) {
            Symbol symbol = symbolAllocator.newSymbol("h" + channel, types.get(channel));
            symbolToInputMapping.put(symbol, channel);
            projections.add(new InputPageProjection(channel, types.get(channel)));
        }

        Map<Symbol, Type> symbolTypes = symbolAllocator.getTypes().allTypes();
        Optional<Expression> hashExpression = HashGenerationOptimizer.getHashExpression(
                session,
                localQueryRunner.getMetadata(),
                symbolAllocator,
                ImmutableList.copyOf(symbolTypes.keySet()));
        verify(hashExpression.isPresent());

        Map<NodeRef<Expression>, Type> expressionTypes = createTestingTypeAnalyzer(localQueryRunner.getPlannerContext())
                .getTypes(session, TypeProvider.copyOf(symbolTypes), hashExpression.get());

        RowExpression translated = translate(hashExpression.get(), expressionTypes, symbolToInputMapping.buildOrThrow(), localQueryRunner.getMetadata(), localQueryRunner.getFunctionManager(), session, false);

        PageFunctionCompiler functionCompiler = new PageFunctionCompiler(localQueryRunner.getFunctionManager(), 0);
        projections.add(functionCompiler.compileProjection(translated, Optional.empty()).get());

        return FilterAndProjectOperator.createOperatorFactory(
                operatorId,
                planNodeId,
                () -> new PageProcessor(Optional.empty(), projections.build()),
                ImmutableList.copyOf(Iterables.concat(types, ImmutableList.of(BIGINT))),
                getFilterAndProjectMinOutputPageSize(session),
                getFilterAndProjectMinOutputPageRowCount(session));
    }

    protected abstract List<Driver> createDrivers(TaskContext taskContext);

    protected Map<String, Long> execute(TaskContext taskContext)
    {
        List<Driver> drivers = createDrivers(taskContext);

        long peakMemory = 0;
        boolean done = false;
        while (!done) {
            boolean processed = false;
            for (Driver driver : drivers) {
                if (!driver.isFinished()) {
                    driver.processForDuration(SPLIT_RUN_QUANTA);
                    long lastPeakMemory = peakMemory;
                    peakMemory = taskContext.getTaskStats().getUserMemoryReservation().toBytes();
                    if (peakMemory <= lastPeakMemory) {
                        peakMemory = lastPeakMemory;
                    }
                    processed = true;
                }
            }
            done = !processed;
        }
        return ImmutableMap.of("peak_memory", peakMemory);
    }

    @Override
    protected Map<String, Long> runOnce()
    {
        Session session = testSessionBuilder()
                .setSystemProperty("optimizer.optimize-hash-generation", "true")
                .setTransactionId(this.session.getRequiredTransactionId())
                .build();
        MemoryPool memoryPool = new MemoryPool(DataSize.of(1, GIGABYTE));
        SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(DataSize.of(1, GIGABYTE));

        TaskContext taskContext = new QueryContext(
                new QueryId("test"),
                DataSize.of(256, MEGABYTE),
                memoryPool,
                new TestingGcMonitor(),
                localQueryRunner.getExecutor(),
                localQueryRunner.getScheduler(),
                DataSize.of(256, MEGABYTE),
                spillSpaceTracker)
                .addTaskContext(new TaskStateMachine(new TaskId(new StageId("query", 0), 0, 0), localQueryRunner.getExecutor()),
                        session,
                        () -> {},
                        false,
                        false);

        CpuTimer cpuTimer = new CpuTimer();
        Map<String, Long> executionStats = execute(taskContext);
        CpuDuration executionTime = cpuTimer.elapsedTime();

        TaskStats taskStats = taskContext.getTaskStats();
        long inputRows = taskStats.getRawInputPositions();
        long inputBytes = taskStats.getRawInputDataSize().toBytes();
        long outputRows = taskStats.getOutputPositions();
        long outputBytes = taskStats.getOutputDataSize().toBytes();

        double inputMegaBytes = ((double) inputBytes) / MEGABYTE.inBytes();

        return ImmutableMap.<String, Long>builder()
                // legacy computed values
                .putAll(executionStats)
                .put("elapsed_millis", executionTime.getWall().toMillis())
                .put("input_rows_per_second", (long) (inputRows / executionTime.getWall().getValue(SECONDS)))
                .put("output_rows_per_second", (long) (outputRows / executionTime.getWall().getValue(SECONDS)))
                .put("input_megabytes", (long) inputMegaBytes)
                .put("input_megabytes_per_second", (long) (inputMegaBytes / executionTime.getWall().getValue(SECONDS)))

                .put("wall_nanos", executionTime.getWall().roundTo(NANOSECONDS))
                .put("cpu_nanos", executionTime.getCpu().roundTo(NANOSECONDS))
                .put("user_nanos", executionTime.getUser().roundTo(NANOSECONDS))
                .put("input_rows", inputRows)
                .put("input_bytes", inputBytes)
                .put("output_rows", outputRows)
                .put("output_bytes", outputBytes)
                .buildOrThrow();
    }
}
