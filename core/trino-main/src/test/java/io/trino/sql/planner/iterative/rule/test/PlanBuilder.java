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
package io.trino.sql.planner.iterative.rule.test;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.metadata.FunctionResolver;
import io.trino.metadata.IndexHandle;
import io.trino.metadata.OutputTableHandle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableFunctionHandle;
import io.trino.metadata.TableHandle;
import io.trino.operator.RetryPolicy;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TestingConnectorIndexHandle;
import io.trino.sql.planner.TestingConnectorTransactionHandle;
import io.trino.sql.planner.TestingWriterTarget;
import io.trino.sql.planner.assertions.AggregationFunction;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.IndexSourceNode;
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.MergeProcessorNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.StatisticAggregations;
import io.trino.sql.planner.plan.StatisticAggregationsDescriptor;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableFunctionNode;
import io.trino.sql.planner.plan.TableFunctionNode.TableArgumentProperties;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TableWriterNode.CreateTarget;
import io.trino.sql.planner.plan.TableWriterNode.MergeParadigmAndTypes;
import io.trino.sql.planner.plan.TableWriterNode.MergeTarget;
import io.trino.sql.planner.plan.TableWriterNode.WriterTarget;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.TopNRankingNode.RankingType;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.QualifiedName;
import io.trino.testing.TestingHandle;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import io.trino.testing.TestingTableExecuteHandle;
import io.trino.testing.TestingTransactionHandle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class PlanBuilder
{
    private final PlanNodeIdAllocator idAllocator;
    private final Session session;
    private final Map<String, Symbol> symbolsByName = new HashMap<>();
    private final FunctionResolver functionResolver;

    public PlanBuilder(PlanNodeIdAllocator idAllocator, PlannerContext plannerContext, Session session)
    {
        this.idAllocator = idAllocator;
        this.session = session;
        functionResolver = plannerContext.getFunctionResolver();
    }

    public Session getSession()
    {
        return session;
    }

    public OutputNode output(List<String> columnNames, List<Symbol> outputs, PlanNode source)
    {
        return new OutputNode(
                idAllocator.getNextId(),
                source,
                columnNames,
                outputs);
    }

    public ExplainAnalyzeNode explainAnalyzeNode(Symbol output, List<Symbol> actualOutputs, PlanNode source)
    {
        return new ExplainAnalyzeNode(
                idAllocator.getNextId(),
                source,
                output,
                actualOutputs,
                false);
    }

    public OutputNode output(Consumer<OutputBuilder> outputBuilderConsumer)
    {
        OutputBuilder outputBuilder = new OutputBuilder();
        outputBuilderConsumer.accept(outputBuilder);
        return outputBuilder.build();
    }

    public class OutputBuilder
    {
        private PlanNode source;
        private final List<String> columnNames = new ArrayList<>();
        private final List<Symbol> outputs = new ArrayList<>();

        public OutputBuilder source(PlanNode source)
        {
            this.source = source;
            return this;
        }

        public OutputBuilder column(Symbol symbol)
        {
            return column(symbol, symbol.name());
        }

        public OutputBuilder column(Symbol symbol, String columnName)
        {
            outputs.add(symbol);
            columnNames.add(columnName);
            return this;
        }

        protected OutputNode build()
        {
            return new OutputNode(idAllocator.getNextId(), source, columnNames, outputs);
        }
    }

    public ValuesNode values(Symbol... columns)
    {
        return values(idAllocator.getNextId(), columns);
    }

    public ValuesNode values(PlanNodeId id, Symbol... columns)
    {
        return values(id, 0, columns);
    }

    public ValuesNode values(int rows, Symbol... columns)
    {
        return values(idAllocator.getNextId(), rows, columns);
    }

    public ValuesNode values(PlanNodeId id, int rows, Symbol... columns)
    {
        List<Expression> row = Arrays.stream(columns)
                .map(symbol -> new Constant(symbol.type(), null))
                .collect(Collectors.toList());

        return values(id, ImmutableList.copyOf(columns), nCopies(rows, row));
    }

    public ValuesNode values(List<Symbol> columns, List<List<Expression>> rows)
    {
        return values(idAllocator.getNextId(), columns, rows);
    }

    public ValuesNode values(PlanNodeId id, List<Symbol> columns, List<List<Expression>> rows)
    {
        return new ValuesNode(id, columns, rows.stream().map(Row::new).collect(toImmutableList()));
    }

    public ValuesNode valuesOfExpressions(List<Symbol> columns, List<Expression> rows)
    {
        return new ValuesNode(idAllocator.getNextId(), columns, rows);
    }

    public EnforceSingleRowNode enforceSingleRow(PlanNode source)
    {
        return new EnforceSingleRowNode(idAllocator.getNextId(), source);
    }

    public SortNode sort(List<Symbol> orderBySymbols, PlanNode source)
    {
        return new SortNode(
                idAllocator.getNextId(),
                source,
                new OrderingScheme(
                        orderBySymbols,
                        Maps.toMap(orderBySymbols, Functions.constant(SortOrder.ASC_NULLS_FIRST))),
                false);
    }

    public OffsetNode offset(long rowCount, PlanNode source)
    {
        return new OffsetNode(idAllocator.getNextId(), source, rowCount);
    }

    public LimitNode limit(long limit, PlanNode source)
    {
        return limit(limit, ImmutableList.of(), source);
    }

    public LimitNode limit(long limit, List<Symbol> tiesResolvers, PlanNode source)
    {
        return limit(limit, tiesResolvers, false, ImmutableList.of(), source);
    }

    public LimitNode limit(long limit, boolean partial, List<Symbol> preSortedInputs, PlanNode source)
    {
        return limit(limit, ImmutableList.of(), partial, preSortedInputs, source);
    }

    public LimitNode limit(long limit, List<Symbol> tiesResolvers, boolean partial, List<Symbol> preSortedInputs, PlanNode source)
    {
        Optional<OrderingScheme> tiesResolvingScheme = Optional.empty();
        if (!tiesResolvers.isEmpty()) {
            tiesResolvingScheme = Optional.of(
                    new OrderingScheme(
                            tiesResolvers,
                            Maps.toMap(tiesResolvers, Functions.constant(SortOrder.ASC_NULLS_FIRST))));
        }
        return new LimitNode(
                idAllocator.getNextId(),
                source,
                limit,
                tiesResolvingScheme,
                partial,
                preSortedInputs);
    }

    public TopNNode topN(long count, List<Symbol> orderBySymbols, PlanNode source)
    {
        return topN(count, orderBySymbols, TopNNode.Step.SINGLE, source);
    }

    public TopNNode topN(long count, List<Symbol> orderBySymbols, TopNNode.Step step, PlanNode source)
    {
        return topN(count, orderBySymbols, step, SortOrder.ASC_NULLS_FIRST, source);
    }

    public TopNNode topN(long count, List<Symbol> orderBySymbols, TopNNode.Step step, SortOrder sortOrder, PlanNode source)
    {
        return new TopNNode(
                idAllocator.getNextId(),
                source,
                count,
                new OrderingScheme(
                        orderBySymbols,
                        Maps.toMap(orderBySymbols, Functions.constant(sortOrder))),
                step);
    }

    public SampleNode sample(double sampleRatio, SampleNode.Type type, PlanNode source)
    {
        return new SampleNode(idAllocator.getNextId(), source, sampleRatio, type);
    }

    public ProjectNode project(Assignments assignments, PlanNode source)
    {
        return new ProjectNode(idAllocator.getNextId(), source, assignments);
    }

    public MarkDistinctNode markDistinct(Symbol markerSymbol, List<Symbol> distinctSymbols, PlanNode source)
    {
        return new MarkDistinctNode(idAllocator.getNextId(), source, markerSymbol, distinctSymbols, Optional.empty());
    }

    public MarkDistinctNode markDistinct(Symbol markerSymbol, List<Symbol> distinctSymbols, Symbol hashSymbol, PlanNode source)
    {
        return new MarkDistinctNode(idAllocator.getNextId(), source, markerSymbol, distinctSymbols, Optional.of(hashSymbol));
    }

    public FilterNode filter(Expression predicate, PlanNode source)
    {
        return filter(idAllocator.getNextId(), predicate, source);
    }

    public FilterNode filter(PlanNodeId planNodeId, Expression predicate, PlanNode source)
    {
        return new FilterNode(planNodeId, source, predicate);
    }

    public AggregationNode aggregation(Consumer<AggregationBuilder> aggregationBuilderConsumer)
    {
        AggregationBuilder aggregationBuilder = new AggregationBuilder();
        aggregationBuilderConsumer.accept(aggregationBuilder);
        return aggregationBuilder.build();
    }

    public GroupIdNode groupId(List<List<Symbol>> groupingSets, List<Symbol> aggregationArguments, Symbol groupIdSymbol, PlanNode source)
    {
        Map<Symbol, Symbol> groupingColumns = groupingSets.stream()
                .flatMap(Collection::stream)
                .distinct()
                .collect(toImmutableMap(identity(), identity()));
        return groupId(groupingSets, groupingColumns, aggregationArguments, groupIdSymbol, source);
    }

    public GroupIdNode groupId(List<List<Symbol>> groupingSets, Map<Symbol, Symbol> groupingColumns, List<Symbol> aggregationArguments, Symbol groupIdSymbol, PlanNode source)
    {
        return new GroupIdNode(
                idAllocator.getNextId(),
                source,
                groupingSets,
                groupingColumns,
                aggregationArguments,
                groupIdSymbol);
    }

    public DistinctLimitNode distinctLimit(long count, List<Symbol> distinctSymbols, PlanNode source)
    {
        return distinctLimit(count, distinctSymbols, Optional.empty(), source);
    }

    public DistinctLimitNode distinctLimit(long count, List<Symbol> distinctSymbols, Optional<Symbol> hashSymbol, PlanNode source)
    {
        return new DistinctLimitNode(
                idAllocator.getNextId(),
                source,
                count,
                false,
                distinctSymbols,
                hashSymbol);
    }

    public class AggregationBuilder
    {
        private PlanNode source;
        private final Map<Symbol, Aggregation> assignments = new LinkedHashMap<>();
        private AggregationNode.GroupingSetDescriptor groupingSets;
        private List<Symbol> preGroupedSymbols = new ArrayList<>();
        private Step step = Step.SINGLE;
        private Optional<Symbol> hashSymbol = Optional.empty();
        private Optional<Symbol> groupIdSymbol = Optional.empty();
        private Optional<PlanNodeId> nodeId = Optional.empty();
        private Optional<Boolean> exchangeInputAggregation = Optional.empty();

        public AggregationBuilder source(PlanNode source)
        {
            this.source = source;
            return this;
        }

        public AggregationBuilder addAggregation(Symbol output, AggregationFunction aggregation, List<Type> inputTypes)
        {
            return addAggregation(output, aggregation, inputTypes, Optional.empty());
        }

        public AggregationBuilder addAggregation(Symbol output, AggregationFunction aggregation, List<Type> inputTypes, Symbol mask)
        {
            return addAggregation(output, aggregation, inputTypes, Optional.of(mask));
        }

        private AggregationBuilder addAggregation(Symbol output, AggregationFunction aggregation, List<Type> inputTypes, Optional<Symbol> mask)
        {
            ResolvedFunction resolvedFunction = functionResolver.resolveFunction(session, QualifiedName.of(aggregation.name()), TypeSignatureProvider.fromTypes(inputTypes), new AllowAllAccessControl());
            return addAggregation(output, new Aggregation(
                    resolvedFunction,
                    aggregation.arguments(),
                    aggregation.distinct(),
                    aggregation.filter(),
                    aggregation.orderingScheme(),
                    mask));
        }

        public AggregationBuilder addAggregation(Symbol output, Aggregation aggregation)
        {
            assignments.put(output, aggregation);
            return this;
        }

        public AggregationBuilder globalGrouping()
        {
            groupingSets(AggregationNode.singleGroupingSet(ImmutableList.of()));
            return this;
        }

        public AggregationBuilder singleGroupingSet(Symbol... symbols)
        {
            groupingSets(AggregationNode.singleGroupingSet(ImmutableList.copyOf(symbols)));
            return this;
        }

        public AggregationBuilder groupingSets(AggregationNode.GroupingSetDescriptor groupingSets)
        {
            checkState(this.groupingSets == null, "groupingSets already defined");
            this.groupingSets = groupingSets;
            return this;
        }

        public AggregationBuilder preGroupedSymbols(Symbol... symbols)
        {
            checkState(this.preGroupedSymbols.isEmpty(), "preGroupedSymbols already defined");
            this.preGroupedSymbols = ImmutableList.copyOf(symbols);
            return this;
        }

        public AggregationBuilder step(Step step)
        {
            this.step = step;
            return this;
        }

        public AggregationBuilder hashSymbol(Symbol hashSymbol)
        {
            this.hashSymbol = Optional.of(hashSymbol);
            return this;
        }

        public AggregationBuilder nodeId(PlanNodeId nodeId)
        {
            this.nodeId = Optional.of(nodeId);
            return this;
        }

        public AggregationBuilder exchangeInputAggregation(boolean exchangeInputAggregation)
        {
            this.exchangeInputAggregation = Optional.of(exchangeInputAggregation);
            return this;
        }

        protected AggregationNode build()
        {
            checkState(groupingSets != null, "No grouping sets defined; use globalGrouping/groupingKeys method");
            return new AggregationNode(
                    nodeId.orElseGet(idAllocator::getNextId),
                    source,
                    assignments,
                    groupingSets,
                    preGroupedSymbols,
                    step,
                    hashSymbol,
                    groupIdSymbol,
                    exchangeInputAggregation);
        }
    }

    public ApplyNode apply(Map<Symbol, ApplyNode.SetExpression> subqueryAssignments, List<Symbol> correlation, PlanNode input, PlanNode subquery)
    {
        io.trino.sql.tree.NullLiteral originSubquery = new io.trino.sql.tree.NullLiteral(); // does not matter for tests
        return new ApplyNode(idAllocator.getNextId(), input, subquery, subqueryAssignments, correlation, originSubquery);
    }

    public AssignUniqueId assignUniqueId(Symbol unique, PlanNode source)
    {
        return new AssignUniqueId(idAllocator.getNextId(), source, unique);
    }

    public CorrelatedJoinNode correlatedJoin(List<Symbol> correlation, PlanNode input, PlanNode subquery)
    {
        return correlatedJoin(correlation, input, JoinType.INNER, TRUE, subquery);
    }

    public CorrelatedJoinNode correlatedJoin(List<Symbol> correlation, PlanNode input, JoinType type, Expression filter, PlanNode subquery)
    {
        io.trino.sql.tree.NullLiteral originSubquery = new io.trino.sql.tree.NullLiteral(); // does not matter for tests
        return new CorrelatedJoinNode(idAllocator.getNextId(), input, subquery, correlation, type, filter, originSubquery);
    }

    public TableScanNode tableScan(List<Symbol> symbols, boolean forDelete)
    {
        return tableScan(tableScan -> tableScan
                .setSymbols(symbols)
                .setAssignmentsForSymbols(symbols)
                .setUpdateTarget(forDelete));
    }

    public TableScanNode tableScan(List<Symbol> symbols, Map<Symbol, ColumnHandle> assignments)
    {
        return tableScan(tableScan -> tableScan
                .setSymbols(symbols)
                .setAssignments(assignments));
    }

    public TableScanNode tableScan(
            TableHandle tableHandle,
            List<Symbol> symbols,
            Map<Symbol, ColumnHandle> assignments)
    {
        return tableScan(tableScan -> tableScan
                .setTableHandle(tableHandle)
                .setSymbols(symbols)
                .setAssignments(assignments));
    }

    public TableScanNode tableScan(
            TableHandle tableHandle,
            List<Symbol> symbols,
            Map<Symbol, ColumnHandle> assignments,
            boolean forDelete)
    {
        return tableScan(tableScan -> tableScan
                .setTableHandle(tableHandle)
                .setSymbols(symbols)
                .setAssignments(assignments)
                .setUpdateTarget(forDelete));
    }

    public TableScanNode tableScan(
            TableHandle tableHandle,
            List<Symbol> symbols,
            Map<Symbol, ColumnHandle> assignments,
            Optional<Boolean> useConnectorNodePartitioning)
    {
        return tableScan(tableScan -> tableScan
                .setTableHandle(tableHandle)
                .setSymbols(symbols)
                .setAssignments(assignments)
                .setUseConnectorNodePartitioning(useConnectorNodePartitioning));
    }

    public TableScanNode tableScan(
            TableHandle tableHandle,
            List<Symbol> symbols,
            Map<Symbol, ColumnHandle> assignments,
            TupleDomain<ColumnHandle> enforcedConstraint)
    {
        return tableScan(tableScan -> tableScan
                .setTableHandle(tableHandle)
                .setSymbols(symbols)
                .setAssignments(assignments)
                .setEnforcedConstraint(enforcedConstraint));
    }

    public TableScanNode tableScan(Consumer<TableScanBuilder> consumer)
    {
        TableScanBuilder tableScan = new TableScanBuilder(idAllocator);
        consumer.accept(tableScan);
        return tableScan.build();
    }

    public static class TableScanBuilder
    {
        private final PlanNodeIdAllocator idAllocator;
        private TableHandle tableHandle = new TableHandle(TEST_CATALOG_HANDLE, new TestingTableHandle(), TestingTransactionHandle.create());
        private List<Symbol> symbols;
        private Map<Symbol, ColumnHandle> assignments;
        private TupleDomain<ColumnHandle> enforcedConstraint = TupleDomain.all();
        private Optional<PlanNodeStatsEstimate> statistics = Optional.empty();
        private boolean updateTarget;
        private Optional<Boolean> useConnectorNodePartitioning = Optional.empty();
        private Optional<PlanNodeId> nodeId = Optional.empty();

        private TableScanBuilder(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        public TableScanBuilder setTableHandle(TableHandle tableHandle)
        {
            this.tableHandle = tableHandle;
            return this;
        }

        public TableScanBuilder setSymbols(List<Symbol> symbols)
        {
            this.symbols = symbols;
            return this;
        }

        public TableScanBuilder setAssignmentsForSymbols(List<Symbol> symbols)
        {
            return setAssignments(symbols.stream().collect(toImmutableMap(identity(), symbol -> new TestingColumnHandle(symbol.name()))));
        }

        public TableScanBuilder setAssignments(Map<Symbol, ColumnHandle> assignments)
        {
            this.assignments = assignments;
            return this;
        }

        public TableScanBuilder setEnforcedConstraint(TupleDomain<ColumnHandle> enforcedConstraint)
        {
            this.enforcedConstraint = enforcedConstraint;
            return this;
        }

        public TableScanBuilder setStatistics(Optional<PlanNodeStatsEstimate> statistics)
        {
            this.statistics = statistics;
            return this;
        }

        public TableScanBuilder setUpdateTarget(boolean updateTarget)
        {
            this.updateTarget = updateTarget;
            return this;
        }

        public TableScanBuilder setNodeId(PlanNodeId id)
        {
            this.nodeId = Optional.of(id);
            return this;
        }

        public TableScanBuilder setUseConnectorNodePartitioning(Optional<Boolean> useConnectorNodePartitioning)
        {
            this.useConnectorNodePartitioning = useConnectorNodePartitioning;
            return this;
        }

        public TableScanNode build()
        {
            return new TableScanNode(
                    nodeId.orElseGet(idAllocator::getNextId),
                    tableHandle,
                    symbols,
                    assignments,
                    enforcedConstraint,
                    statistics,
                    updateTarget,
                    useConnectorNodePartitioning);
        }
    }

    public TableFinishNode tableFinish(PlanNode source, WriterTarget target, Symbol rowCountSymbol)
    {
        return new TableFinishNode(
                idAllocator.getNextId(),
                source,
                target,
                rowCountSymbol,
                Optional.empty(),
                Optional.empty());
    }

    public TableFinishNode tableWithExchangeCreate(WriterTarget target, PlanNode source, Symbol rowCountSymbol)
    {
        return tableFinish(
                exchange(e -> e
                        .addSource(tableWriter(
                                ImmutableList.of(rowCountSymbol),
                                ImmutableList.of("column_a"),
                                Optional.empty(),
                                target,
                                source,
                                rowCountSymbol))
                        .addInputsSet(rowCountSymbol)
                        .partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(rowCountSymbol)))),
                target,
                rowCountSymbol);
    }

    public CreateTarget createTarget(CatalogHandle catalogHandle, SchemaTableName schemaTableName, boolean multipleWritersPerPartitionSupported, OptionalInt maxWriterTasks, WriterScalingOptions writerScalingOptions, boolean replace)
    {
        OutputTableHandle tableHandle = new OutputTableHandle(
                catalogHandle,
                schemaTableName,
                TestingConnectorTransactionHandle.INSTANCE,
                TestingHandle.INSTANCE);
        return new CreateTarget(
                tableHandle,
                schemaTableName,
                multipleWritersPerPartitionSupported,
                maxWriterTasks,
                writerScalingOptions,
                replace);
    }

    public CreateTarget createTarget(CatalogHandle catalogHandle, SchemaTableName schemaTableName, boolean multipleWritersPerPartitionSupported, WriterScalingOptions writerScalingOptions, boolean replace)
    {
        return createTarget(catalogHandle, schemaTableName, multipleWritersPerPartitionSupported, OptionalInt.empty(), writerScalingOptions, replace);
    }

    public MergeWriterNode merge(SchemaTableName schemaTableName, PlanNode mergeSource, Symbol mergeRow, Symbol rowId, List<Symbol> outputs)
    {
        return merge(mergeSource, mergeTarget(schemaTableName), mergeRow, rowId, outputs);
    }

    public MergeWriterNode merge(PlanNode mergeSource, MergeTarget target, Symbol mergeRow, Symbol rowId, List<Symbol> outputs)
    {
        return new MergeWriterNode(
                idAllocator.getNextId(),
                mergeSource,
                target,
                ImmutableList.of(mergeRow, rowId),
                Optional.empty(),
                outputs);
    }

    public MergeProcessorNode mergeProcessor(SchemaTableName schemaTableName, PlanNode source, Symbol mergeRow, Symbol rowId, List<Symbol> dataColumnSymbols, List<Symbol> redistributionColumnSymbols, List<Symbol> outputs)
    {
        return new MergeProcessorNode(
                idAllocator.getNextId(),
                source,
                mergeTarget(schemaTableName),
                rowId,
                mergeRow,
                dataColumnSymbols,
                redistributionColumnSymbols,
                outputs);
    }

    public MergeTarget mergeTarget(SchemaTableName schemaTableName)
    {
        return mergeTarget(schemaTableName, new MergeParadigmAndTypes(Optional.of(DELETE_ROW_AND_INSERT_ROW), ImmutableList.of(), ImmutableList.of(), INTEGER));
    }

    public MergeTarget mergeTarget(SchemaTableName schemaTableName, MergeParadigmAndTypes mergeParadigmAndTypes)
    {
        return new MergeTarget(
                new TableHandle(
                        TEST_CATALOG_HANDLE,
                        new TestingTableHandle(),
                        TestingTransactionHandle.create()),
                Optional.empty(),
                schemaTableName,
                mergeParadigmAndTypes,
                List.of(),
                ImmutableListMultimap.of());
    }

    public ExchangeNode gatheringExchange(ExchangeNode.Scope scope, PlanNode child)
    {
        return exchange(builder -> builder.type(ExchangeNode.Type.GATHER)
                .scope(scope)
                .singleDistributionPartitioningScheme(child.getOutputSymbols())
                .addSource(child)
                .addInputsSet(child.getOutputSymbols()));
    }

    public SemiJoinNode semiJoin(
            Symbol sourceJoinSymbol,
            Symbol filteringSourceJoinSymbol,
            Symbol semiJoinOutput,
            Optional<Symbol> sourceHashSymbol,
            Optional<Symbol> filteringSourceHashSymbol,
            PlanNode source,
            PlanNode filteringSource)
    {
        return semiJoin(
                source,
                filteringSource,
                sourceJoinSymbol,
                filteringSourceJoinSymbol,
                semiJoinOutput,
                sourceHashSymbol,
                filteringSourceHashSymbol,
                Optional.empty(),
                Optional.empty());
    }

    public SemiJoinNode semiJoin(
            PlanNode source,
            PlanNode filteringSource,
            Symbol sourceJoinSymbol,
            Symbol filteringSourceJoinSymbol,
            Symbol semiJoinOutput,
            Optional<Symbol> sourceHashSymbol,
            Optional<Symbol> filteringSourceHashSymbol,
            Optional<SemiJoinNode.DistributionType> distributionType)
    {
        return semiJoin(
                source,
                filteringSource,
                sourceJoinSymbol,
                filteringSourceJoinSymbol,
                semiJoinOutput,
                sourceHashSymbol,
                filteringSourceHashSymbol,
                distributionType,
                Optional.empty());
    }

    public SemiJoinNode semiJoin(
            PlanNode source,
            PlanNode filteringSource,
            Symbol sourceJoinSymbol,
            Symbol filteringSourceJoinSymbol,
            Symbol semiJoinOutput,
            Optional<Symbol> sourceHashSymbol,
            Optional<Symbol> filteringSourceHashSymbol,
            Optional<SemiJoinNode.DistributionType> distributionType,
            Optional<DynamicFilterId> dynamicFilterId)
    {
        return new SemiJoinNode(
                idAllocator.getNextId(),
                source,
                filteringSource,
                sourceJoinSymbol,
                filteringSourceJoinSymbol,
                semiJoinOutput,
                sourceHashSymbol,
                filteringSourceHashSymbol,
                distributionType,
                dynamicFilterId);
    }

    public IndexSourceNode indexSource(
            TableHandle tableHandle,
            Set<Symbol> lookupSymbols,
            List<Symbol> outputSymbols,
            Map<Symbol, ColumnHandle> assignments)
    {
        return new IndexSourceNode(
                idAllocator.getNextId(),
                new IndexHandle(
                        tableHandle.catalogHandle(),
                        TestingConnectorTransactionHandle.INSTANCE,
                        TestingConnectorIndexHandle.INSTANCE),
                tableHandle,
                lookupSymbols,
                outputSymbols,
                assignments);
    }

    public ExchangeNode exchange(Consumer<ExchangeBuilder> exchangeBuilderConsumer)
    {
        ExchangeBuilder exchangeBuilder = new ExchangeBuilder();
        exchangeBuilderConsumer.accept(exchangeBuilder);
        return exchangeBuilder.build();
    }

    public class ExchangeBuilder
    {
        private ExchangeNode.Type type = ExchangeNode.Type.GATHER;
        private ExchangeNode.Scope scope = ExchangeNode.Scope.REMOTE;
        private PartitioningScheme partitioningScheme;
        private OrderingScheme orderingScheme;
        private final List<PlanNode> sources = new ArrayList<>();
        private final List<List<Symbol>> inputs = new ArrayList<>();

        public ExchangeBuilder type(ExchangeNode.Type type)
        {
            this.type = type;
            return this;
        }

        public ExchangeBuilder scope(ExchangeNode.Scope scope)
        {
            this.scope = scope;
            return this;
        }

        public ExchangeBuilder singleDistributionPartitioningScheme(Symbol... outputSymbols)
        {
            return singleDistributionPartitioningScheme(Arrays.asList(outputSymbols));
        }

        public ExchangeBuilder singleDistributionPartitioningScheme(List<Symbol> outputSymbols)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), outputSymbols));
        }

        public ExchangeBuilder fixedHashDistributionPartitioningScheme(List<Symbol> outputSymbols, List<Symbol> partitioningSymbols)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(
                    FIXED_HASH_DISTRIBUTION,
                    ImmutableList.copyOf(partitioningSymbols)),
                    ImmutableList.copyOf(outputSymbols)));
        }

        public ExchangeBuilder fixedHashDistributionPartitioningScheme(List<Symbol> outputSymbols, List<Symbol> partitioningSymbols, Symbol hashSymbol)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(
                    FIXED_HASH_DISTRIBUTION,
                    ImmutableList.copyOf(partitioningSymbols)),
                    ImmutableList.copyOf(outputSymbols),
                    Optional.of(hashSymbol)));
        }

        public ExchangeBuilder fixedHashDistributionPartitioningScheme(List<Symbol> outputSymbols, List<Symbol> partitioningSymbols, int partitionCount)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(
                    FIXED_HASH_DISTRIBUTION,
                    ImmutableList.copyOf(partitioningSymbols)),
                    ImmutableList.copyOf(outputSymbols),
                    Optional.empty(),
                    false,
                    Optional.empty(),
                    Optional.of(partitionCount)));
        }

        public ExchangeBuilder fixedArbitraryDistributionPartitioningScheme(List<Symbol> outputSymbols, int partitionCount)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(
                    FIXED_ARBITRARY_DISTRIBUTION,
                    ImmutableList.of()),
                    ImmutableList.copyOf(outputSymbols),
                    Optional.empty(),
                    false,
                    Optional.empty(),
                    Optional.of(partitionCount)));
        }

        public ExchangeBuilder partitioningScheme(PartitioningScheme partitioningScheme)
        {
            this.partitioningScheme = partitioningScheme;
            return this;
        }

        public ExchangeBuilder addSource(PlanNode source)
        {
            this.sources.add(source);
            return this;
        }

        public ExchangeBuilder addInputsSet(Symbol... inputs)
        {
            return addInputsSet(Arrays.asList(inputs));
        }

        public ExchangeBuilder addInputsSet(List<Symbol> inputs)
        {
            this.inputs.add(inputs);
            return this;
        }

        public ExchangeBuilder orderingScheme(OrderingScheme orderingScheme)
        {
            this.orderingScheme = orderingScheme;
            return this;
        }

        protected ExchangeNode build()
        {
            return new ExchangeNode(idAllocator.getNextId(), type, scope, partitioningScheme, sources, inputs, Optional.ofNullable(orderingScheme));
        }
    }

    public JoinNode join(JoinType joinType, PlanNode left, PlanNode right, JoinNode.EquiJoinClause... criteria)
    {
        return join(joinType, left, right, Optional.empty(), criteria);
    }

    public JoinNode join(JoinType joinType, PlanNode left, PlanNode right, Expression filter, JoinNode.EquiJoinClause... criteria)
    {
        return join(joinType, left, right, Optional.of(filter), criteria);
    }

    private JoinNode join(JoinType joinType, PlanNode left, PlanNode right, Optional<Expression> filter, JoinNode.EquiJoinClause... criteria)
    {
        return join(
                joinType,
                left,
                right,
                ImmutableList.copyOf(criteria),
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                filter,
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
    }

    public JoinNode join(JoinType type, PlanNode left, PlanNode right, List<JoinNode.EquiJoinClause> criteria, List<Symbol> leftOutputSymbols, List<Symbol> rightOutputSymbols, Optional<Expression> filter)
    {
        return join(type, left, right, criteria, leftOutputSymbols, rightOutputSymbols, filter, Optional.empty(), Optional.empty());
    }

    public JoinNode join(JoinType type, JoinNode.DistributionType distributionType, PlanNode left, PlanNode right, JoinNode.EquiJoinClause... criteria)
    {
        return join(
                type,
                left,
                right,
                ImmutableList.copyOf(criteria),
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(distributionType),
                ImmutableMap.of());
    }

    public JoinNode join(
            JoinType type,
            PlanNode left,
            PlanNode right,
            List<JoinNode.EquiJoinClause> criteria,
            List<Symbol> leftOutputSymbols,
            List<Symbol> rightOutputSymbols,
            Optional<Expression> filter,
            Optional<Symbol> leftHashSymbol,
            Optional<Symbol> rightHashSymbol)
    {
        return join(type, left, right, criteria, leftOutputSymbols, rightOutputSymbols, filter, leftHashSymbol, rightHashSymbol, Optional.empty(), ImmutableMap.of());
    }

    public JoinNode join(
            JoinType type,
            PlanNode left,
            PlanNode right,
            List<JoinNode.EquiJoinClause> criteria,
            List<Symbol> leftOutputSymbols,
            List<Symbol> rightOutputSymbols,
            Optional<Expression> filter,
            Optional<Symbol> leftHashSymbol,
            Optional<Symbol> rightHashSymbol,
            Map<DynamicFilterId, Symbol> dynamicFilters)
    {
        return join(type, left, right, criteria, leftOutputSymbols, rightOutputSymbols, filter, leftHashSymbol, rightHashSymbol, Optional.empty(), dynamicFilters);
    }

    public JoinNode join(
            JoinType type,
            PlanNode left,
            PlanNode right,
            List<JoinNode.EquiJoinClause> criteria,
            List<Symbol> leftOutputSymbols,
            List<Symbol> rightOutputSymbols,
            Optional<Expression> filter,
            Optional<Symbol> leftHashSymbol,
            Optional<Symbol> rightHashSymbol,
            Optional<JoinNode.DistributionType> distributionType,
            Map<DynamicFilterId, Symbol> dynamicFilters)
    {
        return join(idAllocator.getNextId(),
                type,
                left,
                right,
                criteria,
                leftOutputSymbols,
                rightOutputSymbols,
                filter,
                leftHashSymbol,
                rightHashSymbol,
                distributionType,
                dynamicFilters);
    }

    public JoinNode join(
            PlanNodeId id,
            JoinType type,
            PlanNode left,
            PlanNode right,
            List<JoinNode.EquiJoinClause> criteria,
            List<Symbol> leftOutputSymbols,
            List<Symbol> rightOutputSymbols,
            Optional<Expression> filter,
            Optional<Symbol> leftHashSymbol,
            Optional<Symbol> rightHashSymbol,
            Optional<JoinNode.DistributionType> distributionType,
            Map<DynamicFilterId, Symbol> dynamicFilters)
    {
        return new JoinNode(
                id,
                type,
                left,
                right,
                criteria,
                leftOutputSymbols,
                rightOutputSymbols,
                false,
                filter,
                leftHashSymbol,
                rightHashSymbol,
                distributionType,
                Optional.empty(),
                dynamicFilters,
                Optional.empty());
    }

    public PlanNode indexJoin(IndexJoinNode.Type type, PlanNode probe, PlanNode index)
    {
        return indexJoin(type, probe, index, emptyList(), Optional.empty(), Optional.empty());
    }

    public PlanNode indexJoin(
            IndexJoinNode.Type type,
            PlanNode probe,
            PlanNode index,
            List<IndexJoinNode.EquiJoinClause> criteria,
            Optional<Symbol> probeHashSymbol,
            Optional<Symbol> indexHashSymbol)
    {
        return new IndexJoinNode(
                idAllocator.getNextId(),
                type,
                probe,
                index,
                criteria,
                probeHashSymbol,
                indexHashSymbol);
    }

    public PlanNode spatialJoin(
            SpatialJoinNode.Type type,
            PlanNode left,
            PlanNode right,
            List<Symbol> outputSymbols,
            Expression filter)
    {
        return spatialJoin(type, left, right, outputSymbols, filter, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public PlanNode spatialJoin(
            SpatialJoinNode.Type type,
            PlanNode left,
            PlanNode right,
            List<Symbol> outputSymbols,
            Expression filter,
            Optional<Symbol> leftPartitionSymbol,
            Optional<Symbol> rightPartitionSymbol,
            Optional<Slice> kdbTree)
    {
        return new SpatialJoinNode(
                idAllocator.getNextId(),
                type,
                left,
                right,
                outputSymbols,
                filter,
                leftPartitionSymbol,
                rightPartitionSymbol,
                kdbTree);
    }

    public UnionNode union(ListMultimap<Symbol, Symbol> outputsToInputs, List<PlanNode> sources)
    {
        List<Symbol> outputs = ImmutableList.copyOf(outputsToInputs.keySet());
        return new UnionNode(idAllocator.getNextId(), sources, outputsToInputs, outputs);
    }

    public IntersectNode intersect(ListMultimap<Symbol, Symbol> outputsToInputs, List<PlanNode> sources)
    {
        return intersect(outputsToInputs, sources, true);
    }

    public IntersectNode intersect(ListMultimap<Symbol, Symbol> outputsToInputs, List<PlanNode> sources, boolean distinct)
    {
        List<Symbol> outputs = ImmutableList.copyOf(outputsToInputs.keySet());
        return new IntersectNode(idAllocator.getNextId(), sources, outputsToInputs, outputs, distinct);
    }

    public ExceptNode except(ListMultimap<Symbol, Symbol> outputsToInputs, List<PlanNode> sources)
    {
        return except(outputsToInputs, sources, true);
    }

    public ExceptNode except(ListMultimap<Symbol, Symbol> outputsToInputs, List<PlanNode> sources, boolean distinct)
    {
        List<Symbol> outputs = ImmutableList.copyOf(outputsToInputs.keySet());
        return new ExceptNode(idAllocator.getNextId(), sources, outputsToInputs, outputs, distinct);
    }

    public TableWriterNode tableWriter(List<Symbol> columns, List<String> columnNames, PlanNode source)
    {
        return tableWriter(columns, columnNames, Optional.empty(), Optional.empty(), Optional.empty(), source);
    }

    public TableWriterNode tableWriter(
            List<Symbol> columns,
            List<String> columnNames,
            Optional<PartitioningScheme> partitioningScheme,
            TableWriterNode.WriterTarget target,
            PlanNode source,
            Symbol rowCountSymbol)
    {
        return new TableWriterNode(
                idAllocator.getNextId(),
                source,
                target,
                rowCountSymbol,
                rowCountSymbol,
                columns,
                columnNames,
                partitioningScheme,
                Optional.empty(),
                Optional.empty());
    }

    public TableWriterNode tableWriter(
            List<Symbol> columns,
            List<String> columnNames,
            Optional<PartitioningScheme> partitioningScheme,
            Optional<StatisticAggregations> statisticAggregations,
            Optional<StatisticAggregationsDescriptor<Symbol>> statisticAggregationsDescriptor,
            PlanNode source)
    {
        return new TableWriterNode(
                idAllocator.getNextId(),
                source,
                new TestingWriterTarget(),
                symbol("partialrows", BIGINT),
                symbol("fragment", VARBINARY),
                columns,
                columnNames,
                partitioningScheme,
                statisticAggregations,
                statisticAggregationsDescriptor);
    }

    public TableExecuteNode tableExecute(List<Symbol> columns, List<String> columnNames, PlanNode source)
    {
        return tableExecute(columns, columnNames, Optional.empty(), source);
    }

    public TableExecuteNode tableExecute(
            List<Symbol> columns,
            List<String> columnNames,
            Optional<PartitioningScheme> partitioningScheme,
            PlanNode source)
    {
        return new TableExecuteNode(
                idAllocator.getNextId(),
                source,
                new TableWriterNode.TableExecuteTarget(
                        new TableExecuteHandle(
                                TEST_CATALOG_HANDLE,
                                TestingTransactionHandle.create(),
                                new TestingTableExecuteHandle()),
                        Optional.empty(),
                        new SchemaTableName("schemaName", "tableName"),
                        WriterScalingOptions.DISABLED),
                symbol("partialrows", BIGINT),
                symbol("fragment", VARBINARY),
                columns,
                columnNames,
                partitioningScheme);
    }

    public TableFunctionNode tableFunction(
            String name,
            List<Symbol> properOutputs,
            List<PlanNode> sources,
            List<TableArgumentProperties> tableArgumentProperties,
            List<List<String>> copartitioningLists)

    {
        return new TableFunctionNode(
                idAllocator.getNextId(),
                name,
                TEST_CATALOG_HANDLE,
                ImmutableMap.of(),
                properOutputs,
                sources,
                tableArgumentProperties,
                copartitioningLists,
                new TableFunctionHandle(TEST_CATALOG_HANDLE, new ConnectorTableFunctionHandle() {}, TestingTransactionHandle.create()));
    }

    public TableFunctionProcessorNode tableFunctionProcessor(Consumer<TableFunctionProcessorBuilder> consumer)
    {
        TableFunctionProcessorBuilder tableFunctionProcessorBuilder = new TableFunctionProcessorBuilder();
        consumer.accept(tableFunctionProcessorBuilder);
        return tableFunctionProcessorBuilder.build(idAllocator);
    }

    public PartitioningScheme partitioningScheme(List<Symbol> outputSymbols, List<Symbol> partitioningSymbols, Symbol hashSymbol)
    {
        return new PartitioningScheme(Partitioning.create(
                FIXED_HASH_DISTRIBUTION,
                ImmutableList.copyOf(partitioningSymbols)),
                ImmutableList.copyOf(outputSymbols),
                Optional.of(hashSymbol));
    }

    public StatisticAggregations statisticAggregations(Map<Symbol, Aggregation> aggregations, List<Symbol> groupingSymbols)
    {
        return new StatisticAggregations(aggregations, groupingSymbols);
    }

    public Aggregation aggregation(AggregationFunction aggregation, List<Type> inputTypes)
    {
        ResolvedFunction resolvedFunction = functionResolver.resolveFunction(session, QualifiedName.of(aggregation.name()), TypeSignatureProvider.fromTypes(inputTypes), new AllowAllAccessControl());
        return new Aggregation(
                resolvedFunction,
                aggregation.arguments(),
                aggregation.distinct(),
                aggregation.filter(),
                aggregation.orderingScheme(),
                Optional.empty());
    }

    @Deprecated
    public Symbol symbol(String name)
    {
        return symbol(name, BIGINT);
    }

    public Symbol symbol(String name, Type type)
    {
        Symbol symbol = new Symbol(type, name);

        Symbol old = symbolsByName.put(symbol.name(), symbol);
        if (old != null && !old.type().equals(type)) {
            throw new IllegalArgumentException(format("Symbol '%s' already registered with type '%s'", name, old.type()));
        }

        return symbol;
    }

    public UnnestNode unnest(List<Symbol> replicateSymbols, List<UnnestNode.Mapping> mappings, PlanNode source)
    {
        return unnest(replicateSymbols, mappings, Optional.empty(), INNER, source);
    }

    public UnnestNode unnest(List<Symbol> replicateSymbols, List<UnnestNode.Mapping> mappings, Optional<Symbol> ordinalitySymbol, JoinType type, PlanNode source)
    {
        return new UnnestNode(
                idAllocator.getNextId(),
                source,
                replicateSymbols,
                mappings,
                ordinalitySymbol,
                type);
    }

    public WindowNode window(DataOrganizationSpecification specification, Map<Symbol, WindowNode.Function> functions, PlanNode source)
    {
        return new WindowNode(
                idAllocator.getNextId(),
                source,
                specification,
                ImmutableMap.copyOf(functions),
                Optional.empty(),
                ImmutableSet.of(),
                0);
    }

    public WindowNode window(DataOrganizationSpecification specification, Map<Symbol, WindowNode.Function> functions, Symbol hashSymbol, PlanNode source)
    {
        return new WindowNode(
                idAllocator.getNextId(),
                source,
                specification,
                ImmutableMap.copyOf(functions),
                Optional.of(hashSymbol),
                ImmutableSet.of(),
                0);
    }

    public RowNumberNode rowNumber(List<Symbol> partitionBy, Optional<Integer> maxRowCountPerPartition, Symbol rowNumberSymbol, PlanNode source)
    {
        return rowNumber(partitionBy, maxRowCountPerPartition, rowNumberSymbol, Optional.empty(), source);
    }

    public RowNumberNode rowNumber(List<Symbol> partitionBy, Optional<Integer> maxRowCountPerPartition, Symbol rowNumberSymbol, Optional<Symbol> hashSymbol, PlanNode source)
    {
        return new RowNumberNode(
                idAllocator.getNextId(),
                source,
                partitionBy,
                false,
                rowNumberSymbol,
                maxRowCountPerPartition,
                hashSymbol);
    }

    public TopNRankingNode topNRanking(DataOrganizationSpecification specification, RankingType rankingType, int maxRankingPerPartition, Symbol rankingSymbol, Optional<Symbol> hashSymbol, PlanNode source)
    {
        return new TopNRankingNode(
                idAllocator.getNextId(),
                source,
                specification,
                rankingType,
                rankingSymbol,
                maxRankingPerPartition,
                false,
                hashSymbol);
    }

    public PatternRecognitionNode patternRecognition(Consumer<PatternRecognitionBuilder> consumer)
    {
        PatternRecognitionBuilder patternRecognitionBuilder = new PatternRecognitionBuilder();
        consumer.accept(patternRecognitionBuilder);
        return patternRecognitionBuilder.build(idAllocator);
    }

    public RemoteSourceNode remoteSource(
            List<PlanFragmentId> sourceFragmentIds,
            List<Symbol> outputs,
            Optional<OrderingScheme> orderingScheme,
            ExchangeNode.Type exchangeType,
            RetryPolicy retryPolicy)
    {
        return new RemoteSourceNode(idAllocator.getNextId(), sourceFragmentIds, outputs, orderingScheme, exchangeType, retryPolicy);
    }

    public RemoteSourceNode remoteSource(
            PlanNodeId id,
            List<PlanFragmentId> sourceFragmentIds,
            List<Symbol> outputs,
            Optional<OrderingScheme> orderingScheme,
            ExchangeNode.Type exchangeType,
            RetryPolicy retryPolicy)
    {
        return new RemoteSourceNode(id, sourceFragmentIds, outputs, orderingScheme, exchangeType, retryPolicy);
    }

    public static AggregationFunction aggregation(String name, List<Expression> arguments)
    {
        return new AggregationFunction(name, Optional.empty(), Optional.empty(), false, arguments);
    }

    public static AggregationFunction aggregation(String name, boolean distinct, List<Expression> arguments)
    {
        return new AggregationFunction(name, Optional.empty(), Optional.empty(), distinct, arguments);
    }

    public static AggregationFunction aggregation(String name, boolean distinct, List<Expression> arguments, Symbol filter)
    {
        return new AggregationFunction(name, Optional.of(filter), Optional.empty(), distinct, arguments);
    }

    public static AggregationFunction aggregation(String name, List<Expression> arguments, Symbol filter)
    {
        return new AggregationFunction(name, Optional.of(filter), Optional.empty(), false, arguments);
    }

    public static AggregationFunction aggregation(String name, List<Expression> arguments, OrderingScheme orderingScheme)
    {
        return new AggregationFunction(name, Optional.empty(), Optional.of(orderingScheme), false, arguments);
    }

    public Collection<Symbol> getSymbols()
    {
        return symbolsByName.values();
    }
}
