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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.MustBeClosed;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.context.Context;
import io.trino.Session;
import io.trino.cost.CachingCostProvider;
import io.trino.cost.CachingStatsProvider;
import io.trino.cost.CachingTableStatsProvider;
import io.trino.cost.CostCalculator;
import io.trino.cost.CostProvider;
import io.trino.cost.RuntimeInfoProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.cost.StatsCalculator;
import io.trino.cost.StatsProvider;
import io.trino.cost.TableStatsProvider;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AnalyzeMetadata;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableLayout;
import io.trino.metadata.TableMetadata;
import io.trino.operator.RetryPolicy;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.RefreshType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Field;
import io.trino.sql.analyzer.RelationId;
import io.trino.sql.analyzer.RelationType;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.StatisticsAggregationPlanner.TableStatisticAggregation;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RefreshMaterializedViewNode;
import io.trino.sql.planner.plan.SimpleTableExecuteNode;
import io.trino.sql.planner.plan.StatisticAggregations;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TableWriterNode.RefreshMaterializedViewReference;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.planprinter.PlanPrinter;
import io.trino.sql.planner.sanity.PlanSanityChecker;
import io.trino.sql.tree.Analyze;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.Merge;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.RefreshMaterializedView;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableExecute;
import io.trino.sql.tree.Update;
import io.trino.tracing.ScopedSpan;
import io.trino.tracing.TrinoAttributes;
import io.trino.type.UnknownType;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Streams.forEachPair;
import static com.google.common.collect.Streams.zip;
import static io.trino.SystemSessionProperties.getMaxWriterTaskCount;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.isCollectPlanStatisticsForAllQueries;
import static io.trino.SystemSessionProperties.isUsePreferredWritePartitioning;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.PlanBuilder.newPlanBuilder;
import static io.trino.sql.planner.QueryPlanner.visibleFields;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.TableWriterNode.CreateReference;
import static io.trino.sql.planner.plan.TableWriterNode.InsertReference;
import static io.trino.sql.planner.plan.TableWriterNode.WriterTarget;
import static io.trino.sql.planner.sanity.PlanSanityChecker.DISTRIBUTED_PLAN_SANITY_CHECKER;
import static io.trino.tracing.ScopedSpan.scopedSpan;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class LogicalPlanner
{
    private static final Logger LOG = Logger.get(LogicalPlanner.class);

    public enum Stage
    {
        CREATED, OPTIMIZED, OPTIMIZED_AND_VALIDATED
    }

    private final PlanNodeIdAllocator idAllocator;

    private final Session session;
    private final List<PlanOptimizer> planOptimizers;
    private final PlanSanityChecker planSanityChecker;
    private final SymbolAllocator symbolAllocator = new SymbolAllocator();
    private final Metadata metadata;
    private final PlannerContext plannerContext;
    private final StatisticsAggregationPlanner statisticsAggregationPlanner;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final WarningCollector warningCollector;
    private final PlanOptimizersStatsCollector planOptimizersStatsCollector;
    private final CachingTableStatsProvider tableStatsProvider;

    public LogicalPlanner(
            Session session,
            List<PlanOptimizer> planOptimizers,
            PlanNodeIdAllocator idAllocator,
            PlannerContext plannerContext,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector,
            CachingTableStatsProvider tableStatsProvider)
    {
        this(session, planOptimizers, DISTRIBUTED_PLAN_SANITY_CHECKER, idAllocator, plannerContext, statsCalculator, costCalculator, warningCollector, planOptimizersStatsCollector, tableStatsProvider);
    }

    public LogicalPlanner(
            Session session,
            List<PlanOptimizer> planOptimizers,
            PlanSanityChecker planSanityChecker,
            PlanNodeIdAllocator idAllocator,
            PlannerContext plannerContext,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector,
            CachingTableStatsProvider tableStatsProvider)
    {
        this.session = requireNonNull(session, "session is null");
        this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
        this.planSanityChecker = requireNonNull(planSanityChecker, "planSanityChecker is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.metadata = plannerContext.getMetadata();
        this.statisticsAggregationPlanner = new StatisticsAggregationPlanner(symbolAllocator, plannerContext, session);
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.planOptimizersStatsCollector = requireNonNull(planOptimizersStatsCollector, "planOptimizersStatsCollector is null");
        this.tableStatsProvider = requireNonNull(tableStatsProvider, "tableStatsProvider is null");
    }

    public Plan plan(Analysis analysis)
    {
        return plan(analysis, OPTIMIZED_AND_VALIDATED);
    }

    public Plan plan(Analysis analysis, Stage stage)
    {
        return plan(analysis, stage, analysis.getStatement() instanceof ExplainAnalyze || isCollectPlanStatisticsForAllQueries(session));
    }

    public Plan plan(Analysis analysis, Stage stage, boolean collectPlanStatistics)
    {
        PlanNode root;
        try (var _ = scopedSpan(plannerContext.getTracer(), "plan")) {
            root = planStatement(analysis, analysis.getStatement());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Initial plan:\n%s", PlanPrinter.textLogicalPlan(
                    root,
                    metadata,
                    plannerContext.getFunctionManager(),
                    StatsAndCosts.empty(),
                    session,
                    0,
                    false));
        }

        try (var _ = scopedSpan(plannerContext.getTracer(), "validate-intermediate")) {
            planSanityChecker.validateIntermediatePlan(root, session, plannerContext, warningCollector);
        }

        if (stage.ordinal() >= OPTIMIZED.ordinal()) {
            try (var _ = scopedSpan(plannerContext.getTracer(), "optimizer")) {
                for (PlanOptimizer optimizer : planOptimizers) {
                    root = runOptimizer(root, tableStatsProvider, optimizer);
                }
            }
        }

        if (stage.ordinal() >= OPTIMIZED_AND_VALIDATED.ordinal()) {
            // make sure we produce a valid plan after optimizations run. This is mainly to catch programming errors
            try (var _ = scopedSpan(plannerContext.getTracer(), "validate-final")) {
                planSanityChecker.validateFinalPlan(root, session, plannerContext, warningCollector);
            }
        }

        TableStatsProvider collectTableStatsProvider;
        if (collectPlanStatistics) {
            collectTableStatsProvider = tableStatsProvider;
        }
        else {
            // If stats collection was not requested explicitly, then use statistics
            // that were fetched and cached during planning.
            Map<TableHandle, TableStatistics> cachedStatistics = tableStatsProvider.getCachedTableStatistics();
            collectTableStatsProvider = handle -> cachedStatistics.getOrDefault(handle, TableStatistics.empty());
        }

        StatsAndCosts statsAndCosts;
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, collectTableStatsProvider);
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.empty(), session);
        try (var _ = scopedSpan(plannerContext.getTracer(), "plan-stats")) {
            statsAndCosts = StatsAndCosts.create(root, statsProvider, costProvider);
        }
        return new Plan(root, statsAndCosts);
    }

    private PlanNode runOptimizer(PlanNode root, TableStatsProvider tableStatsProvider, PlanOptimizer optimizer)
    {
        PlanNode result;
        try (var _ = optimizerSpan(optimizer)) {
            result = optimizer.optimize(root, new PlanOptimizer.Context(session, symbolAllocator, idAllocator, warningCollector, planOptimizersStatsCollector, tableStatsProvider, RuntimeInfoProvider.noImplementation()));
        }
        if (result == null) {
            throw new NullPointerException(optimizer.getClass().getName() + " returned a null plan");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("%s:\n%s", optimizer.getClass().getName(), PlanPrinter.textLogicalPlan(
                    result,
                    metadata,
                    plannerContext.getFunctionManager(),
                    StatsAndCosts.empty(),
                    session,
                    0,
                    false));
        }

        return result;
    }

    @MustBeClosed
    private ScopedSpan optimizerSpan(PlanOptimizer optimizer)
    {
        if (!Span.fromContext(Context.current()).isRecording()) {
            return null;
        }
        SpanBuilder builder = plannerContext.getTracer().spanBuilder("optimize")
                .setAttribute(TrinoAttributes.OPTIMIZER_NAME, optimizer.getClass().getSimpleName());
        if (optimizer instanceof IterativeOptimizer iterative) {
            builder.setAttribute(TrinoAttributes.OPTIMIZER_RULES, iterative.getRules().stream()
                    .map(x -> x.getClass().getSimpleName())
                    .toList());
        }
        return scopedSpan(builder.startSpan());
    }

    public PlanNode planStatement(Analysis analysis, Statement statement)
    {
        if ((statement instanceof CreateTableAsSelect && analysis.getCreate().orElseThrow().isCreateTableAsSelectNoOp()) ||
                statement instanceof RefreshMaterializedView && analysis.isSkipMaterializedViewRefresh()) {
            Symbol symbol = symbolAllocator.newSymbol("rows", BIGINT);
            PlanNode source = new ValuesNode(idAllocator.getNextId(), ImmutableList.of(symbol), ImmutableList.of(new Row(ImmutableList.of(new Constant(BIGINT, 0L)))));
            return new OutputNode(idAllocator.getNextId(), source, ImmutableList.of("rows"), ImmutableList.of(symbol));
        }
        return createOutputPlan(planStatementWithoutOutput(analysis, statement), analysis);
    }

    private RelationPlan planStatementWithoutOutput(Analysis analysis, Statement statement)
    {
        if (statement instanceof CreateTableAsSelect createTableAsSelect) {
            if (analysis.getCreate().orElseThrow().isCreateTableAsSelectNoOp()) {
                throw new TrinoException(NOT_SUPPORTED, "CREATE TABLE IF NOT EXISTS is not supported in this context " + statement.getClass().getSimpleName());
            }
            return createTableCreationPlan(analysis, createTableAsSelect.getQuery());
        }
        if (statement instanceof Analyze analyze) {
            return createAnalyzePlan(analysis, analyze);
        }
        if (statement instanceof Insert insert) {
            checkState(analysis.getInsert().isPresent(), "Insert handle is missing");
            return createInsertPlan(analysis, insert);
        }
        if (statement instanceof RefreshMaterializedView) {
            return createRefreshMaterializedViewPlan(analysis);
        }
        if (statement instanceof Delete delete) {
            return createDeletePlan(analysis, delete);
        }
        if (statement instanceof Update update) {
            return createUpdatePlan(analysis, update);
        }
        if (statement instanceof Merge merge) {
            return createMergePlan(analysis, merge);
        }
        if (statement instanceof Query query) {
            return createRelationPlan(analysis, query);
        }
        if (statement instanceof ExplainAnalyze explainAnalyze) {
            return createExplainAnalyzePlan(analysis, explainAnalyze);
        }
        if (statement instanceof TableExecute tableExecute) {
            return createTableExecutePlan(analysis, tableExecute);
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported statement type " + statement.getClass().getSimpleName());
    }

    private RelationPlan createExplainAnalyzePlan(Analysis analysis, ExplainAnalyze statement)
    {
        RelationPlan underlyingPlan = planStatementWithoutOutput(analysis, statement.getStatement());
        PlanNode root = underlyingPlan.getRoot();
        Scope scope = analysis.getScope(statement);
        Symbol outputSymbol = symbolAllocator.newSymbol(scope.getRelationType().getFieldByIndex(0));

        ImmutableList.Builder<Symbol> actualOutputs = ImmutableList.builder();
        RelationType outputDescriptor = analysis.getOutputDescriptor(statement.getStatement());
        for (Field field : outputDescriptor.getVisibleFields()) {
            int fieldIndex = outputDescriptor.indexOf(field);
            Symbol symbol = underlyingPlan.getSymbol(fieldIndex);
            actualOutputs.add(symbol);
        }
        root = new ExplainAnalyzeNode(idAllocator.getNextId(), root, outputSymbol, actualOutputs.build(), statement.isVerbose());
        return new RelationPlan(root, scope, ImmutableList.of(outputSymbol), Optional.empty());
    }

    private RelationPlan createAnalyzePlan(Analysis analysis, Analyze analyzeStatement)
    {
        AnalyzeMetadata analyzeMetadata = analysis.getAnalyzeMetadata().orElseThrow();
        TableHandle targetTable = analyzeMetadata.tableHandle();
        TableStatisticsMetadata tableStatisticsMetadata = analyzeMetadata.statisticsMetadata();

        // Plan table scan
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, targetTable);
        ImmutableList.Builder<Symbol> tableScanOutputs = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, ColumnHandle> symbolToColumnHandle = ImmutableMap.builder();
        ImmutableMap.Builder<String, Symbol> columnNameToSymbol = ImmutableMap.builder();
        TableMetadata tableMetadata = metadata.getTableMetadata(session, targetTable);
        for (ColumnMetadata column : tableMetadata.columns()) {
            Symbol symbol = symbolAllocator.newSymbol(column.getName(), column.getType());
            tableScanOutputs.add(symbol);
            symbolToColumnHandle.put(symbol, columnHandles.get(column.getName()));
            columnNameToSymbol.put(column.getName(), symbol);
        }

        TableStatisticAggregation tableStatisticAggregation = statisticsAggregationPlanner.createStatisticsAggregation(tableStatisticsMetadata, columnNameToSymbol.buildOrThrow());
        StatisticAggregations statisticAggregations = tableStatisticAggregation.getAggregations();
        List<Symbol> groupingSymbols = statisticAggregations.getGroupingSymbols();

        PlanNode planNode = new StatisticsWriterNode(
                idAllocator.getNextId(),
                singleAggregation(
                        idAllocator.getNextId(),
                        new TableScanNode(idAllocator.getNextId(), targetTable, tableScanOutputs.build(), symbolToColumnHandle.buildOrThrow(), TupleDomain.all(), Optional.empty(), false, Optional.empty()),
                        statisticAggregations.getAggregations(),
                        singleGroupingSet(groupingSymbols)),
                new StatisticsWriterNode.WriteStatisticsReference(targetTable),
                symbolAllocator.newSymbol("rows", BIGINT),
                tableStatisticsMetadata.getTableStatistics().contains(ROW_COUNT),
                tableStatisticAggregation.getDescriptor());
        return new RelationPlan(planNode, analysis.getScope(analyzeStatement), planNode.getOutputSymbols(), Optional.empty());
    }

    private RelationPlan createTableCreationPlan(Analysis analysis, Query query)
    {
        Analysis.Create create = analysis.getCreate().orElseThrow();
        QualifiedObjectName destination = create.getDestination().orElseThrow();

        RelationPlan plan = createRelationPlan(analysis, query);
        if (!create.isCreateTableAsSelectWithData()) {
            PlanNode root = new LimitNode(idAllocator.getNextId(), plan.getRoot(), 0L, false);
            plan = new RelationPlan(root, plan.getScope(), plan.getFieldMappings(), Optional.empty());
        }

        ConnectorTableMetadata tableMetadata = create.getMetadata().orElseThrow();

        Optional<TableLayout> newTableLayout = create.getLayout();

        List<Symbol> visibleFieldMappings = visibleFields(plan);

        String catalogName = destination.catalogName();
        CatalogHandle catalogHandle = metadata.getCatalogHandle(session, catalogName)
                .orElseThrow(() -> semanticException(CATALOG_NOT_FOUND, query, "Destination catalog '%s' not found", catalogName));

        Assignments.Builder assignmentsBuilder = Assignments.builder();
        ImmutableList.Builder<ColumnMetadata> finalColumnsBuilder = ImmutableList.builder();

        checkState(tableMetadata.getColumns().size() == visibleFieldMappings.size(), "Table and visible field count doesn't match");

        forEachPair(tableMetadata.getColumns().stream(), visibleFieldMappings.stream(), (column, fieldMapping) -> {
            assignmentsBuilder.put(
                    symbolAllocator.newSymbol(column.getName(), column.getType()),
                    coerceOrCastToTableType(fieldMapping, column.getType(), fieldMapping.type()));
            finalColumnsBuilder.add(column);
        });

        List<ColumnMetadata> finalColumns = finalColumnsBuilder.build();
        Assignments assignments = assignmentsBuilder.build();

        checkState(assignments.size() == finalColumns.size(), "Assignment and column count must match");
        List<Field> fields = finalColumns.stream()
                .map(column -> Field.newUnqualified(column.getName(), column.getType()))
                .collect(toImmutableList());

        ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments);
        Scope scope = Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(fields)).build();
        plan = new RelationPlan(projectNode, scope, projectNode.getOutputSymbols(), Optional.empty());

        List<String> columnNames = finalColumns.stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());

        TableStatisticsMetadata statisticsMetadata = metadata.getStatisticsCollectionMetadataForWrite(session, catalogHandle, tableMetadata);

        return createTableWriterPlan(
                analysis,
                plan.getRoot(),
                visibleFields(plan),
                new CreateReference(catalogName, tableMetadata, newTableLayout, create.isReplace()),
                columnNames,
                newTableLayout,
                statisticsMetadata);
    }

    private RelationPlan getInsertPlan(
            Analysis analysis,
            Table table,
            Query query,
            TableHandle tableHandle,
            List<ColumnHandle> insertColumns,
            Optional<TableLayout> newTableLayout,
            Optional<RefreshMaterializedViewReference> materializedViewRefreshWriterTarget)
    {
        TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);

        Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap = buildLambdaDeclarationToSymbolMap(analysis, symbolAllocator);
        RelationPlanner planner = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, Optional.empty(), session, ImmutableMap.of());
        RelationPlan plan = planner.process(query, null);

        List<Symbol> visibleFieldMappings = visibleFields(plan);

        Map<String, ColumnHandle> columns = metadata.getColumnHandles(session, tableHandle);
        Assignments.Builder assignments = Assignments.builder();
        boolean supportsMissingColumnsOnInsert = metadata.supportsMissingColumnsOnInsert(session, tableHandle);
        ImmutableList.Builder<ColumnMetadata> insertedColumnsBuilder = ImmutableList.builder();

        for (ColumnMetadata column : tableMetadata.columns()) {
            if (column.isHidden()) {
                continue;
            }
            Symbol output = symbolAllocator.newSymbol(column.getName(), column.getType());
            Expression expression;
            Type tableType = column.getType();
            int index = insertColumns.indexOf(columns.get(column.getName()));
            if (index < 0) {
                if (supportsMissingColumnsOnInsert) {
                    continue;
                }
                expression = new Constant(column.getType(), null);
            }
            else {
                Symbol input = visibleFieldMappings.get(index);
                Type queryType = input.type();

                expression = coerceOrCastToTableType(input, tableType, queryType);
            }
            if (!column.isNullable()) {
                expression = new Coalesce(expression, createNullNotAllowedFailExpression(column.getName(), tableType));
            }
            assignments.put(output, expression);
            insertedColumnsBuilder.add(column);
        }

        ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments.build());

        List<ColumnMetadata> insertedColumns = insertedColumnsBuilder.build();
        List<Field> fields = insertedColumns.stream()
                .map(column -> Field.newUnqualified(column.getName(), column.getType()))
                .collect(toImmutableList());
        Scope scope = Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(fields)).build();

        plan = new RelationPlan(projectNode, scope, projectNode.getOutputSymbols(), Optional.empty());

        plan = planner.addRowFilters(
                table,
                plan,
                failIfPredicateIsNotMet(metadata, PERMISSION_DENIED, AccessDeniedException.PREFIX + "Cannot insert row that does not match a row filter"),
                node -> {
                    Scope accessControlScope = analysis.getAccessControlScope(table);
                    // hidden fields are not accessible in insert
                    return Scope.builder()
                            .like(accessControlScope)
                            .withRelationType(accessControlScope.getRelationId(), accessControlScope.getRelationType().withOnlyVisibleFields())
                            .build();
                });
        plan = planner.addCheckConstraints(
                analysis.getCheckConstraints(table),
                table,
                plan,
                node -> {
                    Scope accessControlScope = analysis.getAccessControlScope(table);
                    // hidden fields are not accessible in insert
                    return Scope.builder()
                            .like(accessControlScope)
                            .withRelationType(accessControlScope.getRelationId(), accessControlScope.getRelationType().withOnlyVisibleFields())
                            .build();
                });

        List<String> insertedTableColumnNames = insertedColumns.stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());

        TableStatisticsMetadata statisticsMetadata = metadata.getStatisticsCollectionMetadataForWrite(session, tableHandle.catalogHandle(), tableMetadata.metadata());

        if (materializedViewRefreshWriterTarget.isPresent()) {
            RefreshType refreshType = IncrementalRefreshVisitor.canIncrementallyRefresh(plan.getRoot());
            WriterTarget writerTarget = materializedViewRefreshWriterTarget.get().withRefreshType(refreshType);
            return createTableWriterPlan(
                    analysis,
                    plan.getRoot(),
                    plan.getFieldMappings(),
                    writerTarget,
                    insertedTableColumnNames,
                    newTableLayout,
                    statisticsMetadata);
        }
        InsertReference insertTarget = new InsertReference(
                tableHandle,
                insertedTableColumnNames.stream()
                        .map(columns::get)
                        .collect(toImmutableList()));
        return createTableWriterPlan(
                analysis,
                plan.getRoot(),
                plan.getFieldMappings(),
                insertTarget,
                insertedTableColumnNames,
                newTableLayout,
                statisticsMetadata);
    }

    private Expression coerceOrCastToTableType(Symbol fieldMapping, Type tableType, Type queryType)
    {
        if (queryType.equals(tableType)) {
            return fieldMapping.toSymbolReference();
        }
        return noTruncationCast(fieldMapping.toSymbolReference(), queryType, tableType);
    }

    private Expression createNullNotAllowedFailExpression(String columnName, Type type)
    {
        return new Cast(failFunction(metadata, CONSTRAINT_VIOLATION, "NULL value not allowed for NOT NULL column: " + columnName), type);
    }

    private static Function<Expression, Expression> failIfPredicateIsNotMet(Metadata metadata, ErrorCodeSupplier errorCode, String errorMessage)
    {
        Call fail = failFunction(metadata, errorCode, errorMessage);
        return predicate -> ifExpression(predicate, TRUE, new Cast(fail, BOOLEAN));
    }

    public static Call failFunction(Metadata metadata, ErrorCodeSupplier errorCode, String errorMessage)
    {
        Object rawValue = Slices.utf8Slice(errorMessage);
        return BuiltinFunctionCallBuilder.resolve(metadata)
                .setName("fail")
                .addArgument(INTEGER, new Constant(INTEGER, (long) errorCode.toErrorCode().getCode()))
                .addArgument(new Constant(VARCHAR, rawValue))
                .build();
    }

    private RelationPlan createInsertPlan(Analysis analysis, Insert insertStatement)
    {
        Analysis.Insert insert = analysis.getInsert().orElseThrow();
        TableHandle tableHandle = insert.getTarget();
        Query query = insertStatement.getQuery();
        Optional<TableLayout> newTableLayout = insert.getNewTableLayout();
        return getInsertPlan(analysis, insert.getTable(), query, tableHandle, insert.getColumns(), newTableLayout, Optional.empty());
    }

    private RelationPlan createRefreshMaterializedViewPlan(Analysis analysis)
    {
        Optional<QualifiedObjectName> delegatedRefreshMaterializedView = analysis.getDelegatedRefreshMaterializedView();
        if (delegatedRefreshMaterializedView.isPresent()) {
            return new RelationPlan(
                    new RefreshMaterializedViewNode(idAllocator.getNextId(), delegatedRefreshMaterializedView.get()),
                    analysis.getRootScope(),
                    ImmutableList.of(),
                    Optional.empty());
        }

        checkState(analysis.getRefreshMaterializedView().isPresent(), "RefreshMaterializedViewAnalysis handle is missing");
        Analysis.RefreshMaterializedViewAnalysis viewAnalysis = analysis.getRefreshMaterializedView().get();
        TableHandle tableHandle = viewAnalysis.getTarget();
        Query query = viewAnalysis.getQuery();
        Optional<TableLayout> newTableLayout = metadata.getInsertLayout(session, viewAnalysis.getTarget());
        List<String> tableFunctions = analysis.getPolymorphicTableFunctions().stream()
                .map(polymorphicTableFunction -> polymorphicTableFunction.getNode().getName().toString())
                .collect(toImmutableList());
        RefreshMaterializedViewReference writerTarget = new RefreshMaterializedViewReference(
                viewAnalysis.getTable().toString(),
                tableHandle,
                ImmutableList.copyOf(analysis.getTables()),
                tableFunctions,
                // this is a placeholder value - refresh type will be determined by getInsertPlan based on the plan tree
                RefreshType.FULL);
        return getInsertPlan(analysis, viewAnalysis.getTable(), query, tableHandle, viewAnalysis.getColumns(), newTableLayout, Optional.of(writerTarget));
    }

    private RelationPlan createTableWriterPlan(
            Analysis analysis,
            PlanNode source,
            List<Symbol> symbols,
            WriterTarget target,
            List<String> columnNames,
            Optional<TableLayout> writeTableLayout,
            TableStatisticsMetadata statisticsMetadata)
    {
        Optional<PartitioningScheme> partitioningScheme = Optional.empty();

        int maxWriterTasks = target.getMaxWriterTasks(plannerContext.getMetadata(), session).orElse(getMaxWriterTaskCount(session));
        Optional<Integer> maxWritersNodesCount = getRetryPolicy(session) != RetryPolicy.TASK
                ? Optional.of(Math.min(maxWriterTasks, getMaxWriterTaskCount(session)))
                : Optional.empty();

        if (writeTableLayout.isPresent()) {
            List<Symbol> partitionFunctionArguments = new ArrayList<>();
            writeTableLayout.get().getPartitionColumns().stream()
                    .mapToInt(columnNames::indexOf)
                    .mapToObj(symbols::get)
                    .forEach(partitionFunctionArguments::add);

            List<Symbol> outputLayout = new ArrayList<>(symbols);

            Optional<PartitioningHandle> partitioningHandle = writeTableLayout.get().getPartitioning();
            if (partitioningHandle.isPresent()) {
                checkState(target.getMaxWriterTasks(plannerContext.getMetadata(), session).isEmpty(), "maxWriterTasks must be empty if partitioning is set by connector");
                partitioningScheme = Optional.of(new PartitioningScheme(
                        Partitioning.create(partitioningHandle.get(), partitionFunctionArguments),
                        outputLayout));
            }
            else if (isUsePreferredWritePartitioning(session)) {
                // empty connector partitioning handle means evenly partitioning on partitioning columns
                partitioningScheme = Optional.of(new PartitioningScheme(
                        Partitioning.create(FIXED_HASH_DISTRIBUTION, partitionFunctionArguments),
                        outputLayout,
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        maxWritersNodesCount));
            }
        }

        verify(columnNames.size() == symbols.size(), "columnNames.size() != symbols.size(): %s and %s", columnNames, symbols);
        Map<String, Symbol> columnToSymbolMap = zip(columnNames.stream(), symbols.stream(), SimpleImmutableEntry::new)
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));

        if (!statisticsMetadata.isEmpty()) {
            TableStatisticAggregation result = statisticsAggregationPlanner.createStatisticsAggregation(statisticsMetadata, columnToSymbolMap);

            StatisticAggregations.Parts aggregations = result.getAggregations().createPartialAggregations(symbolAllocator, session, plannerContext);

            // partial aggregation is run within the TableWriteOperator to calculate the statistics for
            // the data consumed by the TableWriteOperator
            // final aggregation is run within the TableFinishOperator to summarize collected statistics
            // by the partial aggregation from all of the writer nodes
            StatisticAggregations partialAggregation = aggregations.getPartialAggregation();

            TableFinishNode commitNode = new TableFinishNode(
                    idAllocator.getNextId(),
                    new TableWriterNode(
                            idAllocator.getNextId(),
                            source,
                            target,
                            symbolAllocator.newSymbol("partialrows", BIGINT),
                            symbolAllocator.newSymbol("fragment", VARBINARY),
                            symbols,
                            columnNames,
                            partitioningScheme,
                            Optional.of(partialAggregation),
                            Optional.of(result.getDescriptor().map(aggregations.getMappings()::get))),
                    target,
                    symbolAllocator.newSymbol("rows", BIGINT),
                    Optional.of(aggregations.getFinalAggregation()),
                    Optional.of(result.getDescriptor()));

            return new RelationPlan(commitNode, analysis.getRootScope(), commitNode.getOutputSymbols(), Optional.empty());
        }

        TableFinishNode commitNode = new TableFinishNode(
                idAllocator.getNextId(),
                new TableWriterNode(
                        idAllocator.getNextId(),
                        source,
                        target,
                        symbolAllocator.newSymbol("partialrows", BIGINT),
                        symbolAllocator.newSymbol("fragment", VARBINARY),
                        symbols,
                        columnNames,
                        partitioningScheme,
                        Optional.empty(),
                        Optional.empty()),
                target,
                symbolAllocator.newSymbol("rows", BIGINT),
                Optional.empty(),
                Optional.empty());

        return new RelationPlan(commitNode, analysis.getRootScope(), commitNode.getOutputSymbols(), Optional.empty());
    }

    /*
    According to the standard, for the purpose of store assignment (INSERT),
    no non-space characters of a character string, and no non-zero octets
    of a binary string must be lost when the inserted value is truncated to
    fit in the target column type.
    The following method returns a cast from source type to target type
    with a guarantee of no illegal truncation.
    TODO Once BINARY and parametric VARBINARY types are supported, they should be handled here.
    TODO This workaround is insufficient to handle structural types
     */
    private Expression noTruncationCast(Expression expression, Type fromType, Type toType)
    {
        if (fromType instanceof UnknownType || (!(toType instanceof VarcharType) && !(toType instanceof CharType))) {
            return new Cast(expression, toType);
        }
        int targetLength;
        if (toType instanceof VarcharType varcharType) {
            if (varcharType.isUnbounded()) {
                return new Cast(expression, toType);
            }
            targetLength = varcharType.getBoundedLength();
        }
        else {
            targetLength = ((CharType) toType).getLength();
        }

        checkState(fromType instanceof VarcharType || fromType instanceof CharType, "inserting non-character value to column of character type");
        ResolvedFunction spaceTrimmedLength = metadata.resolveBuiltinFunction("$space_trimmed_length", fromTypes(VARCHAR));

        return ifExpression(
                // check if the trimmed value fits in the target type
                new Comparison(
                        GREATER_THAN_OR_EQUAL,
                        new Constant(BIGINT, (long) targetLength),
                        new Coalesce(
                                new Call(
                                        spaceTrimmedLength,
                                        ImmutableList.of(new Cast(expression, VARCHAR))),
                                new Constant(BIGINT, 0L))),
                new Cast(expression, toType),
                new Cast(
                        failFunction(metadata, INVALID_CAST_ARGUMENT, format(
                                "Cannot truncate non-space characters when casting from %s to %s on INSERT",
                                fromType.getDisplayName(),
                                toType.getDisplayName())),
                        toType));
    }

    private RelationPlan createDeletePlan(Analysis analysis, Delete node)
    {
        PlanNode planNode = new QueryPlanner(analysis, symbolAllocator, idAllocator, buildLambdaDeclarationToSymbolMap(analysis, symbolAllocator), plannerContext, Optional.empty(), session, ImmutableMap.of())
                .plan(node);

        WriterTarget target = ((MergeWriterNode) planNode).getTarget();
        TableFinishNode commitNode = new TableFinishNode(
                idAllocator.getNextId(),
                planNode,
                target,
                symbolAllocator.newSymbol("rows", BIGINT),
                Optional.empty(),
                Optional.empty());

        return new RelationPlan(commitNode, analysis.getScope(node), commitNode.getOutputSymbols(), Optional.empty());
    }

    private RelationPlan createUpdatePlan(Analysis analysis, Update node)
    {
        PlanNode planNode = new QueryPlanner(analysis, symbolAllocator, idAllocator, buildLambdaDeclarationToSymbolMap(analysis, symbolAllocator), plannerContext, Optional.empty(), session, ImmutableMap.of())
                .plan(node);

        WriterTarget target = ((MergeWriterNode) planNode).getTarget();
        TableFinishNode commitNode = new TableFinishNode(
                idAllocator.getNextId(),
                planNode,
                target,
                symbolAllocator.newSymbol("rows", BIGINT),
                Optional.empty(),
                Optional.empty());

        return new RelationPlan(commitNode, analysis.getScope(node), commitNode.getOutputSymbols(), Optional.empty());
    }

    private RelationPlan createMergePlan(Analysis analysis, Merge node)
    {
        MergeWriterNode mergeNode = new QueryPlanner(analysis, symbolAllocator, idAllocator, buildLambdaDeclarationToSymbolMap(analysis, symbolAllocator), plannerContext, Optional.empty(), session, ImmutableMap.of())
                .plan(node);

        TableFinishNode commitNode = new TableFinishNode(
                idAllocator.getNextId(),
                mergeNode,
                mergeNode.getTarget(),
                symbolAllocator.newSymbol("rows", BIGINT),
                Optional.empty(),
                Optional.empty());

        return new RelationPlan(commitNode, analysis.getScope(node), commitNode.getOutputSymbols(), Optional.empty());
    }

    private PlanNode createOutputPlan(RelationPlan plan, Analysis analysis)
    {
        ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
        ImmutableList.Builder<String> names = ImmutableList.builder();

        int columnNumber = 0;
        RelationType outputDescriptor = analysis.getOutputDescriptor();
        for (Field field : outputDescriptor.getVisibleFields()) {
            String name = field.getName().orElse("_col" + columnNumber);
            names.add(name);

            int fieldIndex = outputDescriptor.indexOf(field);
            Symbol symbol = plan.getSymbol(fieldIndex);
            outputs.add(symbol);

            columnNumber++;
        }
        return new OutputNode(idAllocator.getNextId(), plan.getRoot(), names.build(), outputs.build());
    }

    private RelationPlan createRelationPlan(Analysis analysis, Query query)
    {
        return getRelationPlanner(analysis).process(query, null);
    }

    private RelationPlan createRelationPlan(Analysis analysis, Table table)
    {
        return getRelationPlanner(analysis).process(table, null);
    }

    private RelationPlanner getRelationPlanner(Analysis analysis)
    {
        return new RelationPlanner(analysis, symbolAllocator, idAllocator, buildLambdaDeclarationToSymbolMap(analysis, symbolAllocator), plannerContext, Optional.empty(), session, ImmutableMap.of());
    }

    public static Map<NodeRef<LambdaArgumentDeclaration>, Symbol> buildLambdaDeclarationToSymbolMap(Analysis analysis, SymbolAllocator symbolAllocator)
    {
        Map<Key, Symbol> allocations = new HashMap<>();
        Map<NodeRef<LambdaArgumentDeclaration>, Symbol> result = new LinkedHashMap<>();

        for (Entry<NodeRef<io.trino.sql.tree.Expression>, Type> entry : analysis.getTypes().entrySet()) {
            if (!(entry.getKey().getNode() instanceof LambdaArgumentDeclaration argument)) {
                continue;
            }

            Key key = new Key(argument, entry.getValue());

            // Allocate the same symbol for all lambda argument names with a given type. This is needed to be able to
            // properly identify multiple instances of syntactically equal lambda expressions during planning as expressions
            // get rewritten via TranslationMap
            Symbol symbol = allocations.get(key);
            if (symbol == null) {
                symbol = symbolAllocator.newSymbol(argument.getName().toString(), entry.getValue());
                allocations.put(key, symbol);
            }

            result.put(NodeRef.of(argument), symbol);
        }

        return result;
    }

    private RelationPlan createTableExecutePlan(Analysis analysis, TableExecute statement)
    {
        Table table = statement.getTable();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, table.getName());
        TableExecuteHandle executeHandle = analysis.getTableExecuteHandle().orElseThrow();

        if (!analysis.isTableExecuteReadsData()) {
            SimpleTableExecuteNode node = new SimpleTableExecuteNode(
                    idAllocator.getNextId(),
                    symbolAllocator.newSymbol("rows", BIGINT),
                    executeHandle);
            return new RelationPlan(node, analysis.getRootScope(), node.getOutputSymbols(), Optional.empty());
        }

        TableHandle tableHandle = analysis.getTableHandle(table);
        RelationPlan tableScanPlan = createRelationPlan(analysis, table);
        PlanBuilder sourcePlanBuilder = newPlanBuilder(tableScanPlan, analysis, ImmutableMap.of(), ImmutableMap.of(), session, plannerContext);
        if (statement.getWhere().isPresent()) {
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner(analysis, symbolAllocator, idAllocator, buildLambdaDeclarationToSymbolMap(analysis, symbolAllocator), plannerContext, Optional.empty(), session, ImmutableMap.of());
            io.trino.sql.tree.Expression whereExpression = statement.getWhere().get();
            sourcePlanBuilder = subqueryPlanner.handleSubqueries(sourcePlanBuilder, whereExpression, analysis.getSubqueries(statement));
            sourcePlanBuilder = sourcePlanBuilder.withNewRoot(new FilterNode(idAllocator.getNextId(), sourcePlanBuilder.getRoot(), sourcePlanBuilder.rewrite(whereExpression)));
        }

        PlanNode sourcePlanRoot = sourcePlanBuilder.getRoot();

        TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
        List<String> columnNames = tableMetadata.columns().stream()
                .filter(column -> !column.isHidden()) // todo this filter is redundant
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());

        TableWriterNode.TableExecuteTarget tableExecuteTarget = new TableWriterNode.TableExecuteTarget(
                executeHandle,
                Optional.empty(),
                tableName.asSchemaTableName(),
                metadata.getInsertWriterScalingOptions(session, tableHandle));

        Optional<TableLayout> layout = metadata.getLayoutForTableExecute(session, executeHandle);

        List<Symbol> symbols = visibleFields(tableScanPlan);

        // todo extract common method to be used here and in createTableWriterPlan()
        Optional<PartitioningScheme> partitioningScheme = Optional.empty();
        if (layout.isPresent()) {
            List<Symbol> partitionFunctionArguments = new ArrayList<>();
            layout.get().getPartitionColumns().stream()
                    .mapToInt(columnNames::indexOf)
                    .mapToObj(symbols::get)
                    .forEach(partitionFunctionArguments::add);

            List<Symbol> outputLayout = new ArrayList<>(symbols);

            Optional<PartitioningHandle> partitioningHandle = layout.get().getPartitioning();
            if (partitioningHandle.isPresent()) {
                checkState(tableExecuteTarget.getMaxWriterTasks(plannerContext.getMetadata(), session).isEmpty(), "maxWriterTasks must be empty if partitioning is set by connector");
                partitioningScheme = Optional.of(new PartitioningScheme(
                        Partitioning.create(partitioningHandle.get(), partitionFunctionArguments),
                        outputLayout));
            }
            else if (isUsePreferredWritePartitioning(session)) {
                // empty connector partitioning handle means evenly partitioning on partitioning columns
                int maxWriterTasks = tableExecuteTarget.getMaxWriterTasks(plannerContext.getMetadata(), session).orElse(getMaxWriterTaskCount(session));
                Optional<Integer> maxWritersNodesCount = getRetryPolicy(session) != RetryPolicy.TASK
                        ? Optional.of(Math.min(maxWriterTasks, getMaxWriterTaskCount(session)))
                        : Optional.empty();
                partitioningScheme = Optional.of(new PartitioningScheme(
                        Partitioning.create(FIXED_HASH_DISTRIBUTION, partitionFunctionArguments),
                        outputLayout,
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        maxWritersNodesCount));
            }
        }

        verify(columnNames.size() == symbols.size(), "columnNames.size() != symbols.size(): %s and %s", columnNames, symbols);
        TableFinishNode commitNode = new TableFinishNode(
                idAllocator.getNextId(),
                new TableExecuteNode(
                        idAllocator.getNextId(),
                        sourcePlanRoot,
                        tableExecuteTarget,
                        symbolAllocator.newSymbol("partialrows", BIGINT),
                        symbolAllocator.newSymbol("fragment", VARBINARY),
                        symbols,
                        columnNames,
                        partitioningScheme),
                tableExecuteTarget,
                symbolAllocator.newSymbol("rows", BIGINT),
                Optional.empty(),
                Optional.empty());

        return new RelationPlan(commitNode, analysis.getRootScope(), commitNode.getOutputSymbols(), Optional.empty());
    }

    private static class Key
    {
        private final LambdaArgumentDeclaration argument;
        private final Type type;

        public Key(LambdaArgumentDeclaration argument, Type type)
        {
            this.argument = requireNonNull(argument, "argument is null");
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return Objects.equals(argument, key.argument) &&
                    Objects.equals(type, key.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(argument, type);
        }
    }
}
