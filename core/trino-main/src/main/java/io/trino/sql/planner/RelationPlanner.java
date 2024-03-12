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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.trino.Session;
import io.trino.json.ir.IrJsonPath;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableFunctionHandle;
import io.trino.metadata.TableHandle;
import io.trino.operator.table.json.JsonTable.JsonTableFunctionHandle;
import io.trino.operator.table.json.JsonTableColumn;
import io.trino.operator.table.json.JsonTableOrdinalityColumn;
import io.trino.operator.table.json.JsonTablePlanCross;
import io.trino.operator.table.json.JsonTablePlanLeaf;
import io.trino.operator.table.json.JsonTablePlanNode;
import io.trino.operator.table.json.JsonTablePlanSingle;
import io.trino.operator.table.json.JsonTablePlanUnion;
import io.trino.operator.table.json.JsonTableQueryColumn;
import io.trino.operator.table.json.JsonTableValueColumn;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.function.table.TableArgument;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analysis.JsonTableAnalysis;
import io.trino.sql.analyzer.Analysis.TableArgumentAnalysis;
import io.trino.sql.analyzer.Analysis.TableFunctionInvocationAnalysis;
import io.trino.sql.analyzer.Analysis.UnnestAnalysis;
import io.trino.sql.analyzer.Field;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.AggregationDescriptor;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.ClassifierDescriptor;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.MatchNumberDescriptor;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.Navigation;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.PatternInputAnalysis;
import io.trino.sql.analyzer.PatternRecognitionAnalysis.ScalarInputDescriptor;
import io.trino.sql.analyzer.RelationType;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.ir.IrUtils;
import io.trino.sql.planner.QueryPlanner.PlanAndMappings;
import io.trino.sql.planner.TranslationMap.ParametersRow;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PatternRecognitionNode.Measure;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RowsPerMatch;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SkipToPosition;
import io.trino.sql.planner.plan.TableFunctionNode;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughColumn;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import io.trino.sql.planner.plan.TableFunctionNode.TableArgumentProperties;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.rowpattern.AggregatedSetDescriptor;
import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.ClassifierValuePointer;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointers.Assignment;
import io.trino.sql.planner.rowpattern.LogicalIndexPointer;
import io.trino.sql.planner.rowpattern.MatchNumberValuePointer;
import io.trino.sql.planner.rowpattern.RowPatternToIrRewriter;
import io.trino.sql.planner.rowpattern.ScalarValuePointer;
import io.trino.sql.planner.rowpattern.ValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.planner.rowpattern.ir.IrRowPattern;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Except;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinUsing;
import io.trino.sql.tree.JsonPathParameter;
import io.trino.sql.tree.JsonQuery;
import io.trino.sql.tree.JsonTable;
import io.trino.sql.tree.JsonTableColumnDefinition;
import io.trino.sql.tree.JsonTableDefaultPlan;
import io.trino.sql.tree.JsonTablePlan.ParentChildPlanType;
import io.trino.sql.tree.JsonTablePlan.SiblingsPlanType;
import io.trino.sql.tree.JsonTableSpecificPlan;
import io.trino.sql.tree.JsonValue;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.Lateral;
import io.trino.sql.tree.MeasureDefinition;
import io.trino.sql.tree.NaturalJoin;
import io.trino.sql.tree.NestedColumns;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.OrdinalityColumn;
import io.trino.sql.tree.PatternRecognitionRelation;
import io.trino.sql.tree.PatternSearchMode;
import io.trino.sql.tree.PlanLeaf;
import io.trino.sql.tree.PlanParentChild;
import io.trino.sql.tree.PlanSiblings;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QueryColumn;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.SampledRelation;
import io.trino.sql.tree.SetOperation;
import io.trino.sql.tree.SkipTo;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubsetDefinition;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableFunctionInvocation;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.ValueColumn;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.VariableDefinition;
import io.trino.sql.util.AstUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.StandardTypes.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.NodeUtils.getSortItemsFromOrderBy;
import static io.trino.sql.analyzer.PatternRecognitionAnalysis.NavigationAnchor.LAST;
import static io.trino.sql.analyzer.PatternRecognitionAnalysis.NavigationMode.RUNNING;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.LogicalPlanner.failFunction;
import static io.trino.sql.planner.PlanBuilder.newPlanBuilder;
import static io.trino.sql.planner.QueryPlanner.coerce;
import static io.trino.sql.planner.QueryPlanner.coerceIfNecessary;
import static io.trino.sql.planner.QueryPlanner.extractPatternRecognitionExpressions;
import static io.trino.sql.planner.QueryPlanner.planWindowSpecification;
import static io.trino.sql.planner.QueryPlanner.pruneInvisibleFields;
import static io.trino.sql.planner.QueryPlanner.translateOrderingScheme;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.Join.Type.CROSS;
import static io.trino.sql.tree.Join.Type.FULL;
import static io.trino.sql.tree.Join.Type.IMPLICIT;
import static io.trino.sql.tree.Join.Type.INNER;
import static io.trino.sql.tree.Join.Type.LEFT;
import static io.trino.sql.tree.JsonQuery.QuotesBehavior.KEEP;
import static io.trino.sql.tree.JsonQuery.QuotesBehavior.OMIT;
import static io.trino.sql.tree.JsonTablePlan.ParentChildPlanType.OUTER;
import static io.trino.sql.tree.JsonTablePlan.SiblingsPlanType.UNION;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.ONE;
import static io.trino.sql.tree.PatternSearchMode.Mode.INITIAL;
import static io.trino.sql.tree.SkipTo.Position.PAST_LAST;
import static io.trino.type.Json2016Type.JSON_2016;
import static java.lang.Boolean.TRUE;
import static java.util.Objects.requireNonNull;

class RelationPlanner
        extends AstVisitor<RelationPlan, Void>
{
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap;
    private final PlannerContext plannerContext;
    private final Optional<TranslationMap> outerContext;
    private final Session session;
    private final SubqueryPlanner subqueryPlanner;
    private final Map<NodeRef<Node>, RelationPlan> recursiveSubqueries;

    RelationPlanner(
            Analysis analysis,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap,
            PlannerContext plannerContext,
            Optional<TranslationMap> outerContext,
            Session session,
            Map<NodeRef<Node>, RelationPlan> recursiveSubqueries)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(lambdaDeclarationToSymbolMap, "lambdaDeclarationToSymbolMap is null");
        requireNonNull(plannerContext, "plannerContext is null");
        requireNonNull(outerContext, "outerContext is null");
        requireNonNull(session, "session is null");
        requireNonNull(recursiveSubqueries, "recursiveSubqueries is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
        this.lambdaDeclarationToSymbolMap = lambdaDeclarationToSymbolMap;
        this.plannerContext = plannerContext;
        this.outerContext = outerContext;
        this.session = session;
        this.subqueryPlanner = new SubqueryPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, outerContext, session, recursiveSubqueries);
        this.recursiveSubqueries = recursiveSubqueries;
    }

    public static JoinType mapJoinType(Join.Type joinType)
    {
        return switch (joinType) {
            case CROSS, IMPLICIT, INNER -> JoinType.INNER;
            case LEFT -> JoinType.LEFT;
            case RIGHT -> JoinType.RIGHT;
            case FULL -> JoinType.FULL;
        };
    }

    public static SampleNode.Type mapSampleType(SampledRelation.Type sampleType)
    {
        return switch (sampleType) {
            case BERNOULLI -> SampleNode.Type.BERNOULLI;
            case SYSTEM -> SampleNode.Type.SYSTEM;
        };
    }

    @Override
    protected RelationPlan visitNode(Node node, Void context)
    {
        throw new IllegalStateException("Unsupported node type: " + node.getClass().getName());
    }

    @Override
    protected RelationPlan visitTable(Table node, Void context)
    {
        // is this a recursive reference in expandable named query? If so, there's base relation already planned.
        RelationPlan expansion = recursiveSubqueries.get(NodeRef.of(node));
        if (expansion != null) {
            // put the pre-planned recursive subquery in the actual outer context to enable resolving correlation
            return new RelationPlan(expansion.getRoot(), expansion.getScope(), expansion.getFieldMappings(), outerContext);
        }

        Query namedQuery = analysis.getNamedQuery(node);
        Scope scope = analysis.getScope(node);

        RelationPlan plan;
        if (namedQuery != null) {
            RelationPlan subPlan;
            if (analysis.isExpandableQuery(namedQuery)) {
                subPlan = new QueryPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, outerContext, session, recursiveSubqueries)
                        .planExpand(namedQuery);
            }
            else {
                subPlan = process(namedQuery, null);
            }

            // Add implicit coercions if view query produces types that don't match the declared output types
            // of the view (e.g., if the underlying tables referenced by the view changed)

            List<Type> types = analysis.getOutputDescriptor(node)
                    .getAllFields().stream()
                    .map(Field::getType)
                    .collect(toImmutableList());

            NodeAndMappings coerced = coerce(subPlan, types, symbolAllocator, idAllocator);

            plan = new RelationPlan(coerced.getNode(), scope, coerced.getFields(), outerContext);
        }
        else {
            TableHandle handle = analysis.getTableHandle(node);

            ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
            ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();

            Collection<Field> fields = analysis.getMaterializedViewStorageTableFields(node);
            boolean hasStorageTableFields = fields != null;
            if (!hasStorageTableFields) {
                fields = scope.getRelationType().getAllFields();
            }
            for (Field field : fields) {
                Symbol symbol = symbolAllocator.newSymbol(field);

                outputSymbolsBuilder.add(symbol);
                columns.put(symbol, analysis.getColumn(field));
            }

            List<Symbol> outputSymbols = outputSymbolsBuilder.build();
            boolean updateTarget = analysis.isUpdateTarget(node);
            PlanNode root = TableScanNode.newInstance(idAllocator.getNextId(), handle, outputSymbols, columns.buildOrThrow(), updateTarget, Optional.empty());

            plan = new RelationPlan(root, scope, outputSymbols, outerContext);

            if (hasStorageTableFields) {
                List<Type> types = scope.getRelationType().getAllFields().stream()
                        .map(Field::getType)
                        .collect(toImmutableList());
                // apply required coercion and prune invisible fields from child outputs
                NodeAndMappings coerced = coerce(plan, types, symbolAllocator, idAllocator);
                plan = new RelationPlan(coerced.getNode(), scope, coerced.getFields(), outerContext);
            }
        }

        plan = addRowFilters(node, plan);
        plan = addColumnMasks(node, plan);

        return plan;
    }

    private RelationPlan addRowFilters(Table node, RelationPlan plan)
    {
        return addRowFilters(node, plan, Function.identity());
    }

    public RelationPlan addRowFilters(Table node, RelationPlan plan, Function<Expression, Expression> predicateTransformation)
    {
        return addRowFilters(node, plan, predicateTransformation, analysis::getAccessControlScope);
    }

    public RelationPlan addRowFilters(Table node, RelationPlan plan, Function<Expression, Expression> predicateTransformation, Function<Table, Scope> accessControlScope)
    {
        List<Expression> filters = analysis.getRowFilters(node);

        if (filters.isEmpty()) {
            return plan;
        }

        PlanBuilder planBuilder = newPlanBuilder(plan, analysis, lambdaDeclarationToSymbolMap, session, plannerContext)
                .withScope(accessControlScope.apply(node), plan.getFieldMappings()); // The fields in the access control scope has the same layout as those for the table scope

        for (Expression filter : filters) {
            planBuilder = subqueryPlanner.handleSubqueries(planBuilder, filter, analysis.getSubqueries(filter));

            Expression predicate = coerceIfNecessary(analysis, filter, planBuilder.rewrite(filter));
            predicate = predicateTransformation.apply(predicate);
            planBuilder = planBuilder.withNewRoot(new FilterNode(
                    idAllocator.getNextId(),
                    planBuilder.getRoot(),
                    predicate));
        }

        return new RelationPlan(planBuilder.getRoot(), plan.getScope(), plan.getFieldMappings(), outerContext);
    }

    public RelationPlan addCheckConstraints(List<Expression> constraints, Table node, RelationPlan plan, Function<Table, Scope> accessControlScope)
    {
        if (constraints.isEmpty()) {
            return plan;
        }

        PlanBuilder planBuilder = newPlanBuilder(plan, analysis, lambdaDeclarationToSymbolMap, session, plannerContext)
                .withScope(accessControlScope.apply(node), plan.getFieldMappings()); // The fields in the access control scope has the same layout as those for the table scope

        for (Expression constraint : constraints) {
            planBuilder = subqueryPlanner.handleSubqueries(planBuilder, constraint, analysis.getSubqueries(constraint));

            Expression predicate = new IfExpression(
                    // When predicate evaluates to UNKNOWN (e.g. NULL > 100), it should not violate the check constraint.
                    new CoalesceExpression(coerceIfNecessary(analysis, constraint, planBuilder.rewrite(constraint)), TRUE_LITERAL),
                    TRUE_LITERAL,
                    new Cast(failFunction(plannerContext.getMetadata(), CONSTRAINT_VIOLATION, "Check constraint violation: " + constraint), toSqlType(BOOLEAN)));

            planBuilder = planBuilder.withNewRoot(new FilterNode(
                    idAllocator.getNextId(),
                    planBuilder.getRoot(),
                    predicate));
        }

        return new RelationPlan(planBuilder.getRoot(), plan.getScope(), plan.getFieldMappings(), outerContext);
    }

    private RelationPlan addColumnMasks(Table table, RelationPlan plan)
    {
        Map<String, Expression> columnMasks = analysis.getColumnMasks(table);

        // A Table can represent a WITH query, which can have anonymous fields. On the other hand,
        // it can't have masks. The loop below expects fields to have proper names, so bail out
        // if the masks are missing
        if (columnMasks.isEmpty()) {
            return plan;
        }

        PlanBuilder planBuilder = newPlanBuilder(plan, analysis, lambdaDeclarationToSymbolMap, session, plannerContext)
                .withScope(analysis.getAccessControlScope(table), plan.getFieldMappings()); // The fields in the access control scope has the same layout as those for the table scope

        Assignments.Builder assignments = Assignments.builder();
        assignments.putIdentities(planBuilder.getRoot().getOutputSymbols());

        List<Symbol> fieldMappings = new ArrayList<>();
        for (int i = 0; i < plan.getDescriptor().getAllFieldCount(); i++) {
            Field field = plan.getDescriptor().getFieldByIndex(i);

            Expression mask = columnMasks.get(field.getName().orElseThrow());
            Symbol symbol = plan.getFieldMappings().get(i);
            Expression projection = symbol.toSymbolReference();
            if (mask != null) {
                planBuilder = subqueryPlanner.handleSubqueries(planBuilder, mask, analysis.getSubqueries(mask));
                symbol = symbolAllocator.newSymbol(symbol);
                projection = coerceIfNecessary(analysis, mask, planBuilder.rewrite(mask));
            }

            assignments.put(symbol, projection);
            fieldMappings.add(symbol);
        }

        planBuilder = planBuilder
                .withNewRoot(new ProjectNode(
                        idAllocator.getNextId(),
                        planBuilder.getRoot(),
                        assignments.build()));

        return new RelationPlan(planBuilder.getRoot(), plan.getScope(), fieldMappings, outerContext);
    }

    @Override
    protected RelationPlan visitTableFunctionInvocation(TableFunctionInvocation node, Void context)
    {
        TableFunctionInvocationAnalysis functionAnalysis = analysis.getTableFunctionAnalysis(node);

        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
        ImmutableList.Builder<TableArgumentProperties> sourceProperties = ImmutableList.builder();
        ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();

        // create new symbols for table function's proper columns
        RelationType relationType = analysis.getScope(node).getRelationType();
        List<Symbol> properOutputs = IntStream.range(0, functionAnalysis.getProperColumnsCount())
                .mapToObj(relationType::getFieldByIndex)
                .map(symbolAllocator::newSymbol)
                .collect(toImmutableList());

        outputSymbols.addAll(properOutputs);

        // process sources in order of argument declarations
        for (TableArgumentAnalysis tableArgument : functionAnalysis.getTableArgumentAnalyses()) {
            RelationPlan sourcePlan = process(tableArgument.getRelation(), context);
            PlanBuilder sourcePlanBuilder = newPlanBuilder(sourcePlan, analysis, lambdaDeclarationToSymbolMap, session, plannerContext);

            // required columns are a subset of visible columns of the source. remap required column indexes to field indexes in source relation type.
            RelationType sourceRelationType = sourcePlan.getScope().getRelationType();
            int[] fieldIndexForVisibleColumn = new int[sourceRelationType.getVisibleFieldCount()];
            int visibleColumn = 0;
            for (int i = 0; i < sourceRelationType.getAllFieldCount(); i++) {
                if (!sourceRelationType.getFieldByIndex(i).isHidden()) {
                    fieldIndexForVisibleColumn[visibleColumn] = i;
                    visibleColumn++;
                }
            }
            List<Symbol> requiredColumns = functionAnalysis.getRequiredColumns().get(tableArgument.getArgumentName()).stream()
                    .map(column -> fieldIndexForVisibleColumn[column])
                    .map(sourcePlan::getSymbol)
                    .collect(toImmutableList());

            Optional<DataOrganizationSpecification> specification = Optional.empty();

            // if the table argument has set semantics, create Specification
            if (!tableArgument.isRowSemantics()) {
                // partition by
                List<Symbol> partitionBy = ImmutableList.of();
                // if there are partitioning columns, they might have to be coerced for copartitioning
                if (tableArgument.getPartitionBy().isPresent() && !tableArgument.getPartitionBy().get().isEmpty()) {
                    List<Expression> partitioningColumns = tableArgument.getPartitionBy().get();
                    PlanAndMappings copartitionCoercions = coerce(sourcePlanBuilder, partitioningColumns, analysis, idAllocator, symbolAllocator);
                    sourcePlanBuilder = copartitionCoercions.getSubPlan();
                    partitionBy = partitioningColumns.stream()
                            .map(copartitionCoercions::get)
                            .collect(toImmutableList());
                }

                // order by
                Optional<OrderingScheme> orderBy = Optional.empty();
                if (tableArgument.getOrderBy().isPresent()) {
                    // the ordering symbols are not coerced
                    orderBy = Optional.of(translateOrderingScheme(tableArgument.getOrderBy().get().getSortItems(), sourcePlanBuilder::translate));
                }

                specification = Optional.of(new DataOrganizationSpecification(partitionBy, orderBy));
            }

            // add output symbols passed from the table argument
            ImmutableList.Builder<PassThroughColumn> passThroughColumns = ImmutableList.builder();
            if (tableArgument.isPassThroughColumns()) {
                // the original output symbols from the source node, not coerced
                // note: hidden columns are included. They are present in sourcePlan.fieldMappings
                outputSymbols.addAll(sourcePlan.getFieldMappings());
                Set<Symbol> partitionBy = specification
                        .map(DataOrganizationSpecification::getPartitionBy)
                        .map(ImmutableSet::copyOf)
                        .orElse(ImmutableSet.of());
                sourcePlan.getFieldMappings().stream()
                        .map(symbol -> new PassThroughColumn(symbol, partitionBy.contains(symbol)))
                        .forEach(passThroughColumns::add);
            }
            else if (tableArgument.getPartitionBy().isPresent()) {
                tableArgument.getPartitionBy().get().stream()
                        // the original symbols for partitioning columns, not coerced
                        .map(sourcePlanBuilder::translate)
                        .forEach(symbol -> {
                            outputSymbols.add(symbol);
                            passThroughColumns.add(new PassThroughColumn(symbol, true));
                        });
            }

            sources.add(sourcePlanBuilder.getRoot());
            sourceProperties.add(new TableArgumentProperties(
                    tableArgument.getArgumentName(),
                    tableArgument.isRowSemantics(),
                    tableArgument.isPruneWhenEmpty(),
                    new PassThroughSpecification(tableArgument.isPassThroughColumns(), passThroughColumns.build()),
                    requiredColumns,
                    specification));
        }

        PlanNode root = new TableFunctionNode(
                idAllocator.getNextId(),
                functionAnalysis.getFunctionName(),
                functionAnalysis.getCatalogHandle(),
                functionAnalysis.getArguments(),
                properOutputs,
                sources.build(),
                sourceProperties.build(),
                functionAnalysis.getCopartitioningLists(),
                new TableFunctionHandle(
                        functionAnalysis.getCatalogHandle(),
                        functionAnalysis.getConnectorTableFunctionHandle(),
                        functionAnalysis.getTransactionHandle()));

        return new RelationPlan(root, analysis.getScope(node), outputSymbols.build(), outerContext);
    }

    @Override
    protected RelationPlan visitAliasedRelation(AliasedRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        PlanNode root = subPlan.getRoot();
        List<Symbol> mappings = subPlan.getFieldMappings();

        if (node.getColumnNames() != null) {
            ImmutableList.Builder<Symbol> newMappings = ImmutableList.builder();

            // Adjust the mappings to expose only the columns visible in the scope of the aliased relation
            for (int i = 0; i < subPlan.getDescriptor().getAllFieldCount(); i++) {
                if (!subPlan.getDescriptor().getFieldByIndex(i).isHidden()) {
                    newMappings.add(subPlan.getFieldMappings().get(i));
                }
            }

            mappings = newMappings.build();
        }

        return new RelationPlan(root, analysis.getScope(node), mappings, outerContext);
    }

    @Override
    protected RelationPlan visitPatternRecognitionRelation(PatternRecognitionRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getInput(), context);

        // Pre-project inputs for PARTITION BY and ORDER BY
        List<Expression> inputs = ImmutableList.<Expression>builder()
                .addAll(node.getPartitionBy())
                .addAll(getSortItemsFromOrderBy(node.getOrderBy()).stream()
                        .map(SortItem::getSortKey)
                        .collect(toImmutableList()))
                .build();

        PlanBuilder planBuilder = newPlanBuilder(subPlan, analysis, lambdaDeclarationToSymbolMap, session, plannerContext);

        // no handleSubqueries because subqueries are not allowed here
        planBuilder = planBuilder.appendProjections(inputs, symbolAllocator, idAllocator);

        ImmutableList.Builder<Symbol> outputLayout = ImmutableList.builder();
        RowsPerMatch rowsPerMatch = mapRowsPerMatch(node.getRowsPerMatch().orElse(ONE));
        boolean oneRowOutput = rowsPerMatch.isOneRow();

        DataOrganizationSpecification specification = planWindowSpecification(node.getPartitionBy(), node.getOrderBy(), planBuilder::translate);
        outputLayout.addAll(specification.getPartitionBy());
        if (!oneRowOutput) {
            getSortItemsFromOrderBy(node.getOrderBy()).stream()
                    .map(SortItem::getSortKey)
                    .map(planBuilder::translate)
                    .forEach(outputLayout::add);
        }

        planBuilder = subqueryPlanner.handleSubqueries(planBuilder, extractPatternRecognitionExpressions(node.getVariableDefinitions(), node.getMeasures()), analysis.getSubqueries(node));

        PatternRecognitionComponents components = planPatternRecognitionComponents(
                planBuilder.getTranslations(),
                node.getSubsets(),
                node.getMeasures(),
                node.getAfterMatchSkipTo(),
                node.getPatternSearchMode(),
                node.getPattern(),
                node.getVariableDefinitions());

        outputLayout.addAll(components.getMeasureOutputs());

        if (!oneRowOutput) {
            Set<Symbol> inputSymbolsOnOutput = ImmutableSet.copyOf(outputLayout.build());
            subPlan.getFieldMappings().stream()
                    .filter(symbol -> !inputSymbolsOnOutput.contains(symbol))
                    .forEach(outputLayout::add);
        }

        PatternRecognitionNode planNode = new PatternRecognitionNode(
                idAllocator.getNextId(),
                planBuilder.getRoot(),
                specification,
                Optional.empty(),
                ImmutableSet.of(),
                0,
                ImmutableMap.of(),
                components.getMeasures(),
                Optional.empty(),
                rowsPerMatch,
                components.getSkipToLabels(),
                components.getSkipToPosition(),
                components.isInitial(),
                components.getPattern(),
                components.getVariableDefinitions());

        return new RelationPlan(planNode, analysis.getScope(node), outputLayout.build(), outerContext);
    }

    private RowsPerMatch mapRowsPerMatch(PatternRecognitionRelation.RowsPerMatch rowsPerMatch)
    {
        return switch (rowsPerMatch) {
            case ONE -> RowsPerMatch.ONE;
            case ALL_SHOW_EMPTY -> RowsPerMatch.ALL_SHOW_EMPTY;
            case ALL_OMIT_EMPTY -> RowsPerMatch.ALL_OMIT_EMPTY;
            case ALL_WITH_UNMATCHED -> RowsPerMatch.ALL_WITH_UNMATCHED;
            case WINDOW -> RowsPerMatch.WINDOW;
        };
    }

    public PatternRecognitionComponents planPatternRecognitionComponents(
            TranslationMap translations,
            List<SubsetDefinition> subsets,
            List<MeasureDefinition> measures,
            Optional<SkipTo> skipTo,
            Optional<PatternSearchMode> searchMode,
            RowPattern pattern,
            List<VariableDefinition> variableDefinitions)
    {
        // NOTE: There might be aggregate functions in measure definitions and variable definitions.
        // They are handled different than top level aggregations in a query:
        // 1. Their arguments are not pre-projected and replaced with single symbols. This is because the arguments might
        //    not be eligible for pre-projection, when they contain references to CLASSIFIER() or MATCH_NUMBER() functions
        //    which are evaluated at runtime. If some aggregation arguments can be pre-projected, it will be done in the
        //    Optimizer.
        // 2. Their arguments do not need to be coerced by hand. Since the pattern aggregation arguments are rewritten as
        //    parts of enclosing expressions, and not as standalone expressions, all necessary coercions will be applied by the
        //    TranslationMap.

        // rewrite subsets
        ImmutableMap.Builder<IrLabel, Set<IrLabel>> rewrittenSubsetsBuilder = ImmutableMap.builder();
        for (SubsetDefinition subsetDefinition : subsets) {
            String label = analysis.getResolvedLabel(subsetDefinition.getName());
            Set<IrLabel> elements = analysis.getLabels(subsetDefinition).stream()
                    .map(IrLabel::new)
                    .collect(toImmutableSet());
            rewrittenSubsetsBuilder.put(new IrLabel(label), elements);
        }
        Map<IrLabel, Set<IrLabel>> rewrittenSubsets = rewrittenSubsetsBuilder.buildOrThrow();

        // rewrite measures
        ImmutableMap.Builder<Symbol, Measure> rewrittenMeasures = ImmutableMap.builder();
        ImmutableList.Builder<Symbol> measureOutputs = ImmutableList.builder();

        for (MeasureDefinition definition : measures) {
            Type type = analysis.getType(definition.getExpression());
            Symbol symbol = symbolAllocator.newSymbol(definition.getName().getValue(), type);
            ExpressionAndValuePointers measure = planPatternRecognitionExpression(translations, rewrittenSubsets, definition.getName().getValue(), definition.getExpression());
            rewrittenMeasures.put(symbol, new Measure(measure, type));
            measureOutputs.add(symbol);
        }

        // rewrite variable definitions
        ImmutableMap.Builder<IrLabel, ExpressionAndValuePointers> rewrittenVariableDefinitions = ImmutableMap.builder();
        for (VariableDefinition definition : variableDefinitions) {
            String label = analysis.getResolvedLabel(definition.getName());
            ExpressionAndValuePointers variable = planPatternRecognitionExpression(translations, rewrittenSubsets, definition.getName().getValue(), definition.getExpression());
            rewrittenVariableDefinitions.put(new IrLabel(label), variable);
        }
        // add `true` definition for undefined labels
        for (String label : analysis.getUndefinedLabels(pattern)) {
            rewrittenVariableDefinitions.put(irLabel(label), ExpressionAndValuePointers.TRUE);
        }

        Set<IrLabel> skipToLabels = skipTo.flatMap(SkipTo::getIdentifier)
                .map(Identifier::getValue)
                .map(label -> rewrittenSubsets.getOrDefault(new IrLabel(label), ImmutableSet.of(new IrLabel(label))))
                .orElse(ImmutableSet.of());

        return new PatternRecognitionComponents(
                rewrittenMeasures.buildOrThrow(),
                measureOutputs.build(),
                skipToLabels,
                mapSkipToPosition(skipTo.map(SkipTo::getPosition).orElse(PAST_LAST)),
                searchMode.map(mode -> mode.getMode() == INITIAL).orElse(TRUE),
                RowPatternToIrRewriter.rewrite(pattern, analysis),
                rewrittenVariableDefinitions.buildOrThrow());
    }

    private ExpressionAndValuePointers planPatternRecognitionExpression(TranslationMap translations, Map<IrLabel, Set<IrLabel>> subsets, String name, Expression expression)
    {
        Map<NodeRef<Expression>, Symbol> patternVariableTranslations = new HashMap<>();

        ImmutableList.Builder<Assignment> assignments = ImmutableList.builder();
        for (PatternInputAnalysis accessor : analysis.getPatternInputsAnalysis(expression)) {
            ValuePointer pointer = switch (accessor.descriptor()) {
                case MatchNumberDescriptor descriptor -> new MatchNumberValuePointer();
                case ClassifierDescriptor descriptor -> new ClassifierValuePointer(
                        planValuePointer(descriptor.label(), descriptor.navigation(), subsets));
                case ScalarInputDescriptor descriptor -> new ScalarValuePointer(
                        planValuePointer(descriptor.label(), descriptor.navigation(), subsets),
                        Symbol.from(translations.rewrite(accessor.expression())));
                case AggregationDescriptor descriptor -> {
                    Map<NodeRef<Expression>, Symbol> mappings = new HashMap<>();

                    Optional<Symbol> matchNumberSymbol = Optional.empty();
                    if (!descriptor.matchNumberCalls().isEmpty()) {
                        Symbol symbol = symbolAllocator.newSymbol("match_number", BIGINT);
                        for (Expression call : descriptor.matchNumberCalls()) {
                            mappings.put(NodeRef.of(call), symbol);
                        }
                        matchNumberSymbol = Optional.of(symbol);
                    }

                    Optional<Symbol> classifierSymbol = Optional.empty();
                    if (!descriptor.classifierCalls().isEmpty()) {
                        Symbol symbol = symbolAllocator.newSymbol("classifier", VARCHAR);

                        for (Expression call : descriptor.classifierCalls()) {
                            mappings.put(NodeRef.of(call), symbol);
                        }
                        classifierSymbol = Optional.of(symbol);
                    }

                    TranslationMap argumentTranslation = translations.withAdditionalIdentityMappings(mappings);

                    Set<IrLabel> labels = descriptor.labels().stream()
                            .flatMap(label -> planLabels(Optional.of(label), subsets).stream())
                            .collect(Collectors.toSet());

                    yield new AggregationValuePointer(
                            descriptor.function(),
                            new AggregatedSetDescriptor(labels, descriptor.mode() == RUNNING),
                            descriptor.arguments().stream()
                                    .filter(argument -> !DereferenceExpression.isQualifiedAllFieldsReference(argument))
                                    .map(argument -> coerceIfNecessary(analysis, argument, argumentTranslation.rewrite(argument)))
                                    .toList(),
                            classifierSymbol,
                            matchNumberSymbol);
                }
            };

            Symbol symbol = symbolAllocator.newSymbol(name, analysis.getType(accessor.expression()));
            assignments.add(new Assignment(symbol, pointer));

            patternVariableTranslations.put(NodeRef.of(accessor.expression()), symbol);
        }

        Expression rewritten = translations.withAdditionalIdentityMappings(patternVariableTranslations)
                .rewrite(expression);

        return new ExpressionAndValuePointers(rewritten, assignments.build());
    }

    private Set<IrLabel> planLabels(Optional<String> label, Map<IrLabel, Set<IrLabel>> subsets)
    {
        return label
                .map(IrLabel::new)
                .map(value -> subsets.getOrDefault(value, ImmutableSet.of(value)))
                .orElse(ImmutableSet.of());
    }

    private LogicalIndexPointer planValuePointer(Optional<String> label, Navigation navigation, Map<IrLabel, Set<IrLabel>> subsets)
    {
        return new LogicalIndexPointer(
                planLabels(label, subsets),
                navigation.anchor() == LAST,
                navigation.mode() == RUNNING,
                navigation.logicalOffset(),
                navigation.physicalOffset());
    }

    private SkipToPosition mapSkipToPosition(SkipTo.Position position)
    {
        return switch (position) {
            case NEXT -> SkipToPosition.NEXT;
            case PAST_LAST -> SkipToPosition.PAST_LAST;
            case FIRST -> SkipToPosition.FIRST;
            case LAST -> SkipToPosition.LAST;
        };
    }

    private static IrLabel irLabel(String label)
    {
        return new IrLabel(label);
    }

    @Override
    protected RelationPlan visitSampledRelation(SampledRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        double ratio = analysis.getSampleRatio(node);
        PlanNode planNode = new SampleNode(idAllocator.getNextId(),
                subPlan.getRoot(),
                ratio,
                mapSampleType(node.getType()));
        return new RelationPlan(planNode, analysis.getScope(node), subPlan.getFieldMappings(), outerContext);
    }

    @Override
    protected RelationPlan visitLateral(Lateral node, Void context)
    {
        RelationPlan plan = process(node.getQuery(), context);
        return new RelationPlan(plan.getRoot(), analysis.getScope(node), plan.getFieldMappings(), outerContext);
    }

    @Override
    protected RelationPlan visitJoin(Join node, Void context)
    {
        // TODO: translate the RIGHT join into a mirrored LEFT join when we refactor (@martint)
        RelationPlan leftPlan = process(node.getLeft(), context);

        Optional<Unnest> unnest = getUnnest(node.getRight());
        if (unnest.isPresent()) {
            return planJoinUnnest(leftPlan, node, unnest.get());
        }

        Optional<JsonTable> jsonTable = getJsonTable(node.getRight());
        if (jsonTable.isPresent()) {
            return planJoinJsonTable(
                    newPlanBuilder(leftPlan, analysis, lambdaDeclarationToSymbolMap, session, plannerContext),
                    leftPlan.getFieldMappings(),
                    node.getType(),
                    jsonTable.get(),
                    analysis.getScope(node));
        }

        Optional<Lateral> lateral = getLateral(node.getRight());
        if (lateral.isPresent()) {
            return planCorrelatedJoin(node, leftPlan, lateral.get());
        }

        RelationPlan rightPlan = process(node.getRight(), context);

        if (node.getCriteria().isPresent() && node.getCriteria().get() instanceof JoinUsing) {
            return planJoinUsing(node, leftPlan, rightPlan);
        }

        return planJoin(analysis.getJoinCriteria(node), node.getType(), analysis.getScope(node), leftPlan, rightPlan, analysis.getSubqueries(node));
    }

    public RelationPlan planJoin(Expression criteria, Join.Type type, Scope scope, RelationPlan leftPlan, RelationPlan rightPlan, Analysis.SubqueryAnalysis subqueries)
    {
        // NOTE: symbols must be in the same order as the outputDescriptor
        List<Symbol> outputSymbols = ImmutableList.<Symbol>builder()
                .addAll(leftPlan.getFieldMappings())
                .addAll(rightPlan.getFieldMappings())
                .build();

        PlanBuilder leftPlanBuilder = newPlanBuilder(leftPlan, analysis, lambdaDeclarationToSymbolMap, session, plannerContext)
                .withScope(scope, outputSymbols);
        PlanBuilder rightPlanBuilder = newPlanBuilder(rightPlan, analysis, lambdaDeclarationToSymbolMap, session, plannerContext)
                .withScope(scope, outputSymbols);

        ImmutableList.Builder<JoinNode.EquiJoinClause> equiClauses = ImmutableList.builder();
        List<Expression> complexJoinExpressions = new ArrayList<>();
        List<Expression> postInnerJoinConditions = new ArrayList<>();

        RelationType left = leftPlan.getDescriptor();
        RelationType right = rightPlan.getDescriptor();

        if (type != CROSS && type != IMPLICIT) {
            List<Expression> leftComparisonExpressions = new ArrayList<>();
            List<Expression> rightComparisonExpressions = new ArrayList<>();
            List<ComparisonExpression.Operator> joinConditionComparisonOperators = new ArrayList<>();

            for (Expression conjunct : AstUtils.extractConjuncts(criteria)) {
                if (!isEqualComparisonExpression(conjunct) && type != INNER) {
                    complexJoinExpressions.add(conjunct);
                    continue;
                }

                Set<QualifiedName> dependencies = SymbolsExtractor.extractNames(conjunct, analysis.getColumnReferences());

                if (dependencies.stream().allMatch(left::canResolve) || dependencies.stream().allMatch(right::canResolve)) {
                    // If the conjunct can be evaluated entirely with the inputs on either side of the join, add
                    // it to the list complex expressions and let the optimizers figure out how to push it down later.
                    complexJoinExpressions.add(conjunct);
                }
                else if (conjunct instanceof ComparisonExpression) {
                    Expression firstExpression = ((ComparisonExpression) conjunct).getLeft();
                    Expression secondExpression = ((ComparisonExpression) conjunct).getRight();
                    ComparisonExpression.Operator comparisonOperator = ((ComparisonExpression) conjunct).getOperator();
                    Set<QualifiedName> firstDependencies = SymbolsExtractor.extractNames(firstExpression, analysis.getColumnReferences());
                    Set<QualifiedName> secondDependencies = SymbolsExtractor.extractNames(secondExpression, analysis.getColumnReferences());

                    if (firstDependencies.stream().allMatch(left::canResolve) && secondDependencies.stream().allMatch(right::canResolve)) {
                        leftComparisonExpressions.add(firstExpression);
                        rightComparisonExpressions.add(secondExpression);
                        joinConditionComparisonOperators.add(comparisonOperator);
                    }
                    else if (firstDependencies.stream().allMatch(right::canResolve) && secondDependencies.stream().allMatch(left::canResolve)) {
                        leftComparisonExpressions.add(secondExpression);
                        rightComparisonExpressions.add(firstExpression);
                        joinConditionComparisonOperators.add(comparisonOperator.flip());
                    }
                    else {
                        // the case when we mix symbols from both left and right join side on either side of condition.
                        complexJoinExpressions.add(conjunct);
                    }
                }
                else {
                    complexJoinExpressions.add(conjunct);
                }
            }

            leftPlanBuilder = subqueryPlanner.handleSubqueries(leftPlanBuilder, leftComparisonExpressions, subqueries);
            rightPlanBuilder = subqueryPlanner.handleSubqueries(rightPlanBuilder, rightComparisonExpressions, subqueries);

            // Add projections for join criteria
            leftPlanBuilder = leftPlanBuilder.appendProjections(leftComparisonExpressions, symbolAllocator, idAllocator);
            rightPlanBuilder = rightPlanBuilder.appendProjections(rightComparisonExpressions, symbolAllocator, idAllocator);

            PlanAndMappings leftCoercions = coerce(leftPlanBuilder, leftComparisonExpressions, analysis, idAllocator, symbolAllocator);
            leftPlanBuilder = leftCoercions.getSubPlan();
            PlanAndMappings rightCoercions = coerce(rightPlanBuilder, rightComparisonExpressions, analysis, idAllocator, symbolAllocator);
            rightPlanBuilder = rightCoercions.getSubPlan();

            for (int i = 0; i < leftComparisonExpressions.size(); i++) {
                if (joinConditionComparisonOperators.get(i) == ComparisonExpression.Operator.EQUAL) {
                    Symbol leftSymbol = leftCoercions.get(leftComparisonExpressions.get(i));
                    Symbol rightSymbol = rightCoercions.get(rightComparisonExpressions.get(i));

                    equiClauses.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
                }
                else {
                    postInnerJoinConditions.add(
                            new ComparisonExpression(joinConditionComparisonOperators.get(i),
                                    leftCoercions.get(leftComparisonExpressions.get(i)).toSymbolReference(),
                                    rightCoercions.get(rightComparisonExpressions.get(i)).toSymbolReference()));
                }
            }
        }

        PlanNode root = new JoinNode(idAllocator.getNextId(),
                mapJoinType(type),
                leftPlanBuilder.getRoot(),
                rightPlanBuilder.getRoot(),
                equiClauses.build(),
                leftPlanBuilder.getRoot().getOutputSymbols(),
                rightPlanBuilder.getRoot().getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        if (type != INNER) {
            for (Expression complexExpression : complexJoinExpressions) {
                Set<QualifiedName> dependencies = SymbolsExtractor.extractNamesNoSubqueries(complexExpression, analysis.getColumnReferences());

                // This is for handling uncorreled subqueries. Correlated subqueries are not currently supported and are dealt with
                // during analysis.
                // Make best effort to plan the subquery in the branch of the join involving the other inputs to the expression.
                // E.g.,
                //  t JOIN u ON t.x = (...) get's planned on the t side
                //  t JOIN u ON t.x = (...) get's planned on the u side
                //  t JOIN u ON t.x + u.x = (...) get's planned on an arbitrary side
                if (dependencies.stream().allMatch(left::canResolve)) {
                    leftPlanBuilder = subqueryPlanner.handleSubqueries(leftPlanBuilder, complexExpression, subqueries);
                }
                else {
                    rightPlanBuilder = subqueryPlanner.handleSubqueries(rightPlanBuilder, complexExpression, subqueries);
                }
            }
        }
        TranslationMap translationMap = new TranslationMap(outerContext, scope, analysis, lambdaDeclarationToSymbolMap, outputSymbols, session, plannerContext)
                .withAdditionalMappings(leftPlanBuilder.getTranslations().getMappings())
                .withAdditionalMappings(rightPlanBuilder.getTranslations().getMappings());

        if (type != INNER && !complexJoinExpressions.isEmpty()) {
            root = new JoinNode(idAllocator.getNextId(),
                    mapJoinType(type),
                    leftPlanBuilder.getRoot(),
                    rightPlanBuilder.getRoot(),
                    equiClauses.build(),
                    leftPlanBuilder.getRoot().getOutputSymbols(),
                    rightPlanBuilder.getRoot().getOutputSymbols(),
                    false,
                    Optional.of(IrUtils.and(complexJoinExpressions.stream()
                            .map(e -> coerceIfNecessary(analysis, e, translationMap.rewrite(e)))
                            .collect(Collectors.toList()))),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of(),
                    Optional.empty());
        }

        if (type == INNER) {
            // rewrite all the other conditions using output symbols from left + right plan node.
            PlanBuilder rootPlanBuilder = new PlanBuilder(translationMap, root);
            rootPlanBuilder = subqueryPlanner.handleSubqueries(rootPlanBuilder, complexJoinExpressions, subqueries);

            for (Expression expression : complexJoinExpressions) {
                postInnerJoinConditions.add(coerceIfNecessary(analysis, expression, rootPlanBuilder.rewrite(expression)));
            }
            root = rootPlanBuilder.getRoot();

            Expression postInnerJoinCriteria;
            if (!postInnerJoinConditions.isEmpty()) {
                postInnerJoinCriteria = IrUtils.and(postInnerJoinConditions);
                root = new FilterNode(idAllocator.getNextId(), root, postInnerJoinCriteria);
            }
        }

        return new RelationPlan(root, scope, outputSymbols, outerContext);
    }

    private RelationPlan planJoinUsing(Join node, RelationPlan left, RelationPlan right)
    {
        /* Given: l JOIN r USING (k1, ..., kn)

           produces:

            - project
                    coalesce(l.k1, r.k1)
                    ...,
                    coalesce(l.kn, r.kn)
                    l.v1,
                    ...,
                    l.vn,
                    r.v1,
                    ...,
                    r.vn
              - join (l.k1 = r.k1 and ... l.kn = r.kn)
                    - project
                        cast(l.k1 as commonType(l.k1, r.k1))
                        ...
                    - project
                        cast(rl.k1 as commonType(l.k1, r.k1))

            If casts are redundant (due to column type and common type being equal),
            they will be removed by optimization passes.
        */

        List<Identifier> joinColumns = ((JoinUsing) node.getCriteria().orElseThrow()).getColumns();

        Analysis.JoinUsingAnalysis joinAnalysis = analysis.getJoinUsing(node);

        ImmutableList.Builder<JoinNode.EquiJoinClause> clauses = ImmutableList.builder();

        Map<Identifier, Symbol> leftJoinColumns = new HashMap<>();
        Map<Identifier, Symbol> rightJoinColumns = new HashMap<>();

        Assignments.Builder leftCoercions = Assignments.builder();
        Assignments.Builder rightCoercions = Assignments.builder();

        leftCoercions.putIdentities(left.getRoot().getOutputSymbols());
        rightCoercions.putIdentities(right.getRoot().getOutputSymbols());
        for (int i = 0; i < joinColumns.size(); i++) {
            Identifier identifier = joinColumns.get(i);
            Type type = analysis.getType(identifier);

            // compute the coercion for the field on the left to the common supertype of left & right
            Symbol leftOutput = symbolAllocator.newSymbol(identifier, type);
            int leftField = joinAnalysis.getLeftJoinFields().get(i);
            leftCoercions.put(leftOutput, new Cast(
                    left.getSymbol(leftField).toSymbolReference(),
                    toSqlType(type),
                    false));
            leftJoinColumns.put(identifier, leftOutput);

            // compute the coercion for the field on the right to the common supertype of left & right
            Symbol rightOutput = symbolAllocator.newSymbol(identifier, type);
            int rightField = joinAnalysis.getRightJoinFields().get(i);
            rightCoercions.put(rightOutput, new Cast(
                    right.getSymbol(rightField).toSymbolReference(),
                    toSqlType(type),
                    false));
            rightJoinColumns.put(identifier, rightOutput);

            clauses.add(new JoinNode.EquiJoinClause(leftOutput, rightOutput));
        }

        ProjectNode leftCoercion = new ProjectNode(idAllocator.getNextId(), left.getRoot(), leftCoercions.build());
        ProjectNode rightCoercion = new ProjectNode(idAllocator.getNextId(), right.getRoot(), rightCoercions.build());

        JoinNode join = new JoinNode(
                idAllocator.getNextId(),
                mapJoinType(node.getType()),
                leftCoercion,
                rightCoercion,
                clauses.build(),
                leftCoercion.getOutputSymbols(),
                rightCoercion.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        // Add a projection to produce the outputs of the columns in the USING clause,
        // which are defined as coalesce(l.k, r.k)
        Assignments.Builder assignments = Assignments.builder();

        ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
        for (Identifier column : joinColumns) {
            Symbol output = symbolAllocator.newSymbol(column, analysis.getType(column));
            outputs.add(output);
            assignments.put(output, new CoalesceExpression(
                    leftJoinColumns.get(column).toSymbolReference(),
                    rightJoinColumns.get(column).toSymbolReference()));
        }

        for (int field : joinAnalysis.getOtherLeftFields()) {
            Symbol symbol = left.getFieldMappings().get(field);
            outputs.add(symbol);
            assignments.putIdentity(symbol);
        }

        for (int field : joinAnalysis.getOtherRightFields()) {
            Symbol symbol = right.getFieldMappings().get(field);
            outputs.add(symbol);
            assignments.putIdentity(symbol);
        }

        return new RelationPlan(
                new ProjectNode(idAllocator.getNextId(), join, assignments.build()),
                analysis.getScope(node),
                outputs.build(),
                outerContext);
    }

    private static Optional<Unnest> getUnnest(Relation relation)
    {
        if (relation instanceof AliasedRelation) {
            return getUnnest(((AliasedRelation) relation).getRelation());
        }
        if (relation instanceof Unnest) {
            return Optional.of((Unnest) relation);
        }
        return Optional.empty();
    }

    private static Optional<JsonTable> getJsonTable(Relation relation)
    {
        if (relation instanceof AliasedRelation) {
            return getJsonTable(((AliasedRelation) relation).getRelation());
        }
        if (relation instanceof JsonTable) {
            return Optional.of((JsonTable) relation);
        }
        return Optional.empty();
    }

    private static Optional<Lateral> getLateral(Relation relation)
    {
        if (relation instanceof AliasedRelation) {
            return getLateral(((AliasedRelation) relation).getRelation());
        }
        if (relation instanceof Lateral) {
            return Optional.of((Lateral) relation);
        }
        return Optional.empty();
    }

    private RelationPlan planCorrelatedJoin(Join join, RelationPlan leftPlan, Lateral lateral)
    {
        PlanBuilder leftPlanBuilder = newPlanBuilder(leftPlan, analysis, lambdaDeclarationToSymbolMap, session, plannerContext);

        RelationPlan rightPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, Optional.of(leftPlanBuilder.getTranslations()), session, recursiveSubqueries)
                .process(lateral.getQuery(), null);

        PlanBuilder rightPlanBuilder = newPlanBuilder(rightPlan, analysis, lambdaDeclarationToSymbolMap, session, plannerContext);

        List<Symbol> outputSymbols = ImmutableList.<Symbol>builder()
                .addAll(leftPlan.getFieldMappings())
                .addAll(rightPlan.getFieldMappings())
                .build();
        TranslationMap translationMap = new TranslationMap(outerContext, analysis.getScope(join), analysis, lambdaDeclarationToSymbolMap, outputSymbols, session, plannerContext)
                .withAdditionalMappings(leftPlanBuilder.getTranslations().getMappings())
                .withAdditionalMappings(rightPlanBuilder.getTranslations().getMappings());

        Expression rewrittenFilterCondition;
        if (join.getCriteria().isEmpty()) {
            rewrittenFilterCondition = TRUE_LITERAL;
        }
        else {
            JoinCriteria criteria = join.getCriteria().get();
            if (criteria instanceof JoinUsing || criteria instanceof NaturalJoin) {
                throw semanticException(NOT_SUPPORTED, join, "Correlated join with criteria other than ON is not supported");
            }

            Expression filterExpression = (Expression) getOnlyElement(criteria.getNodes());
            rewrittenFilterCondition = coerceIfNecessary(analysis, filterExpression, translationMap.rewrite(filterExpression));
        }

        PlanBuilder planBuilder = subqueryPlanner.appendCorrelatedJoin(
                leftPlanBuilder,
                rightPlanBuilder.getRoot(),
                lateral.getQuery(),
                mapJoinType(join.getType()),
                rewrittenFilterCondition,
                ImmutableMap.of());

        return new RelationPlan(planBuilder.getRoot(), analysis.getScope(join), outputSymbols, outerContext);
    }

    private static boolean isEqualComparisonExpression(Expression conjunct)
    {
        return conjunct instanceof ComparisonExpression && ((ComparisonExpression) conjunct).getOperator() == ComparisonExpression.Operator.EQUAL;
    }

    private RelationPlan planJoinUnnest(RelationPlan leftPlan, Join joinNode, Unnest node)
    {
        if (joinNode.getCriteria().isPresent()) {
            JoinCriteria criteria = joinNode.getCriteria().get();
            if (criteria instanceof NaturalJoin) {
                throw semanticException(NOT_SUPPORTED, joinNode, "Natural join involving UNNEST is not supported");
            }
            if (criteria instanceof JoinUsing) {
                throw semanticException(NOT_SUPPORTED, joinNode, "USING for join involving UNNEST is not supported");
            }
            Expression filter = (Expression) getOnlyElement(criteria.getNodes());
            if (!filter.equals(TRUE_LITERAL)) {
                throw semanticException(NOT_SUPPORTED, joinNode, "JOIN involving UNNEST on condition other than TRUE is not supported");
            }
        }

        return planUnnest(
                newPlanBuilder(leftPlan, analysis, lambdaDeclarationToSymbolMap, session, plannerContext),
                node,
                leftPlan.getFieldMappings(),
                joinNode.getType(),
                analysis.getScope(joinNode));
    }

    private RelationPlan planUnnest(PlanBuilder subPlan, Unnest node, List<Symbol> replicatedColumns, Join.Type type, Scope outputScope)
    {
        subPlan = subqueryPlanner.handleSubqueries(subPlan, node.getExpressions(), analysis.getSubqueries(node));
        subPlan = subPlan.appendProjections(node.getExpressions(), symbolAllocator, idAllocator);

        Map<Field, Symbol> allocations = analysis.getOutputDescriptor(node)
                .getVisibleFields().stream()
                .collect(toImmutableMap(Function.identity(), symbolAllocator::newSymbol));

        UnnestAnalysis unnestAnalysis = analysis.getUnnest(node);

        ImmutableList.Builder<UnnestNode.Mapping> mappings = ImmutableList.builder();
        for (Expression expression : node.getExpressions()) {
            Symbol input = subPlan.translate(expression);
            List<Symbol> outputs = unnestAnalysis.getMappings().get(NodeRef.of(expression)).stream()
                    .map(allocations::get)
                    .collect(toImmutableList());

            mappings.add(new UnnestNode.Mapping(input, outputs));
        }

        UnnestNode unnestNode = new UnnestNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                replicatedColumns,
                mappings.build(),
                unnestAnalysis.getOrdinalityField().map(allocations::get),
                mapJoinType(type));

        // TODO: Technically, we should derive the field mappings from the layout of fields and how they relate to the output symbols of the Unnest node.
        //       That's tricky to do for a Join+Unnest because the allocations come from the Unnest, but the mappings need to be done based on the Join output fields.
        //       Currently, it works out because, by construction, the order of the output symbols in the UnnestNode will match the order of the fields in the Join node.
        return new RelationPlan(unnestNode, outputScope, unnestNode.getOutputSymbols(), outerContext);
    }

    private RelationPlan planJoinJsonTable(PlanBuilder leftPlan, List<Symbol> leftFieldMappings, Join.Type joinType, JsonTable jsonTable, Scope outputScope)
    {
        PlanBuilder planBuilder = leftPlan;

        // extract input expressions
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        Expression inputExpression = jsonTable.getJsonPathInvocation().getInputExpression();
        builder.add(inputExpression);
        List<JsonPathParameter> pathParameters = jsonTable.getJsonPathInvocation().getPathParameters();
        pathParameters.stream()
                .map(JsonPathParameter::getParameter)
                .forEach(builder::add);
        List<Expression> defaultExpressions = getDefaultExpressions(jsonTable.getColumns());
        builder.addAll(defaultExpressions);
        List<Expression> inputExpressions = builder.build();

        planBuilder = subqueryPlanner.handleSubqueries(planBuilder, inputExpressions, analysis.getSubqueries(jsonTable));
        planBuilder = planBuilder.appendProjections(inputExpressions, symbolAllocator, idAllocator);

        // apply coercions
        // coercions might be necessary for the context item and path parameters before the input functions are applied
        // also, the default expressions in value columns (DEFAULT ... ON EMPTY / ON ERROR) might need a coercion to match the required output type
        PlanAndMappings coerced = coerce(planBuilder, inputExpressions, analysis, idAllocator, symbolAllocator);
        planBuilder = coerced.getSubPlan();

        // apply the input function to the input expression
        BooleanLiteral failOnError = new BooleanLiteral(jsonTable.getErrorBehavior().orElse(JsonTable.ErrorBehavior.EMPTY) == JsonTable.ErrorBehavior.ERROR ? "true" : "false");
        ResolvedFunction inputToJson = analysis.getJsonInputFunction(inputExpression);
        Expression inputJson = new FunctionCall(inputToJson.toQualifiedName(), ImmutableList.of(coerced.get(inputExpression).toSymbolReference(), failOnError));

        // apply the input functions to the JSON path parameters having FORMAT,
        // and collect all JSON path parameters in a Row
        List<Expression> coercedParameters = pathParameters.stream()
                .map(parameter -> coerced.get(parameter.getParameter()).toSymbolReference())
                .collect(toImmutableList());
        JsonTableAnalysis jsonTableAnalysis = analysis.getJsonTableAnalysis(jsonTable);
        RowType parametersType = jsonTableAnalysis.parametersType();
        ParametersRow orderedParameters = planBuilder.getTranslations().getParametersRow(pathParameters, coercedParameters, parametersType, failOnError);
        Expression parametersRow = orderedParameters.getParametersRow();

        // append projections for inputJson and parametersRow
        // cannot use the 'appendProjections()' method because the projected expressions include resolved input functions, so they are not pure AST expressions
        Symbol inputJsonSymbol = symbolAllocator.newSymbol("inputJson", JSON_2016);
        Symbol parametersRowSymbol = symbolAllocator.newSymbol("parametersRow", parametersType);
        ProjectNode appended = new ProjectNode(
                idAllocator.getNextId(),
                planBuilder.getRoot(),
                Assignments.builder()
                        .putIdentities(planBuilder.getRoot().getOutputSymbols())
                        .put(inputJsonSymbol, inputJson)
                        .put(parametersRowSymbol, parametersRow)
                        .build());
        planBuilder = planBuilder.withNewRoot(appended);

        // identify the required symbols
        ImmutableList.Builder<Symbol> requiredSymbolsBuilder = ImmutableList.<Symbol>builder()
                .add(inputJsonSymbol)
                .add(parametersRowSymbol);
        defaultExpressions.stream()
                .map(coerced::get)
                .distinct()
                .forEach(requiredSymbolsBuilder::add);
        List<Symbol> requiredSymbols = requiredSymbolsBuilder.build();

        // map the default expressions of value columns to indexes in the required columns list
        // use a HashMap because there might be duplicate expressions
        Map<Expression, Integer> defaultExpressionsMapping = new HashMap<>();
        for (Expression defaultExpression : defaultExpressions) {
            defaultExpressionsMapping.put(defaultExpression, requiredSymbols.indexOf(coerced.get(defaultExpression)));
        }

        // rewrite the root JSON path to IR using parameters
        IrJsonPath rootPath = new JsonPathTranslator(session, plannerContext).rewriteToIr(analysis.getJsonPathAnalysis(jsonTable), orderedParameters.getParametersOrder());

        // create json_table execution plan
        List<NodeRef<JsonTableColumnDefinition>> orderedColumns = jsonTableAnalysis.orderedOutputColumns();
        Map<NodeRef<JsonTableColumnDefinition>, Integer> outputIndexMapping = IntStream.range(0, orderedColumns.size())
                .boxed()
                .collect(toImmutableMap(orderedColumns::get, Function.identity()));
        JsonTablePlanNode executionPlan;
        boolean defaultErrorOnError = jsonTable.getErrorBehavior().map(errorBehavior -> errorBehavior == JsonTable.ErrorBehavior.ERROR).orElse(false);
        if (jsonTable.getPlan().isEmpty()) {
            executionPlan = getPlanFromDefaults(rootPath, jsonTable.getColumns(), OUTER, UNION, defaultErrorOnError, outputIndexMapping, defaultExpressionsMapping);
        }
        else if (jsonTable.getPlan().orElseThrow() instanceof JsonTableDefaultPlan defaultPlan) {
            executionPlan = getPlanFromDefaults(rootPath, jsonTable.getColumns(), defaultPlan.getParentChild(), defaultPlan.getSiblings(), defaultErrorOnError, outputIndexMapping, defaultExpressionsMapping);
        }
        else {
            executionPlan = getPlanFromSpecification(rootPath, jsonTable.getColumns(), (JsonTableSpecificPlan) jsonTable.getPlan().orElseThrow(), defaultErrorOnError, outputIndexMapping, defaultExpressionsMapping);
        }

        // create new symbols for json_table function's proper columns
        // These are the types produced by the table function.
        // For ordinality and value columns, the types match the expected output type.
        // Query columns return JSON_2016. Later we need to apply an output function, and potentially a coercion to match the declared output type.
        RelationType jsonTableRelationType = analysis.getScope(jsonTable).getRelationType();
        List<Symbol> properOutputs = IntStream.range(0, orderedColumns.size())
                .mapToObj(index -> {
                    if (orderedColumns.get(index).getNode() instanceof QueryColumn queryColumn) {
                        return symbolAllocator.newSymbol(queryColumn.getName().getCanonicalValue(), JSON_2016);
                    }
                    return symbolAllocator.newSymbol(jsonTableRelationType.getFieldByIndex(index));
                })
                .collect(toImmutableList());

        // pass through all columns from the left side of the join
        List<PassThroughColumn> passThroughColumns = leftFieldMappings.stream()
                .map(symbol -> new PassThroughColumn(symbol, false))
                .collect(toImmutableList());

        // determine the join type between the input, and the json_table result
        // this join type is not described in the plan, it depends on the enclosing join whose right source is the json_table
        // since json_table is a lateral relation, and the join condition is 'true', effectively the join type is either LEFT OUTER or INNER
        boolean outer = joinType == LEFT || joinType == FULL;

        // create the TableFunctionNode and TableFunctionHandle
        JsonTableFunctionHandle functionHandle = new JsonTableFunctionHandle(
                executionPlan,
                outer,
                defaultErrorOnError,
                parametersType,
                properOutputs.stream()
                        .map(symbolAllocator.getTypes()::get)
                        .toArray(Type[]::new));

        TableFunctionNode tableFunctionNode = new TableFunctionNode(
                idAllocator.getNextId(),
                "$json_table",
                jsonTableAnalysis.catalogHandle(),
                ImmutableMap.of("$input", new TableArgument(getRowType(planBuilder.getRoot()), ImmutableList.of(), ImmutableList.of())),
                properOutputs,
                ImmutableList.of(planBuilder.getRoot()),
                ImmutableList.of(new TableArgumentProperties(
                        "$input",
                        true,
                        true,
                        new PassThroughSpecification(true, passThroughColumns),
                        requiredSymbols,
                        Optional.empty())),
                ImmutableList.of(),
                new TableFunctionHandle(
                        jsonTableAnalysis.catalogHandle(),
                        functionHandle,
                        jsonTableAnalysis.transactionHandle()));

        // append output functions and coercions for query columns
        // The table function returns JSON_2016 for query columns. We need to apply output functions and coercions to match the declared output type.
        // create output layout: first the left side of the join, next the proper columns
        ImmutableList.Builder<Symbol> outputLayout = ImmutableList.<Symbol>builder()
                .addAll(leftFieldMappings);
        Assignments.Builder assignments = Assignments.builder()
                .putIdentities(leftFieldMappings);
        for (int i = 0; i < properOutputs.size(); i++) {
            Symbol properOutput = properOutputs.get(i);
            if (orderedColumns.get(i).getNode() instanceof QueryColumn queryColumn) {
                // apply output function
                GenericLiteral errorBehavior = new GenericLiteral(
                        TINYINT,
                        String.valueOf(queryColumn.getErrorBehavior().orElse(defaultErrorOnError ? JsonQuery.EmptyOrErrorBehavior.ERROR : JsonQuery.EmptyOrErrorBehavior.NULL).ordinal()));
                BooleanLiteral omitQuotes = new BooleanLiteral(queryColumn.getQuotesBehavior().orElse(KEEP) == OMIT ? "true" : "false");
                ResolvedFunction outputFunction = analysis.getJsonOutputFunction(queryColumn);
                Expression result = new FunctionCall(outputFunction.toQualifiedName(), ImmutableList.of(properOutput.toSymbolReference(), errorBehavior, omitQuotes));

                // cast to declared returned type
                Type expectedType = jsonTableRelationType.getFieldByIndex(i).getType();
                Type resultType = outputFunction.getSignature().getReturnType();
                if (!resultType.equals(expectedType)) {
                    result = new Cast(result, toSqlType(expectedType));
                }

                Symbol output = symbolAllocator.newSymbol(result, expectedType);
                outputLayout.add(output);
                assignments.put(output, result);
            }
            else {
                outputLayout.add(properOutput);
                assignments.putIdentity(properOutput);
            }
        }

        ProjectNode projectNode = new ProjectNode(
                idAllocator.getNextId(),
                tableFunctionNode,
                assignments.build());

        return new RelationPlan(projectNode, outputScope, outputLayout.build(), outerContext);
    }

    private static List<Expression> getDefaultExpressions(List<JsonTableColumnDefinition> columns)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        for (JsonTableColumnDefinition column : columns) {
            if (column instanceof ValueColumn valueColumn) {
                valueColumn.getEmptyDefault().ifPresent(builder::add);
                valueColumn.getErrorDefault().ifPresent(builder::add);
            }
            else if (column instanceof NestedColumns nestedColumns) {
                builder.addAll(getDefaultExpressions(nestedColumns.getColumns()));
            }
        }
        return builder.build();
    }

    private JsonTablePlanNode getPlanFromDefaults(
            IrJsonPath path,
            List<JsonTableColumnDefinition> columnDefinitions,
            ParentChildPlanType parentChildPlanType,
            SiblingsPlanType siblingsPlanType,
            boolean defaultErrorOnError,
            Map<NodeRef<JsonTableColumnDefinition>, Integer> outputIndexMapping,
            Map<Expression, Integer> defaultExpressionsMapping)
    {
        ImmutableList.Builder<JsonTableColumn> columns = ImmutableList.builder();
        ImmutableList.Builder<JsonTablePlanNode> childrenBuilder = ImmutableList.builder();

        for (JsonTableColumnDefinition columnDefinition : columnDefinitions) {
            if (columnDefinition instanceof NestedColumns nestedColumns) {
                IrJsonPath nestedPath = new JsonPathTranslator(session, plannerContext).rewriteToIr(analysis.getJsonPathAnalysis(nestedColumns), ImmutableList.of());
                childrenBuilder.add(getPlanFromDefaults(
                        nestedPath,
                        nestedColumns.getColumns(),
                        parentChildPlanType,
                        siblingsPlanType,
                        defaultErrorOnError,
                        outputIndexMapping,
                        defaultExpressionsMapping));
            }
            else {
                columns.add(getColumn(columnDefinition, defaultErrorOnError, outputIndexMapping, defaultExpressionsMapping));
            }
        }

        List<JsonTablePlanNode> children = childrenBuilder.build();
        if (children.isEmpty()) {
            return new JsonTablePlanLeaf(path, columns.build());
        }

        JsonTablePlanNode child;
        if (children.size() == 1) {
            child = getOnlyElement(children);
        }
        else if (siblingsPlanType == UNION) {
            child = new JsonTablePlanUnion(children);
        }
        else {
            child = new JsonTablePlanCross(children);
        }

        return new JsonTablePlanSingle(path, columns.build(), parentChildPlanType == OUTER, child);
    }

    private JsonTablePlanNode getPlanFromSpecification(
            IrJsonPath path,
            List<JsonTableColumnDefinition> columnDefinitions,
            JsonTableSpecificPlan specificPlan,
            boolean defaultErrorOnError,
            Map<NodeRef<JsonTableColumnDefinition>, Integer> outputIndexMapping,
            Map<Expression, Integer> defaultExpressionsMapping)
    {
        ImmutableList.Builder<JsonTableColumn> columns = ImmutableList.builder();
        ImmutableMap.Builder<String, JsonTablePlanNode> childrenBuilder = ImmutableMap.builder();
        Map<String, JsonTableSpecificPlan> planSiblings;
        if (specificPlan instanceof PlanLeaf) {
            planSiblings = ImmutableMap.of();
        }
        else {
            planSiblings = getSiblings(((PlanParentChild) specificPlan).getChild());
        }

        for (JsonTableColumnDefinition columnDefinition : columnDefinitions) {
            if (columnDefinition instanceof NestedColumns nestedColumns) {
                IrJsonPath nestedPath = new JsonPathTranslator(session, plannerContext).rewriteToIr(analysis.getJsonPathAnalysis(nestedColumns), ImmutableList.of());
                String nestedPathName = nestedColumns.getPathName().orElseThrow().getCanonicalValue();
                JsonTablePlanNode child = getPlanFromSpecification(
                        nestedPath,
                        nestedColumns.getColumns(),
                        planSiblings.get(nestedPathName),
                        defaultErrorOnError,
                        outputIndexMapping,
                        defaultExpressionsMapping);
                childrenBuilder.put(nestedPathName, child);
            }
            else {
                columns.add(getColumn(columnDefinition, defaultErrorOnError, outputIndexMapping, defaultExpressionsMapping));
            }
        }

        Map<String, JsonTablePlanNode> children = childrenBuilder.buildOrThrow();
        if (children.isEmpty()) {
            return new JsonTablePlanLeaf(path, columns.build());
        }

        PlanParentChild planParentChild = (PlanParentChild) specificPlan;
        boolean outer = planParentChild.getType() == OUTER;
        JsonTablePlanNode child = combineSiblings(children, planParentChild.getChild());
        return new JsonTablePlanSingle(path, columns.build(), outer, child);
    }

    private Map<String, JsonTableSpecificPlan> getSiblings(JsonTableSpecificPlan plan)
    {
        if (plan instanceof PlanLeaf planLeaf) {
            return ImmutableMap.of(planLeaf.getName().getCanonicalValue(), planLeaf);
        }
        if (plan instanceof PlanParentChild planParentChild) {
            return ImmutableMap.of(planParentChild.getParent().getName().getCanonicalValue(), planParentChild);
        }
        PlanSiblings planSiblings = (PlanSiblings) plan;
        ImmutableMap.Builder<String, JsonTableSpecificPlan> siblings = ImmutableMap.builder();
        for (JsonTableSpecificPlan sibling : planSiblings.getSiblings()) {
            siblings.putAll(getSiblings(sibling));
        }
        return siblings.buildOrThrow();
    }

    private JsonTableColumn getColumn(
            JsonTableColumnDefinition columnDefinition,
            boolean defaultErrorOnError,
            Map<NodeRef<JsonTableColumnDefinition>, Integer> outputIndexMapping,
            Map<Expression, Integer> defaultExpressionsMapping)
    {
        int index = outputIndexMapping.get(NodeRef.of(columnDefinition));

        if (columnDefinition instanceof OrdinalityColumn) {
            return new JsonTableOrdinalityColumn(index);
        }
        ResolvedFunction columnFunction = analysis.getResolvedFunction(columnDefinition);
        IrJsonPath columnPath = new JsonPathTranslator(session, plannerContext).rewriteToIr(analysis.getJsonPathAnalysis(columnDefinition), ImmutableList.of());
        if (columnDefinition instanceof QueryColumn queryColumn) {
            return new JsonTableQueryColumn(
                    index,
                    columnFunction,
                    columnPath,
                    queryColumn.getWrapperBehavior().ordinal(),
                    queryColumn.getEmptyBehavior().ordinal(),
                    queryColumn.getErrorBehavior().orElse(defaultErrorOnError ? JsonQuery.EmptyOrErrorBehavior.ERROR : JsonQuery.EmptyOrErrorBehavior.NULL).ordinal());
        }
        if (columnDefinition instanceof ValueColumn valueColumn) {
            int emptyDefault = valueColumn.getEmptyDefault()
                    .map(defaultExpressionsMapping::get)
                    .orElse(-1);
            int errorDefault = valueColumn.getErrorDefault()
                    .map(defaultExpressionsMapping::get)
                    .orElse(-1);
            return new JsonTableValueColumn(
                    index,
                    columnFunction,
                    columnPath,
                    valueColumn.getEmptyBehavior().ordinal(),
                    emptyDefault,
                    valueColumn.getErrorBehavior().orElse(defaultErrorOnError ? JsonValue.EmptyOrErrorBehavior.ERROR : JsonValue.EmptyOrErrorBehavior.NULL).ordinal(),
                    errorDefault);
        }
        throw new IllegalStateException("unexpected column definition: " + columnDefinition.getClass().getSimpleName());
    }

    private JsonTablePlanNode combineSiblings(Map<String, JsonTablePlanNode> siblings, JsonTableSpecificPlan plan)
    {
        if (plan instanceof PlanLeaf planLeaf) {
            return siblings.get(planLeaf.getName().getCanonicalValue());
        }
        if (plan instanceof PlanParentChild planParentChild) {
            return siblings.get(planParentChild.getParent().getName().getCanonicalValue());
        }
        PlanSiblings planSiblings = (PlanSiblings) plan;
        List<JsonTablePlanNode> siblingNodes = planSiblings.getSiblings().stream()
                .map(sibling -> combineSiblings(siblings, sibling))
                .collect(toImmutableList());
        if (planSiblings.getType() == UNION) {
            return new JsonTablePlanUnion(siblingNodes);
        }
        return new JsonTablePlanCross(siblingNodes);
    }

    private RowType getRowType(PlanNode node)
    {
        // create a RowType based on output symbols of a node
        // The node is an intermediate stage of planning json_table. There's no recorded relation type available for this node.
        // The returned RowType is only used in plan printer
        return RowType.from(node.getOutputSymbols().stream()
                .map(symbol -> new RowType.Field(Optional.of(symbol.getName()), symbolAllocator.getTypes().get(symbol)))
                .collect(toImmutableList()));
    }

    @Override
    protected RelationPlan visitTableSubquery(TableSubquery node, Void context)
    {
        RelationPlan plan = process(node.getQuery(), context);
        return new RelationPlan(plan.getRoot(), analysis.getScope(node), plan.getFieldMappings(), outerContext);
    }

    @Override
    protected RelationPlan visitQuery(Query node, Void context)
    {
        return new QueryPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, outerContext, session, recursiveSubqueries)
                .plan(node);
    }

    @Override
    protected RelationPlan visitQuerySpecification(QuerySpecification node, Void context)
    {
        return new QueryPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, outerContext, session, recursiveSubqueries)
                .plan(node);
    }

    @Override
    protected RelationPlan visitSubqueryExpression(SubqueryExpression node, Void context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected RelationPlan visitValues(Values node, Void context)
    {
        Scope scope = analysis.getScope(node);
        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
        for (Field field : scope.getRelationType().getVisibleFields()) {
            Symbol symbol = symbolAllocator.newSymbol(field);
            outputSymbolsBuilder.add(symbol);
        }
        List<Symbol> outputSymbols = outputSymbolsBuilder.build();
        TranslationMap translationMap = new TranslationMap(outerContext, analysis.getScope(node), analysis, lambdaDeclarationToSymbolMap, outputSymbols, session, plannerContext);

        ImmutableList.Builder<Expression> rows = ImmutableList.builder();
        for (Expression row : node.getRows()) {
            if (row instanceof Row) {
                rows.add(new Row(((Row) row).getItems().stream()
                        .map(item -> coerceIfNecessary(analysis, item, translationMap.rewrite(item)))
                        .collect(toImmutableList())));
            }
            else if (analysis.getType(row) instanceof RowType) {
                rows.add(coerceIfNecessary(analysis, row, translationMap.rewrite(row)));
            }
            else {
                rows.add(new Row(ImmutableList.of(coerceIfNecessary(analysis, row, translationMap.rewrite(row)))));
            }
        }

        ValuesNode valuesNode = new ValuesNode(idAllocator.getNextId(), outputSymbols, rows.build());
        return new RelationPlan(valuesNode, scope, outputSymbols, outerContext);
    }

    @Override
    protected RelationPlan visitUnnest(Unnest node, Void context)
    {
        Scope scope = analysis.getScope(node);

        return planUnnest(
                planSingleEmptyRow(scope.getOuterQueryParent()),
                node,
                ImmutableList.of(),
                INNER,
                scope);
    }

    private PlanBuilder planSingleEmptyRow(Optional<Scope> parent)
    {
        Scope.Builder scope = Scope.builder();
        parent.ifPresent(scope::withOuterQueryParent);

        PlanNode values = new ValuesNode(idAllocator.getNextId(), 1);
        TranslationMap translations = new TranslationMap(outerContext, scope.build(), analysis, lambdaDeclarationToSymbolMap, ImmutableList.of(), session, plannerContext);
        return new PlanBuilder(translations, values);
    }

    @Override
    protected RelationPlan visitJsonTable(JsonTable node, Void context)
    {
        return planJoinJsonTable(
                planSingleEmptyRow(analysis.getScope(node).getOuterQueryParent()),
                ImmutableList.of(),
                INNER,
                node,
                analysis.getScope(node));
    }

    @Override
    protected RelationPlan visitUnion(Union node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for UNION");

        SetOperationPlan setOperationPlan = process(node);

        PlanNode planNode = new UnionNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getSymbolMapping(), ImmutableList.copyOf(setOperationPlan.getSymbolMapping()
                .keySet()));
        if (node.isDistinct()) {
            planNode = distinct(planNode);
        }
        return new RelationPlan(planNode, analysis.getScope(node), planNode.getOutputSymbols(), outerContext);
    }

    @Override
    protected RelationPlan visitIntersect(Intersect node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for INTERSECT");

        SetOperationPlan setOperationPlan = process(node);

        PlanNode planNode = new IntersectNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getSymbolMapping(), ImmutableList.copyOf(setOperationPlan.getSymbolMapping()
                .keySet()), node.isDistinct());
        return new RelationPlan(planNode, analysis.getScope(node), planNode.getOutputSymbols(), outerContext);
    }

    @Override
    protected RelationPlan visitExcept(Except node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for EXCEPT");

        SetOperationPlan setOperationPlan = process(node);

        PlanNode planNode = new ExceptNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getSymbolMapping(), ImmutableList.copyOf(setOperationPlan.getSymbolMapping()
                .keySet()), node.isDistinct());
        return new RelationPlan(planNode, analysis.getScope(node), planNode.getOutputSymbols(), outerContext);
    }

    private SetOperationPlan process(SetOperation node)
    {
        RelationType outputFields = analysis.getOutputDescriptor(node);
        List<Symbol> outputs = outputFields
                .getAllFields().stream()
                .map(symbolAllocator::newSymbol)
                .collect(toImmutableList());

        ImmutableListMultimap.Builder<Symbol, Symbol> symbolMapping = ImmutableListMultimap.builder();
        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();

        for (Relation child : node.getRelations()) {
            RelationPlan plan = process(child, null);

            NodeAndMappings planAndMappings;
            List<Type> types = analysis.getRelationCoercion(child);
            if (types == null) {
                // no coercion required, only prune invisible fields from child outputs
                planAndMappings = pruneInvisibleFields(plan, idAllocator);
            }
            else {
                // apply required coercion and prune invisible fields from child outputs
                planAndMappings = coerce(plan, types, symbolAllocator, idAllocator);
            }
            for (int i = 0; i < outputFields.getAllFields().size(); i++) {
                symbolMapping.put(outputs.get(i), planAndMappings.getFields().get(i));
            }

            sources.add(planAndMappings.getNode());
        }
        return new SetOperationPlan(sources.build(), symbolMapping.build());
    }

    private PlanNode distinct(PlanNode node)
    {
        return singleAggregation(idAllocator.getNextId(),
                node,
                ImmutableMap.of(),
                singleGroupingSet(node.getOutputSymbols()));
    }

    private static final class SetOperationPlan
    {
        private final List<PlanNode> sources;
        private final ListMultimap<Symbol, Symbol> symbolMapping;

        private SetOperationPlan(List<PlanNode> sources, ListMultimap<Symbol, Symbol> symbolMapping)
        {
            this.sources = sources;
            this.symbolMapping = symbolMapping;
        }

        public List<PlanNode> getSources()
        {
            return sources;
        }

        public ListMultimap<Symbol, Symbol> getSymbolMapping()
        {
            return symbolMapping;
        }
    }

    public static class PatternRecognitionComponents
    {
        private final Map<Symbol, Measure> measures;
        private final List<Symbol> measureOutputs;
        private final Set<IrLabel> skipToLabels;
        private final SkipToPosition skipToPosition;
        private final boolean initial;
        private final IrRowPattern pattern;
        private final Map<IrLabel, ExpressionAndValuePointers> variableDefinitions;

        public PatternRecognitionComponents(
                Map<Symbol, Measure> measures,
                List<Symbol> measureOutputs,
                Set<IrLabel> skipToLabels,
                SkipToPosition skipToPosition,
                boolean initial,
                IrRowPattern pattern,
                Map<IrLabel, ExpressionAndValuePointers> variableDefinitions)
        {
            this.measures = requireNonNull(measures, "measures is null");
            this.measureOutputs = requireNonNull(measureOutputs, "measureOutputs is null");
            this.skipToLabels = ImmutableSet.copyOf(skipToLabels);
            this.skipToPosition = requireNonNull(skipToPosition, "skipToPosition is null");
            this.initial = initial;
            this.pattern = requireNonNull(pattern, "pattern is null");
            this.variableDefinitions = requireNonNull(variableDefinitions, "variableDefinitions is null");
        }

        public Map<Symbol, Measure> getMeasures()
        {
            return measures;
        }

        public List<Symbol> getMeasureOutputs()
        {
            return measureOutputs;
        }

        public Set<IrLabel> getSkipToLabels()
        {
            return skipToLabels;
        }

        public SkipToPosition getSkipToPosition()
        {
            return skipToPosition;
        }

        public boolean isInitial()
        {
            return initial;
        }

        public IrRowPattern getPattern()
        {
            return pattern;
        }

        public Map<IrLabel, ExpressionAndValuePointers> getVariableDefinitions()
        {
            return variableDefinitions;
        }
    }
}
