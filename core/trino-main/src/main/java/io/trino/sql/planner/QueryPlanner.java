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
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableLayout;
import io.trino.metadata.TableMetadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.sql.ExpressionUtils;
import io.trino.sql.NodeUtils;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analysis.GroupingSetAnalysis;
import io.trino.sql.analyzer.Analysis.MergeAnalysis;
import io.trino.sql.analyzer.Analysis.ResolvedWindow;
import io.trino.sql.analyzer.Analysis.SelectExpression;
import io.trino.sql.analyzer.FieldId;
import io.trino.sql.analyzer.RelationType;
import io.trino.sql.planner.RelationPlanner.PatternRecognitionComponents;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.MergeProcessorNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableWriterNode.MergeParadigmAndTypes;
import io.trino.sql.planner.plan.TableWriterNode.MergeTarget;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FetchFirst;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.FunctionCall.NullTreatment;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.MeasureDefinition;
import io.trino.sql.tree.Merge;
import io.trino.sql.tree.MergeCase;
import io.trino.sql.tree.MergeDelete;
import io.trino.sql.tree.MergeInsert;
import io.trino.sql.tree.MergeUpdate;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Update;
import io.trino.sql.tree.VariableDefinition;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowOperation;
import io.trino.type.TypeCoercion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.getMaxRecursionDepth;
import static io.trino.SystemSessionProperties.isSkipRedundantSort;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_WINDOW_FRAME;
import static io.trino.spi.StandardErrorCode.MERGE_TARGET_ROW_MULTIPLE_MATCHES;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.INSERT_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.UPDATE_OPERATION_NUMBER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.NodeUtils.getSortItemsFromOrderBy;
import static io.trino.sql.analyzer.ExpressionAnalyzer.isNumericType;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.GroupingOperationRewriter.rewriteGroupingOperation;
import static io.trino.sql.planner.LogicalPlanner.failFunction;
import static io.trino.sql.planner.OrderingScheme.sortItemToSortOrder;
import static io.trino.sql.planner.PlanBuilder.newPlanBuilder;
import static io.trino.sql.planner.ScopeAware.scopeAwareKey;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.plan.AggregationNode.groupingSets;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.tree.IntervalLiteral.IntervalField.DAY;
import static io.trino.sql.tree.IntervalLiteral.IntervalField.YEAR;
import static io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE;
import static io.trino.sql.tree.WindowFrame.Type.GROUPS;
import static io.trino.sql.tree.WindowFrame.Type.RANGE;
import static io.trino.sql.tree.WindowFrame.Type.ROWS;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class QueryPlanner
{
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap;
    private final PlannerContext plannerContext;
    private final TypeCoercion typeCoercion;
    private final Session session;
    private final SubqueryPlanner subqueryPlanner;
    private final Optional<TranslationMap> outerContext;
    private final Map<NodeRef<Node>, RelationPlan> recursiveSubqueries;

    QueryPlanner(
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
        requireNonNull(session, "session is null");
        requireNonNull(outerContext, "outerContext is null");
        requireNonNull(recursiveSubqueries, "recursiveSubqueries is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
        this.lambdaDeclarationToSymbolMap = lambdaDeclarationToSymbolMap;
        this.plannerContext = plannerContext;
        this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
        this.session = session;
        this.outerContext = outerContext;
        this.subqueryPlanner = new SubqueryPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, typeCoercion, outerContext, session, recursiveSubqueries);
        this.recursiveSubqueries = recursiveSubqueries;
    }

    public RelationPlan plan(Query query)
    {
        PlanBuilder builder = planQueryBody(query);

        List<Expression> orderBy = analysis.getOrderByExpressions(query);
        builder = subqueryPlanner.handleSubqueries(builder, orderBy, analysis.getSubqueries(query));

        List<SelectExpression> selectExpressions = analysis.getSelectExpressions(query);
        List<Expression> outputs = selectExpressions.stream()
                .map(SelectExpression::getExpression)
                .collect(toImmutableList());
        builder = builder.appendProjections(Iterables.concat(orderBy, outputs), symbolAllocator, idAllocator);

        Optional<OrderingScheme> orderingScheme = orderingScheme(builder, query.getOrderBy(), analysis.getOrderByExpressions(query));
        builder = sort(builder, orderingScheme);
        builder = offset(builder, query.getOffset());
        builder = limit(builder, query.getLimit(), orderingScheme);
        builder = builder.appendProjections(outputs, symbolAllocator, idAllocator);

        return new RelationPlan(
                builder.getRoot(),
                analysis.getScope(query),
                computeOutputs(builder, outputs),
                outerContext);
    }

    public RelationPlan planExpand(Query query)
    {
        checkArgument(analysis.isExpandableQuery(query), "query is not registered as expandable");

        Union union = (Union) query.getQueryBody();
        ImmutableList.Builder<NodeAndMappings> recursionSteps = ImmutableList.builder();

        // plan anchor relation
        Relation anchorNode = union.getRelations().get(0);
        RelationPlan anchorPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, outerContext, session, recursiveSubqueries)
                .process(anchorNode, null);

        // prune anchor plan outputs to contain only the symbols exposed in the scope
        NodeAndMappings prunedAnchorPlan = pruneInvisibleFields(anchorPlan, idAllocator);

        // if the anchor plan has duplicate output symbols, add projection on top to make the symbols unique
        // This is necessary to successfully unroll recursion: the recursion step relation must follow
        // the same layout while it might not have duplicate outputs where the anchor plan did
        NodeAndMappings disambiguatedAnchorPlan = disambiguateOutputs(prunedAnchorPlan, symbolAllocator, idAllocator);
        anchorPlan = new RelationPlan(disambiguatedAnchorPlan.getNode(), analysis.getScope(query), disambiguatedAnchorPlan.getFields(), outerContext);

        recursionSteps.add(copy(anchorPlan.getRoot(), anchorPlan.getFieldMappings()));

        // plan recursion step
        Relation recursionStepRelation = union.getRelations().get(1);
        RelationPlan recursionStepPlan = new RelationPlanner(
                analysis,
                symbolAllocator,
                idAllocator,
                lambdaDeclarationToSymbolMap,
                plannerContext,
                outerContext,
                session,
                ImmutableMap.of(NodeRef.of(analysis.getRecursiveReference(query)), anchorPlan))
                .process(recursionStepRelation, null);

        // coerce recursion step outputs and prune them to contain only the symbols exposed in the scope
        NodeAndMappings coercedRecursionStep;
        List<Type> types = analysis.getRelationCoercion(recursionStepRelation);
        if (types == null) {
            coercedRecursionStep = pruneInvisibleFields(recursionStepPlan, idAllocator);
        }
        else {
            coercedRecursionStep = coerce(recursionStepPlan, types, symbolAllocator, idAllocator);
        }

        NodeAndMappings replacementSpot = new NodeAndMappings(anchorPlan.getRoot(), anchorPlan.getFieldMappings());
        PlanNode recursionStep = coercedRecursionStep.getNode();
        List<Symbol> mappings = coercedRecursionStep.getFields();

        // unroll recursion
        int maxRecursionDepth = getMaxRecursionDepth(session);
        for (int i = 0; i < maxRecursionDepth; i++) {
            recursionSteps.add(copy(recursionStep, mappings));
            NodeAndMappings replacement = copy(recursionStep, mappings);

            // if the recursion step plan has duplicate output symbols, add projection on top to make the symbols unique
            // This is necessary to successfully unroll recursion: the relation on the next recursion step must follow
            // the same layout while it might not have duplicate outputs where the plan for this step did
            replacement = disambiguateOutputs(replacement, symbolAllocator, idAllocator);
            recursionStep = replace(recursionStep, replacementSpot, replacement);
            replacementSpot = replacement;
        }

        // after the last recursion step, check if the recursion converged. the last step is expected to return empty result
        // 1. append window to count rows
        NodeAndMappings checkConvergenceStep = copy(recursionStep, mappings);
        Symbol countSymbol = symbolAllocator.newSymbol("count", BIGINT);
        ResolvedFunction function = plannerContext.getMetadata().resolveFunction(session, QualifiedName.of("count"), ImmutableList.of());
        WindowNode.Function countFunction = new WindowNode.Function(function, ImmutableList.of(), DEFAULT_FRAME, false);

        WindowNode windowNode = new WindowNode(
                idAllocator.getNextId(),
                checkConvergenceStep.getNode(),
                new DataOrganizationSpecification(ImmutableList.of(), Optional.empty()),
                ImmutableMap.of(countSymbol, countFunction),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        // 2. append filter to fail on non-empty result
        String recursionLimitExceededMessage = format("Recursion depth limit exceeded (%s). Use 'max_recursion_depth' session property to modify the limit.", maxRecursionDepth);
        Expression predicate = new IfExpression(
                new ComparisonExpression(
                        GREATER_THAN_OR_EQUAL,
                        countSymbol.toSymbolReference(),
                        new GenericLiteral("BIGINT", "0")),
                new Cast(
                        failFunction(plannerContext.getMetadata(), session, NOT_SUPPORTED, recursionLimitExceededMessage),
                        toSqlType(BOOLEAN)),
                TRUE_LITERAL);
        FilterNode filterNode = new FilterNode(idAllocator.getNextId(), windowNode, predicate);

        recursionSteps.add(new NodeAndMappings(filterNode, checkConvergenceStep.getFields()));

        // union all the recursion steps
        List<NodeAndMappings> recursionStepsToUnion = recursionSteps.build();

        List<Symbol> unionOutputSymbols = anchorPlan.getFieldMappings().stream()
                .map(symbol -> symbolAllocator.newSymbol(symbol, "_expanded"))
                .collect(toImmutableList());

        ImmutableListMultimap.Builder<Symbol, Symbol> unionSymbolMapping = ImmutableListMultimap.builder();
        for (NodeAndMappings plan : recursionStepsToUnion) {
            for (int i = 0; i < unionOutputSymbols.size(); i++) {
                unionSymbolMapping.put(unionOutputSymbols.get(i), plan.getFields().get(i));
            }
        }

        List<PlanNode> nodesToUnion = recursionStepsToUnion.stream()
                .map(NodeAndMappings::getNode)
                .collect(toImmutableList());

        PlanNode result = new UnionNode(idAllocator.getNextId(), nodesToUnion, unionSymbolMapping.build(), unionOutputSymbols);

        if (union.isDistinct()) {
            result = singleAggregation(
                    idAllocator.getNextId(),
                    result,
                    ImmutableMap.of(),
                    singleGroupingSet(result.getOutputSymbols()));
        }

        return new RelationPlan(result, anchorPlan.getScope(), unionOutputSymbols, outerContext);
    }

    // Return a copy of the plan and remapped field mappings. In the copied plan:
    // - all PlanNodeIds are replaced with new values,
    // - all symbols are replaced with new symbols.
    // Copying the plan might reorder symbols. The returned field mappings keep the original
    // order and might be used to identify the original output symbols with their copies.
    private NodeAndMappings copy(PlanNode plan, List<Symbol> fields)
    {
        return PlanCopier.copyPlan(plan, fields, plannerContext.getMetadata(), symbolAllocator, idAllocator);
    }

    private PlanNode replace(PlanNode plan, NodeAndMappings replacementSpot, NodeAndMappings replacement)
    {
        checkArgument(
                replacementSpot.getFields().size() == replacement.getFields().size(),
                "mismatching outputs in replacement, expected: %s, got: %s",
                replacementSpot.getFields().size(),
                replacement.getFields().size());

        return SimplePlanRewriter.rewriteWith(new SimplePlanRewriter<Void>()
        {
            @Override
            protected PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
            {
                return node.replaceChildren(node.getSources().stream()
                        .map(child -> {
                            if (child == replacementSpot.getNode()) {
                                // add projection to adjust symbols
                                Assignments.Builder assignments = Assignments.builder();
                                for (int i = 0; i < replacementSpot.getFields().size(); i++) {
                                    assignments.put(replacementSpot.getFields().get(i), replacement.getFields().get(i).toSymbolReference());
                                }
                                return new ProjectNode(idAllocator.getNextId(), replacement.getNode(), assignments.build());
                            }
                            return context.rewrite(child);
                        })
                        .collect(toImmutableList()));
            }
        }, plan, null);
    }

    public RelationPlan plan(QuerySpecification node)
    {
        PlanBuilder builder = planFrom(node);

        builder = filter(builder, analysis.getWhere(node), node);
        builder = aggregate(builder, node);
        builder = filter(builder, analysis.getHaving(node), node);
        builder = planWindowFunctions(node, builder, ImmutableList.copyOf(analysis.getWindowFunctions(node)));
        builder = planWindowMeasures(node, builder, ImmutableList.copyOf(analysis.getWindowMeasures(node)));

        List<SelectExpression> selectExpressions = analysis.getSelectExpressions(node);
        List<Expression> expressions = selectExpressions.stream()
                .map(SelectExpression::getExpression)
                .collect(toImmutableList());
        builder = subqueryPlanner.handleSubqueries(builder, expressions, analysis.getSubqueries(node));

        if (hasExpressionsToUnfold(selectExpressions)) {
            // pre-project the folded expressions to preserve any non-deterministic semantics of functions that might be referenced
            builder = builder.appendProjections(expressions, symbolAllocator, idAllocator);
        }

        List<Expression> outputs = outputExpressions(selectExpressions);
        if (node.getOrderBy().isPresent()) {
            // ORDER BY requires outputs of SELECT to be visible.
            // For queries with aggregation, it also requires grouping keys and translated aggregations.
            if (analysis.isAggregation(node)) {
                // Add projections for aggregations required by ORDER BY. After this step, grouping keys and translated
                // aggregations are visible.
                List<Expression> orderByAggregates = analysis.getOrderByAggregates(node.getOrderBy().get());
                builder = builder.appendProjections(orderByAggregates, symbolAllocator, idAllocator);
            }

            // Add projections for the outputs of SELECT, but stack them on top of the ones from the FROM clause so both are visible
            // when resolving the ORDER BY clause.
            builder = builder.appendProjections(outputs, symbolAllocator, idAllocator);

            // The new scope is the composite of the fields from the FROM and SELECT clause (local nested scopes). Fields from the bottom of
            // the scope stack need to be placed first to match the expected layout for nested scopes.
            List<Symbol> newFields = new ArrayList<>();
            newFields.addAll(builder.getTranslations().getFieldSymbols());

            outputs.stream()
                    .map(builder::translate)
                    .forEach(newFields::add);

            builder = builder.withScope(analysis.getScope(node.getOrderBy().get()), newFields);

            builder = planWindowFunctions(node, builder, ImmutableList.copyOf(analysis.getOrderByWindowFunctions(node.getOrderBy().get())));
            builder = planWindowMeasures(node, builder, ImmutableList.copyOf(analysis.getOrderByWindowMeasures(node.getOrderBy().get())));
        }

        List<Expression> orderBy = analysis.getOrderByExpressions(node);
        builder = subqueryPlanner.handleSubqueries(builder, orderBy, analysis.getSubqueries(node));
        builder = builder.appendProjections(Iterables.concat(orderBy, outputs), symbolAllocator, idAllocator);

        builder = distinct(builder, node, outputs);
        Optional<OrderingScheme> orderingScheme = orderingScheme(builder, node.getOrderBy(), analysis.getOrderByExpressions(node));
        builder = sort(builder, orderingScheme);
        builder = offset(builder, node.getOffset());
        builder = limit(builder, node.getLimit(), orderingScheme);
        builder = builder.appendProjections(outputs, symbolAllocator, idAllocator);

        return new RelationPlan(
                builder.getRoot(),
                analysis.getScope(node),
                computeOutputs(builder, outputs),
                outerContext);
    }

    private static boolean hasExpressionsToUnfold(List<SelectExpression> selectExpressions)
    {
        return selectExpressions.stream()
                .map(SelectExpression::getUnfoldedExpressions)
                .anyMatch(Optional::isPresent);
    }

    private static List<Expression> outputExpressions(List<SelectExpression> selectExpressions)
    {
        ImmutableList.Builder<Expression> result = ImmutableList.builder();
        for (SelectExpression selectExpression : selectExpressions) {
            if (selectExpression.getUnfoldedExpressions().isPresent()) {
                result.addAll(selectExpression.getUnfoldedExpressions().get());
            }
            else {
                result.add(selectExpression.getExpression());
            }
        }
        return result.build();
    }

    public PlanNode plan(Delete node)
    {
        Table table = node.getTable();
        TableHandle handle = analysis.getTableHandle(table);

        // create table scan
        RelationPlan relationPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, outerContext, session, recursiveSubqueries)
                .process(table, null);

        PlanBuilder builder = newPlanBuilder(relationPlan, analysis, lambdaDeclarationToSymbolMap, session, plannerContext);
        if (node.getWhere().isPresent()) {
            builder = filter(builder, node.getWhere().get(), node);
        }

        FieldReference reference = analysis.getRowIdField(table);
        Symbol rowIdSymbol = builder.translate(reference);
        List<Symbol> outputs = ImmutableList.of(
                symbolAllocator.newSymbol("partialrows", BIGINT),
                symbolAllocator.newSymbol("fragment", VARBINARY));

        TableMetadata tableMetadata = plannerContext.getMetadata().getTableMetadata(session, handle);
        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();
        ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
        tableMetadata.getMetadata().getColumns().stream()
                .filter(column -> !column.isHidden())
                .forEach(columnMetadata -> {
                    typeBuilder.add(columnMetadata.getType());
                    namesBuilder.add(columnMetadata.getName());
                });

        Type rowIdType = analysis.getType(analysis.getRowIdField(table));
        MergeParadigmAndTypes paradigmAndTypes = new MergeParadigmAndTypes(Optional.empty(), typeBuilder.build(), namesBuilder.build(), rowIdType);
        MergeAnalysis mergeAnalysis = analysis.getMergeAnalysis().orElseThrow(() -> new IllegalArgumentException("Didn't find mergeAnalysis in analysis"));

        // Create a ProjectNode with the references
        Assignments.Builder assignmentsBuilder = new Assignments.Builder();
        ImmutableList.Builder<Symbol> columnSymbolsBuilder = ImmutableList.builder();
        for (ColumnHandle columnHandle : mergeAnalysis.getDataColumnHandles()) {
            int fieldIndex = requireNonNull(mergeAnalysis.getColumnHandleFieldNumbers().get(columnHandle), "Could not find field number for column handle");
            Symbol symbol = relationPlan.getFieldMappings().get(fieldIndex);
            columnSymbolsBuilder.add(symbol);
            if (mergeAnalysis.getRedistributionColumnHandles().contains(columnHandle)) {
                assignmentsBuilder.put(symbol, symbol.toSymbolReference());
            }
            else {
                assignmentsBuilder.put(symbol, new NullLiteral());
            }
        }
        List<Symbol> columnSymbols = columnSymbolsBuilder.build();
        Symbol operationSymbol = symbolAllocator.newSymbol("operation", TINYINT);
        assignmentsBuilder.put(operationSymbol, new GenericLiteral("TINYINT", String.valueOf(DELETE_OPERATION_NUMBER)));
        Symbol projectedRowIdSymbol = symbolAllocator.newSymbol(rowIdSymbol.getName(), rowIdType);
        assignmentsBuilder.put(projectedRowIdSymbol, rowIdSymbol.toSymbolReference());
        assignmentsBuilder.put(symbolAllocator.newSymbol("insert_from_update", TINYINT), new GenericLiteral("TINYINT", "0"));
        Assignments assignments = assignmentsBuilder.build();
        ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), builder.getRoot(), assignments);

        Optional<PartitioningScheme> partitioningScheme = createMergePartitioningScheme(
                mergeAnalysis.getInsertLayout(),
                columnSymbols,
                mergeAnalysis.getInsertPartitioningArgumentIndexes(),
                mergeAnalysis.getUpdateLayout(),
                projectedRowIdSymbol,
                operationSymbol);

        return new MergeWriterNode(
                idAllocator.getNextId(),
                projectNode,
                new MergeTarget(
                        handle,
                        Optional.empty(),
                        tableMetadata.getTable(),
                        paradigmAndTypes),
                projectNode.getOutputSymbols(),
                partitioningScheme,
                outputs);
    }

    public PlanNode plan(Update node)
    {
        MergeAnalysis mergeAnalysis = analysis.getMergeAnalysis().orElseThrow();
        Table table = mergeAnalysis.getTargetTable();

        List<ColumnSchema> dataColumnSchemas = mergeAnalysis.getDataColumnSchemas();
        List<ColumnHandle> dataColumnHandles = mergeAnalysis.getDataColumnHandles();
        List<ColumnHandle> updatedColumnHandles = mergeAnalysis.getMergeCaseColumnHandles().get(0);

        ImmutableMap.Builder<String, ColumnHandle> nameToHandleBuilder = ImmutableMap.builder();
        for (int columnIndex = 0; columnIndex < mergeAnalysis.getDataColumnSchemas().size(); columnIndex++) {
            nameToHandleBuilder.put(dataColumnSchemas.get(columnIndex).getName(), dataColumnHandles.get(columnIndex));
        }
        Map<String, ColumnHandle> nameToHandle = nameToHandleBuilder.buildOrThrow();

        Expression[] orderedColumnValuesArray = new Expression[updatedColumnHandles.size()];
        node.getAssignments().forEach(assignment -> {
            String name = assignment.getName().getValue();
            ColumnHandle handle = nameToHandle.get(name);
            int index = updatedColumnHandles.indexOf(handle);
            if (index >= 0) {
                orderedColumnValuesArray[index] = assignment.getValue();
            }
        });
        List<Expression> orderedColumnValues = Arrays.stream(orderedColumnValuesArray).toList();

        // create table scan
        RelationPlan relationPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, outerContext, session, recursiveSubqueries)
                .process(table, null);

        PlanBuilder subPlanBuilder = newPlanBuilder(relationPlan, analysis, lambdaDeclarationToSymbolMap, session, plannerContext);

        // Add the WHERE clause, if any
        if (node.getWhere().isPresent()) {
            subPlanBuilder = filter(subPlanBuilder, node.getWhere().get(), node);
        }

        // Handle subqueries in the update SET expression values
        subPlanBuilder = subqueryPlanner.handleSubqueries(subPlanBuilder, orderedColumnValues, analysis.getSubqueries(node));

        // Build the merge rowblock, containing
        // All data columns in table order
        // The boolean present field, always TRUE for update
        // The tinyint operation number, always UPDATE_OPERATION_NUMBER for update
        // The integer merge case number, always 0 for update
        Metadata metadata = plannerContext.getMetadata();
        ImmutableList.Builder<Expression> rowBuilder = ImmutableList.builder();

        // Add column values to the rowBuilder - - the SET expression value for updated
        // columns, and the existing column value for non-updated columns
        for (int columnIndex = 0; columnIndex < mergeAnalysis.getDataColumnHandles().size(); columnIndex++) {
            ColumnHandle dataColumnHandle = mergeAnalysis.getDataColumnHandles().get(columnIndex);
            ColumnSchema columnSchema = mergeAnalysis.getDataColumnSchemas().get(columnIndex);
            int index = updatedColumnHandles.indexOf(dataColumnHandle);
            if (index >= 0) {
                // This column is updated...
                Expression original = orderedColumnValues.get(index);
                Expression setExpression = coerceIfNecessary(analysis, original, original);
                subPlanBuilder = subqueryPlanner.handleSubqueries(subPlanBuilder, setExpression, analysis.getSubqueries(node));
                Expression rewritten = subPlanBuilder.rewrite(setExpression);

                // If the updated column is non-null, check that the value is not null
                if (mergeAnalysis.getNonNullableColumnHandles().contains(dataColumnHandle)) {
                    String columnName = columnSchema.getName();
                    rewritten = new CoalesceExpression(rewritten, new Cast(failFunction(metadata, session, INVALID_ARGUMENTS, "NULL value not allowed for NOT NULL column: " + columnName), toSqlType(columnSchema.getType())));
                }
                rowBuilder.add(rewritten);
            }
            else {
                // Get the non-updated column value from the table
                Integer fieldNumber = requireNonNull(mergeAnalysis.getColumnHandleFieldNumbers().get(dataColumnHandle), "Field number for ColumnHandle is null");
                rowBuilder.add(relationPlan.getFieldMappings().get(fieldNumber).toSymbolReference());
            }
        }

        // Add the "present" field
        rowBuilder.add(new GenericLiteral("BOOLEAN", "TRUE"));

        // Add the operation number
        rowBuilder.add(new GenericLiteral("TINYINT", String.valueOf(UPDATE_OPERATION_NUMBER)));

        // Add the merge case number
        rowBuilder.add(new GenericLiteral("INTEGER", "0"));

        // Finally, the merge row is complete
        Expression mergeRow = new Row(rowBuilder.build());

        // Build the page, containing:
        // The write redistribution columns if any
        // For partitioned or bucketed tables, a long hash value column.
        // The rowId column for the row to be updated
        // The merge case RowBlock
        // The integer case number block, always 0 for update
        // The byte is_distinct block, always true for update
        FieldReference rowIdReference = analysis.getRowIdField(mergeAnalysis.getTargetTable());
        Symbol rowIdSymbol = relationPlan.getFieldMappings().get(rowIdReference.getFieldIndex());
        Symbol mergeRowSymbol = symbolAllocator.newSymbol("merge_row", mergeAnalysis.getMergeRowType());
        Symbol caseNumberSymbol = symbolAllocator.newSymbol("case_number", INTEGER);
        Symbol isDistinctSymbol = symbolAllocator.newSymbol("is_distinct", BOOLEAN);

        Assignments.Builder projectionAssignmentsBuilder = Assignments.builder();

        // Copy the redistribution columns
        for (ColumnHandle column : mergeAnalysis.getRedistributionColumnHandles()) {
            int fieldIndex = requireNonNull(mergeAnalysis.getColumnHandleFieldNumbers().get(column), "Could not find fieldIndex for redistribution column");
            Symbol symbol = relationPlan.getFieldMappings().get(fieldIndex);
            projectionAssignmentsBuilder.put(symbol, symbol.toSymbolReference());
        }

        // Add the rest of the page columns: rowId, merge row, case number and is_distinct
        projectionAssignmentsBuilder.put(rowIdSymbol, rowIdSymbol.toSymbolReference());
        projectionAssignmentsBuilder.put(mergeRowSymbol, mergeRow);
        projectionAssignmentsBuilder.put(caseNumberSymbol, new GenericLiteral("INTEGER", "0"));
        projectionAssignmentsBuilder.put(isDistinctSymbol, TRUE_LITERAL);

        ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), subPlanBuilder.getRoot(), projectionAssignmentsBuilder.build());

        return createMergePipeline(table, relationPlan, projectNode, rowIdSymbol, mergeRowSymbol);
    }

    public MergeWriterNode plan(Merge merge)
    {
        MergeAnalysis mergeAnalysis = analysis.getMergeAnalysis().orElseThrow(() -> new IllegalArgumentException("analysis.getMergeAnalysis() isn't present"));

        List<List<ColumnHandle>> mergeCaseColumnsHandles = mergeAnalysis.getMergeCaseColumnHandles();

        // Make the plan for the merge target table scan
        RelationPlan targetTablePlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, outerContext, session, recursiveSubqueries)
                .process(merge.getTarget());

        // Assign a unique id to every target table row
        Symbol uniqueIdSymbol = symbolAllocator.newSymbol("unique_id", BIGINT);
        RelationPlan planWithUniqueId = new RelationPlan(
                new AssignUniqueId(idAllocator.getNextId(), targetTablePlan.getRoot(), uniqueIdSymbol),
                mergeAnalysis.getTargetTableScope(),
                targetTablePlan.getFieldMappings(),
                outerContext);

        // Project the "present" column
        Assignments.Builder projections = Assignments.builder();
        projections.putIdentities(planWithUniqueId.getRoot().getOutputSymbols());

        Symbol presentColumn = symbolAllocator.newSymbol("present", BOOLEAN);
        projections.put(presentColumn, TRUE_LITERAL);

        RelationPlan planWithPresentColumn = new RelationPlan(
                new ProjectNode(idAllocator.getNextId(), planWithUniqueId.getRoot(), projections.build()),
                mergeAnalysis.getTargetTableScope(),
                planWithUniqueId.getFieldMappings(),
                outerContext);

        RelationPlan source = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, outerContext, session, recursiveSubqueries)
                .process(merge.getSource());

        RelationPlan joinPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, outerContext, session, recursiveSubqueries)
                .planJoin(coerceIfNecessary(analysis, merge.getPredicate(), merge.getPredicate()), Join.Type.RIGHT, mergeAnalysis.getJoinScope(), planWithPresentColumn, source, analysis.getSubqueries(merge));

        PlanBuilder subPlan = newPlanBuilder(joinPlan, analysis, lambdaDeclarationToSymbolMap, session, plannerContext);

        // Build the SearchedCaseExpression that creates the project merge_row
        Metadata metadata = plannerContext.getMetadata();
        List<ColumnSchema> dataColumnSchemas = mergeAnalysis.getDataColumnSchemas();
        ImmutableList.Builder<WhenClause> whenClauses = ImmutableList.builder();
        Set<ColumnHandle> nonNullableColumnHandles = mergeAnalysis.getNonNullableColumnHandles();
        for (int caseNumber = 0; caseNumber < merge.getMergeCases().size(); caseNumber++) {
            MergeCase mergeCase = merge.getMergeCases().get(caseNumber);

            Optional<Expression> casePredicate = Optional.empty();
            if (mergeCase.getExpression().isPresent()) {
                Expression original = mergeCase.getExpression().get();
                Expression predicate = coerceIfNecessary(analysis, original, original);
                casePredicate = Optional.of(predicate);
                subPlan = subqueryPlanner.handleSubqueries(subPlan, predicate, analysis.getSubqueries(merge));
            }

            ImmutableList.Builder<Expression> rowBuilder = ImmutableList.builder();
            List<ColumnHandle> mergeCaseSetColumns = mergeCaseColumnsHandles.get(caseNumber);
            for (ColumnHandle dataColumnHandle : mergeAnalysis.getDataColumnHandles()) {
                int index = mergeCaseSetColumns.indexOf(dataColumnHandle);
                if (index >= 0) {
                    Expression setExpression = mergeCase.getSetExpressions().get(index);
                    subPlan = subqueryPlanner.handleSubqueries(subPlan, setExpression, analysis.getSubqueries(merge));
                    Expression rewritten = subPlan.rewrite(setExpression);
                    rewritten = coerceIfNecessary(analysis, setExpression, rewritten);
                    if (nonNullableColumnHandles.contains(dataColumnHandle)) {
                        int fieldIndex = requireNonNull(mergeAnalysis.getColumnHandleFieldNumbers().get(dataColumnHandle), "Could not find fieldIndex for non nullable column");
                        ColumnSchema columnSchema = dataColumnSchemas.get(fieldIndex);
                        String columnName = columnSchema.getName();
                        rewritten = new CoalesceExpression(rewritten, new Cast(failFunction(metadata, session, INVALID_ARGUMENTS, "Assigning NULL to non-null MERGE target table column " + columnName), toSqlType(columnSchema.getType())));
                    }
                    rowBuilder.add(rewritten);
                }
                else {
                    Integer fieldNumber = requireNonNull(mergeAnalysis.getColumnHandleFieldNumbers().get(dataColumnHandle), "Field number for ColumnHandle is null");
                    rowBuilder.add(planWithPresentColumn.getFieldMappings().get(fieldNumber).toSymbolReference());
                }
            }

            // Build the match condition for the MERGE case

            // Add a boolean column which is true if a target table row was matched
            rowBuilder.add(new IsNotNullPredicate(presentColumn.toSymbolReference()));

            // Add the operation number
            rowBuilder.add(new GenericLiteral("TINYINT", String.valueOf(getMergeCaseOperationNumber(mergeCase))));

            // Add the merge case number, needed by MarkDistinct
            rowBuilder.add(new GenericLiteral("INTEGER", String.valueOf(caseNumber)));

            Optional<Expression> rewritten = casePredicate.map(subPlan::rewrite);
            Expression condition = presentColumn.toSymbolReference();
            if (mergeCase instanceof MergeInsert) {
                condition = new IsNullPredicate(presentColumn.toSymbolReference());
            }

            if (rewritten.isPresent()) {
                condition = ExpressionUtils.and(condition, rewritten.get());
            }

            whenClauses.add(new WhenClause(condition, new Row(rowBuilder.build())));
        }

        // Build the "else" clause for the SearchedCaseExpression
        ImmutableList.Builder<Expression> rowBuilder = ImmutableList.builder();
        dataColumnSchemas.forEach(columnSchema ->
                rowBuilder.add(new Cast(new NullLiteral(), toSqlType(columnSchema.getType()))));
        rowBuilder.add(new IsNotNullPredicate(presentColumn.toSymbolReference()));
        // The operation number
        rowBuilder.add(new GenericLiteral("TINYINT", "-1"));
        // The case number
        rowBuilder.add(new GenericLiteral("INTEGER", "-1"));

        SearchedCaseExpression caseExpression = new SearchedCaseExpression(whenClauses.build(), Optional.of(new Row(rowBuilder.build())));

        FieldReference rowIdReference = analysis.getRowIdField(mergeAnalysis.getTargetTable());
        Symbol rowIdSymbol = planWithPresentColumn.getFieldMappings().get(rowIdReference.getFieldIndex());
        Symbol mergeRowSymbol = symbolAllocator.newSymbol("merge_row", mergeAnalysis.getMergeRowType());
        Symbol caseNumberSymbol = symbolAllocator.newSymbol("case_number", INTEGER);

        // Project the partition symbols, the merge_row, the rowId, and the unique_id symbol
        Assignments.Builder projectionAssignmentsBuilder = Assignments.builder();
        for (ColumnHandle column : mergeAnalysis.getRedistributionColumnHandles()) {
            int fieldIndex = requireNonNull(mergeAnalysis.getColumnHandleFieldNumbers().get(column), "Could not find fieldIndex for redistribution column");
            Symbol symbol = planWithPresentColumn.getFieldMappings().get(fieldIndex);
            projectionAssignmentsBuilder.put(symbol, symbol.toSymbolReference());
        }
        projectionAssignmentsBuilder.put(uniqueIdSymbol, uniqueIdSymbol.toSymbolReference());
        projectionAssignmentsBuilder.put(rowIdSymbol, rowIdSymbol.toSymbolReference());
        projectionAssignmentsBuilder.put(mergeRowSymbol, caseExpression);

        ProjectNode subPlanProject = new ProjectNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                projectionAssignmentsBuilder.build());

        // Now add a column for the case_number, gotten from the merge_row
        ProjectNode project = new ProjectNode(
                idAllocator.getNextId(),
                subPlanProject,
                Assignments.builder()
                        .putIdentities(subPlanProject.getOutputSymbols())
                        .put(caseNumberSymbol, new SubscriptExpression(mergeRowSymbol.toSymbolReference(), new LongLiteral(Long.toString(mergeAnalysis.getMergeRowType().getFields().size()))))
                        .build());

        // Mark distinct combinations of the unique_id value and the case_number
        Symbol isDistinctSymbol = symbolAllocator.newSymbol("is_distinct", BOOLEAN);
        MarkDistinctNode markDistinctNode = new MarkDistinctNode(idAllocator.getNextId(), project, isDistinctSymbol, ImmutableList.of(uniqueIdSymbol, caseNumberSymbol), Optional.empty());

        // Raise an error if unique_id symbol is non-null and the unique_id/case_number combination was not distinct
        Expression filter = new IfExpression(
                LogicalExpression.and(
                        new NotExpression(isDistinctSymbol.toSymbolReference()),
                        new IsNotNullPredicate(uniqueIdSymbol.toSymbolReference())),
                new Cast(
                        failFunction(metadata, session, MERGE_TARGET_ROW_MULTIPLE_MATCHES, "One MERGE target table row matched more than one source row"),
                        toSqlType(BOOLEAN)),
                TRUE_LITERAL);

        FilterNode filterNode = new FilterNode(idAllocator.getNextId(), markDistinctNode, filter);

        return createMergePipeline(merge.getTargetTable(), planWithPresentColumn, filterNode, rowIdSymbol, mergeRowSymbol);
    }

    private MergeWriterNode createMergePipeline(Table table, RelationPlan relationPlan, PlanNode planNode, Symbol rowIdSymbol, Symbol mergeRowSymbol)
    {
        TableHandle handle = analysis.getTableHandle(table);
        MergeAnalysis mergeAnalysis = analysis.getMergeAnalysis().orElseThrow();

        Metadata metadata = plannerContext.getMetadata();
        RowChangeParadigm paradigm = metadata.getRowChangeParadigm(session, handle);
        Type rowIdType = analysis.getType(analysis.getRowIdField(table));
        ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
        ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();
        mergeAnalysis.getDataColumnSchemas().stream()
                .filter(columnSchema -> !columnSchema.isHidden())
                .forEach(columnSchema -> {
                    typesBuilder.add(columnSchema.getType());
                    columnNamesBuilder.add(columnSchema.getName());
                });
        MergeParadigmAndTypes mergeParadigmAndTypes = new MergeParadigmAndTypes(Optional.of(paradigm), typesBuilder.build(), columnNamesBuilder.build(), rowIdType);
        MergeTarget mergeTarget = new MergeTarget(handle, Optional.empty(), metadata.getTableName(session, handle).getSchemaTableName(), mergeParadigmAndTypes);

        ImmutableList.Builder<Symbol> columnSymbolsBuilder = ImmutableList.builder();
        for (ColumnHandle columnHandle : mergeAnalysis.getDataColumnHandles()) {
            int fieldIndex = requireNonNull(mergeAnalysis.getColumnHandleFieldNumbers().get(columnHandle), "Could not find field number for column handle");
            columnSymbolsBuilder.add(relationPlan.getFieldMappings().get(fieldIndex));
        }
        List<Symbol> columnSymbols = columnSymbolsBuilder.build();
        ImmutableList.Builder<Symbol> redistributionSymbolsBuilder = ImmutableList.builder();
        for (ColumnHandle columnHandle : mergeAnalysis.getRedistributionColumnHandles()) {
            int fieldIndex = requireNonNull(mergeAnalysis.getColumnHandleFieldNumbers().get(columnHandle), "Could not find field number for column handle");
            redistributionSymbolsBuilder.add(relationPlan.getFieldMappings().get(fieldIndex));
        }

        Symbol operationSymbol = symbolAllocator.newSymbol("operation", TINYINT);
        Symbol insertFromUpdateSymbol = symbolAllocator.newSymbol("insert_from_update", TINYINT);

        List<Symbol> projectedSymbols = ImmutableList.<Symbol>builder()
                .addAll(columnSymbols)
                .add(operationSymbol)
                .add(rowIdSymbol)
                .add(insertFromUpdateSymbol)
                .build();

        MergeProcessorNode mergeProcessorNode = new MergeProcessorNode(
                idAllocator.getNextId(),
                planNode,
                mergeTarget,
                rowIdSymbol,
                mergeRowSymbol,
                columnSymbols,
                redistributionSymbolsBuilder.build(),
                projectedSymbols);

        Optional<PartitioningScheme> partitioningScheme = createMergePartitioningScheme(
                mergeAnalysis.getInsertLayout(),
                columnSymbols,
                mergeAnalysis.getInsertPartitioningArgumentIndexes(),
                mergeAnalysis.getUpdateLayout(),
                rowIdSymbol,
                operationSymbol);

        List<Symbol> outputs = ImmutableList.of(
                symbolAllocator.newSymbol("partialrows", BIGINT),
                symbolAllocator.newSymbol("fragment", VARBINARY));

        return new MergeWriterNode(
                idAllocator.getNextId(),
                mergeProcessorNode,
                mergeTarget,
                projectedSymbols,
                partitioningScheme,
                outputs);
    }

    private static int getMergeCaseOperationNumber(MergeCase mergeCase)
    {
        if (mergeCase instanceof MergeInsert) {
            return INSERT_OPERATION_NUMBER;
        }
        if (mergeCase instanceof MergeUpdate) {
            return UPDATE_OPERATION_NUMBER;
        }
        if (mergeCase instanceof MergeDelete) {
            return DELETE_OPERATION_NUMBER;
        }
        throw new IllegalArgumentException("Unrecognized MergeCase: " + mergeCase);
    }

    public static Optional<PartitioningScheme> createMergePartitioningScheme(
            Optional<TableLayout> insertLayout,
            List<Symbol> symbols,
            List<Integer> insertPartitioningArgumentIndexes,
            Optional<PartitioningHandle> updateLayout,
            Symbol rowIdSymbol,
            Symbol operationSymbol)
    {
        if (insertLayout.isEmpty() && updateLayout.isEmpty()) {
            return Optional.empty();
        }

        Optional<PartitioningScheme> insertPartitioning = insertLayout.map(layout -> {
            List<Symbol> arguments = insertPartitioningArgumentIndexes.stream()
                    .map(symbols::get)
                    .collect(toImmutableList());

            return layout.getPartitioning()
                    .map(handle -> new PartitioningScheme(Partitioning.create(handle, arguments), symbols))
                    // empty connector partitioning handle means evenly partitioning on partitioning columns
                    .orElseGet(() -> new PartitioningScheme(Partitioning.create(FIXED_HASH_DISTRIBUTION, arguments), symbols));
        });

        Optional<PartitioningScheme> updatePartitioning = updateLayout.map(handle ->
                new PartitioningScheme(Partitioning.create(handle, ImmutableList.of(rowIdSymbol)), ImmutableList.of(rowIdSymbol)));

        PartitioningHandle partitioningHandle = new PartitioningHandle(
                Optional.empty(),
                Optional.empty(),
                new MergePartitioningHandle(insertPartitioning, updatePartitioning));

        List<Symbol> combinedSymbols = new ArrayList<>();
        combinedSymbols.add(operationSymbol);
        insertPartitioning.ifPresent(scheme -> combinedSymbols.addAll(partitioningSymbols(scheme)));
        updatePartitioning.ifPresent(scheme -> combinedSymbols.addAll(partitioningSymbols(scheme)));

        return Optional.of(new PartitioningScheme(Partitioning.create(partitioningHandle, combinedSymbols), combinedSymbols));
    }

    private static List<Symbol> partitioningSymbols(PartitioningScheme scheme)
    {
        return scheme.getPartitioning().getArguments().stream()
                .map(Partitioning.ArgumentBinding::getColumn)
                .collect(toImmutableList());
    }

    private static List<Symbol> computeOutputs(PlanBuilder builder, List<Expression> outputExpressions)
    {
        ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
        for (Expression expression : outputExpressions) {
            outputSymbols.add(builder.translate(expression));
        }
        return outputSymbols.build();
    }

    private PlanBuilder planQueryBody(Query query)
    {
        RelationPlan relationPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, outerContext, session, recursiveSubqueries)
                .process(query.getQueryBody(), null);

        return newPlanBuilder(relationPlan, analysis, lambdaDeclarationToSymbolMap, session, plannerContext);
    }

    private PlanBuilder planFrom(QuerySpecification node)
    {
        if (node.getFrom().isPresent()) {
            RelationPlan relationPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, outerContext, session, recursiveSubqueries)
                    .process(node.getFrom().get(), null);
            return newPlanBuilder(relationPlan, analysis, lambdaDeclarationToSymbolMap, session, plannerContext);
        }

        return new PlanBuilder(
                new TranslationMap(outerContext, analysis.getImplicitFromScope(node), analysis, lambdaDeclarationToSymbolMap, ImmutableList.of(), session, plannerContext),
                new ValuesNode(idAllocator.getNextId(), 1));
    }

    private PlanBuilder filter(PlanBuilder subPlan, Expression predicate, Node node)
    {
        if (predicate == null) {
            return subPlan;
        }

        subPlan = subqueryPlanner.handleSubqueries(subPlan, predicate, analysis.getSubqueries(node));

        return subPlan.withNewRoot(new FilterNode(idAllocator.getNextId(), subPlan.getRoot(), coerceIfNecessary(analysis, predicate, subPlan.rewrite(predicate))));
    }

    private PlanBuilder aggregate(PlanBuilder subPlan, QuerySpecification node)
    {
        if (!analysis.isAggregation(node)) {
            return subPlan;
        }

        ImmutableList.Builder<Expression> inputBuilder = ImmutableList.builder();
        analysis.getAggregates(node).stream()
                .map(FunctionCall::getArguments)
                .flatMap(List::stream)
                .filter(expression -> !(expression instanceof LambdaExpression)) // lambda expression is generated at execution time
                .forEach(inputBuilder::add);

        analysis.getAggregates(node).stream()
                .map(FunctionCall::getOrderBy)
                .map(NodeUtils::getSortItemsFromOrderBy)
                .flatMap(List::stream)
                .map(SortItem::getSortKey)
                .forEach(inputBuilder::add);

        // filter expressions need to be projected first
        analysis.getAggregates(node).stream()
                .map(FunctionCall::getFilter)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(inputBuilder::add);

        GroupingSetAnalysis groupingSetAnalysis = analysis.getGroupingSets(node);
        inputBuilder.addAll(groupingSetAnalysis.getComplexExpressions());

        List<Expression> inputs = inputBuilder.build();
        subPlan = subqueryPlanner.handleSubqueries(subPlan, inputs, analysis.getSubqueries(node));
        subPlan = subPlan.appendProjections(inputs, symbolAllocator, idAllocator);

        // Add projection to coerce inputs to their site-specific types.
        // This is important because the same lexical expression may need to be coerced
        // in different ways if it's referenced by multiple arguments to the window function.
        // For example, given v::integer,
        //    avg(v)
        // Needs to be rewritten as
        //    avg(CAST(v AS double))
        PlanAndMappings coercions = coerce(subPlan, inputs, analysis, idAllocator, symbolAllocator, typeCoercion);
        subPlan = coercions.getSubPlan();

        GroupingSetsPlan groupingSets = planGroupingSets(subPlan, node, groupingSetAnalysis);

        subPlan = planAggregation(groupingSets.getSubPlan(), groupingSets.getGroupingSets(), groupingSets.getGroupIdSymbol(), analysis.getAggregates(node), coercions::get);

        return planGroupingOperations(subPlan, node, groupingSets.getGroupIdSymbol(), groupingSets.getColumnOnlyGroupingSets());
    }

    private GroupingSetsPlan planGroupingSets(PlanBuilder subPlan, QuerySpecification node, GroupingSetAnalysis groupingSetAnalysis)
    {
        Map<Symbol, Symbol> groupingSetMappings = new LinkedHashMap<>();

        // Compute a set of artificial columns that will contain the values of the original columns
        // filtered by whether the column is included in the grouping set
        // This will become the basis for the scope for any column references
        Symbol[] fields = new Symbol[subPlan.getTranslations().getFieldSymbols().size()];
        for (FieldId field : groupingSetAnalysis.getAllFields()) {
            Symbol input = subPlan.getTranslations().getFieldSymbols().get(field.getFieldIndex());
            Symbol output = symbolAllocator.newSymbol(input, "gid");
            fields[field.getFieldIndex()] = output;
            groupingSetMappings.put(output, input);
        }

        Map<ScopeAware<Expression>, Symbol> complexExpressions = new HashMap<>();
        for (Expression expression : groupingSetAnalysis.getComplexExpressions()) {
            if (!complexExpressions.containsKey(scopeAwareKey(expression, analysis, subPlan.getScope()))) {
                Symbol input = subPlan.translate(expression);
                Symbol output = symbolAllocator.newSymbol(expression, analysis.getType(expression), "gid");
                complexExpressions.put(scopeAwareKey(expression, analysis, subPlan.getScope()), output);
                groupingSetMappings.put(output, input);
            }
        }

        // For the purpose of "distinct", we need to canonicalize column references that may have varying
        // syntactic forms (e.g., "t.a" vs "a"). Thus we need to enumerate grouping sets based on the underlying
        // fieldId associated with each column reference expression.

        // The catch is that simple group-by expressions can be arbitrary expressions (this is a departure from the SQL specification).
        // But, they don't affect the number of grouping sets or the behavior of "distinct" . We can compute all the candidate
        // grouping sets in terms of fieldId, dedup as appropriate and then cross-join them with the complex expressions.

        // This tracks the grouping sets before complex expressions are considered.
        // It's also used to compute the descriptors needed to implement grouping()
        List<Set<FieldId>> columnOnlyGroupingSets = enumerateGroupingSets(groupingSetAnalysis);
        if (node.getGroupBy().isPresent() && node.getGroupBy().get().isDistinct()) {
            columnOnlyGroupingSets = columnOnlyGroupingSets.stream()
                    .distinct()
                    .collect(toImmutableList());
        }

        // translate from FieldIds to Symbols
        List<List<Symbol>> sets = columnOnlyGroupingSets.stream()
                .map(set -> set.stream()
                        .map(FieldId::getFieldIndex)
                        .map(index -> fields[index])
                        .collect(toImmutableList()))
                .collect(toImmutableList());

        // combine (cartesian product) with complex expressions
        List<List<Symbol>> groupingSets = sets.stream()
                .map(set -> ImmutableList.<Symbol>builder()
                        .addAll(set)
                        .addAll(complexExpressions.values())
                        .build())
                .collect(toImmutableList());

        // Generate GroupIdNode (multiple grouping sets) or ProjectNode (single grouping set)
        PlanNode groupId;
        Optional<Symbol> groupIdSymbol = Optional.empty();
        if (groupingSets.size() > 1) {
            groupIdSymbol = Optional.of(symbolAllocator.newSymbol("groupId", BIGINT));
            groupId = new GroupIdNode(
                    idAllocator.getNextId(),
                    subPlan.getRoot(),
                    groupingSets,
                    groupingSetMappings,
                    subPlan.getRoot().getOutputSymbols(),
                    groupIdSymbol.get());
        }
        else {
            Assignments.Builder assignments = Assignments.builder();
            assignments.putIdentities(subPlan.getRoot().getOutputSymbols());
            groupingSetMappings.forEach((key, value) -> assignments.put(key, value.toSymbolReference()));

            groupId = new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), assignments.build());
        }

        subPlan = new PlanBuilder(
                subPlan.getTranslations()
                        .withNewMappings(complexExpressions, Arrays.asList(fields)),
                groupId);

        return new GroupingSetsPlan(subPlan, columnOnlyGroupingSets, groupingSets, groupIdSymbol);
    }

    private PlanBuilder planAggregation(PlanBuilder subPlan, List<List<Symbol>> groupingSets, Optional<Symbol> groupIdSymbol, List<FunctionCall> aggregates, Function<Expression, Symbol> coercions)
    {
        ImmutableList.Builder<AggregationAssignment> aggregateMappingBuilder = ImmutableList.builder();

        // deduplicate based on scope-aware equality
        for (FunctionCall function : scopeAwareDistinct(subPlan, aggregates)) {
            Symbol symbol = symbolAllocator.newSymbol(function, analysis.getType(function));

            // TODO: for ORDER BY arguments, rewrite them such that they match the actual arguments to the function. This is necessary to maintain the semantics of DISTINCT + ORDER BY,
            //   which requires that ORDER BY be a subset of arguments
            //   What can happen currently is that if the argument requires a coercion, the argument will take a different input that the ORDER BY clause, which is undefined behavior
            Aggregation aggregation = new Aggregation(
                    analysis.getResolvedFunction(function),
                    function.getArguments().stream()
                            .map(argument -> {
                                if (argument instanceof LambdaExpression) {
                                    return subPlan.rewrite(argument);
                                }
                                return coercions.apply(argument).toSymbolReference();
                            })
                            .collect(toImmutableList()),
                    function.isDistinct(),
                    function.getFilter().map(coercions),
                    function.getOrderBy().map(orderBy -> translateOrderingScheme(orderBy.getSortItems(), coercions)),
                    Optional.empty());

            aggregateMappingBuilder.add(new AggregationAssignment(symbol, function, aggregation));
        }
        List<AggregationAssignment> aggregateMappings = aggregateMappingBuilder.build();

        ImmutableSet.Builder<Integer> globalGroupingSets = ImmutableSet.builder();
        for (int i = 0; i < groupingSets.size(); i++) {
            if (groupingSets.get(i).isEmpty()) {
                globalGroupingSets.add(i);
            }
        }

        ImmutableList.Builder<Symbol> groupingKeys = ImmutableList.builder();
        groupingSets.stream()
                .flatMap(List::stream)
                .distinct()
                .forEach(groupingKeys::add);
        groupIdSymbol.ifPresent(groupingKeys::add);

        AggregationNode aggregationNode = new AggregationNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                aggregateMappings.stream()
                        .collect(toImmutableMap(AggregationAssignment::getSymbol, AggregationAssignment::getRewritten)),
                groupingSets(
                        groupingKeys.build(),
                        groupingSets.size(),
                        globalGroupingSets.build()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                groupIdSymbol);

        return new PlanBuilder(
                subPlan.getTranslations()
                        .withAdditionalMappings(aggregateMappings.stream()
                                .collect(toImmutableMap(assignment -> scopeAwareKey(assignment.getAstExpression(), analysis, subPlan.getScope()), AggregationAssignment::getSymbol))),
                aggregationNode);
    }

    private <T extends Expression> List<T> scopeAwareDistinct(PlanBuilder subPlan, List<T> expressions)
    {
        return expressions.stream()
                .map(function -> scopeAwareKey(function, analysis, subPlan.getScope()))
                .distinct()
                .map(ScopeAware::getNode)
                .collect(toImmutableList());
    }

    public static OrderingScheme translateOrderingScheme(List<SortItem> items, Function<Expression, Symbol> coercions)
    {
        List<Symbol> coerced = items.stream()
                .map(SortItem::getSortKey)
                .map(coercions)
                .collect(toImmutableList());

        ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
        Map<Symbol, SortOrder> orders = new HashMap<>();
        for (int i = 0; i < coerced.size(); i++) {
            Symbol symbol = coerced.get(i);
            // for multiple sort items based on the same expression, retain the first one:
            // ORDER BY x DESC, x ASC, y --> ORDER BY x DESC, y
            if (!orders.containsKey(symbol)) {
                symbols.add(symbol);
                orders.put(symbol, OrderingScheme.sortItemToSortOrder(items.get(i)));
            }
        }

        return new OrderingScheme(symbols.build(), orders);
    }

    private static List<Set<FieldId>> enumerateGroupingSets(GroupingSetAnalysis groupingSetAnalysis)
    {
        List<List<Set<FieldId>>> partialSets = new ArrayList<>();

        for (List<Set<FieldId>> cube : groupingSetAnalysis.getCubes()) {
            List<Set<FieldId>> sets = Sets.powerSet(ImmutableSet.copyOf(cube)).stream()
                    .map(set -> set.stream()
                            .flatMap(Collection::stream)
                            .collect(toImmutableSet()))
                    .collect(toImmutableList());

            partialSets.add(sets);
        }

        for (List<Set<FieldId>> rollup : groupingSetAnalysis.getRollups()) {
            List<Set<FieldId>> sets = IntStream.rangeClosed(0, rollup.size())
                    .mapToObj(prefixLength -> rollup.subList(0, prefixLength).stream()
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet()))
                    .collect(toImmutableList());

            partialSets.add(sets);
        }

        partialSets.addAll(groupingSetAnalysis.getOrdinarySets());

        if (partialSets.isEmpty()) {
            return ImmutableList.of(ImmutableSet.of());
        }

        // compute the cross product of the partial sets
        List<Set<FieldId>> allSets = new ArrayList<>();
        partialSets.get(0)
                .stream()
                .map(ImmutableSet::copyOf)
                .forEach(allSets::add);

        for (int i = 1; i < partialSets.size(); i++) {
            List<Set<FieldId>> groupingSets = partialSets.get(i);
            List<Set<FieldId>> oldGroupingSetsCrossProduct = ImmutableList.copyOf(allSets);
            allSets.clear();
            for (Set<FieldId> existingSet : oldGroupingSetsCrossProduct) {
                for (Set<FieldId> groupingSet : groupingSets) {
                    Set<FieldId> concatenatedSet = ImmutableSet.<FieldId>builder()
                            .addAll(existingSet)
                            .addAll(groupingSet)
                            .build();
                    allSets.add(concatenatedSet);
                }
            }
        }

        return allSets;
    }

    private PlanBuilder planGroupingOperations(PlanBuilder subPlan, QuerySpecification node, Optional<Symbol> groupIdSymbol, List<Set<FieldId>> groupingSets)
    {
        if (analysis.getGroupingOperations(node).isEmpty()) {
            return subPlan;
        }

        List<Set<Integer>> descriptor = groupingSets.stream()
                .map(set -> set.stream()
                        .map(FieldId::getFieldIndex)
                        .collect(toImmutableSet()))
                .collect(toImmutableList());

        return subPlan.appendProjections(
                analysis.getGroupingOperations(node),
                symbolAllocator,
                idAllocator,
                (translations, groupingOperation) -> rewriteGroupingOperation(groupingOperation, descriptor, analysis.getColumnReferenceFields(), groupIdSymbol),
                (translations, groupingOperation) -> false);
    }

    private PlanBuilder planWindowFunctions(Node node, PlanBuilder subPlan, List<FunctionCall> windowFunctions)
    {
        if (windowFunctions.isEmpty()) {
            return subPlan;
        }

        for (FunctionCall windowFunction : scopeAwareDistinct(subPlan, windowFunctions)) {
            checkArgument(windowFunction.getFilter().isEmpty(), "Window functions cannot have filter");

            ResolvedWindow window = analysis.getWindow(windowFunction);
            checkState(window != null, "no resolved window for: " + windowFunction);

            // Pre-project inputs.
            // Predefined window parts (specified in WINDOW clause) can only use source symbols, and no output symbols.
            // It matters in case when this window planning takes place in ORDER BY clause, where both source and output
            // symbols are visible.
            // This issue is solved by analyzing window definitions in the source scope. After analysis, the expressions
            // are recorded as belonging to the source scope, and consequentially source symbols will be used to plan them.
            ImmutableList.Builder<Expression> inputsBuilder = ImmutableList.<Expression>builder()
                    .addAll(windowFunction.getArguments().stream()
                            .filter(argument -> !(argument instanceof LambdaExpression)) // lambda expression is generated at execution time
                            .collect(Collectors.toList()))
                    .addAll(window.getPartitionBy())
                    .addAll(getSortItemsFromOrderBy(window.getOrderBy()).stream()
                            .map(SortItem::getSortKey)
                            .iterator());

            if (window.getFrame().isPresent()) {
                WindowFrame frame = window.getFrame().get();
                frame.getStart().getValue().ifPresent(inputsBuilder::add);

                if (frame.getEnd().isPresent()) {
                    frame.getEnd().get().getValue().ifPresent(inputsBuilder::add);
                }
            }

            List<Expression> inputs = inputsBuilder.build();

            subPlan = subqueryPlanner.handleSubqueries(subPlan, inputs, analysis.getSubqueries(node));
            subPlan = subPlan.appendProjections(inputs, symbolAllocator, idAllocator);

            // Add projection to coerce inputs to their site-specific types.
            // This is important because the same lexical expression may need to be coerced
            // in different ways if it's referenced by multiple arguments to the window function.
            // For example, given v::integer,
            //    avg(v) OVER (ORDER BY v)
            // Needs to be rewritten as
            //    avg(CAST(v AS double)) OVER (ORDER BY v)
            PlanAndMappings coercions = coerce(subPlan, inputs, analysis, idAllocator, symbolAllocator, typeCoercion);
            subPlan = coercions.getSubPlan();

            // For frame of type RANGE, append casts and functions necessary for frame bound calculations
            Optional<Symbol> frameStart = Optional.empty();
            Optional<Symbol> frameEnd = Optional.empty();
            Optional<Symbol> sortKeyCoercedForFrameStartComparison = Optional.empty();
            Optional<Symbol> sortKeyCoercedForFrameEndComparison = Optional.empty();

            if (window.getFrame().isPresent() && window.getFrame().get().getType() == RANGE) {
                Optional<Expression> startValue = window.getFrame().get().getStart().getValue();
                Optional<Expression> endValue = window.getFrame().get().getEnd().flatMap(FrameBound::getValue);
                // record sortKey coercions for reuse
                Map<Type, Symbol> sortKeyCoercions = new HashMap<>();

                // process frame start
                FrameBoundPlanAndSymbols plan = planFrameBound(subPlan, coercions, startValue, window, sortKeyCoercions);
                subPlan = plan.getSubPlan();
                frameStart = plan.getFrameBoundSymbol();
                sortKeyCoercedForFrameStartComparison = plan.getSortKeyCoercedForFrameBoundComparison();

                // process frame end
                plan = planFrameBound(subPlan, coercions, endValue, window, sortKeyCoercions);
                subPlan = plan.getSubPlan();
                frameEnd = plan.getFrameBoundSymbol();
                sortKeyCoercedForFrameEndComparison = plan.getSortKeyCoercedForFrameBoundComparison();
            }
            else if (window.getFrame().isPresent() && (window.getFrame().get().getType() == ROWS || window.getFrame().get().getType() == GROUPS)) {
                Optional<Expression> startValue = window.getFrame().get().getStart().getValue();
                Optional<Expression> endValue = window.getFrame().get().getEnd().flatMap(FrameBound::getValue);

                // process frame start
                FrameOffsetPlanAndSymbol plan = planFrameOffset(subPlan, startValue.map(coercions::get));
                subPlan = plan.getSubPlan();
                frameStart = plan.getFrameOffsetSymbol();

                // process frame end
                plan = planFrameOffset(subPlan, endValue.map(coercions::get));
                subPlan = plan.getSubPlan();
                frameEnd = plan.getFrameOffsetSymbol();
            }
            else if (window.getFrame().isPresent()) {
                throw new IllegalArgumentException("unexpected window frame type: " + window.getFrame().get().getType());
            }

            if (window.getFrame().isPresent() && window.getFrame().get().getPattern().isPresent()) {
                WindowFrame frame = window.getFrame().get();
                subPlan = subqueryPlanner.handleSubqueries(subPlan, extractPatternRecognitionExpressions(frame.getVariableDefinitions(), frame.getMeasures()), analysis.getSubqueries(node));
                subPlan = planPatternRecognition(subPlan, windowFunction, window, coercions, frameEnd);
            }
            else {
                subPlan = planWindow(subPlan, windowFunction, window, coercions, frameStart, sortKeyCoercedForFrameStartComparison, frameEnd, sortKeyCoercedForFrameEndComparison);
            }
        }

        return subPlan;
    }

    private FrameBoundPlanAndSymbols planFrameBound(PlanBuilder subPlan, PlanAndMappings coercions, Optional<Expression> frameOffset, ResolvedWindow window, Map<Type, Symbol> sortKeyCoercions)
    {
        Optional<ResolvedFunction> frameBoundCalculationFunction = frameOffset.map(analysis::getFrameBoundCalculation);

        // Empty frameBoundCalculationFunction indicates that frame bound type is CURRENT ROW or UNBOUNDED.
        // Handling it doesn't require any additional symbols.
        if (frameBoundCalculationFunction.isEmpty()) {
            return new FrameBoundPlanAndSymbols(subPlan, Optional.empty(), Optional.empty());
        }

        // Present frameBoundCalculationFunction indicates that frame bound type is <expression> PRECEDING or <expression> FOLLOWING.
        // It requires adding certain projections to the plan so that the operator can determine frame bounds.

        // First, append filter to validate offset values. They mustn't be negative or null.
        Symbol offsetSymbol = coercions.get(frameOffset.get());
        Expression zeroOffset = zeroOfType(symbolAllocator.getTypes().get(offsetSymbol));
        Expression predicate = new IfExpression(
                new ComparisonExpression(
                        GREATER_THAN_OR_EQUAL,
                        offsetSymbol.toSymbolReference(),
                        zeroOffset),
                TRUE_LITERAL,
                new Cast(
                        failFunction(plannerContext.getMetadata(), session, INVALID_WINDOW_FRAME, "Window frame offset value must not be negative or null"),
                        toSqlType(BOOLEAN)));
        subPlan = subPlan.withNewRoot(new FilterNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                predicate));

        // Then, coerce the sortKey so that we can add / subtract the offset.
        // Note: for that we cannot rely on the usual mechanism of using the coerce() method. The coerce() method can only handle one coercion for a node,
        // while the sortKey node might require several different coercions, e.g. one for frame start and one for frame end.
        Expression sortKey = Iterables.getOnlyElement(window.getOrderBy().orElseThrow().getSortItems()).getSortKey();
        Symbol sortKeyCoercedForFrameBoundCalculation = coercions.get(sortKey);
        Optional<Type> coercion = frameOffset.map(analysis::getSortKeyCoercionForFrameBoundCalculation);
        if (coercion.isPresent()) {
            Type expectedType = coercion.get();
            Symbol alreadyCoerced = sortKeyCoercions.get(expectedType);
            if (alreadyCoerced != null) {
                sortKeyCoercedForFrameBoundCalculation = alreadyCoerced;
            }
            else {
                Expression cast = new Cast(
                        coercions.get(sortKey).toSymbolReference(),
                        toSqlType(expectedType),
                        false,
                        typeCoercion.isTypeOnlyCoercion(analysis.getType(sortKey), expectedType));
                sortKeyCoercedForFrameBoundCalculation = symbolAllocator.newSymbol(cast, expectedType);
                sortKeyCoercions.put(expectedType, sortKeyCoercedForFrameBoundCalculation);
                subPlan = subPlan.withNewRoot(new ProjectNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        Assignments.builder()
                                .putIdentities(subPlan.getRoot().getOutputSymbols())
                                .put(sortKeyCoercedForFrameBoundCalculation, cast)
                                .build()));
            }
        }

        // Next, pre-project the function which combines sortKey with the offset.
        // Note: if frameOffset needs a coercion, it was added before by a call to coerce() method.
        ResolvedFunction function = frameBoundCalculationFunction.get();
        Expression functionCall = new FunctionCall(
                function.toQualifiedName(),
                ImmutableList.of(
                        sortKeyCoercedForFrameBoundCalculation.toSymbolReference(),
                        offsetSymbol.toSymbolReference()));
        Symbol frameBoundSymbol = symbolAllocator.newSymbol(functionCall, function.getSignature().getReturnType());
        subPlan = subPlan.withNewRoot(new ProjectNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                Assignments.builder()
                        .putIdentities(subPlan.getRoot().getOutputSymbols())
                        .put(frameBoundSymbol, functionCall)
                        .build()));

        // Finally, coerce the sortKey to the type of frameBound so that the operator can perform comparisons on them
        Optional<Symbol> sortKeyCoercedForFrameBoundComparison = Optional.of(coercions.get(sortKey));
        coercion = frameOffset.map(analysis::getSortKeyCoercionForFrameBoundComparison);
        if (coercion.isPresent()) {
            Type expectedType = coercion.get();
            Symbol alreadyCoerced = sortKeyCoercions.get(expectedType);
            if (alreadyCoerced != null) {
                sortKeyCoercedForFrameBoundComparison = Optional.of(alreadyCoerced);
            }
            else {
                Expression cast = new Cast(
                        coercions.get(sortKey).toSymbolReference(),
                        toSqlType(expectedType),
                        false,
                        typeCoercion.isTypeOnlyCoercion(analysis.getType(sortKey), expectedType));
                Symbol castSymbol = symbolAllocator.newSymbol(cast, expectedType);
                sortKeyCoercions.put(expectedType, castSymbol);
                subPlan = subPlan.withNewRoot(new ProjectNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        Assignments.builder()
                                .putIdentities(subPlan.getRoot().getOutputSymbols())
                                .put(castSymbol, cast)
                                .build()));
                sortKeyCoercedForFrameBoundComparison = Optional.of(castSymbol);
            }
        }

        return new FrameBoundPlanAndSymbols(subPlan, Optional.of(frameBoundSymbol), sortKeyCoercedForFrameBoundComparison);
    }

    private FrameOffsetPlanAndSymbol planFrameOffset(PlanBuilder subPlan, Optional<Symbol> frameOffset)
    {
        if (frameOffset.isEmpty()) {
            return new FrameOffsetPlanAndSymbol(subPlan, Optional.empty());
        }

        Symbol offsetSymbol = frameOffset.get();
        Type offsetType = symbolAllocator.getTypes().get(offsetSymbol);

        // Append filter to validate offset values. They mustn't be negative or null.
        Expression zeroOffset = zeroOfType(offsetType);
        Expression predicate = new IfExpression(
                new ComparisonExpression(GREATER_THAN_OR_EQUAL, offsetSymbol.toSymbolReference(), zeroOffset),
                TRUE_LITERAL,
                new Cast(
                        failFunction(plannerContext.getMetadata(), session, INVALID_WINDOW_FRAME, "Window frame offset value must not be negative or null"),
                        toSqlType(BOOLEAN)));
        subPlan = subPlan.withNewRoot(new FilterNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                predicate));

        if (offsetType.equals(BIGINT)) {
            return new FrameOffsetPlanAndSymbol(subPlan, Optional.of(offsetSymbol));
        }

        Expression offsetToBigint;

        if (offsetType instanceof DecimalType && !((DecimalType) offsetType).isShort()) {
            String maxBigint = Long.toString(Long.MAX_VALUE);
            int maxBigintPrecision = maxBigint.length();
            int actualPrecision = ((DecimalType) offsetType).getPrecision();

            if (actualPrecision < maxBigintPrecision) {
                offsetToBigint = new Cast(offsetSymbol.toSymbolReference(), toSqlType(BIGINT));
            }
            else if (actualPrecision > maxBigintPrecision) {
                // If the offset value exceeds max bigint, it implies that the frame bound falls beyond the partition bound.
                // In such case, the frame bound is set to the partition bound. Passing max bigint as the offset value has
                // the same effect. The offset value can be truncated to max bigint for the purpose of cast.
                offsetToBigint = new GenericLiteral("BIGINT", maxBigint);
            }
            else {
                offsetToBigint = new IfExpression(
                        new ComparisonExpression(LESS_THAN_OR_EQUAL, offsetSymbol.toSymbolReference(), new DecimalLiteral(maxBigint)),
                        new Cast(offsetSymbol.toSymbolReference(), toSqlType(BIGINT)),
                        new GenericLiteral("BIGINT", maxBigint));
            }
        }
        else {
            offsetToBigint = new Cast(
                    offsetSymbol.toSymbolReference(),
                    toSqlType(BIGINT),
                    false,
                    typeCoercion.isTypeOnlyCoercion(offsetType, BIGINT));
        }

        Symbol coercedOffsetSymbol = symbolAllocator.newSymbol(offsetToBigint, BIGINT);
        subPlan = subPlan.withNewRoot(new ProjectNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                Assignments.builder()
                        .putIdentities(subPlan.getRoot().getOutputSymbols())
                        .put(coercedOffsetSymbol, offsetToBigint)
                        .build()));

        return new FrameOffsetPlanAndSymbol(subPlan, Optional.of(coercedOffsetSymbol));
    }

    private static Expression zeroOfType(Type type)
    {
        if (isNumericType(type)) {
            return new Cast(new LongLiteral("0"), toSqlType(type));
        }
        if (type.equals(INTERVAL_DAY_TIME)) {
            return new IntervalLiteral("0", POSITIVE, DAY);
        }
        if (type.equals(INTERVAL_YEAR_MONTH)) {
            return new IntervalLiteral("0", POSITIVE, YEAR);
        }
        throw new IllegalArgumentException("unexpected type: " + type);
    }

    private PlanBuilder planWindow(
            PlanBuilder subPlan,
            FunctionCall windowFunction,
            ResolvedWindow window,
            PlanAndMappings coercions,
            Optional<Symbol> frameStartSymbol,
            Optional<Symbol> sortKeyCoercedForFrameStartComparison,
            Optional<Symbol> frameEndSymbol,
            Optional<Symbol> sortKeyCoercedForFrameEndComparison)
    {
        WindowFrame.Type frameType = WindowFrame.Type.RANGE;
        FrameBound.Type frameStartType = FrameBound.Type.UNBOUNDED_PRECEDING;
        FrameBound.Type frameEndType = FrameBound.Type.CURRENT_ROW;

        Optional<Expression> frameStartExpression = Optional.empty();
        Optional<Expression> frameEndExpression = Optional.empty();

        if (window.getFrame().isPresent()) {
            WindowFrame frame = window.getFrame().get();
            frameType = frame.getType();

            frameStartType = frame.getStart().getType();
            frameStartExpression = frame.getStart().getValue();

            if (frame.getEnd().isPresent()) {
                frameEndType = frame.getEnd().get().getType();
                frameEndExpression = frame.getEnd().get().getValue();
            }
        }

        DataOrganizationSpecification specification = planWindowSpecification(window.getPartitionBy(), window.getOrderBy(), coercions::get);

        // Rewrite frame bounds in terms of pre-projected inputs
        WindowNode.Frame frame = new WindowNode.Frame(
                frameType,
                frameStartType,
                frameStartSymbol,
                sortKeyCoercedForFrameStartComparison,
                frameEndType,
                frameEndSymbol,
                sortKeyCoercedForFrameEndComparison,
                frameStartExpression,
                frameEndExpression);

        Symbol newSymbol = symbolAllocator.newSymbol(windowFunction, analysis.getType(windowFunction));

        NullTreatment nullTreatment = windowFunction.getNullTreatment()
                .orElse(NullTreatment.RESPECT);

        WindowNode.Function function = new WindowNode.Function(
                analysis.getResolvedFunction(windowFunction),
                windowFunction.getArguments().stream()
                        .map(argument -> {
                            if (argument instanceof LambdaExpression) {
                                return subPlan.rewrite(argument);
                            }
                            return coercions.get(argument).toSymbolReference();
                        })
                        .collect(toImmutableList()),
                frame,
                nullTreatment == NullTreatment.IGNORE);

        // create window node
        return new PlanBuilder(
                subPlan.getTranslations()
                        .withAdditionalMappings(ImmutableMap.of(scopeAwareKey(windowFunction, analysis, subPlan.getScope()), newSymbol)),
                new WindowNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        specification,
                        ImmutableMap.of(newSymbol, function),
                        Optional.empty(),
                        ImmutableSet.of(),
                        0));
    }

    private PlanBuilder planPatternRecognition(
            PlanBuilder subPlan,
            FunctionCall windowFunction,
            ResolvedWindow window,
            PlanAndMappings coercions,
            Optional<Symbol> frameEndSymbol)
    {
        DataOrganizationSpecification specification = planWindowSpecification(window.getPartitionBy(), window.getOrderBy(), coercions::get);

        // in window frame with pattern recognition, the frame extent is specified as `ROWS BETWEEN CURRENT ROW AND ... `
        WindowFrame frame = window.getFrame().orElseThrow();
        FrameBound frameEnd = frame.getEnd().orElseThrow();
        WindowNode.Frame baseFrame = new WindowNode.Frame(
                WindowFrame.Type.ROWS,
                FrameBound.Type.CURRENT_ROW,
                Optional.empty(),
                Optional.empty(),
                frameEnd.getType(),
                frameEndSymbol,
                Optional.empty(),
                Optional.empty(),
                frameEnd.getValue());

        Symbol newSymbol = symbolAllocator.newSymbol(windowFunction, analysis.getType(windowFunction));

        NullTreatment nullTreatment = windowFunction.getNullTreatment()
                .orElse(NullTreatment.RESPECT);

        WindowNode.Function function = new WindowNode.Function(
                analysis.getResolvedFunction(windowFunction),
                windowFunction.getArguments().stream()
                        .map(argument -> {
                            if (argument instanceof LambdaExpression) {
                                return subPlan.rewrite(argument);
                            }
                            return coercions.get(argument).toSymbolReference();
                        })
                        .collect(toImmutableList()),
                baseFrame,
                nullTreatment == NullTreatment.IGNORE);

        PatternRecognitionComponents components = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, outerContext, session, recursiveSubqueries)
                .planPatternRecognitionComponents(
                        subPlan::rewrite,
                        frame.getSubsets(),
                        ImmutableList.of(),
                        frame.getAfterMatchSkipTo(),
                        frame.getPatternSearchMode(),
                        frame.getPattern().orElseThrow(),
                        frame.getVariableDefinitions());

        // create pattern recognition node
        return new PlanBuilder(
                subPlan.getTranslations()
                        .withAdditionalMappings(ImmutableMap.of(scopeAwareKey(windowFunction, analysis, subPlan.getScope()), newSymbol)),
                new PatternRecognitionNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        specification,
                        Optional.empty(),
                        ImmutableSet.of(),
                        0,
                        ImmutableMap.of(newSymbol, function),
                        components.getMeasures(),
                        Optional.of(baseFrame),
                        RowsPerMatch.WINDOW,
                        components.getSkipToLabel(),
                        components.getSkipToPosition(),
                        components.isInitial(),
                        components.getPattern(),
                        components.getSubsets(),
                        components.getVariableDefinitions()));
    }

    public static DataOrganizationSpecification planWindowSpecification(List<Expression> partitionBy, Optional<OrderBy> orderBy, Function<Expression, Symbol> expressionRewrite)
    {
        // Rewrite PARTITION BY
        ImmutableList.Builder<Symbol> partitionBySymbols = ImmutableList.builder();
        for (Expression expression : partitionBy) {
            partitionBySymbols.add(expressionRewrite.apply(expression));
        }

        // Rewrite ORDER BY
        LinkedHashMap<Symbol, SortOrder> orderings = new LinkedHashMap<>();
        for (SortItem item : getSortItemsFromOrderBy(orderBy)) {
            Symbol symbol = expressionRewrite.apply(item.getSortKey());
            // don't override existing keys, i.e. when "ORDER BY a ASC, a DESC" is specified
            orderings.putIfAbsent(symbol, sortItemToSortOrder(item));
        }

        Optional<OrderingScheme> orderingScheme = Optional.empty();
        if (!orderings.isEmpty()) {
            orderingScheme = Optional.of(new OrderingScheme(ImmutableList.copyOf(orderings.keySet()), orderings));
        }

        return new DataOrganizationSpecification(partitionBySymbols.build(), orderingScheme);
    }

    private PlanBuilder planWindowMeasures(Node node, PlanBuilder subPlan, List<WindowOperation> windowMeasures)
    {
        if (windowMeasures.isEmpty()) {
            return subPlan;
        }

        for (WindowOperation windowMeasure : scopeAwareDistinct(subPlan, windowMeasures)) {
            ResolvedWindow window = analysis.getWindow(windowMeasure);
            checkState(window != null, "no resolved window for: " + windowMeasure);

            // pre-project inputs
            ImmutableList.Builder<Expression> inputsBuilder = ImmutableList.<Expression>builder()
                    .addAll(window.getPartitionBy())
                    .addAll(getSortItemsFromOrderBy(window.getOrderBy()).stream()
                            .map(SortItem::getSortKey)
                            .iterator());
            WindowFrame frame = window.getFrame().orElseThrow();
            Optional<Expression> endValue = frame.getEnd().orElseThrow().getValue();
            endValue.ifPresent(inputsBuilder::add);

            List<Expression> inputs = inputsBuilder.build();

            subPlan = subqueryPlanner.handleSubqueries(subPlan, inputs, analysis.getSubqueries(node));
            subPlan = subPlan.appendProjections(inputs, symbolAllocator, idAllocator);

            // process frame end
            FrameOffsetPlanAndSymbol plan = planFrameOffset(subPlan, endValue.map(subPlan::translate));
            subPlan = plan.getSubPlan();
            Optional<Symbol> frameEnd = plan.getFrameOffsetSymbol();

            subPlan = subqueryPlanner.handleSubqueries(subPlan, extractPatternRecognitionExpressions(frame.getVariableDefinitions(), frame.getMeasures()), analysis.getSubqueries(node));
            subPlan = planPatternRecognition(subPlan, windowMeasure, window, frameEnd);
        }

        return subPlan;
    }

    public static List<Expression> extractPatternRecognitionExpressions(List<VariableDefinition> variableDefinitions, List<MeasureDefinition> measureDefinitions)
    {
        ImmutableList.Builder<Expression> expressions = ImmutableList.builder();

        variableDefinitions.stream()
                .map(VariableDefinition::getExpression)
                .forEach(expressions::add);

        measureDefinitions.stream()
                .map(MeasureDefinition::getExpression)
                .forEach(expressions::add);

        return expressions.build();
    }

    private PlanBuilder planPatternRecognition(
            PlanBuilder subPlan,
            WindowOperation windowMeasure,
            ResolvedWindow window,
            Optional<Symbol> frameEndSymbol)
    {
        DataOrganizationSpecification specification = planWindowSpecification(window.getPartitionBy(), window.getOrderBy(), subPlan::translate);

        // in window frame with pattern recognition, the frame extent is specified as `ROWS BETWEEN CURRENT ROW AND ... `
        WindowFrame frame = window.getFrame().orElseThrow();
        FrameBound frameEnd = frame.getEnd().orElseThrow();
        WindowNode.Frame baseFrame = new WindowNode.Frame(
                WindowFrame.Type.ROWS,
                FrameBound.Type.CURRENT_ROW,
                Optional.empty(),
                Optional.empty(),
                frameEnd.getType(),
                frameEndSymbol,
                Optional.empty(),
                Optional.empty(),
                frameEnd.getValue());

        PatternRecognitionComponents components = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, outerContext, session, recursiveSubqueries)
                .planPatternRecognitionComponents(
                        subPlan::rewrite,
                        frame.getSubsets(),
                        ImmutableList.of(analysis.getMeasureDefinition(windowMeasure)),
                        frame.getAfterMatchSkipTo(),
                        frame.getPatternSearchMode(),
                        frame.getPattern().orElseThrow(),
                        frame.getVariableDefinitions());

        Symbol measureSymbol = getOnlyElement(components.getMeasures().keySet());

        // create pattern recognition node
        return new PlanBuilder(
                subPlan.getTranslations()
                        .withAdditionalMappings(ImmutableMap.of(scopeAwareKey(windowMeasure, analysis, subPlan.getScope()), measureSymbol)),
                new PatternRecognitionNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        specification,
                        Optional.empty(),
                        ImmutableSet.of(),
                        0,
                        ImmutableMap.of(),
                        components.getMeasures(),
                        Optional.of(baseFrame),
                        RowsPerMatch.WINDOW,
                        components.getSkipToLabel(),
                        components.getSkipToPosition(),
                        components.isInitial(),
                        components.getPattern(),
                        components.getSubsets(),
                        components.getVariableDefinitions()));
    }

    /**
     * Creates a projection with any additional coercions by identity of the provided expressions.
     *
     * @return the new subplan and a mapping of each expression to the symbol representing the coercion or an existing symbol if a coercion wasn't needed
     */
    public static PlanAndMappings coerce(PlanBuilder subPlan, List<Expression> expressions, Analysis analysis, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, TypeCoercion typeCoercion)
    {
        Assignments.Builder assignments = Assignments.builder();
        assignments.putIdentities(subPlan.getRoot().getOutputSymbols());

        Map<NodeRef<Expression>, Symbol> mappings = new HashMap<>();
        for (Expression expression : expressions) {
            Type coercion = analysis.getCoercion(expression);

            // expressions may be repeated, for example, when resolving ordinal references in a GROUP BY clause
            if (!mappings.containsKey(NodeRef.of(expression))) {
                if (coercion != null) {
                    Type type = analysis.getType(expression);
                    Symbol symbol = symbolAllocator.newSymbol(expression, coercion);

                    assignments.put(symbol, new Cast(
                            subPlan.rewrite(expression),
                            toSqlType(coercion),
                            false,
                            typeCoercion.isTypeOnlyCoercion(type, coercion)));

                    mappings.put(NodeRef.of(expression), symbol);
                }
                else {
                    mappings.put(NodeRef.of(expression), subPlan.translate(expression));
                }
            }
        }

        subPlan = subPlan.withNewRoot(
                new ProjectNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        assignments.build()));

        return new PlanAndMappings(subPlan, mappings);
    }

    public static Expression coerceIfNecessary(Analysis analysis, Expression original, Expression rewritten)
    {
        Type coercion = analysis.getCoercion(original);
        if (coercion == null) {
            return rewritten;
        }

        return new Cast(
                rewritten,
                toSqlType(coercion),
                false,
                analysis.isTypeOnlyCoercion(original));
    }

    public static NodeAndMappings coerce(RelationPlan plan, List<Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        List<Symbol> visibleFields = visibleFields(plan);
        checkArgument(visibleFields.size() == types.size());

        Assignments.Builder assignments = Assignments.builder();
        ImmutableList.Builder<Symbol> mappings = ImmutableList.builder();
        for (int i = 0; i < types.size(); i++) {
            Symbol input = visibleFields.get(i);
            Type type = types.get(i);

            if (!symbolAllocator.getTypes().get(input).equals(type)) {
                Symbol coerced = symbolAllocator.newSymbol(input.getName(), type);
                assignments.put(coerced, new Cast(input.toSymbolReference(), toSqlType(type)));
                mappings.add(coerced);
            }
            else {
                assignments.putIdentity(input);
                mappings.add(input);
            }
        }

        ProjectNode coerced = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments.build());
        return new NodeAndMappings(coerced, mappings.build());
    }

    public static List<Symbol> visibleFields(RelationPlan subPlan)
    {
        RelationType descriptor = subPlan.getDescriptor();
        return descriptor.getAllFields().stream()
                .filter(field -> !field.isHidden())
                .map(descriptor::indexOf)
                .map(subPlan.getFieldMappings()::get)
                .collect(toImmutableList());
    }

    public static NodeAndMappings pruneInvisibleFields(RelationPlan plan, PlanNodeIdAllocator idAllocator)
    {
        List<Symbol> visibleFields = visibleFields(plan);
        ProjectNode pruned = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), Assignments.identity(visibleFields));
        return new NodeAndMappings(pruned, visibleFields);
    }

    public static NodeAndMappings disambiguateOutputs(NodeAndMappings plan, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        Set<Symbol> distinctOutputs = ImmutableSet.copyOf(plan.getFields());

        if (distinctOutputs.size() < plan.getFields().size()) {
            Assignments.Builder assignments = Assignments.builder();
            ImmutableList.Builder<Symbol> newOutputs = ImmutableList.builder();
            Set<Symbol> uniqueOutputs = new HashSet<>();

            for (Symbol output : plan.getFields()) {
                if (uniqueOutputs.add(output)) {
                    assignments.putIdentity(output);
                    newOutputs.add(output);
                }
                else {
                    Symbol newOutput = symbolAllocator.newSymbol(output);
                    assignments.put(newOutput, output.toSymbolReference());
                    newOutputs.add(newOutput);
                }
            }

            return new NodeAndMappings(new ProjectNode(idAllocator.getNextId(), plan.getNode(), assignments.build()), newOutputs.build());
        }

        return plan;
    }

    private PlanBuilder distinct(PlanBuilder subPlan, QuerySpecification node, List<Expression> expressions)
    {
        if (node.getSelect().isDistinct()) {
            List<Symbol> symbols = expressions.stream()
                    .map(subPlan::translate)
                    .collect(Collectors.toList());

            return subPlan.withNewRoot(
                    singleAggregation(
                            idAllocator.getNextId(),
                            subPlan.getRoot(),
                            ImmutableMap.of(),
                            singleGroupingSet(symbols)));
        }

        return subPlan;
    }

    private Optional<OrderingScheme> orderingScheme(PlanBuilder subPlan, Optional<OrderBy> orderBy, List<Expression> orderByExpressions)
    {
        if (orderBy.isEmpty() || (isSkipRedundantSort(session)) && analysis.isOrderByRedundant(orderBy.get())) {
            return Optional.empty();
        }

        Iterator<SortItem> sortItems = orderBy.get().getSortItems().iterator();

        ImmutableList.Builder<Symbol> orderBySymbols = ImmutableList.builder();
        Map<Symbol, SortOrder> orderings = new HashMap<>();
        for (Expression fieldOrExpression : orderByExpressions) {
            Symbol symbol = subPlan.translate(fieldOrExpression);

            SortItem sortItem = sortItems.next();
            if (!orderings.containsKey(symbol)) {
                orderBySymbols.add(symbol);
                orderings.put(symbol, sortItemToSortOrder(sortItem));
            }
        }
        return Optional.of(new OrderingScheme(orderBySymbols.build(), orderings));
    }

    private PlanBuilder sort(PlanBuilder subPlan, Optional<OrderingScheme> orderingScheme)
    {
        if (orderingScheme.isEmpty()) {
            return subPlan;
        }

        return subPlan.withNewRoot(
                new SortNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        orderingScheme.get(),
                        false));
    }

    private PlanBuilder offset(PlanBuilder subPlan, Optional<Offset> offset)
    {
        if (offset.isEmpty()) {
            return subPlan;
        }

        return subPlan.withNewRoot(
                new OffsetNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        analysis.getOffset(offset.get())));
    }

    private PlanBuilder limit(PlanBuilder subPlan, Optional<Node> limit, Optional<OrderingScheme> orderingScheme)
    {
        if (limit.isPresent() && analysis.getLimit(limit.get()).isPresent()) {
            Optional<OrderingScheme> tiesResolvingScheme = Optional.empty();
            if (limit.get() instanceof FetchFirst && ((FetchFirst) limit.get()).isWithTies()) {
                tiesResolvingScheme = orderingScheme;
            }
            return subPlan.withNewRoot(
                    new LimitNode(
                            idAllocator.getNextId(),
                            subPlan.getRoot(),
                            analysis.getLimit(limit.get()).getAsLong(),
                            tiesResolvingScheme,
                            false,
                            ImmutableList.of()));
        }
        return subPlan;
    }

    private static class GroupingSetsPlan
    {
        private final PlanBuilder subPlan;
        private final List<Set<FieldId>> columnOnlyGroupingSets;
        private final List<List<Symbol>> groupingSets;
        private final Optional<Symbol> groupIdSymbol;

        public GroupingSetsPlan(PlanBuilder subPlan, List<Set<FieldId>> columnOnlyGroupingSets, List<List<Symbol>> groupingSets, Optional<Symbol> groupIdSymbol)
        {
            this.columnOnlyGroupingSets = columnOnlyGroupingSets;
            this.groupingSets = groupingSets;
            this.groupIdSymbol = groupIdSymbol;
            this.subPlan = subPlan;
        }

        public PlanBuilder getSubPlan()
        {
            return subPlan;
        }

        public List<Set<FieldId>> getColumnOnlyGroupingSets()
        {
            return columnOnlyGroupingSets;
        }

        public List<List<Symbol>> getGroupingSets()
        {
            return groupingSets;
        }

        public Optional<Symbol> getGroupIdSymbol()
        {
            return groupIdSymbol;
        }
    }

    public static class PlanAndMappings
    {
        private final PlanBuilder subPlan;
        private final Map<NodeRef<Expression>, Symbol> mappings;

        public PlanAndMappings(PlanBuilder subPlan, Map<NodeRef<Expression>, Symbol> mappings)
        {
            this.subPlan = subPlan;
            this.mappings = ImmutableMap.copyOf(mappings);
        }

        public PlanBuilder getSubPlan()
        {
            return subPlan;
        }

        public Symbol get(Expression expression)
        {
            return tryGet(expression)
                    .orElseThrow(() -> new IllegalArgumentException(format("No mapping for expression: %s (%s)", expression, System.identityHashCode(expression))));
        }

        public Optional<Symbol> tryGet(Expression expression)
        {
            Symbol result = mappings.get(NodeRef.of(expression));

            if (result != null) {
                return Optional.of(result);
            }

            return Optional.empty();
        }
    }

    private static class AggregationAssignment
    {
        private final Symbol symbol;
        private final Expression astExpression;
        private final Aggregation aggregation;

        public AggregationAssignment(Symbol symbol, Expression astExpression, Aggregation aggregation)
        {
            this.astExpression = astExpression;
            this.symbol = symbol;
            this.aggregation = aggregation;
        }

        public Symbol getSymbol()
        {
            return symbol;
        }

        public Expression getAstExpression()
        {
            return astExpression;
        }

        public Aggregation getRewritten()
        {
            return aggregation;
        }
    }

    private static class FrameBoundPlanAndSymbols
    {
        private final PlanBuilder subPlan;
        private final Optional<Symbol> frameBoundSymbol;
        private final Optional<Symbol> sortKeyCoercedForFrameBoundComparison;

        public FrameBoundPlanAndSymbols(PlanBuilder subPlan, Optional<Symbol> frameBoundSymbol, Optional<Symbol> sortKeyCoercedForFrameBoundComparison)
        {
            this.subPlan = subPlan;
            this.frameBoundSymbol = frameBoundSymbol;
            this.sortKeyCoercedForFrameBoundComparison = sortKeyCoercedForFrameBoundComparison;
        }

        public PlanBuilder getSubPlan()
        {
            return subPlan;
        }

        public Optional<Symbol> getFrameBoundSymbol()
        {
            return frameBoundSymbol;
        }

        public Optional<Symbol> getSortKeyCoercedForFrameBoundComparison()
        {
            return sortKeyCoercedForFrameBoundComparison;
        }
    }

    private static class FrameOffsetPlanAndSymbol
    {
        private final PlanBuilder subPlan;
        private final Optional<Symbol> frameOffsetSymbol;

        public FrameOffsetPlanAndSymbol(PlanBuilder subPlan, Optional<Symbol> frameOffsetSymbol)
        {
            this.subPlan = subPlan;
            this.frameOffsetSymbol = frameOffsetSymbol;
        }

        public PlanBuilder getSubPlan()
        {
            return subPlan;
        }

        public Optional<Symbol> getFrameOffsetSymbol()
        {
            return frameOffsetSymbol;
        }
    }
}
