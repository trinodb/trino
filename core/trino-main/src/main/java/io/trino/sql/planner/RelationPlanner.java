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
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ExpressionUtils;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analysis.UnnestAnalysis;
import io.trino.sql.analyzer.Field;
import io.trino.sql.analyzer.RelationType;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PatternRecognitionNode.Measure;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.RowPatternToIrRewriter;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.planner.rowpattern.ir.IrRowPattern;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Except;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinUsing;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.Lateral;
import io.trino.sql.tree.MeasureDefinition;
import io.trino.sql.tree.NaturalJoin;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.PatternRecognitionRelation;
import io.trino.sql.tree.PatternSearchMode;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
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
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.VariableDefinition;
import io.trino.type.TypeCoercion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.sql.NodeUtils.getSortItemsFromOrderBy;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.PlanBuilder.newPlanBuilder;
import static io.trino.sql.planner.QueryPlanner.coerce;
import static io.trino.sql.planner.QueryPlanner.coerceIfNecessary;
import static io.trino.sql.planner.QueryPlanner.extractPatternRecognitionExpressions;
import static io.trino.sql.planner.QueryPlanner.planWindowSpecification;
import static io.trino.sql.planner.QueryPlanner.pruneInvisibleFields;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.Join.Type.CROSS;
import static io.trino.sql.tree.Join.Type.IMPLICIT;
import static io.trino.sql.tree.Join.Type.INNER;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.ONE;
import static io.trino.sql.tree.PatternSearchMode.Mode.INITIAL;
import static io.trino.sql.tree.SkipTo.Position.PAST_LAST;
import static java.lang.Boolean.TRUE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

class RelationPlanner
        extends AstVisitor<RelationPlan, Void>
{
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap;
    private final Metadata metadata;
    private final TypeCoercion typeCoercion;
    private final Optional<TranslationMap> outerContext;
    private final Session session;
    private final SubqueryPlanner subqueryPlanner;
    private final Map<NodeRef<Node>, RelationPlan> recursiveSubqueries;

    RelationPlanner(
            Analysis analysis,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap,
            Metadata metadata,
            Optional<TranslationMap> outerContext,
            Session session,
            Map<NodeRef<Node>, RelationPlan> recursiveSubqueries)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(lambdaDeclarationToSymbolMap, "lambdaDeclarationToSymbolMap is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(outerContext, "outerContext is null");
        requireNonNull(session, "session is null");
        requireNonNull(recursiveSubqueries, "recursiveSubqueries is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
        this.lambdaDeclarationToSymbolMap = lambdaDeclarationToSymbolMap;
        this.metadata = metadata;
        this.typeCoercion = new TypeCoercion(metadata::getType);
        this.outerContext = outerContext;
        this.session = session;
        this.subqueryPlanner = new SubqueryPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, typeCoercion, outerContext, session, recursiveSubqueries);
        this.recursiveSubqueries = recursiveSubqueries;
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
                subPlan = new QueryPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, outerContext, session, recursiveSubqueries)
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
            for (Field field : scope.getRelationType().getAllFields()) {
                Symbol symbol = symbolAllocator.newSymbol(field);

                outputSymbolsBuilder.add(symbol);
                columns.put(symbol, analysis.getColumn(field));
            }

            List<Symbol> outputSymbols = outputSymbolsBuilder.build();
            boolean updateTarget = analysis.isUpdateTarget(node);
            PlanNode root = TableScanNode.newInstance(idAllocator.getNextId(), handle, outputSymbols, columns.build(), updateTarget, Optional.empty());

            plan = new RelationPlan(root, scope, outputSymbols, outerContext);

            List<Type> types = analysis.getRelationCoercion(node);
            if (types != null) {
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
        List<Expression> filters = analysis.getRowFilters(node);

        if (filters.isEmpty()) {
            return plan;
        }

        PlanBuilder planBuilder = newPlanBuilder(plan, analysis, lambdaDeclarationToSymbolMap)
                .withScope(analysis.getAccessControlScope(node), plan.getFieldMappings()); // The fields in the access control scope has the same layout as those for the table scope

        for (Expression filter : filters) {
            planBuilder = subqueryPlanner.handleSubqueries(planBuilder, filter, analysis.getSubqueries(filter));

            planBuilder = planBuilder.withNewRoot(new FilterNode(
                    idAllocator.getNextId(),
                    planBuilder.getRoot(),
                    planBuilder.rewrite(filter)));
        }

        return new RelationPlan(planBuilder.getRoot(), plan.getScope(), plan.getFieldMappings(), outerContext);
    }

    private RelationPlan addColumnMasks(Table table, RelationPlan plan)
    {
        Map<String, List<Expression>> columnMasks = analysis.getColumnMasks(table);

        // A Table can represent a WITH query, which can have anonymous fields. On the other hand,
        // it can't have masks. The loop below expects fields to have proper names, so bail out
        // if the masks are missing
        if (columnMasks.isEmpty()) {
            return plan;
        }

        PlanBuilder planBuilder = newPlanBuilder(plan, analysis, lambdaDeclarationToSymbolMap)
                .withScope(analysis.getAccessControlScope(table), plan.getFieldMappings()); // The fields in the access control scope has the same layout as those for the table scope

        for (int i = 0; i < plan.getDescriptor().getAllFieldCount(); i++) {
            Field field = plan.getDescriptor().getFieldByIndex(i);

            for (Expression mask : columnMasks.getOrDefault(field.getName().orElseThrow(), ImmutableList.of())) {
                planBuilder = subqueryPlanner.handleSubqueries(planBuilder, mask, analysis.getSubqueries(mask));

                Map<Symbol, Expression> assignments = new LinkedHashMap<>();
                for (Symbol symbol : planBuilder.getRoot().getOutputSymbols()) {
                    assignments.put(symbol, symbol.toSymbolReference());
                }
                assignments.put(plan.getFieldMappings().get(i), coerceIfNecessary(analysis, mask, planBuilder.rewrite(mask)));

                planBuilder = planBuilder
                        .withNewRoot(new ProjectNode(
                                idAllocator.getNextId(),
                                planBuilder.getRoot(),
                                Assignments.copyOf(assignments)));
            }
        }

        return new RelationPlan(planBuilder.getRoot(), plan.getScope(), plan.getFieldMappings(), outerContext);
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

        PlanBuilder planBuilder = newPlanBuilder(subPlan, analysis, lambdaDeclarationToSymbolMap);

        // no handleSubqueries because subqueries are not allowed here
        planBuilder = planBuilder.appendProjections(inputs, symbolAllocator, idAllocator);

        ImmutableList.Builder<Symbol> outputLayout = ImmutableList.builder();
        boolean oneRowOutput = node.getRowsPerMatch().isEmpty() || node.getRowsPerMatch().get().isOneRow();

        WindowNode.Specification specification = planWindowSpecification(node.getPartitionBy(), node.getOrderBy(), planBuilder::translate);
        outputLayout.addAll(specification.getPartitionBy());
        if (!oneRowOutput) {
            getSortItemsFromOrderBy(node.getOrderBy()).stream()
                    .map(SortItem::getSortKey)
                    .map(planBuilder::translate)
                    .forEach(outputLayout::add);
        }

        planBuilder = subqueryPlanner.handleSubqueries(planBuilder, extractPatternRecognitionExpressions(node.getVariableDefinitions(), node.getMeasures()), analysis.getSubqueries(node));

        PatternRecognitionComponents components = planPatternRecognitionComponents(
                planBuilder::rewrite,
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
                node.getRowsPerMatch().orElse(ONE),
                components.getSkipToLabel(),
                components.getSkipToPosition(),
                components.isInitial(),
                components.getPattern(),
                components.getSubsets(),
                components.getVariableDefinitions());

        return new RelationPlan(planNode, analysis.getScope(node), outputLayout.build(), outerContext);
    }

    public PatternRecognitionComponents planPatternRecognitionComponents(
            Function<Expression, Expression> expressionRewrite,
            List<SubsetDefinition> subsets,
            List<MeasureDefinition> measures,
            Optional<SkipTo> skipTo,
            Optional<PatternSearchMode> searchMode,
            RowPattern pattern,
            List<VariableDefinition> variableDefinitions)
    {
        // rewrite subsets
        ImmutableMap.Builder<IrLabel, Set<IrLabel>> rewrittenSubsets = ImmutableMap.builder();
        for (SubsetDefinition subsetDefinition : subsets) {
            IrLabel label = irLabel(subsetDefinition.getName());
            Set<IrLabel> elements = subsetDefinition.getIdentifiers().stream()
                    .map(RelationPlanner::irLabel)
                    .collect(toImmutableSet());
            rewrittenSubsets.put(label, elements);
        }

        // rewrite measures
        ImmutableMap.Builder<Symbol, Measure> rewrittenMeasures = ImmutableMap.builder();
        ImmutableList.Builder<Symbol> measureOutputs = ImmutableList.builder();
        for (MeasureDefinition measureDefinition : measures) {
            Type type = analysis.getType(measureDefinition.getExpression());
            Symbol symbol = symbolAllocator.newSymbol(measureDefinition.getName().getValue().toLowerCase(ENGLISH), type);
            Expression expression = expressionRewrite.apply(measureDefinition.getExpression());
            ExpressionAndValuePointers measure = LogicalIndexExtractor.rewrite(expression, rewrittenSubsets.build(), symbolAllocator);
            rewrittenMeasures.put(symbol, new Measure(measure, type));
            measureOutputs.add(symbol);
        }

        // rewrite pattern to IR
        IrRowPattern rewrittenPattern = RowPatternToIrRewriter.rewrite(pattern, analysis);

        // rewrite variable definitions
        ImmutableMap.Builder<IrLabel, ExpressionAndValuePointers> rewrittenVariableDefinitions = ImmutableMap.builder();
        for (VariableDefinition variableDefinition : variableDefinitions) {
            IrLabel label = irLabel(variableDefinition.getName());
            Expression expression = expressionRewrite.apply(variableDefinition.getExpression());
            ExpressionAndValuePointers definition = LogicalIndexExtractor.rewrite(expression, rewrittenSubsets.build(), symbolAllocator);
            rewrittenVariableDefinitions.put(label, definition);
        }
        // add `true` definition for undefined labels
        for (String label : analysis.getUndefinedLabels(pattern)) {
            rewrittenVariableDefinitions.put(irLabel(label), ExpressionAndValuePointers.TRUE);
        }

        return new PatternRecognitionComponents(
                rewrittenSubsets.build(),
                rewrittenMeasures.build(),
                measureOutputs.build(),
                skipTo.flatMap(SkipTo::getIdentifier).map(RelationPlanner::irLabel),
                skipTo.map(SkipTo::getPosition).orElse(PAST_LAST),
                searchMode.map(mode -> mode.getMode() == INITIAL).orElse(TRUE),
                rewrittenPattern,
                rewrittenVariableDefinitions.build());
    }

    private static IrLabel irLabel(Identifier identifier)
    {
        return new IrLabel(identifier.getCanonicalValue());
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
                SampleNode.Type.fromType(node.getType()));
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

    private RelationPlan planJoin(Expression criteria, Join.Type type, Scope scope, RelationPlan leftPlan, RelationPlan rightPlan, Analysis.SubqueryAnalysis subqueries)
    {
        // NOTE: symbols must be in the same order as the outputDescriptor
        List<Symbol> outputSymbols = ImmutableList.<Symbol>builder()
                .addAll(leftPlan.getFieldMappings())
                .addAll(rightPlan.getFieldMappings())
                .build();

        PlanBuilder leftPlanBuilder = newPlanBuilder(leftPlan, analysis, lambdaDeclarationToSymbolMap)
                .withScope(scope, outputSymbols);
        PlanBuilder rightPlanBuilder = newPlanBuilder(rightPlan, analysis, lambdaDeclarationToSymbolMap)
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

            for (Expression conjunct : ExpressionUtils.extractConjuncts(criteria)) {
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

            QueryPlanner.PlanAndMappings leftCoercions = coerce(leftPlanBuilder, leftComparisonExpressions, analysis, idAllocator, symbolAllocator, typeCoercion);
            leftPlanBuilder = leftCoercions.getSubPlan();
            QueryPlanner.PlanAndMappings rightCoercions = coerce(rightPlanBuilder, rightComparisonExpressions, analysis, idAllocator, symbolAllocator, typeCoercion);
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
                JoinNode.Type.typeConvert(type),
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
        TranslationMap translationMap = new TranslationMap(outerContext, scope, analysis, lambdaDeclarationToSymbolMap, outputSymbols)
                .withAdditionalMappings(leftPlanBuilder.getTranslations().getMappings())
                .withAdditionalMappings(rightPlanBuilder.getTranslations().getMappings());

        if (type != INNER && !complexJoinExpressions.isEmpty()) {
            Expression joinedFilterCondition = ExpressionUtils.and(complexJoinExpressions);
            Expression rewrittenFilterCondition = translationMap.rewrite(joinedFilterCondition);
            root = new JoinNode(idAllocator.getNextId(),
                    JoinNode.Type.typeConvert(type),
                    leftPlanBuilder.getRoot(),
                    rightPlanBuilder.getRoot(),
                    equiClauses.build(),
                    leftPlanBuilder.getRoot().getOutputSymbols(),
                    rightPlanBuilder.getRoot().getOutputSymbols(),
                    false,
                    Optional.of(rewrittenFilterCondition),
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
                postInnerJoinConditions.add(rootPlanBuilder.rewrite(expression));
            }
            root = rootPlanBuilder.getRoot();

            Expression postInnerJoinCriteria;
            if (!postInnerJoinConditions.isEmpty()) {
                postInnerJoinCriteria = ExpressionUtils.and(postInnerJoinConditions);
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
                    false,
                    typeCoercion.isTypeOnlyCoercion(left.getDescriptor().getFieldByIndex(leftField).getType(), type)));
            leftJoinColumns.put(identifier, leftOutput);

            // compute the coercion for the field on the right to the common supertype of left & right
            Symbol rightOutput = symbolAllocator.newSymbol(identifier, type);
            int rightField = joinAnalysis.getRightJoinFields().get(i);
            rightCoercions.put(rightOutput, new Cast(
                    right.getSymbol(rightField).toSymbolReference(),
                    toSqlType(type),
                    false,
                    typeCoercion.isTypeOnlyCoercion(right.getDescriptor().getFieldByIndex(rightField).getType(), type)));
            rightJoinColumns.put(identifier, rightOutput);

            clauses.add(new JoinNode.EquiJoinClause(leftOutput, rightOutput));
        }

        ProjectNode leftCoercion = new ProjectNode(idAllocator.getNextId(), left.getRoot(), leftCoercions.build());
        ProjectNode rightCoercion = new ProjectNode(idAllocator.getNextId(), right.getRoot(), rightCoercions.build());

        JoinNode join = new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.typeConvert(node.getType()),
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
            assignments.put(symbol, symbol.toSymbolReference());
        }

        for (int field : joinAnalysis.getOtherRightFields()) {
            Symbol symbol = right.getFieldMappings().get(field);
            outputs.add(symbol);
            assignments.put(symbol, symbol.toSymbolReference());
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
        PlanBuilder leftPlanBuilder = newPlanBuilder(leftPlan, analysis, lambdaDeclarationToSymbolMap);

        RelationPlan rightPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, Optional.of(leftPlanBuilder.getTranslations()), session, recursiveSubqueries)
                .process(lateral.getQuery(), null);

        PlanBuilder rightPlanBuilder = newPlanBuilder(rightPlan, analysis, lambdaDeclarationToSymbolMap);

        Expression filterExpression;
        if (join.getCriteria().isEmpty()) {
            filterExpression = TRUE_LITERAL;
        }
        else {
            JoinCriteria criteria = join.getCriteria().get();
            if (criteria instanceof JoinUsing || criteria instanceof NaturalJoin) {
                throw semanticException(NOT_SUPPORTED, join, "Correlated join with criteria other than ON is not supported");
            }
            filterExpression = (Expression) getOnlyElement(criteria.getNodes());
        }

        List<Symbol> outputSymbols = ImmutableList.<Symbol>builder()
                .addAll(leftPlan.getFieldMappings())
                .addAll(rightPlan.getFieldMappings())
                .build();
        TranslationMap translationMap = new TranslationMap(outerContext, analysis.getScope(join), analysis, lambdaDeclarationToSymbolMap, outputSymbols)
                .withAdditionalMappings(leftPlanBuilder.getTranslations().getMappings())
                .withAdditionalMappings(rightPlanBuilder.getTranslations().getMappings());

        Expression rewrittenFilterCondition = translationMap.rewrite(filterExpression);

        PlanBuilder planBuilder = subqueryPlanner.appendCorrelatedJoin(
                leftPlanBuilder,
                rightPlanBuilder.getRoot(),
                lateral.getQuery(),
                CorrelatedJoinNode.Type.typeConvert(join.getType()),
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
        Optional<Expression> filterExpression = Optional.empty();
        if (joinNode.getCriteria().isPresent()) {
            JoinCriteria criteria = joinNode.getCriteria().get();
            if (criteria instanceof NaturalJoin) {
                throw semanticException(NOT_SUPPORTED, joinNode, "Natural join involving UNNEST is not supported");
            }
            if (criteria instanceof JoinUsing) {
                throw semanticException(NOT_SUPPORTED, joinNode, "USING for join involving UNNEST is not supported");
            }
            Expression filter = (Expression) getOnlyElement(criteria.getNodes());
            if (filter.equals(TRUE_LITERAL)) {
                filterExpression = Optional.of(filter);
            }
            else { //TODO rewrite filter to support non-trivial join criteria
                throw semanticException(NOT_SUPPORTED, joinNode, "JOIN involving UNNEST on condition other than TRUE is not supported");
            }
        }

        return planUnnest(
                newPlanBuilder(leftPlan, analysis, lambdaDeclarationToSymbolMap),
                node,
                leftPlan.getFieldMappings(),
                filterExpression,
                joinNode.getType(),
                analysis.getScope(joinNode));
    }

    private RelationPlan planUnnest(PlanBuilder subPlan, Unnest node, List<Symbol> replicatedColumns, Optional<Expression> filter, Join.Type type, Scope outputScope)
    {
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
                JoinNode.Type.typeConvert(type),
                filter);

        // TODO: Technically, we should derive the field mappings from the layout of fields and how they relate to the output symbols of the Unnest node.
        //       That's tricky to do for a Join+Unnest because the allocations come from the Unnest, but the mappings need to be done based on the Join output fields.
        //       Currently, it works out because, by construction, the order of the output symbols in the UnnestNode will match the order of the fields in the Join node.
        return new RelationPlan(unnestNode, outputScope, unnestNode.getOutputSymbols(), outerContext);
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
        return new QueryPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, outerContext, session, recursiveSubqueries)
                .plan(node);
    }

    @Override
    protected RelationPlan visitQuerySpecification(QuerySpecification node, Void context)
    {
        return new QueryPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, outerContext, session, recursiveSubqueries)
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
        TranslationMap translationMap = new TranslationMap(outerContext, analysis.getScope(node), analysis, lambdaDeclarationToSymbolMap, outputSymbols);

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
                Optional.empty(),
                INNER,
                scope);
    }

    private PlanBuilder planSingleEmptyRow(Optional<Scope> parent)
    {
        Scope.Builder scope = Scope.builder();
        parent.ifPresent(scope::withOuterQueryParent);

        PlanNode values = new ValuesNode(idAllocator.getNextId(), 1);
        TranslationMap translations = new TranslationMap(outerContext, scope.build(), analysis, lambdaDeclarationToSymbolMap, ImmutableList.of());
        return new PlanBuilder(translations, values);
    }

    @Override
    protected RelationPlan visitUnion(Union node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for UNION");

        SetOperationPlan setOperationPlan = process(node);

        PlanNode planNode = new UnionNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getSymbolMapping(), ImmutableList.copyOf(setOperationPlan.getSymbolMapping().keySet()));
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

        PlanNode planNode = new IntersectNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getSymbolMapping(), ImmutableList.copyOf(setOperationPlan.getSymbolMapping().keySet()), node.isDistinct());
        return new RelationPlan(planNode, analysis.getScope(node), planNode.getOutputSymbols(), outerContext);
    }

    @Override
    protected RelationPlan visitExcept(Except node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for EXCEPT");

        SetOperationPlan setOperationPlan = process(node);

        PlanNode planNode = new ExceptNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getSymbolMapping(), ImmutableList.copyOf(setOperationPlan.getSymbolMapping().keySet()), node.isDistinct());
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
        return new AggregationNode(idAllocator.getNextId(),
                node,
                ImmutableMap.of(),
                singleGroupingSet(node.getOutputSymbols()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());
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
        private final Map<IrLabel, Set<IrLabel>> subsets;
        private final Map<Symbol, Measure> measures;
        private final List<Symbol> measureOutputs;
        private final Optional<IrLabel> skipToLabel;
        private final SkipTo.Position skipToPosition;
        private final boolean initial;
        private final IrRowPattern pattern;
        private final Map<IrLabel, ExpressionAndValuePointers> variableDefinitions;

        public PatternRecognitionComponents(
                Map<IrLabel, Set<IrLabel>> subsets,
                Map<Symbol, Measure> measures,
                List<Symbol> measureOutputs,
                Optional<IrLabel> skipToLabel,
                SkipTo.Position skipToPosition,
                boolean initial,
                IrRowPattern pattern,
                Map<IrLabel, ExpressionAndValuePointers> variableDefinitions)
        {
            this.subsets = requireNonNull(subsets, "subsets is null");
            this.measures = requireNonNull(measures, "measures is null");
            this.measureOutputs = requireNonNull(measureOutputs, "measureOutputs is null");
            this.skipToLabel = requireNonNull(skipToLabel, "skipToLabel is null");
            this.skipToPosition = requireNonNull(skipToPosition, "skipToPosition is null");
            this.initial = initial;
            this.pattern = requireNonNull(pattern, "pattern is null");
            this.variableDefinitions = requireNonNull(variableDefinitions, "variableDefinitions is null");
        }

        public Map<IrLabel, Set<IrLabel>> getSubsets()
        {
            return subsets;
        }

        public Map<Symbol, Measure> getMeasures()
        {
            return measures;
        }

        public List<Symbol> getMeasureOutputs()
        {
            return measureOutputs;
        }

        public Optional<IrLabel> getSkipToLabel()
        {
            return skipToLabel;
        }

        public SkipTo.Position getSkipToPosition()
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
