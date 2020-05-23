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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.analyzer.Analysis.UnnestAnalysis;
import io.prestosql.sql.analyzer.Field;
import io.prestosql.sql.analyzer.RelationId;
import io.prestosql.sql.analyzer.RelationType;
import io.prestosql.sql.analyzer.Scope;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.CorrelatedJoinNode;
import io.prestosql.sql.planner.plan.ExceptNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.IntersectNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.SampleNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.UnionNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.tree.AliasedRelation;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CoalesceExpression;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Except;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.Intersect;
import io.prestosql.sql.tree.Join;
import io.prestosql.sql.tree.JoinCriteria;
import io.prestosql.sql.tree.JoinUsing;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.Lateral;
import io.prestosql.sql.tree.NaturalJoin;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.Relation;
import io.prestosql.sql.tree.Row;
import io.prestosql.sql.tree.SampledRelation;
import io.prestosql.sql.tree.SetOperation;
import io.prestosql.sql.tree.SubqueryExpression;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.Table;
import io.prestosql.sql.tree.TableSubquery;
import io.prestosql.sql.tree.Union;
import io.prestosql.sql.tree.Unnest;
import io.prestosql.sql.tree.Values;
import io.prestosql.type.TypeCoercion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;
import static io.prestosql.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.prestosql.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.Join.Type.CROSS;
import static io.prestosql.sql.tree.Join.Type.IMPLICIT;
import static io.prestosql.sql.tree.Join.Type.INNER;
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
    private final Session session;
    private final SubqueryPlanner subqueryPlanner;

    RelationPlanner(
            Analysis analysis,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap,
            Metadata metadata,
            Session session)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(lambdaDeclarationToSymbolMap, "lambdaDeclarationToSymbolMap is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
        this.lambdaDeclarationToSymbolMap = lambdaDeclarationToSymbolMap;
        this.metadata = metadata;
        this.typeCoercion = new TypeCoercion(metadata::getType);
        this.session = session;
        this.subqueryPlanner = new SubqueryPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, session);
    }

    @Override
    protected RelationPlan visitNode(Node node, Void context)
    {
        throw new IllegalStateException("Unsupported node type: " + node.getClass().getName());
    }

    @Override
    protected RelationPlan visitTable(Table node, Void context)
    {
        Query namedQuery = analysis.getNamedQuery(node);
        Scope scope = analysis.getScope(node);

        RelationPlan plan;
        if (namedQuery != null) {
            RelationPlan subPlan = process(namedQuery, null);

            // Add implicit coercions if view query produces types that don't match the declared output types
            // of the view (e.g., if the underlying tables referenced by the view changed)
            Type[] types = scope.getRelationType().getAllFields().stream().map(Field::getType).toArray(Type[]::new);
            RelationPlan withCoercions = addCoercions(subPlan, types);

            plan = new RelationPlan(withCoercions.getRoot(), scope, withCoercions.getFieldMappings());
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
            PlanNode root = TableScanNode.newInstance(idAllocator.getNextId(), handle, outputSymbols, columns.build());

            plan = new RelationPlan(root, scope, outputSymbols);
        }

        plan = addRowFilters(node, plan);
        plan = addColumnMasks(node, plan);

        return plan;
    }

    private RelationPlan addRowFilters(Table node, RelationPlan plan)
    {
        PlanBuilder planBuilder = initializePlanBuilder(plan);

        for (Expression filter : analysis.getRowFilters(node)) {
            planBuilder = subqueryPlanner.handleSubqueries(planBuilder, filter, filter);

            planBuilder = planBuilder.withNewRoot(new FilterNode(
                    idAllocator.getNextId(),
                    planBuilder.getRoot(),
                    planBuilder.rewrite(filter)));
        }

        return new RelationPlan(planBuilder.getRoot(), plan.getScope(), plan.getFieldMappings());
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

        PlanBuilder planBuilder = initializePlanBuilder(plan);
        TranslationMap translations = planBuilder.getTranslations();

        for (int i = 0; i < plan.getDescriptor().getAllFieldCount(); i++) {
            Field field = plan.getDescriptor().getFieldByIndex(i);

            for (Expression mask : columnMasks.getOrDefault(field.getName().get(), ImmutableList.of())) {
                planBuilder = subqueryPlanner.handleSubqueries(planBuilder, mask, mask);

                Map<Symbol, Expression> assignments = new LinkedHashMap<>();
                for (Symbol symbol : plan.getRoot().getOutputSymbols()) {
                    assignments.put(symbol, symbol.toSymbolReference());
                }
                assignments.put(plan.getFieldMappings().get(i), translations.rewrite(mask));

                planBuilder = planBuilder.withNewRoot(new ProjectNode(
                        idAllocator.getNextId(),
                        planBuilder.getRoot(),
                        Assignments.copyOf(assignments)));
            }
        }

        return new RelationPlan(planBuilder.getRoot(), plan.getScope(), plan.getFieldMappings());
    }

    @Override
    protected RelationPlan visitAliasedRelation(AliasedRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        PlanNode root = subPlan.getRoot();
        List<Symbol> mappings = subPlan.getFieldMappings();

        if (node.getColumnNames() != null) {
            ImmutableList.Builder<Symbol> newMappings = ImmutableList.builder();
            Assignments.Builder assignments = Assignments.builder();

            // project only the visible columns from the underlying relation
            for (int i = 0; i < subPlan.getDescriptor().getAllFieldCount(); i++) {
                Field field = subPlan.getDescriptor().getFieldByIndex(i);
                if (!field.isHidden()) {
                    Symbol aliasedColumn = symbolAllocator.newSymbol(field);
                    assignments.put(aliasedColumn, subPlan.getFieldMappings().get(i).toSymbolReference());
                    newMappings.add(aliasedColumn);
                }
            }

            root = new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), assignments.build());
            mappings = newMappings.build();
        }

        return new RelationPlan(root, analysis.getScope(node), mappings);
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
        return new RelationPlan(planNode, analysis.getScope(node), subPlan.getFieldMappings());
    }

    @Override
    protected RelationPlan visitLateral(Lateral node, Void context)
    {
        return process(node.getQuery(), context);
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

        PlanBuilder leftPlanBuilder = initializePlanBuilder(leftPlan);
        PlanBuilder rightPlanBuilder = initializePlanBuilder(rightPlan);

        // NOTE: symbols must be in the same order as the outputDescriptor
        List<Symbol> outputSymbols = ImmutableList.<Symbol>builder()
                .addAll(leftPlan.getFieldMappings())
                .addAll(rightPlan.getFieldMappings())
                .build();

        ImmutableList.Builder<JoinNode.EquiJoinClause> equiClauses = ImmutableList.builder();
        List<Expression> complexJoinExpressions = new ArrayList<>();
        List<Expression> postInnerJoinConditions = new ArrayList<>();

        if (node.getType() != CROSS && node.getType() != IMPLICIT) {
            Expression criteria = analysis.getJoinCriteria(node);

            RelationType left = analysis.getOutputDescriptor(node.getLeft());
            RelationType right = analysis.getOutputDescriptor(node.getRight());

            List<Expression> leftComparisonExpressions = new ArrayList<>();
            List<Expression> rightComparisonExpressions = new ArrayList<>();
            List<ComparisonExpression.Operator> joinConditionComparisonOperators = new ArrayList<>();

            for (Expression conjunct : ExpressionUtils.extractConjuncts(criteria)) {
                conjunct = ExpressionUtils.normalize(conjunct);

                if (!isEqualComparisonExpression(conjunct) && node.getType() != INNER) {
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

            leftPlanBuilder = subqueryPlanner.handleSubqueries(leftPlanBuilder, leftComparisonExpressions, node);
            rightPlanBuilder = subqueryPlanner.handleSubqueries(rightPlanBuilder, rightComparisonExpressions, node);

            // Add projections for join criteria
            leftPlanBuilder = leftPlanBuilder.appendProjections(leftComparisonExpressions, symbolAllocator, idAllocator);
            rightPlanBuilder = rightPlanBuilder.appendProjections(rightComparisonExpressions, symbolAllocator, idAllocator);

            for (int i = 0; i < leftComparisonExpressions.size(); i++) {
                if (joinConditionComparisonOperators.get(i) == ComparisonExpression.Operator.EQUAL) {
                    Symbol leftSymbol = leftPlanBuilder.translate(leftComparisonExpressions.get(i));
                    Symbol rightSymbol = rightPlanBuilder.translate(rightComparisonExpressions.get(i));

                    equiClauses.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
                }
                else {
                    Expression leftExpression = leftPlanBuilder.rewrite(leftComparisonExpressions.get(i));
                    Expression rightExpression = rightPlanBuilder.rewrite(rightComparisonExpressions.get(i));
                    postInnerJoinConditions.add(new ComparisonExpression(joinConditionComparisonOperators.get(i), leftExpression, rightExpression));
                }
            }
        }

        PlanNode root = new JoinNode(idAllocator.getNextId(),
                JoinNode.Type.typeConvert(node.getType()),
                leftPlanBuilder.getRoot(),
                rightPlanBuilder.getRoot(),
                equiClauses.build(),
                leftPlanBuilder.getRoot().getOutputSymbols(),
                rightPlanBuilder.getRoot().getOutputSymbols(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        if (node.getType() != INNER) {
            for (Expression complexExpression : complexJoinExpressions) {
                Set<InPredicate> inPredicates = subqueryPlanner.collectInPredicateSubqueries(complexExpression, node);
                if (!inPredicates.isEmpty()) {
                    InPredicate inPredicate = Iterables.getLast(inPredicates);
                    throw semanticException(NOT_SUPPORTED, inPredicate, "IN with subquery predicate in join condition is not supported");
                }
            }

            // subqueries can be applied only to one side of join - left side is selected in arbitrary way
            leftPlanBuilder = subqueryPlanner.handleSubqueries(leftPlanBuilder, complexJoinExpressions, node);
        }
        TranslationMap translationMap = initializeTranslationMap(node, outputSymbols);
        translationMap.setFieldMappings(outputSymbols);
        translationMap.putExpressionMappingsFrom(leftPlanBuilder.getTranslations());
        translationMap.putExpressionMappingsFrom(rightPlanBuilder.getTranslations());

        if (node.getType() != INNER && !complexJoinExpressions.isEmpty()) {
            Expression joinedFilterCondition = ExpressionUtils.and(complexJoinExpressions);
            Expression rewrittenFilterCondition = translationMap.rewrite(joinedFilterCondition);
            root = new JoinNode(idAllocator.getNextId(),
                    JoinNode.Type.typeConvert(node.getType()),
                    leftPlanBuilder.getRoot(),
                    rightPlanBuilder.getRoot(),
                    equiClauses.build(),
                    leftPlanBuilder.getRoot().getOutputSymbols(),
                    rightPlanBuilder.getRoot().getOutputSymbols(),
                    Optional.of(rewrittenFilterCondition),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of(),
                    Optional.empty());
        }

        if (node.getType() == INNER) {
            // rewrite all the other conditions using output symbols from left + right plan node.
            PlanBuilder rootPlanBuilder = new PlanBuilder(translationMap, root);
            rootPlanBuilder = subqueryPlanner.handleSubqueries(rootPlanBuilder, complexJoinExpressions, node);

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

        return new RelationPlan(root, analysis.getScope(node), outputSymbols);
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

        List<Identifier> joinColumns = ((JoinUsing) node.getCriteria().get()).getColumns();

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
                outputs.build());
    }

    private Optional<Unnest> getUnnest(Relation relation)
    {
        if (relation instanceof AliasedRelation) {
            return getUnnest(((AliasedRelation) relation).getRelation());
        }
        if (relation instanceof Unnest) {
            return Optional.of((Unnest) relation);
        }
        return Optional.empty();
    }

    private Optional<Lateral> getLateral(Relation relation)
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
        RelationPlan rightPlan = process(lateral.getQuery(), null);
        PlanBuilder leftPlanBuilder = initializePlanBuilder(leftPlan);
        PlanBuilder rightPlanBuilder = initializePlanBuilder(rightPlan);

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
        TranslationMap translationMap = initializeTranslationMap(join, outputSymbols);
        translationMap.setFieldMappings(outputSymbols);
        translationMap.putExpressionMappingsFrom(leftPlanBuilder.getTranslations());
        translationMap.putExpressionMappingsFrom(rightPlanBuilder.getTranslations());

        Expression rewrittenFilterCondition = translationMap.rewrite(filterExpression);

        PlanBuilder planBuilder = subqueryPlanner.appendCorrelatedJoin(
                leftPlanBuilder,
                rightPlanBuilder,
                lateral.getQuery(),
                CorrelatedJoinNode.Type.typeConvert(join.getType()),
                rewrittenFilterCondition);

        return new RelationPlan(planBuilder.getRoot(), analysis.getScope(join), outputSymbols);
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
                initializePlanBuilder(leftPlan),
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
        return new RelationPlan(unnestNode, outputScope, unnestNode.getOutputSymbols());
    }

    @Override
    protected RelationPlan visitTableSubquery(TableSubquery node, Void context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected RelationPlan visitQuery(Query node, Void context)
    {
        return new QueryPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, session)
                .plan(node);
    }

    @Override
    protected RelationPlan visitQuerySpecification(QuerySpecification node, Void context)
    {
        return new QueryPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, session)
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
        TranslationMap translationMap = initializeTranslationMap(node, outputSymbols);

        ImmutableList.Builder<List<Expression>> rows = ImmutableList.builder();
        for (Expression row : node.getRows()) {
            ImmutableList.Builder<Expression> values = ImmutableList.builder();
            if (row instanceof Row) {
                for (Expression item : ((Row) row).getItems()) {
                    Expression expression = translationMap.rewrite(item);
                    values.add(ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(analysis), expression));
                }
            }
            else {
                Expression expression = translationMap.rewrite(row);
                values.add(ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(analysis), expression));
            }

            rows.add(values.build());
        }

        ValuesNode valuesNode = new ValuesNode(idAllocator.getNextId(), outputSymbols, rows.build());
        return new RelationPlan(valuesNode, scope, outputSymbols);
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

        PlanNode values = new ValuesNode(idAllocator.getNextId(), ImmutableList.of(), ImmutableList.of(ImmutableList.of()));
        TranslationMap translations = new TranslationMap(new RelationPlan(values, scope.build(), ImmutableList.of()), analysis, lambdaDeclarationToSymbolMap);
        return new PlanBuilder(translations, values);
    }

    private TranslationMap initializeTranslationMap(Node node, List<Symbol> outputSymbols)
    {
        PlanNode dummy = new ValuesNode(
                idAllocator.getNextId(),
                ImmutableList.copyOf(requireNonNull(outputSymbols, "outputSymbols is null")),
                ImmutableList.of());

        RelationPlan dummyRelationPlan = new RelationPlan(dummy, analysis.getScope(node), outputSymbols);
        return new TranslationMap(dummyRelationPlan, analysis, lambdaDeclarationToSymbolMap);
    }

    private RelationPlan processAndCoerceIfNecessary(Relation node, Void context)
    {
        Type[] coerceToTypes = analysis.getRelationCoercion(node);

        RelationPlan plan = this.process(node, context);

        if (coerceToTypes == null) {
            return plan;
        }

        return addCoercions(plan, coerceToTypes);
    }

    private RelationPlan addCoercions(RelationPlan plan, Type[] targetColumnTypes)
    {
        List<Symbol> oldSymbols = plan.getFieldMappings();
        RelationType oldDescriptor = plan.getDescriptor().withOnlyVisibleFields();
        verify(targetColumnTypes.length == oldSymbols.size());
        ImmutableList.Builder<Symbol> newSymbols = new ImmutableList.Builder<>();
        Field[] newFields = new Field[targetColumnTypes.length];
        Assignments.Builder assignments = Assignments.builder();
        for (int i = 0; i < targetColumnTypes.length; i++) {
            Symbol inputSymbol = oldSymbols.get(i);
            Type inputType = symbolAllocator.getTypes().get(inputSymbol);
            Type outputType = targetColumnTypes[i];
            if (!outputType.equals(inputType)) {
                Expression cast = new Cast(inputSymbol.toSymbolReference(), toSqlType(outputType));
                Symbol outputSymbol = symbolAllocator.newSymbol(cast, outputType);
                assignments.put(outputSymbol, cast);
                newSymbols.add(outputSymbol);
            }
            else {
                SymbolReference symbolReference = inputSymbol.toSymbolReference();
                Symbol outputSymbol = symbolAllocator.newSymbol(symbolReference, outputType);
                assignments.put(outputSymbol, symbolReference);
                newSymbols.add(outputSymbol);
            }
            Field oldField = oldDescriptor.getFieldByIndex(i);
            newFields[i] = new Field(
                    oldField.getRelationAlias(),
                    oldField.getName(),
                    targetColumnTypes[i],
                    oldField.isHidden(),
                    oldField.getOriginTable(),
                    oldField.getOriginColumnName(),
                    oldField.isAliased());
        }
        ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments.build());
        return new RelationPlan(projectNode, Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(newFields)).build(), newSymbols.build());
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
        return new RelationPlan(planNode, analysis.getScope(node), planNode.getOutputSymbols());
    }

    @Override
    protected RelationPlan visitIntersect(Intersect node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for INTERSECT");

        SetOperationPlan setOperationPlan = process(node);

        PlanNode planNode = new IntersectNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getSymbolMapping(), ImmutableList.copyOf(setOperationPlan.getSymbolMapping().keySet()));
        return new RelationPlan(planNode, analysis.getScope(node), planNode.getOutputSymbols());
    }

    @Override
    protected RelationPlan visitExcept(Except node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for EXCEPT");

        SetOperationPlan setOperationPlan = process(node);

        PlanNode planNode = new ExceptNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getSymbolMapping(), ImmutableList.copyOf(setOperationPlan.getSymbolMapping().keySet()));
        return new RelationPlan(planNode, analysis.getScope(node), planNode.getOutputSymbols());
    }

    private SetOperationPlan process(SetOperation node)
    {
        List<Symbol> outputs = null;
        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
        ImmutableListMultimap.Builder<Symbol, Symbol> symbolMapping = ImmutableListMultimap.builder();

        List<RelationPlan> subPlans = node.getRelations().stream()
                .map(relation -> processAndCoerceIfNecessary(relation, null))
                .collect(toImmutableList());

        for (RelationPlan relationPlan : subPlans) {
            List<Symbol> childOutputSymbols = relationPlan.getFieldMappings();
            if (outputs == null) {
                // Use the first Relation to derive output symbol names
                RelationType descriptor = relationPlan.getDescriptor();
                ImmutableList.Builder<Symbol> outputSymbolBuilder = ImmutableList.builder();
                for (Field field : descriptor.getVisibleFields()) {
                    int fieldIndex = descriptor.indexOf(field);
                    Symbol symbol = childOutputSymbols.get(fieldIndex);
                    outputSymbolBuilder.add(symbolAllocator.newSymbol(symbol.getName(), symbolAllocator.getTypes().get(symbol)));
                }
                outputs = outputSymbolBuilder.build();
            }

            RelationType descriptor = relationPlan.getDescriptor();
            checkArgument(descriptor.getVisibleFieldCount() == outputs.size(),
                    "Expected relation to have %s symbols but has %s symbols",
                    descriptor.getVisibleFieldCount(),
                    outputs.size());

            int fieldId = 0;
            for (Field field : descriptor.getVisibleFields()) {
                int fieldIndex = descriptor.indexOf(field);
                symbolMapping.put(outputs.get(fieldId), childOutputSymbols.get(fieldIndex));
                fieldId++;
            }

            sources.add(relationPlan.getRoot());
        }

        return new SetOperationPlan(sources.build(), symbolMapping.build());
    }

    private PlanBuilder initializePlanBuilder(RelationPlan relationPlan)
    {
        TranslationMap translations = new TranslationMap(relationPlan, analysis, lambdaDeclarationToSymbolMap);

        // Make field->symbol mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the underlying tuple directly
        translations.setFieldMappings(relationPlan.getFieldMappings());

        return new PlanBuilder(translations, relationPlan.getRoot());
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

    private static class SetOperationPlan
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
}
