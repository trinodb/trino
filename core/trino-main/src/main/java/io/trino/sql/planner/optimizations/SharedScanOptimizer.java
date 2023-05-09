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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.cost.TableStatsProvider;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.ExpressionUtils;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.EqualityInference;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.rule.ReorderJoins;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.WindowFrame;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.useQueryFusion;
import static io.trino.SystemSessionProperties.useQueryFusionForGbjoingb;
import static io.trino.SystemSessionProperties.useQueryFusionForJoingb;
import static io.trino.SystemSessionProperties.useQueryFusionForPushunionbelowjoin;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.EqualityInference.nonInferrableConjuncts;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.AggregationNode.Aggregation;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class SharedScanOptimizer
        implements PlanOptimizer
{
    private static final Logger log = Logger.get(SharedScanOptimizer.class);

    private final PlannerContext plannerContext;
    private final TypeAnalyzer typeAnalyzer;

    // query rewrite rules
    public static final String JOIN_GB_RULE = "JoinGb";
    public static final String GB_JOIN_GB_RULE = "GbJoinGb";
    public static final String PUSH_UNION_BELOW_JOIN_RULE = "PushUnionBelowJoin";

    public SharedScanOptimizer(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public PlanNode optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector,
            TableStatsProvider tableStatsProvider)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(warningCollector, "warningCollector is null");
        requireNonNull(tableStatsProvider, "tableStatsProvider is null");

        if (!useQueryFusion(session) ||
                !(useQueryFusionForJoingb(session) ||
                        useQueryFusionForGbjoingb(session) ||
                        useQueryFusionForPushunionbelowjoin(session))) {
            return plan;
        }

        // Before optimization, find all TableScans in the plan tree
        List<TableScanNode> tableScanNodesBefore = findTableScanNodes(plan);
        // Build a frequency map for TableScan wrappers
        Map<TableScanFusionWrapper, Long> tableScanFreqMapBefore = tableScanNodesBefore.stream()
                .collect(
                        Collectors.groupingBy(
                                TableScanFusionWrapper::toWrapper,
                                HashMap::new,
                                Collectors.counting()));

        // Nothing to be fused if there is only one TableScan, or only one instance of TableScan per TableHandle
        if (tableScanNodesBefore.size() <= 1 ||
                tableScanFreqMapBefore.values().stream().allMatch(count -> count.equals(1L))) {
            return plan;
        }

        log.info("Starting Query Fusion optimization");
        log.info("Before Query Fusion, there are %d TableScans: %s", tableScanNodesBefore.size(), tableScanFreqMapBefore);

        // Optimize the plan tree
        PlanNode result = SimplePlanRewriter.rewriteWith(new SharedScanRewriter(idAllocator, symbolAllocator, plannerContext, session, typeAnalyzer, types), plan);

        if (result == plan) {
            log.info("Query Fusion finishes evaluation without changing the plan");
            return plan;
        }

        // After optimization, find all TableScans in the plan tree
        List<TableScanNode> tableScanNodesAfter = findTableScanNodes(result);
        Map<TableScanFusionWrapper, Long> tableScanFreqMapAfter = tableScanNodesAfter.stream()
                .collect(
                        Collectors.groupingBy(
                                TableScanFusionWrapper::toWrapper,
                                HashMap::new,
                                Collectors.counting()));

        log.info("After Query Fusion, there are %d TableScans: %s", tableScanNodesAfter.size(), tableScanFreqMapAfter);

        return result;
    }

    // A wrapper class used solely for table handle equality checks based on fusibility (not the actual object equality)
    private record TableScanFusionWrapper(TableScanNode tableScanNode)
    {
        public static TableScanFusionWrapper toWrapper(TableScanNode tableScanNode)
        {
            return new TableScanFusionWrapper(tableScanNode);
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
            TableScanFusionWrapper other = (TableScanFusionWrapper) o;
            return tableScanNode.getTable().equalsForFusion(other.tableScanNode.getTable());
        }

        @Override
        public int hashCode()
        {
            return tableScanNode.getTable().hashCodeForFusion();
        }

        @Override
        public String toString()
        {
            return tableScanNode.toString();
        }
    }

    /**
     * @param planNode Fused result.
     * @param mapping Mapping from the original 'right' columns to equivalent ones in planNode (for those that changed).
     * @param leftExpr Post-filters to apply to planNode to recover the original 'left' expressions.
     * @param rightExpr Post-filters to apply to planNode to recover the original 'right' expressions.
     */
    private record FusionResult(PlanNode planNode, Map<Symbol, Symbol> mapping, Expression leftExpr, Expression rightExpr)
    {
        private FusionResult {
            requireNonNull(planNode, "planNode is null");
            requireNonNull(mapping, "mapping is null");
            requireNonNull(leftExpr, "leftExpr is null");
            requireNonNull(rightExpr, "rightExpr is null");
        }

        public boolean hasLeftExpr()
        {
            return !leftExpr.equals(TRUE_LITERAL);
        }

        public boolean hasRightExpr()
        {
            return !rightExpr.equals(TRUE_LITERAL);
        }
    }

    private record FusionContext(PlanNode planNode) {}

    private static class FusionVisitor
            extends PlanVisitor<FusionResult, FusionContext>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final PlannerContext plannerContext;
        private final Session session;

        private FusionVisitor(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, PlannerContext plannerContext, Session session)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.session = requireNonNull(session, "session is null");
        }

        public FusionResult tryFuse(PlanNode planLeft, PlanNode planRight)
        {
            checkState(Collections.disjoint(planLeft.getOutputSymbols(), planRight.getOutputSymbols()));

            // Simple forwarding case for same operator class.
            if (planLeft.getClass().equals(planRight.getClass())) {
                return planLeft.accept(this, new FusionContext(planRight));
            }

            // Simple 'compensating' transformations
            if (planLeft instanceof ProjectNode project) {
                // Skip identity projects, or align with identities on the other side
                return project.isIdentity() ?
                        tryFuse(project.getSource(), planRight) :
                        tryFuse(project, new ProjectNode(idAllocator.getNextId(), planRight, Assignments.identity(planRight.getOutputSymbols())));
            }
            else if (planRight instanceof ProjectNode project) {
                return project.isIdentity() ?
                        tryFuse(planLeft, project.getSource()) :
                        tryFuse(new ProjectNode(idAllocator.getNextId(), planLeft, Assignments.identity(planLeft.getOutputSymbols())), planRight);
            }
            else if (planLeft instanceof MarkDistinctNode markDistinct) {
                return tryFuse(planLeft, new MarkDistinctNode(idAllocator.getNextId(), planRight, symbolAllocator.newSymbol("out", BOOLEAN), markDistinct.getDistinctSymbols(), markDistinct.getHashSymbol()));
            }
            else if (planRight instanceof MarkDistinctNode markDistinct) {
                return tryFuse(new MarkDistinctNode(idAllocator.getNextId(), planLeft, symbolAllocator.newSymbol("out", BOOLEAN), markDistinct.getDistinctSymbols(), markDistinct.getHashSymbol()), planRight);
            }
            else if (planLeft instanceof FilterNode) {
                // Align filters with TRUE
                return tryFuse(planLeft, new FilterNode(idAllocator.getNextId(), planRight, TRUE_LITERAL));
            }
            else if (planRight instanceof FilterNode) {
                // Align filters with TRUE
                return tryFuse(new FilterNode(idAllocator.getNextId(), planLeft, TRUE_LITERAL), planRight);
            }

            return null;
        }

        @Override
        protected FusionResult visitPlan(PlanNode node, FusionContext context)
        {
            // No fusion by default.
            return null;
        }

        @Override
        public FusionResult visitTableScan(TableScanNode node, FusionContext context)
        {
            TableScanNode rightNode = (TableScanNode) context.planNode;

            // Two TableScans need to be "equal" (in the sense of fusibility) in order to be eligible for fusion
            // We first check TableHandle's equality, and then less strictly, TableHandle's fusibility.
            // Fusibility checks by and large does the same job as equality checks, except it has fewer fields to compare.
            if (!(rightNode.getTable().equals(node.getTable()) || rightNode.getTable().equalsForFusion(node.getTable()))
                    || !node.getEnforcedConstraint().equals(rightNode.getEnforcedConstraint())) {
                return null;
            }

            HashMap<Symbol, Symbol> mapping = new HashMap<>();

            Map<ColumnHandle, Symbol> inverseAssignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

            ImmutableMap.Builder<Symbol, ColumnHandle> builder = ImmutableMap.builder();
            builder.putAll(node.getAssignments());
            rightNode.getAssignments().forEach((symbol, handle) -> {
                if (inverseAssignments.containsKey(handle)) {
                    mapping.put(symbol, inverseAssignments.get(handle));
                }
                else {
                    builder.put(symbol, handle);
                }
            });

            Map<Symbol, ColumnHandle> newAssignments = builder.buildOrThrow();
            List<Symbol> newOutputs = newAssignments.keySet().stream().collect(toImmutableList());

            PlanNode tableScanNode = new TableScanNode(
                    idAllocator.getNextId(),
                    node.getTable(),
                    newOutputs,
                    newAssignments,
                    node.getEnforcedConstraint(),
                    node.getStatistics(),
                    node.isUpdateTarget(),
                    node.getUseConnectorNodePartitioning());

            return new FusionResult(tableScanNode, mapping, TRUE_LITERAL, TRUE_LITERAL);
        }

        @Override
        public FusionResult visitFilter(FilterNode node, FusionContext context)
        {
            FilterNode rightNode = (FilterNode) context.planNode();

            FusionResult sourceFusionResult = tryFuse(node.getSource(), rightNode.getSource());
            if (sourceFusionResult == null) {
                return null;
            }

            SymbolMapper mapper = SymbolMapper.symbolMapper(sourceFusionResult.mapping());

            Expression rightPredicate = mapper.map(rightNode.getPredicate());
            if (node.getPredicate().equals(rightPredicate)) {
                return new FusionResult(
                        node.replaceChildren(ImmutableList.of(sourceFusionResult.planNode())),
                        sourceFusionResult.mapping(),
                        sourceFusionResult.leftExpr(),
                        sourceFusionResult.rightExpr());
            }

            return new FusionResult(
                    new FilterNode(
                            idAllocator.getNextId(),
                            sourceFusionResult.planNode(),
                            ExpressionUtils.combineDisjuncts(plannerContext.getMetadata(), node.getPredicate(), rightPredicate)),
                    sourceFusionResult.mapping(),
                    ExpressionUtils.combineConjuncts(plannerContext.getMetadata(), sourceFusionResult.leftExpr(), node.getPredicate()),
                    ExpressionUtils.combineConjuncts(plannerContext.getMetadata(), sourceFusionResult.rightExpr(), rightPredicate));
        }

        @Override
        public FusionResult visitProject(ProjectNode node, FusionContext context)
        {
            ProjectNode rightNode = (ProjectNode) context.planNode();

            FusionResult sourceFusionResult = tryFuse(node.getSource(), rightNode.getSource());
            if (sourceFusionResult == null) {
                return null;
            }

            SymbolMapper mapper = SymbolMapper.symbolMapper(sourceFusionResult.mapping());

            Map<Symbol, Symbol> newMapping = new HashMap<>(sourceFusionResult.mapping());

            HashMap<Symbol, Expression> newAssignment = new HashMap<>(node.getAssignments().getMap());

            HashMap<Expression, Symbol> inverseAssignment = new HashMap<>();
            newAssignment.forEach((key, value) -> inverseAssignment.put(value, key));

            for (Map.Entry<Symbol, Expression> entry : rightNode.getAssignments().entrySet()) {
                Symbol newSymbol = mapper.map(entry.getKey());
                Expression newExpr = mapper.map(entry.getValue());
                if (newAssignment.containsKey(newSymbol)) {
                    if (!newExpr.equals(newAssignment.get(newSymbol))) {
                        log.info("Inconsistent symbol mapping found. Symbol: %s, new mapping: %s, old mapping: %s", newSymbol, newAssignment.get(newSymbol), newExpr);
                        log.info("This is likely due to column masking expression.");
                    }
                }
                else if (inverseAssignment.containsKey(newExpr)) {
                    newMapping.put(newSymbol, inverseAssignment.get(newExpr));
                }
                else {
                    newAssignment.put(newSymbol, newExpr);
                    inverseAssignment.put(newExpr, newSymbol);
                }
            }

            // Add identities from symbols that might be used to create upstream masks.
            for (Symbol symbol : SymbolsExtractor.extractUnique(and(sourceFusionResult.leftExpr(), sourceFusionResult.rightExpr()))) {
                if (!newAssignment.containsKey(symbol) && !sourceFusionResult.mapping().containsKey(symbol)) {
                    newAssignment.put(symbol, symbol.toSymbolReference());
                }
            }

            return new FusionResult(
                    new ProjectNode(idAllocator.getNextId(), sourceFusionResult.planNode(), Assignments.copyOf(newAssignment)),
                    newMapping,
                    sourceFusionResult.leftExpr(),
                    sourceFusionResult.rightExpr());
        }

        @Override
        public FusionResult visitJoin(JoinNode node, FusionContext context)
        {
            JoinNode rightNode = (JoinNode) context.planNode();

            // Cheap checks
            // TODO: Can be generalized to non-matching filter/criteria as long as there is a common sub-criteria, by
            //   considering the remaining residual predicates as a post-filter.
            if (node.getType() != rightNode.getType() ||
                    node.getType() != INNER ||
                    node.getFilter().isPresent() != rightNode.getFilter().isPresent() ||
                    node.getCriteria().size() != rightNode.getCriteria().size() ||
                    !node.getLeftHashSymbol().equals(rightNode.getLeftHashSymbol()) ||
                    !node.getRightHashSymbol().equals(rightNode.getRightHashSymbol()) ||
                    !node.getDistributionType().equals(rightNode.getDistributionType()) ||
                    !node.isSpillable().equals(rightNode.isSpillable()) ||
                    !node.getDynamicFilters().equals(rightNode.getDynamicFilters())) {
                return null;
            }

            FusionResult leftFusionResult = tryFuse(node.getLeft(), rightNode.getLeft());
            if (leftFusionResult == null) {
                return null;
            }

            FusionResult rightFusionResult = tryFuse(node.getRight(), rightNode.getRight());
            if (rightFusionResult == null) {
                return null;
            }

            // Build overall mapping
            checkState(Collections.disjoint(leftFusionResult.mapping().keySet(), rightFusionResult.mapping().keySet()));
            Map<Symbol, Symbol> newMapping = new HashMap<>();
            newMapping.putAll(leftFusionResult.mapping());
            newMapping.putAll(rightFusionResult.mapping());
            SymbolMapper mapper = SymbolMapper.symbolMapper(newMapping);

            // TODO: Can be generalized by using equivalent variables for matching.
            if (node.getFilter().isPresent() && !node.getFilter().get().equals(mapper.map(rightNode.getFilter().get()))) {
                return null;
            }

            // TODO: Can be generalized to any order and not just positional equality.
            boolean criteriaMatches = Streams.zip(
                    node.getCriteria().stream(),
                    rightNode.getCriteria().stream(),
                    (c1, c2) -> c1.getLeft().equals(mapper.map(c2.getLeft())) && c1.getRight().equals(mapper.map(c2.getRight()))).allMatch(Boolean::valueOf);
            if (!criteriaMatches) {
                return null;
            }

            List<Symbol> newLeftOutputSymbols =
                    ImmutableList.<Symbol>builder()
                            .addAll(node.getLeftOutputSymbols())
                            .addAll(mapper.map(rightNode.getLeftOutputSymbols()))
                            .addAll(SymbolsExtractor.extractAll(leftFusionResult.leftExpr()))
                            .addAll(SymbolsExtractor.extractAll(leftFusionResult.rightExpr())).build();

            List<Symbol> newRightOutputSymbols =
                    ImmutableList.<Symbol>builder()
                            .addAll(node.getRightOutputSymbols())
                            .addAll(mapper.map(rightNode.getRightOutputSymbols()))
                            .addAll(mapper.map(SymbolsExtractor.extractAll(rightFusionResult.leftExpr())))
                            .addAll(mapper.map(SymbolsExtractor.extractAll(rightFusionResult.rightExpr()))).build();

            JoinNode newNode = new JoinNode(
                    idAllocator.getNextId(),
                    node.getType(),
                    leftFusionResult.planNode(),
                    rightFusionResult.planNode(),
                    node.getCriteria(),
                    newLeftOutputSymbols.stream().distinct().toList(),
                    newRightOutputSymbols.stream().distinct().toList(),
                    node.isMaySkipOutputDuplicates(),
                    node.getFilter(),
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol(),
                    node.getDistributionType(),
                    node.isSpillable(),
                    node.getDynamicFilters(),
                    node.getReorderJoinStatsAndCost());

            return new FusionResult(
                    newNode,
                    newMapping,
                    ExpressionUtils.combineConjuncts(plannerContext.getMetadata(), leftFusionResult.leftExpr(), mapper.map(rightFusionResult.leftExpr())),
                    ExpressionUtils.combineConjuncts(plannerContext.getMetadata(), leftFusionResult.rightExpr(), mapper.map(rightFusionResult.rightExpr())));
        }

        @Override
        public FusionResult visitAggregation(AggregationNode node, FusionContext context)
        {
            AggregationNode rightNode = (AggregationNode) context.planNode();

            // Cheap checks
            if (node.getGroupingKeys().size() != rightNode.getGroupingKeys().size() ||
                    node.getGroupingSetCount() != rightNode.getGroupingSetCount() ||
                    !node.getGlobalGroupingSets().equals(rightNode.getGlobalGroupingSets()) ||
                    node.getPreGroupedSymbols().size() != rightNode.getPreGroupedSymbols().size() ||
                    node.getStep() != rightNode.getStep() ||
                    !node.getHashSymbol().equals(rightNode.getHashSymbol()) ||
                    !node.getGroupIdSymbol().equals(rightNode.getGroupIdSymbol())) {
                return null;
            }

            FusionResult sourceFusionResult = tryFuse(node.getSource(), rightNode.getSource());
            if (sourceFusionResult == null) {
                return null;
            }

            // Build result mapping
            SymbolMapper mapper = SymbolMapper.symbolMapper(sourceFusionResult.mapping());
            Map<Symbol, Symbol> newMapping = new HashMap<>(sourceFusionResult.mapping());

            // Check grouping sets
            if (!node.getGroupingKeys().equals(mapper.map(rightNode.getGroupingKeys()))) {
                return null;
            }

            // Set up the initial assigment for masks, if needed and given by the fused filters.
            Assignments.Builder assignmentBuilder = Assignments.builder();
            Symbol leftMask = null;
            if (sourceFusionResult.hasLeftExpr()) {
                leftMask = symbolAllocator.newSymbol("maskL", BOOLEAN);
                assignmentBuilder.put(leftMask, sourceFusionResult.leftExpr());
            }
            Symbol rightMask = null;
            if (sourceFusionResult.hasRightExpr()) {
                rightMask = symbolAllocator.newSymbol("maskR", BOOLEAN);
                assignmentBuilder.put(rightMask, sourceFusionResult.rightExpr());
            }

            // For combined masks we need another project node, since we use what we define in the first one.
            Assignments.Builder postAssignmentBuilder = Assignments.builder();

            // Extract assignments to compare existing masks with new ones.
            Assignments.Builder existingAssignmentsBuilder = Assignments.builder();
            extractProjections(sourceFusionResult.planNode(), existingAssignmentsBuilder);
            Assignments existingAssignments = existingAssignmentsBuilder.build();

            // Process aggregations.
            HashMap<Symbol, Aggregation> newAggregations = new HashMap<>();
            HashMap<Aggregation, Symbol> inverseAggregationMap = new HashMap<>();
            boolean hasCombinedMask = false;
            for (int i = 0; i < 2; ++i) {
                boolean processingLeft = (i == 0);
                Map<Symbol, Aggregation> aggregations = processingLeft ? node.getAggregations() : rightNode.getAggregations();
                Symbol mask = processingLeft ? leftMask : rightMask;

                for (Map.Entry<Symbol, Aggregation> entry : aggregations.entrySet()) {
                    checkState(!newMapping.containsKey(entry.getKey()));
                    Symbol newSymbol = entry.getKey();
                    Aggregation newAggregation = mapper.map(entry.getValue());
                    Symbol newMask = mask;
                    if (mask != null) {
                        // We have a mask, add it to the aggregation.
                        Optional<Symbol> existingMask = newAggregation.getMask();
                        if (existingMask.isPresent()) {
                            // There is an existing mask in the aggregation, combine it and add a postproject.
                            // However, if existingMask is stricter than newMask, keep the existingMask itself as the combination would be unnecessary.
                            // e.g. existingMask: P, newMask: P OR Q. It's unnecessary to create P AND (P OR Q). We can just keep P.
                            // TODO: The check for stricter-ness can be made more general (e.g., when existingMaskExpr is a disjunct).
                            Expression newMaskExpr = processingLeft ? sourceFusionResult.leftExpr() : sourceFusionResult.rightExpr();
                            Expression existingMaskExpr = existingAssignments.get(existingMask.get());
                            if (existingMaskExpr != null && ExpressionUtils.extractDisjuncts(newMaskExpr).contains(existingMaskExpr)) {
                                newMask = existingMask.get();
                            }
                            else {
                                newMask = symbolAllocator.newSymbol("maskC", BOOLEAN);
                                postAssignmentBuilder.put(newMask, and(existingMask.get().toSymbolReference(), mask.toSymbolReference()));
                                hasCombinedMask = true;
                            }
                        }

                        newAggregation = new Aggregation(
                                newAggregation.getResolvedFunction(),
                                newAggregation.getArguments(),
                                newAggregation.isDistinct(),
                                newAggregation.getFilter(),
                                newAggregation.getOrderingScheme(),
                                Optional.of(newMask));
                    }

                    if (!processingLeft && inverseAggregationMap.containsKey(newAggregation)) {
                        // We already saw this, reuse it.
                        newMapping.put(newSymbol, inverseAggregationMap.get(newAggregation));
                    }
                    else {
                        newAggregations.put(newSymbol, newAggregation);
                        inverseAggregationMap.put(newAggregation, newSymbol);
                    }
                }
            }

            // Do we need to add compensating count(*) aggregations/filters?
            Expression compensatingFilterLeft = TRUE_LITERAL;
            Expression compensatingFilterRight = TRUE_LITERAL;

            if (!isScalarAgg(node)) {
                if (leftMask != null) {
                    Symbol compensatingLeft = symbolAllocator.newSymbol("compL", BIGINT);
                    newAggregations.put(compensatingLeft, getCountAggregate(leftMask));
                    compensatingFilterLeft = getIsPositiveExpression(compensatingLeft);
                }
                if (rightMask != null) {
                    Symbol compensatingRight = symbolAllocator.newSymbol("compR", BIGINT);
                    newAggregations.put(compensatingRight, getCountAggregate(rightMask));
                    compensatingFilterRight = getIsPositiveExpression(compensatingRight);
                }
            }

            // Assemble final result, starting from the fused input.
            PlanNode result = sourceFusionResult.planNode();

            // Add the projection with mask information.
            if (leftMask != null || rightMask != null) {
                assignmentBuilder.putIdentities(result.getOutputSymbols());
                result = new ProjectNode(idAllocator.getNextId(), result, assignmentBuilder.build());

                if (hasCombinedMask) {
                    postAssignmentBuilder.putIdentities(result.getOutputSymbols());
                    result = new ProjectNode(idAllocator.getNextId(), result, postAssignmentBuilder.build());
                }
            }

            // Add the fused aggregate.
            result = new AggregationNode(
                    idAllocator.getNextId(),
                    result,
                    ImmutableMap.copyOf(newAggregations),
                    node.getGroupingSets(),
                    node.getPreGroupedSymbols(),
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());

            return new FusionResult(result, newMapping, compensatingFilterLeft, compensatingFilterRight);
        }

        private Aggregation getCountAggregate(Symbol mask)
        {
            return new Aggregation(
                    plannerContext.getMetadata().resolveFunction(session, QualifiedName.of("count"), emptyList()),
                    ImmutableList.of(),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(mask));
        }

        private Expression getIsPositiveExpression(Symbol symbol)
        {
            return new ComparisonExpression(
                    ComparisonExpression.Operator.GREATER_THAN,
                    symbol.toSymbolReference(),
                    new Cast(new LongLiteral("0"), toSqlType(BIGINT)));
        }

        private void extractProjections(PlanNode node, Assignments.Builder assignmentsBuilder)
        {
            for (PlanNode childNode : node.getSources()) {
                extractProjections(childNode, assignmentsBuilder);
            }

            if (node instanceof ProjectNode) {
                ((ProjectNode) node).getAssignments().entrySet().stream()
                        .filter(e -> !e.getKey().toSymbolReference().equals(e.getValue()))
                        .forEach(assignmentsBuilder::put);
            }
        }

        @Override
        public FusionResult visitMarkDistinct(MarkDistinctNode node, FusionContext context)
        {
            MarkDistinctNode rightNode = (MarkDistinctNode) context.planNode();
            checkState(!node.getMarkerSymbol().equals(rightNode.getMarkerSymbol()));

            // Get a chain of MarkDistinctNodes.
            List<MarkDistinctNode> markDistinctNodes = getMarkDistinctNodeChain(node);
            checkState(!markDistinctNodes.isEmpty(), "markDistinctNodes is empty");

            List<MarkDistinctNode> rightMarkDistinctNodes = getMarkDistinctNodeChain(rightNode);
            checkState(!rightMarkDistinctNodes.isEmpty(), "rightMarkDistinctNodes is empty");

            PlanNode source = markDistinctNodes.get(markDistinctNodes.size() - 1).getSource();
            PlanNode rightSource = rightMarkDistinctNodes.get(rightMarkDistinctNodes.size() - 1).getSource();

            if (markDistinctNodes.stream().anyMatch(n -> n.getHashSymbol().isPresent()) ||
                    rightMarkDistinctNodes.stream().anyMatch(n -> n.getHashSymbol().isPresent())) {
                // Hash symbols should not be present here. Bail out.
                return null;
            }

            FusionResult sourceFusionResult = tryFuse(source, rightSource);
            if (sourceFusionResult == null) {
                return null;
            }

            SymbolMapper mapper = SymbolMapper.symbolMapper(sourceFusionResult.mapping());

            // Special case for a single markDistinct on each side, no fused filters, and the distinct symbols
            // are the same in both MarkDistinct operators.
            if (markDistinctNodes.size() == 1 && rightMarkDistinctNodes.size() == 1
                    && !sourceFusionResult.hasLeftExpr() && !sourceFusionResult.hasRightExpr()
                    && node.getDistinctSymbols().equals(mapper.map(rightNode.getDistinctSymbols()))) {
                Map<Symbol, Symbol> newMapping = ImmutableMap.<Symbol, Symbol>builder()
                        .putAll(sourceFusionResult.mapping())
                        .put(rightNode.getMarkerSymbol(), node.getMarkerSymbol())
                        .buildOrThrow();
                return new FusionResult(
                        node.replaceChildren(ImmutableList.of(sourceFusionResult.planNode())),
                        newMapping,
                        sourceFusionResult.leftExpr(),
                        sourceFusionResult.rightExpr());
            }

            // Use any existing mask as a extra distinct column, which is then filtered by the combined mask.
            // TODO: An alternative, better approach is to consider MarkDistinct nodes that have 'masks', and return
            //   one column for each 'mask'. That way we reduce memory consumption and can pipeline multiple markDistinct
            //   operators over the same columns.
            Assignments.Builder assignmentBuilder = Assignments.builder();
            Symbol newMaskLeft = null;
            if (sourceFusionResult.hasLeftExpr()) {
                newMaskLeft = symbolAllocator.newSymbol("maskL", BOOLEAN);
                assignmentBuilder.put(newMaskLeft, sourceFusionResult.leftExpr());
            }
            Symbol newMaskRight = null;
            if (sourceFusionResult.hasRightExpr()) {
                newMaskRight = symbolAllocator.newSymbol("maskR", BOOLEAN);
                assignmentBuilder.put(newMaskRight, sourceFusionResult.rightExpr());
            }

            // Assemble final result by stacking MarkDistinct nodes.
            // TODO: There are opportunities to improve reordering of [right]MarkDistinctNodes based on distinct keys.
            PlanNode result = sourceFusionResult.planNode();

            if (newMaskLeft != null || newMaskRight != null) {
                assignmentBuilder.putIdentities(result.getOutputSymbols());
                result = new ProjectNode(idAllocator.getNextId(), result, assignmentBuilder.build());
                if (newMaskLeft != null && newMaskRight != null) {
                    result = new FilterNode(
                            idAllocator.getNextId(),
                            result,
                            ExpressionUtils.combineDisjuncts(plannerContext.getMetadata(), sourceFusionResult.leftExpr(), sourceFusionResult.rightExpr()));
                }
            }

            // stack both chains of markDistinctNodes.
            for (MarkDistinctNode markDistinctNode : markDistinctNodes) {
                List<Symbol> distinctSymbols = markDistinctNode.getDistinctSymbols();
                if (newMaskLeft != null) {
                    distinctSymbols = ImmutableList.<Symbol>builder().addAll(markDistinctNode.getDistinctSymbols()).add(newMaskLeft).build();
                }
                result = new MarkDistinctNode(
                        idAllocator.getNextId(),
                        result,
                        markDistinctNode.getMarkerSymbol(),
                        distinctSymbols,
                        markDistinctNode.getHashSymbol());
            }

            for (MarkDistinctNode markDistinctNode : rightMarkDistinctNodes) {
                List<Symbol> rightDistinctSymbols = mapper.map(markDistinctNode.getDistinctSymbols());
                if (newMaskRight != null) {
                    rightDistinctSymbols = ImmutableList.<Symbol>builder().addAll(rightDistinctSymbols).add(newMaskRight).build();
                }
                result = new MarkDistinctNode(
                        idAllocator.getNextId(),
                        result,
                        markDistinctNode.getMarkerSymbol(),
                        rightDistinctSymbols,
                        markDistinctNode.getHashSymbol());
            }

            return new FusionResult(
                    result,
                    sourceFusionResult.mapping(),
                    newMaskLeft == null ? TRUE_LITERAL : newMaskLeft.toSymbolReference(),
                    newMaskRight == null ? TRUE_LITERAL : newMaskRight.toSymbolReference());
        }

        private List<MarkDistinctNode> getMarkDistinctNodeChain(PlanNode node)
        {
            ImmutableList.Builder<MarkDistinctNode> builder = ImmutableList.builder();
            while (node instanceof MarkDistinctNode markDistinctNode) {
                builder.add(markDistinctNode);
                node = markDistinctNode.getSource();
            }

            return builder.build();
        }

        @Override
        public FusionResult visitEnforceSingleRow(EnforceSingleRowNode node, FusionContext context)
        {
            FusionResult sourceFusionResult = tryFuse(node.getSource(), ((EnforceSingleRowNode) context.planNode()).getSource());
            if (sourceFusionResult == null) {
                return null;
            }

            PlanNode result = sourceFusionResult.planNode();

            result = new EnforceSingleRowNode(idAllocator.getNextId(), result);

            return new FusionResult(
                    result,
                    sourceFusionResult.mapping(),
                    sourceFusionResult.leftExpr(),
                    sourceFusionResult.rightExpr());
        }
    }

    private static class SharedScanRewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final PlannerContext plannerContext;
        private final Map<String, Boolean> rulesControl;
        private final Session session;
        private final TypeAnalyzer typeAnalyzer;
        private final TypeProvider types;

        private static final int MAX_JOIN_SIZE = 16;

        private SharedScanRewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, PlannerContext plannerContext, Session session, TypeAnalyzer typeAnalyzer, TypeProvider types)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.session = requireNonNull(session, "session is null");
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.types = requireNonNull(types, "types is null");
            rulesControl = new HashMap<>();
            rulesControl.put(JOIN_GB_RULE, useQueryFusionForJoingb(session));
            rulesControl.put(GB_JOIN_GB_RULE, useQueryFusionForGbjoingb(session));
            rulesControl.put(PUSH_UNION_BELOW_JOIN_RULE, useQueryFusionForPushunionbelowjoin(session));
        }

        private interface JoinPattern
        {
            PlanNode apply(PlanNode left, PlanNode right, List<JoinNode.EquiJoinClause> criteria, Optional<Expression> filter);
        }

        private PlanNode rewriteJoinBasedOnPattern(
                PlanNode left,
                PlanNode right,
                List<JoinNode.EquiJoinClause> criteria,
                Optional<Expression> filter,
                Map<String, Boolean> rulesControl)
        {
            List<JoinPattern> patterns = new ArrayList<>();
            if (rulesControl.get(GB_JOIN_GB_RULE)) {
                patterns.add(this::rewriteGbJoinGbPattern);
            }
            if (rulesControl.get(JOIN_GB_RULE)) {
                patterns.add(this::rewriteJoinGbPattern);
            }

            for (JoinPattern pattern : patterns) {
                PlanNode result = pattern.apply(left, right, criteria, filter);
                if (result != null) {
                    return result;
                }
            }

            return null;
        }

        // Rewrite for GbJoinGb pattern: Join(Gb(Q), Gb'(Q)) -> Gb''(Q)
        private PlanNode rewriteGbJoinGbPattern(
                PlanNode left,
                PlanNode right,
                List<JoinNode.EquiJoinClause> criteria,
                Optional<Expression> filter)
        {
            // grab the original rhs output symbols
            List<Symbol> rightOutputSymbols = right.getOutputSymbols();

            // grab the original left and right node
            PlanNode originalLeft = left;
            PlanNode originalRight = right;

            // Skip pass-trough ProjectNode or FilterNode on top of left and right.
            while (right instanceof ProjectNode || right instanceof FilterNode) {
                right = right.getSources().get(0);
            }

            while (left instanceof ProjectNode || left instanceof FilterNode) {
                left = left.getSources().get(0);
            }

            // Check we have agg on rhs and lhs
            if (!(right instanceof AggregationNode rightAggregation) || !(left instanceof AggregationNode leftAggregation)) {
                return null;
            }

            // restore the original left and right node because we want to fuse from the top level operators,
            // in this case we don't need to skip the project or filter and then add it back, we can rely on the infrastructure already provided by FusionVisitor.
            right = originalRight;
            left = originalLeft;

            // Move EquiJoinClauses that do not refer to a grouping column.
            ImmutableList.Builder<JoinNode.EquiJoinClause> newCriteriaBuilder = ImmutableList.builder();
            ImmutableList.Builder<Expression> filterBuilder = ImmutableList.builder();
            criteria.forEach(clause -> {
                if (rightAggregation.getGroupingKeys().contains(clause.getRight()) && leftAggregation.getGroupingKeys().contains(clause.getLeft())) {
                    newCriteriaBuilder.add(clause);
                }
                else {
                    filterBuilder.add(clause.toExpression());
                }
            });
            List<JoinNode.EquiJoinClause> newCriteria = newCriteriaBuilder.build();
            Expression newFilter = and(filterBuilder.build());
            if (!newFilter.equals(TRUE_LITERAL)) {
                filter = filter.map(expression -> and(expression, newFilter)).or(() -> Optional.of(newFilter));
            }

            // Check that equi-join columns on left and right are the same as group by columns.
            boolean sameEquiJoinAndAggColsRight = ImmutableSet.copyOf(rightAggregation.getGroupingKeys())
                    .equals(newCriteria.stream().map(JoinNode.EquiJoinClause::getRight).collect(Collectors.toSet()));
            boolean sameEquiJoinAndAggColsLeft = ImmutableSet.copyOf(leftAggregation.getGroupingKeys())
                    .equals(newCriteria.stream().map(JoinNode.EquiJoinClause::getLeft).collect(Collectors.toSet()));
            if (!sameEquiJoinAndAggColsRight || !sameEquiJoinAndAggColsLeft) {
                return null;
            }

            // Check that left and right sub queries can be fused.
            FusionResult fusionResult = new FusionVisitor(idAllocator, symbolAllocator, plannerContext, session).tryFuse(left, right);
            if (fusionResult == null) {
                return null;
            }

            // Get mapping to fused plan.
            Map<Symbol, Symbol> mapping = fusionResult.mapping();
            SymbolMapper mapper = SymbolMapper.symbolMapper(mapping);

            // Check that the equijoin has the same columns from left and right modulo mapping.
            // TODO: There could be equivalence classes that makes this check more general.
            boolean canHandleJoin = newCriteria.stream().allMatch(c -> c.getLeft().equals(mapper.map(c.getRight())));
            if (!canHandleJoin) {
                return null;
            }

            // Start with the fused plan.
            PlanNode result = fusionResult.planNode();

            // Remove rows that would have been filtered out by the join and the left and right predicate
            if (!newCriteria.isEmpty() || !fusionResult.leftExpr().equals(TRUE_LITERAL) || !fusionResult.rightExpr().equals(TRUE_LITERAL)) {
                result = new FilterNode(
                        idAllocator.getNextId(),
                        result,
                        and(fusionResult.leftExpr, mapper.map(fusionResult.rightExpr),
                                and(newCriteria.stream().map(c -> new IsNotNullPredicate(c.getLeft().toSymbolReference())).collect(toList()))));
            }

            // Add optional join filter.
            if (filter.isPresent()) {
                result = new FilterNode(idAllocator.getNextId(), result, mapper.map(filter.get()));
            }

            if (rightOutputSymbols.stream().anyMatch(mapping::containsKey)) {
                Assignments.Builder builder = Assignments.builder().putIdentities(result.getOutputSymbols());

                rightOutputSymbols.stream().filter(mapping::containsKey)
                        .forEach(s -> { builder.put(s, mapping.get(s).toSymbolReference()); });

                result = new ProjectNode(idAllocator.getNextId(), result, builder.build());
            }

            return result;
        }

        // Rewrite for JoinGb pattern: Join(Q, Gb(Q)) -> Window
        private PlanNode rewriteJoinGbPattern(
                PlanNode left,
                PlanNode right,
                List<JoinNode.EquiJoinClause> criteria,
                Optional<Expression> filter)
        {
            PlanNode result = rewriteJoinGbPatternHelper(left, right, criteria, filter);
            if (result == null) {
                List<JoinNode.EquiJoinClause> inverseCriteria = criteria.stream().map(JoinNode.EquiJoinClause::flip).collect(toList());
                result = rewriteJoinGbPatternHelper(right, left, inverseCriteria, filter);
            }
            return result;
        }

        private PlanNode rewriteJoinGbPatternHelper(
                PlanNode left,
                PlanNode right,
                List<JoinNode.EquiJoinClause> criteria,
                Optional<Expression> filter)
        {
            // Grab the original right node output symbols.
            List<Symbol> rightOutputSymbols = right.getOutputSymbols();

            // Skip an optional ProjectNode on top of right.
            ProjectNode rootProject = null;
            if (right instanceof ProjectNode) {
                rootProject = (ProjectNode) right;
                right = rootProject.getSource();
            }

            // Skip an optional FilterNode on top of right.
            // TODO: Can we generalize this to a chain of 'pass-through' operators?
            FilterNode rootFilter = null;
            if (right instanceof FilterNode) {
                rootFilter = (FilterNode) right;
                right = rootFilter.getSource();
            }

            // Check that the right side is an aggregation node.
            if (!(right instanceof AggregationNode aggregation)) {
                return null;
            }

            if (isScalarAgg(aggregation)) { // the fused plan will be less efficient than the original plan in this case
                return null;
            }

            // Distinct in Window functions is currently not supported. We have to be conservative and bail out for such cases.
            if (hasDistinctAggregate(aggregation)) {
                return null;
            }

            right = aggregation.getSource();

            // Move EquiJoinClauses that do not refer to a grouping column.
            ImmutableList.Builder<JoinNode.EquiJoinClause> newCriteriaBuilder = ImmutableList.builder();
            ImmutableList.Builder<Expression> filterBuilder = ImmutableList.builder();
            criteria.forEach(clause -> {
                if (aggregation.getGroupingKeys().contains(clause.getRight())) {
                    newCriteriaBuilder.add(clause);
                }
                else {
                    filterBuilder.add(clause.toExpression());
                }
            });

            List<JoinNode.EquiJoinClause> newCriteria = newCriteriaBuilder.build();
            Expression newFilter = and(filterBuilder.build());
            if (!newFilter.equals(TRUE_LITERAL)) {
                filter = filter.map(expression -> and(expression, newFilter)).or(() -> Optional.of(newFilter));
            }

            // Check that equi-join columns on right are the same as group by columns.
            boolean sameEquiJoinAndAggCols = ImmutableSet.copyOf(aggregation.getGroupingKeys())
                    .equals(newCriteria.stream().map(JoinNode.EquiJoinClause::getRight).collect(Collectors.toSet()));
            if (!sameEquiJoinAndAggCols) {
                return null;
            }

            // Check that left and right (below the aggregation) can be fused.
            FusionResult fusionResult = new FusionVisitor(idAllocator, symbolAllocator, plannerContext, session).tryFuse(left, right);
            if (fusionResult == null || fusionResult.hasLeftExpr() || fusionResult.hasRightExpr()) {
                return null;
            }

            // Get mapping to fused plan.
            Map<Symbol, Symbol> mapping = fusionResult.mapping();
            SymbolMapper mapper = SymbolMapper.symbolMapper(mapping);

            // Check that the equijoin has the same columns from left and right modulo mapping.
            // TODO: There could be equivalence classes that makes this check more general.
            boolean canHandleJoin = newCriteria.stream().allMatch(c -> c.getLeft().equals(mapper.map(c.getRight())));
            if (!canHandleJoin) {
                return null;
            }

            // Start with the fused plan.
            PlanNode result = fusionResult.planNode();

            result = new FilterNode(
                    idAllocator.getNextId(),
                    result,
                    and(newCriteria.stream().map(c -> new IsNotNullPredicate(c.getLeft().toSymbolReference())).collect(toList())));

            // Add a Window operator for the corresponding Aggregation functions
            DataOrganizationSpecification spec = new DataOrganizationSpecification(ImmutableList.copyOf(mapper.map(aggregation.getGroupingKeys())), Optional.empty());

            WindowNode.Frame frame = new WindowNode.Frame(
                    WindowFrame.Type.RANGE,
                    FrameBound.Type.UNBOUNDED_PRECEDING,
                    Optional.empty(),
                    Optional.empty(),
                    FrameBound.Type.CURRENT_ROW,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

            Map<Symbol, WindowNode.Function> windowFunctions = new HashMap<>();
            aggregation.getAggregations().forEach((symbol, function) -> {
                windowFunctions.put(
                        symbol,
                        new WindowNode.Function(
                                function.getResolvedFunction(),
                                function.getArguments().stream().map(mapper::map).collect(toImmutableList()),
                                frame,
                                true));
            });

            result = new WindowNode(
                    idAllocator.getNextId(),
                    result,
                    spec,
                    windowFunctions,
                    Optional.empty(),
                    ImmutableSet.of(),
                    0);

            // Add optional filter
            if (rootFilter != null) {
                result = new FilterNode(idAllocator.getNextId(), result, mapper.map(rootFilter.getPredicate()));
            }

            // Add optional project, plus any column from right that is in the unified node but not on the original left input.
            if (rootProject != null || rightOutputSymbols.stream().anyMatch(mapping::containsKey)) {
                Assignments.Builder builder = Assignments.builder().putIdentities(result.getOutputSymbols());

                rightOutputSymbols.stream().filter(mapping::containsKey)
                        .forEach(s -> { builder.put(s, mapping.get(s).toSymbolReference()); });

                if (rootProject != null) {
                    for (Entry<Symbol, Expression> entry : rootProject.getAssignments().entrySet()) {
                        if (!mapping.containsKey(entry.getKey())) {
                            builder.put(entry.getKey(), mapper.map(entry.getValue()));
                        }
                    }
                }

                result = new ProjectNode(idAllocator.getNextId(), result, builder.build());
            }

            // Add optional join filter.
            if (filter.isPresent()) {
                result = new FilterNode(idAllocator.getNextId(), result, mapper.map(filter.get()));
            }

            return result;
        }

        // Duplicated from ReorderJoins for simplicity.
        private List<Expression> getJoinPredicates(EqualityInference allFilterInference, Expression allFilter, Set<Symbol> leftSymbols, Set<Symbol> rightSymbols)
        {
            // get join predicates
            ImmutableList.Builder<Expression> joinPredicatesBuilder = ImmutableList.builder();

            // This takes all conjuncts that were part of allFilters that
            // could not be used for equality inference.
            // If they use both the left and right symbols, we add them to the list of joinPredicates
            nonInferrableConjuncts(plannerContext.getMetadata(), allFilter)
                    .map(conjunct -> allFilterInference.rewrite(conjunct, Sets.union(leftSymbols, rightSymbols)))
                    .filter(Objects::nonNull)
                    // filter expressions that contain only left or right symbols
                    .filter(conjunct -> allFilterInference.rewrite(conjunct, leftSymbols) == null)
                    .filter(conjunct -> allFilterInference.rewrite(conjunct, rightSymbols) == null)
                    .forEach(joinPredicatesBuilder::add);

            // create equality inference on available symbols
            List<Expression> joinEqualities = allFilterInference.generateEqualitiesPartitionedBy(Sets.union(leftSymbols, rightSymbols)).getScopeEqualities();
            EqualityInference joinInference = EqualityInference.newInstance(plannerContext.getMetadata(), joinEqualities.toArray(new Expression[0]));
            joinPredicatesBuilder.addAll(joinInference.generateEqualitiesPartitionedBy(leftSymbols).getScopeStraddlingEqualities());

            return joinPredicatesBuilder.build();
        }

        static Optional<JoinNode.EquiJoinClause> getEquiJoinClause(Expression predicate, Set<Symbol> leftSymbols)
        {
            if (predicate instanceof ComparisonExpression comparison) {
                if (comparison.getOperator() == EQUAL
                        && comparison.getLeft() instanceof SymbolReference
                        && comparison.getRight() instanceof SymbolReference) {
                    Symbol leftSymbol = Symbol.from(comparison.getLeft());
                    Symbol rightSymbol = Symbol.from(comparison.getRight());

                    if (leftSymbols.contains(leftSymbol) && !leftSymbols.contains(rightSymbol)) {
                        return Optional.of(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
                    }
                    else if (leftSymbols.contains(rightSymbol) && !leftSymbols.contains(leftSymbol)) {
                        return Optional.of(new JoinNode.EquiJoinClause(rightSymbol, leftSymbol));
                    }
                }
            }

            return Optional.empty();
        }

        public static class JoinCriteriaAndFilter
        {
            public final List<JoinNode.EquiJoinClause> criteria;
            public final Optional<Expression> filter;

            public JoinCriteriaAndFilter(List<JoinNode.EquiJoinClause> criteria, Optional<Expression> filter)
            {
                this.criteria = criteria;
                this.filter = filter;
            }
        }

        public static JoinCriteriaAndFilter getCriteriaAndFilter(List<Expression> conjuncts, Set<Symbol> leftSymbols)
        {
            ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList.builder();
            ImmutableList.Builder<Expression> filterBuilder = ImmutableList.builder();
            for (Expression conjunct : conjuncts) {
                Optional<JoinNode.EquiJoinClause> clause = getEquiJoinClause(conjunct, leftSymbols);
                if (clause.isPresent()) {
                    criteriaBuilder.add(clause.get());
                }
                else {
                    filterBuilder.add(conjunct);
                }
            }

            Expression filter = and(filterBuilder.build());
            return new JoinCriteriaAndFilter(
                    criteriaBuilder.build(),
                    filter.equals(TRUE_LITERAL) ? Optional.empty() : Optional.of(filter));
        }

        public Optional<PlanNode> tryJoinBasedTransformations(List<PlanNode> nodes, Expression allFilter)
        {
            boolean changed = false;
            EqualityInference allFilterInference = EqualityInference.newInstance(plannerContext.getMetadata(), allFilter);

            // First try all possibilities and combine as applicable. Keep track in nodes the partial results as we go,
            // and put nulls whenever we remove some subtree.
            for (int i = 0; i < nodes.size(); ++i) {
                PlanNode left = nodes.get(i);
                if (left == null) {
                    continue;
                }

                for (int j = i + 1; j < nodes.size(); ++j) {
                    PlanNode right = nodes.get(j);
                    if (right == null) {
                        continue;
                    }

                    Set<Symbol> leftSymbols = left.getOutputSymbols().stream().collect(toImmutableSet());
                    Set<Symbol> rightSymbols = right.getOutputSymbols().stream().collect(toImmutableSet());
                    List<Expression> joinPredicates = getJoinPredicates(allFilterInference, allFilter, leftSymbols, rightSymbols);

                    // Separate into criteria and filter
                    JoinCriteriaAndFilter criteriaAndFilter = getCriteriaAndFilter(joinPredicates, leftSymbols);

                    PlanNode rewrittenPlan = rewriteJoinBasedOnPattern(left, right, criteriaAndFilter.criteria, criteriaAndFilter.filter, rulesControl);
                    if (rewrittenPlan != null) {
                        changed = true;
                        left = rewrittenPlan;
                        nodes.set(j, null);
                    }
                }
                nodes.set(i, left);
            }

            // Second pass, start assembling the join in this order, but check every time with the remaining elements. This
            // would work whenever the join has a left-deep portion that can match with another child.
            // TODO: In general, we are searching a heuristically chosen subspace of possibilities, which could be improved.
            PlanNode result = nodes.get(0);
            for (int i = 1; i < nodes.size(); ++i) {
                PlanNode right = nodes.get(i);
                if (right == null) {
                    continue;
                }

                Set<Symbol> leftSymbols = result.getOutputSymbols().stream().collect(toImmutableSet());
                Set<Symbol> rightSymbols = right.getOutputSymbols().stream().collect(toImmutableSet());
                List<Expression> joinPredicates = getJoinPredicates(allFilterInference, allFilter, leftSymbols, rightSymbols);

                // Separate into criteria and filter
                JoinCriteriaAndFilter criteriaAndFilter = getCriteriaAndFilter(joinPredicates, leftSymbols);

                result = new JoinNode(
                        idAllocator.getNextId(),
                        INNER,
                        result,
                        right,
                        criteriaAndFilter.criteria,
                        result.getOutputSymbols(),
                        right.getOutputSymbols(),
                        false,
                        criteriaAndFilter.filter,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(),
                        Optional.empty());

                // Now try to fuse with something later
                for (int j = i + 1; j < nodes.size(); ++j) {
                    PlanNode candidate = nodes.get(j);
                    if (candidate == null) {
                        continue;
                    }

                    leftSymbols = result.getOutputSymbols().stream().collect(toImmutableSet());
                    rightSymbols = candidate.getOutputSymbols().stream().collect(toImmutableSet());
                    joinPredicates = getJoinPredicates(allFilterInference, allFilter, leftSymbols, rightSymbols);

                    // Separate into criteria and filter
                    criteriaAndFilter = getCriteriaAndFilter(joinPredicates, leftSymbols);
                    PlanNode rewrittenPlan = rewriteJoinBasedOnPattern(result, candidate, criteriaAndFilter.criteria, criteriaAndFilter.filter, rulesControl);

                    if (rewrittenPlan != null) {
                        changed = true;
                        result = rewrittenPlan;
                        nodes.set(j, null);
                        break;
                    }
                }
            }

            return changed ? Optional.of(result) : Optional.empty();
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            // We don't handle nondeterministic predicates here
            if (!isDeterministic(node.getFilter().orElse(TRUE_LITERAL), plannerContext.getMetadata())) {
                return visitPlan(node, context);
            }

            if (node.getType() != INNER) {
                // TODO: Handle OUTER variants.
                return visitPlan(node, context);
            }

            // To heuristically reduce the plans we want to check for matching, we consider right join subtrees as a unit
            // (presumably coming from CTEs or explicit subqueries), and explicitly handle left-deep join trees.
            // In general, a better treatment of matching would improve the coverage of rules like this one.
            ReorderJoins.MultiJoinNode multiJoinNode = ReorderJoins.MultiJoinNode.toLeftDeepMultiJoinNode(
                    plannerContext,
                    node,
                    Lookup.noLookup(),
                    idAllocator,
                    MAX_JOIN_SIZE,
                    true,
                    session,
                    typeAnalyzer,
                    types);

            List<PlanNode> nodes = multiJoinNode.getSources().stream()
                    .map(n -> n.accept(this, context))
                    .collect(toList());
            Optional<PlanNode> result = tryJoinBasedTransformations(nodes, multiJoinNode.getFilter());
            return result.orElseGet(() -> visitPlan(node, context));
        }

        /**
         * Try to push Union below Join. A plan like this:
         *      UnionAll(X join Z, Y join Z)
         * can be transformed into:
         *      (X UnionAll Y) join Z
         * to reduce one branch of Z.
         *
         * Current limitations:
         * 1. Only supports Union with two sources
         * 2. Only supports Inner joins in the two sources
         * 3. Assumes the common source is on the right side in both joins, so we only fuse RHS
         *
         * @param node the input Union node
         * @param context rewrite context
         * @return transformed node, or Optional.empty() if transformation is not applicable
         */
        public Optional<PlanNode> tryPushUnionBelowJoin(UnionNode node, RewriteContext<Void> context)
        {
            if (node.getSources().size() != 2) {
                return Optional.empty();
            }

            // Skip (and keep track of) Projects on left/right children.
            PlanNode leftChild = node.getSources().get(0);
            List<ProjectNode> leftProjects = new ArrayList<>();
            while (leftChild instanceof ProjectNode) {
                leftProjects.add((ProjectNode) leftChild);
                leftChild = leftChild.getSources().get(0);
            }

            PlanNode rightChild = node.getSources().get(1);
            List<ProjectNode> rightProjects = new ArrayList<>();
            while (rightChild instanceof ProjectNode) {
                rightProjects.add((ProjectNode) rightChild);
                rightChild = rightChild.getSources().get(0);
            }

            // Check that the inputs are joins.
            if (!(leftChild instanceof JoinNode leftJoin && rightChild instanceof JoinNode rightJoin)) {
                // TODO: Generalize for SemiJoinNode.
                return Optional.empty();
            }

            if (leftJoin.getType() != INNER || rightJoin.getType() != INNER
                    || leftJoin.getFilter().isPresent() || rightJoin.getFilter().isPresent()
                    || leftJoin.getCriteria().size() != 1 || rightJoin.getCriteria().size() != 1
                    || !Collections.disjoint(leftJoin.getOutputSymbols(), leftJoin.getRight().getOutputSymbols())
                    || !Collections.disjoint(rightJoin.getOutputSymbols(), rightJoin.getRight().getOutputSymbols())) {
                return Optional.empty();
            }

            // Try fusing the RHS of both joins.
            FusionResult fusionResult = new FusionVisitor(idAllocator, symbolAllocator, plannerContext, session).tryFuse(leftJoin.getRight(), rightJoin.getRight());
            if (fusionResult == null) {
                return Optional.empty();
            }

            // Disallow fused queries with residual predicates.
            if (fusionResult.hasLeftExpr() || fusionResult.hasRightExpr()) {
                return Optional.empty();
            }

            // Check that the criteria's RHS of both joins maps to the same column in the fused result.
            SymbolMapper mapper = SymbolMapper.symbolMapper(fusionResult.mapping());
            if (!leftJoin.getCriteria().get(0).getRight().equals(mapper.map(rightJoin.getCriteria().get(0).getRight()))) {
                return Optional.empty();
            }

            // See if the Union operator already exposes the LHS join columns of the left/right joins.
            // If not, create a new column for the pushed Union All.
            Symbol origLeftSymbolForLeftJoin = leftJoin.getCriteria().get(0).getLeft();
            Symbol origLeftSymbolForRightJoin = rightJoin.getCriteria().get(0).getLeft();
            Symbol newLeftSymbol = null;    // this will be used as the join key on the left side when joining the new Union to fused node
            for (Entry<Symbol, Collection<Symbol>> entry : node.getSymbolMapping().asMap().entrySet()) {
                if (entry.getValue().contains(origLeftSymbolForLeftJoin) && entry.getValue().contains(origLeftSymbolForRightJoin)) {
                    newLeftSymbol = entry.getKey();
                    break;
                }
            }

            ImmutableListMultimap.Builder<Symbol, Symbol> newSymbolMapping = ImmutableListMultimap.<Symbol, Symbol>builder().putAll(node.getSymbolMapping());
            ImmutableList.Builder<Symbol> newOutputColumns = ImmutableList.<Symbol>builder().addAll(node.getOutputSymbols());
            if (newLeftSymbol == null) {
                newLeftSymbol = symbolAllocator.newSymbol(leftJoin.getCriteria().get(0).getLeft());
                newSymbolMapping.putAll(newLeftSymbol, origLeftSymbolForLeftJoin, origLeftSymbolForRightJoin);
                newOutputColumns.add(newLeftSymbol);
            }

            // Propagate Projects to the left sides of the join. Add origLeftSymbolFor[Left/Right]Join if it does not exist.
            PlanNode newLeft = leftJoin.getLeft();
            for (ProjectNode project : Lists.reverse(leftProjects)) {
                newLeft = project.getOutputSymbols().contains(origLeftSymbolForLeftJoin) ?
                        project.replaceChildren(ImmutableList.of(newLeft)) :
                        new ProjectNode(
                                idAllocator.getNextId(),
                                newLeft,
                                Assignments.builder().putAll(project.getAssignments()).putIdentity(origLeftSymbolForLeftJoin).build());
            }

            PlanNode newRight = rightJoin.getLeft();
            for (ProjectNode project : Lists.reverse(rightProjects)) {
                newRight = project.getOutputSymbols().contains(origLeftSymbolForRightJoin) ?
                        project.replaceChildren(ImmutableList.of(newRight)) :
                        new ProjectNode(
                                idAllocator.getNextId(),
                                newRight,
                                Assignments.builder().putAll(project.getAssignments()).putIdentity(origLeftSymbolForRightJoin).build());
            }

            // Get new UnionNode
            PlanNode resultUnion = new UnionNode(
                    idAllocator.getNextId(),
                    ImmutableList.of(newLeft, newRight),
                    newSymbolMapping.build(),
                    newOutputColumns.build());

            // Recursively attempt to push this union all if possible.
            resultUnion = resultUnion.accept(this, context);

            // Build the result by joining back the new Union to the fused node
            PlanNode result = new JoinNode(
                    idAllocator.getNextId(),
                    INNER,
                    resultUnion,
                    fusionResult.planNode(),
                    ImmutableList.of(new JoinNode.EquiJoinClause(newLeftSymbol, leftJoin.getCriteria().get(0).getRight())),
                    resultUnion.getOutputSymbols(),
                    fusionResult.planNode().getOutputSymbols(),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of(),
                    Optional.empty());

            return Optional.of(result);
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Void> context)
        {
            Optional<PlanNode> result = Optional.empty();
            if (rulesControl.get(PUSH_UNION_BELOW_JOIN_RULE)) {
                result = tryPushUnionBelowJoin(node, context);
            }
            return result.orElseGet(() -> visitPlan(node, context));
        }
    }

    private static boolean isScalarAgg(AggregationNode node)
    {
        return node.getGroupingKeys().isEmpty()
                && node.getGroupingSetCount() == 1
                && node.getGlobalGroupingSets().size() == 1;
    }

    private static boolean hasDistinctAggregate(AggregationNode node)
    {
        return node.getAggregations().entrySet().stream()
                .anyMatch(entry -> entry.getValue().isDistinct()
                        || entry.getValue().getMask().isPresent());
    }

    private static List<TableScanNode> findTableScanNodes(PlanNode node)
    {
        return searchFrom(node)
                .where(TableScanNode.class::isInstance)
                .findAll();
    }
}
