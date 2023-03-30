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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode.Type;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.matching.Pattern.nonEmpty;
import static io.trino.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.LogicalPlanner.failFunction;
import static io.trino.sql.planner.iterative.rule.ImplementLimitWithTies.rewriteLimitWithTiesWithPartitioning;
import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.correlation;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.filter;
import static io.trino.sql.planner.plan.Patterns.correlatedJoin;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static java.util.Objects.requireNonNull;

/**
 * This rule decorrelates plans with correlated UnnestNode and optional EnforceSingleRowNode,
 * optional LimitNode, optional TopNNode and optional projections in the subquery.
 * <p>
 * The rule finds correlated UnnestNode in CorrelatedJoinNode's subquery and folds
 * them into UnnestNode representing INNER or LEFT JOIN UNNEST.
 * It transforms plans, where:
 * - CorrelatedJoinNode is INNER or LEFT on true
 * - UnnestNode in subquery is based only on correlation symbols
 * - UnnestNode in subquery is INNER or LEFT without filter
 * <p>
 * Transforms:
 * <pre>
 * - CorrelatedJoin (INNER or LEFT) on true, correlation(c)
 *      - Input (a, c)
 *     [- EnforceSingleRow]
 *          [- Limit (5)]
 *               [- TopN (10) order by x]
 *                    [- Project x <- foo(u)]
 *                          - Unnest INNER or LEFT
 *                               u <- unnest(c)
 *                               replicate: ()
 * </pre>
 * Into:
 * <pre>
 * - Project (restrict outputs)
 *     [- Project [*1]
 *        a <- a
 *        c <- c
 *        x <- IF(ordinality IS NULL, null, x)]
 *          [- Filter (fail if row_number > 1)] [*2]
 *               [- Filter (row_number < 5)] [*3]
 *                   [- Filter (row_number < 10) [*4]
 *                          - Window partition by (unique), order by (x)
 *                            row_number <- row_number()]
 *                              [- Projection x <- foo(u)] [*5]
 *                                    - Unnest (LEFT or INNER) [*6] WITH ORDINALITY (ordinality)
 *                                      u <- unnest(c)
 *                                      replicate: (a, c, unique)
 *                                         - AssignUniqueId (unique)
 *                                              - Input (a, c)
 * [1] If UnnestNode is rewritten from INNER to LEFT, synthetic rows with nulls are added by the LEFT unnest at the bottom of the plan.
 *     In the correlated plan, they would be added in EnforceSingleRowNode or during join, that is near the root of the plan after all projections.
 *     This ProjectNode restores null values which might have been modified by projections. It uses ordinality symbol to distinguish between
 *     unnested rows and synthetic rows: `x <- IF(ordinality IS NULL, null, x)`
 * [2] If the original plan has EnforceSingleRowNode in the subquery, it has to be restored. EnforceSingleRowNode is responsible for:
 *     - adding a synthetic row of nulls where there are no rows,
 *     - checking that there is no more than 1 row.
 *     In this rewrite, if EnforceSingleRowNode is present in the original plan, the rewritten UnnestNode is LEFT. This ensures that there is
 *     at least 1 row for each input row. To restore the semantics of EnforceSingleRowNode, it is sufficient to add a check that there is no more
 *     than 1 row for each input row. This is achieved by RowNumberNode partitioned by input rows (unique) + FilterNode. If RowNumberNode
 *     is already present in the plan, only a FilterNode is added.
 * [3] If the original plan has LimitNode in the subquery, it has to be restored. It is achieved by RowNumberNode partitioned by input rows (unique)
 *     and a FilterNode. If RowNumberNode is already present in the plan, only a FilterNode is added.
 * [4] If the original plan has TopNNode in the subquery, it has to be restored. It is achieved by row_number() function over window
 *     partitioned by input rows (unique) and ordered by TopNNode's ordering scheme + FilterNode. Even if RowNumberNode is present
 *     in the plan, the rowNumberSymbol cannot be reused because its numbering order might not match the TopNNode's ordering.
 * [5] All projections present in the subquery are restored on top of the rewritten UnnestNode. Apart from their original assignments,
 *     they pass all the input symbols, the ordinality symbol and the row number symbol (if present) which might be useful in the upstream plan.
 * [6] Type of the rewritten UnnestNode is LEFT with one exception: if both CorrelatedJoinNode and original UnnestNode are INNER, and there is no
 *     EnforceSingleRowNode in the subquery, then the type is INNER. If the unnest type is rewritten from INNER to LEFT, the INNER semantics
 *     is restored by a projection ([1]).
 * </pre>
 * Note: The RowNumberNodes and WindowNodes produced by the rewrite along with filters, can be further optimized to TopNRankingNodes.
 * <p>
 * Note: this rule captures and transforms different plans. Therefore there is some redundancy. Not every flow will use ordinality symbol
 * or unique symbol. However, for simplicity, they are always present. It is up to other optimizer rules to prune unused symbols
 * or redundant nodes.
 */
public class DecorrelateUnnest
        implements Rule<CorrelatedJoinNode>
{
    private static final Pattern<CorrelatedJoinNode> PATTERN = correlatedJoin()
            .with(nonEmpty(correlation()))
            .with(filter().equalTo(TRUE_LITERAL))
            .matching(node -> node.getType() == CorrelatedJoinNode.Type.INNER || node.getType() == CorrelatedJoinNode.Type.LEFT);

    private final Metadata metadata;

    public DecorrelateUnnest(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<CorrelatedJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(CorrelatedJoinNode correlatedJoinNode, Captures captures, Context context)
    {
        // determine shape of the subquery
        PlanNode searchRoot = correlatedJoinNode.getSubquery();

        // 1. find EnforceSingleRowNode in the subquery
        Optional<EnforceSingleRowNode> enforceSingleRow = PlanNodeSearcher.searchFrom(searchRoot, context.getLookup())
                .where(EnforceSingleRowNode.class::isInstance)
                .recurseOnlyWhen(planNode -> false)
                .findFirst();

        if (enforceSingleRow.isPresent()) {
            searchRoot = enforceSingleRow.get().getSource();
        }

        // 2. find correlated UnnestNode in the subquery
        Optional<UnnestNode> subqueryUnnest = PlanNodeSearcher.searchFrom(searchRoot, context.getLookup())
                .where(node -> isSupportedUnnest(node, correlatedJoinNode.getCorrelation(), context.getLookup()))
                .recurseOnlyWhen(node -> node instanceof ProjectNode ||
                        (node instanceof LimitNode && ((LimitNode) node).getCount() > 0) ||
                        (node instanceof TopNNode && ((TopNNode) node).getCount() > 0))
                .findFirst();

        if (subqueryUnnest.isEmpty()) {
            return Result.empty();
        }

        UnnestNode unnestNode = subqueryUnnest.get();

        // assign unique id to input rows
        Symbol uniqueSymbol = context.getSymbolAllocator().newSymbol("unique", BIGINT);
        PlanNode input = new AssignUniqueId(
                context.getIdAllocator().getNextId(),
                correlatedJoinNode.getInput(),
                uniqueSymbol);

        // pre-project unnest symbols if they were pre-projected in subquery
        // The correlated UnnestNode either unnests correlation symbols directly, or unnests symbols produced by a projection that uses only correlation symbols.
        // Here, any underlying projection that was a source of the correlated UnnestNode, is appended as a source of the rewritten UnnestNode.
        // If the projection is not necessary for UnnestNode (i.e. it does not produce any unnest symbols), it should be pruned afterwards.
        PlanNode unnestSource = context.getLookup().resolve(unnestNode.getSource());
        if (unnestSource instanceof ProjectNode sourceProjection) {
            input = new ProjectNode(
                    sourceProjection.getId(),
                    input,
                    Assignments.builder()
                            .putIdentities(input.getOutputSymbols())
                            .putAll(sourceProjection.getAssignments())
                            .build());
        }

        // determine join type for rewritten UnnestNode
        Type unnestJoinType = LEFT;
        if (enforceSingleRow.isEmpty() && correlatedJoinNode.getType() == CorrelatedJoinNode.Type.INNER && unnestNode.getJoinType() == INNER) {
            unnestJoinType = INNER;
        }

        // make sure that the rewritten node is with ordinality, which might be necessary to restore inner unnest semantics after rewrite.
        Symbol ordinalitySymbol = unnestNode.getOrdinalitySymbol().orElseGet(() -> context.getSymbolAllocator().newSymbol("ordinality", BIGINT));

        // rewrite correlated join to UnnestNode.
        UnnestNode rewrittenUnnest = new UnnestNode(
                context.getIdAllocator().getNextId(),
                input,
                input.getOutputSymbols(),
                unnestNode.getMappings(),
                Optional.of(ordinalitySymbol),
                unnestJoinType,
                Optional.empty());

        // restore all nodes from the subquery
        PlanNode rewrittenPlan = Rewriter.rewriteNodeSequence(
                correlatedJoinNode.getSubquery(),
                input.getOutputSymbols(),
                ordinalitySymbol,
                uniqueSymbol,
                rewrittenUnnest,
                context.getSession(),
                metadata,
                context.getLookup(),
                context.getIdAllocator(),
                context.getSymbolAllocator());

        // restore INNER semantics in case when the original UnnestNode was INNER and the rewritten UnnestNode is LEFT.
        // It happens in two cases:
        // 1. when CorrelatedJoinNode type is LEFT and the UnnestNode in the subquery is INNER,
        // 2. when there is EnforceSingleRowNode in the subquery above the UnnestNode and the UnnestNode is INNER.
        // In such case, after the rewrite nulls are appended before projections while in the original query they would be appended
        // after projections, i.e. in EnforceSingleRowNode or in left correlated join. Because after the rewrite nulls are appended
        // earlier, they might be projected to something else. Project them back to nulls using ordinalitySymbol to distinguish
        // between unnested rows and synthetic rows added by left unnest.
        if (unnestNode.getJoinType() == INNER && rewrittenUnnest.getJoinType() == LEFT) {
            Assignments.Builder assignments = Assignments.builder()
                    .putIdentities(correlatedJoinNode.getInput().getOutputSymbols());
            for (Symbol subquerySymbol : correlatedJoinNode.getSubquery().getOutputSymbols()) {
                assignments.put(
                        subquerySymbol,
                        new IfExpression(
                                new IsNullPredicate(ordinalitySymbol.toSymbolReference()),
                                new Cast(new NullLiteral(), toSqlType(context.getSymbolAllocator().getTypes().get(subquerySymbol))),
                                subquerySymbol.toSymbolReference()));
            }
            rewrittenPlan = new ProjectNode(
                    context.getIdAllocator().getNextId(),
                    rewrittenPlan,
                    assignments.build());
        }

        // restrict outputs
        return Result.ofPlanNode(restrictOutputs(context.getIdAllocator(), rewrittenPlan, ImmutableSet.copyOf(correlatedJoinNode.getOutputSymbols())).orElse(rewrittenPlan));
    }

    /**
     * This rule supports decorrelation of UnnestNode meeting certain conditions:
     * - the UnnestNode should be based on correlation symbols, that is: either unnest correlation symbols directly,
     * or unnest symbols produced by a projection that uses only correlation symbols.
     * - the UnnestNode should not have any replicate symbols,
     * - the UnnestNode should be of type INNER or LEFT,
     * - the UnnestNode should not have a filter.
     */
    private static boolean isSupportedUnnest(PlanNode node, List<Symbol> correlation, Lookup lookup)
    {
        if (!(node instanceof UnnestNode unnestNode)) {
            return false;
        }

        List<Symbol> unnestSymbols = unnestNode.getMappings().stream()
                .map(UnnestNode.Mapping::getInput)
                .collect(toImmutableList());
        PlanNode unnestSource = lookup.resolve(unnestNode.getSource());
        boolean basedOnCorrelation = ImmutableSet.copyOf(correlation).containsAll(unnestSymbols) ||
                unnestSource instanceof ProjectNode && ImmutableSet.copyOf(correlation).containsAll(SymbolsExtractor.extractUnique(((ProjectNode) unnestSource).getAssignments().getExpressions()));

        return isScalar(unnestNode.getSource(), lookup) &&
                unnestNode.getReplicateSymbols().isEmpty() &&
                basedOnCorrelation &&
                (unnestNode.getJoinType() == INNER || unnestNode.getJoinType() == LEFT) &&
                (unnestNode.getFilter().isEmpty() || unnestNode.getFilter().get().equals(TRUE_LITERAL));
    }

    private static class Rewriter
            extends PlanVisitor<RewriteResult, Void>
    {
        private final List<Symbol> leftOutputs;
        private final Symbol ordinalitySymbol;
        private final Symbol uniqueSymbol;
        private final PlanNode sequenceSource;
        private final Session session;
        private final Metadata metadata;
        private final Lookup lookup;
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;

        private Rewriter(
                List<Symbol> leftOutputs,
                Symbol ordinalitySymbol,
                Symbol uniqueSymbol,
                PlanNode sequenceSource,
                Session session,
                Metadata metadata,
                Lookup lookup,
                PlanNodeIdAllocator idAllocator,
                SymbolAllocator symbolAllocator)
        {
            this.leftOutputs = ImmutableList.copyOf(requireNonNull(leftOutputs, "leftOutputs is null"));
            this.ordinalitySymbol = requireNonNull(ordinalitySymbol, "ordinalitySymbol is null");
            this.uniqueSymbol = requireNonNull(uniqueSymbol, "uniqueSymbol is null");
            this.sequenceSource = requireNonNull(sequenceSource, "sequenceSource is null");
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.lookup = requireNonNull(lookup, "lookup is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        public static PlanNode rewriteNodeSequence(
                PlanNode root,
                List<Symbol> leftOutputs,
                Symbol ordinalitySymbol,
                Symbol uniqueSymbol,
                PlanNode sequenceSource,
                Session session,
                Metadata metadata,
                Lookup lookup,
                PlanNodeIdAllocator idAllocator,
                SymbolAllocator symbolAllocator)
        {
            return new Rewriter(leftOutputs, ordinalitySymbol, uniqueSymbol, sequenceSource, session, metadata, lookup, idAllocator, symbolAllocator)
                    .rewrite(root)
                    .getPlan();
        }

        private RewriteResult rewrite(PlanNode node)
        {
            return lookup.resolve(node).accept(this, null);
        }

        @Override
        protected RewriteResult visitPlan(PlanNode node, Void context)
        {
            throw new IllegalStateException("Unexpected node type: " + node.getClass().getSimpleName());
        }

        @Override
        public RewriteResult visitUnnest(UnnestNode node, Void context)
        {
            return new RewriteResult(sequenceSource, Optional.empty());
        }

        @Override
        public RewriteResult visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            // If EnforceSingleRowNode is present in the subquery, the type of rewritten UnnestNode is LEFT.
            // Therefore, for each input row there is produced at least one row.
            // To restore the semantics of EnforceSingleRowNode, it is sufficient to check that for each
            // input row there is at most one row.
            RewriteResult source = rewrite(node.getSource());

            if (isScalar(source.getPlan(), lookup)) {
                return source;
            }

            Symbol rowNumberSymbol;
            PlanNode sourceNode;
            if (source.getRowNumberSymbol().isPresent()) {
                rowNumberSymbol = source.getRowNumberSymbol().get();
                sourceNode = source.getPlan();
            }
            else {
                rowNumberSymbol = symbolAllocator.newSymbol("row_number", BIGINT);
                sourceNode = new RowNumberNode(
                        idAllocator.getNextId(),
                        source.getPlan(),
                        ImmutableList.of(uniqueSymbol),
                        false,
                        rowNumberSymbol,
                        Optional.of(2),
                        Optional.empty());
            }
            Expression predicate = new IfExpression(
                    new ComparisonExpression(
                            GREATER_THAN,
                            rowNumberSymbol.toSymbolReference(),
                            new GenericLiteral("BIGINT", "1")),
                    new Cast(
                            failFunction(metadata, session, SUBQUERY_MULTIPLE_ROWS, "Scalar sub-query has returned multiple rows"),
                            toSqlType(BOOLEAN)),
                    TRUE_LITERAL);

            return new RewriteResult(new FilterNode(idAllocator.getNextId(), sourceNode, predicate), Optional.of(rowNumberSymbol));
        }

        @Override
        public RewriteResult visitLimit(LimitNode node, Void context)
        {
            // To restore the semantics of LimitNode, subquery rows must be limited for each input row.
            RewriteResult source = rewrite(node.getSource());

            if (node.isWithTies()) {
                return new RewriteResult(
                        rewriteLimitWithTiesWithPartitioning(node, source.getPlan(), session, metadata, idAllocator, symbolAllocator, ImmutableList.of(uniqueSymbol)),
                        Optional.empty());
            }

            Symbol rowNumberSymbol;
            PlanNode sourceNode;
            if (source.getRowNumberSymbol().isPresent()) {
                rowNumberSymbol = source.getRowNumberSymbol().get();
                sourceNode = source.getPlan();
            }
            else {
                rowNumberSymbol = symbolAllocator.newSymbol("row_number", BIGINT);
                sourceNode = new RowNumberNode(
                        idAllocator.getNextId(),
                        source.getPlan(),
                        ImmutableList.of(uniqueSymbol),
                        false,
                        rowNumberSymbol,
                        Optional.empty(),
                        Optional.empty());
            }

            return new RewriteResult(
                    new FilterNode(
                            idAllocator.getNextId(),
                            sourceNode,
                            new ComparisonExpression(LESS_THAN_OR_EQUAL, rowNumberSymbol.toSymbolReference(), new GenericLiteral("BIGINT", Long.toString(node.getCount())))),
                    Optional.of(rowNumberSymbol));
        }

        @Override
        public RewriteResult visitTopN(TopNNode node, Void context)
        {
            RewriteResult source = rewrite(node.getSource());

            // Do not reuse source's rowNumberSymbol, because it might not follow the TopNNode's ordering.
            Symbol rowNumberSymbol = symbolAllocator.newSymbol("row_number", BIGINT);
            WindowNode.Function rowNumberFunction = new WindowNode.Function(
                    metadata.resolveFunction(session, QualifiedName.of("row_number"), ImmutableList.of()),
                    ImmutableList.of(),
                    DEFAULT_FRAME,
                    false);
            WindowNode windowNode = new WindowNode(
                    idAllocator.getNextId(),
                    source.getPlan(),
                    new DataOrganizationSpecification(ImmutableList.of(uniqueSymbol), Optional.of(node.getOrderingScheme())),
                    ImmutableMap.of(rowNumberSymbol, rowNumberFunction),
                    Optional.empty(),
                    ImmutableSet.of(),
                    0);

            return new RewriteResult(
                    new FilterNode(
                            idAllocator.getNextId(),
                            windowNode,
                            new ComparisonExpression(LESS_THAN_OR_EQUAL, rowNumberSymbol.toSymbolReference(), new GenericLiteral("BIGINT", Long.toString(node.getCount())))),
                    Optional.of(rowNumberSymbol));
        }

        @Override
        public RewriteResult visitProject(ProjectNode node, Void context)
        {
            RewriteResult source = rewrite(node.getSource());

            Assignments.Builder assignments = Assignments.builder()
                    .putAll(node.getAssignments())
                    .putIdentities(leftOutputs)
                    .putIdentity(ordinalitySymbol);
            source.getRowNumberSymbol().ifPresent(assignments::putIdentity);

            return new RewriteResult(new ProjectNode(node.getId(), source.getPlan(), assignments.build()), source.getRowNumberSymbol());
        }
    }

    private static class RewriteResult
    {
        PlanNode plan;
        Optional<Symbol> rowNumberSymbol;

        public RewriteResult(PlanNode plan, Optional<Symbol> rowNumberSymbol)
        {
            this.plan = requireNonNull(plan, "plan is null");
            this.rowNumberSymbol = requireNonNull(rowNumberSymbol, "rowNumberSymbol is null");
        }

        public PlanNode getPlan()
        {
            return plan;
        }

        public Optional<Symbol> getRowNumberSymbol()
        {
            return rowNumberSymbol;
        }
    }
}
