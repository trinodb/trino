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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableProperties;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.Rule.Context;
import io.trino.sql.planner.iterative.Rule.Result;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.DynamicFilters.isDynamicFilter;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan.computeEnforced;
import static io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan.verifyTablePartitioning;
import static io.trino.sql.planner.optimizations.PredicatePushDown.isInliningCandidate;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

/**
 * Derives a connector constraint from a filter that sits above a projection, without moving the filter.
 * <p>
 * Predicates over projected expressions often cannot be pushed below the projection by
 * {@link io.trino.sql.planner.optimizations.PredicatePushDown}: a conjunct referencing a projected
 * complex expression more than once (e.g. {@code expr BETWEEN a AND b OR expr BETWEEN c AND d}
 * distributed into disjunctive conjuncts) is not an inlining candidate, because inlining would
 * duplicate the computation. The predicate is then stranded above the projection and the table scan
 * receives no constraint at all.
 * <p>
 * These rules inline the projection's assignments into the predicate <b>only to derive a
 * {@link TupleDomain} constraint</b> for the connector. The inlined expression is never placed in the
 * plan or executed, so the cost concern behind the inlining restriction does not apply, and the
 * original filter and projection are left untouched. Since the inlined predicate is semantically
 * identical to the original filter over the projection's outputs, the extracted domain — a superset
 * of the rows it can match — is a valid scan constraint.
 */
public class DeriveTableScanConstraintThroughProject
{
    private final PlannerContext plannerContext;

    public DeriveTableScanConstraintThroughProject(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new DeriveTableScanConstraintThroughProjectWithoutFilter(plannerContext),
                new DeriveTableScanConstraintThroughProjectWithFilter(plannerContext));
    }

    @VisibleForTesting
    public static final class DeriveTableScanConstraintThroughProjectWithoutFilter
            implements Rule<FilterNode>
    {
        private static final Capture<ProjectNode> PROJECT = newCapture();
        private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

        private static final Pattern<FilterNode> PATTERN = filter().with(source().matching(
                project().capturedAs(PROJECT).with(source().matching(
                        tableScan().capturedAs(TABLE_SCAN)))));

        private final PlannerContext plannerContext;

        public DeriveTableScanConstraintThroughProjectWithoutFilter(PlannerContext plannerContext)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isAllowPushdownIntoConnectors(session);
        }

        @Override
        public Result apply(FilterNode filterNode, Captures captures, Context context)
        {
            return deriveConstraint(filterNode, captures.get(PROJECT), Optional.empty(), captures.get(TABLE_SCAN), plannerContext, context);
        }
    }

    @VisibleForTesting
    public static final class DeriveTableScanConstraintThroughProjectWithFilter
            implements Rule<FilterNode>
    {
        private static final Capture<ProjectNode> PROJECT = newCapture();
        private static final Capture<FilterNode> RESIDUAL_FILTER = newCapture();
        private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

        private static final Pattern<FilterNode> PATTERN = filter().with(source().matching(
                project().capturedAs(PROJECT).with(source().matching(
                        filter().capturedAs(RESIDUAL_FILTER).with(source().matching(
                                tableScan().capturedAs(TABLE_SCAN)))))));

        private final PlannerContext plannerContext;

        public DeriveTableScanConstraintThroughProjectWithFilter(PlannerContext plannerContext)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isAllowPushdownIntoConnectors(session);
        }

        @Override
        public Result apply(FilterNode filterNode, Captures captures, Context context)
        {
            // the constraint derived from the outer filter is valid regardless of the rows the residual filter removes
            return deriveConstraint(filterNode, captures.get(PROJECT), Optional.of(captures.get(RESIDUAL_FILTER)), captures.get(TABLE_SCAN), plannerContext, context);
        }
    }

    private static Result deriveConstraint(
            FilterNode filterNode,
            ProjectNode project,
            Optional<FilterNode> residualFilter,
            TableScanNode tableScan,
            PlannerContext plannerContext,
            Context context)
    {
        Session session = context.getSession();

        // Substituting an assignment into the predicate is semantics-preserving only for
        // deterministic assignments; conjuncts must also be deterministic and dynamic filters
        // are meaningless to connectors.
        List<Expression> candidateConjuncts = extractConjuncts(filterNode.getPredicate()).stream()
                .filter(conjunct -> !isDynamicFilter(conjunct) && isDeterministic(conjunct))
                // Inlinable conjuncts are left to PredicatePushDown and PushPredicateIntoTableScan,
                // which enforce them in the connector and remove them from the filter; enforcing the
                // domain here first would leave the conjunct in the filter permanently.
                .filter(conjunct -> !isInliningCandidate(conjunct, project))
                .filter(conjunct -> SymbolsExtractor.extractUnique(conjunct).stream()
                        .allMatch(symbol -> {
                            Expression assignment = project.getAssignments().get(symbol);
                            return assignment != null && isDeterministic(assignment);
                        }))
                .collect(toImmutableList());
        if (candidateConjuncts.isEmpty()) {
            return Result.empty();
        }

        Expression inlined = inlineSymbols(project.getAssignments()::get, combineConjuncts(candidateConjuncts));

        DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.getExtractionResult(plannerContext, session, inlined);

        TupleDomain<ColumnHandle> newDomain = decomposedPredicate.getTupleDomain()
                .transformKeys(tableScan.getAssignments()::get)
                .intersect(tableScan.getEnforcedConstraint());

        if (newDomain.contains(tableScan.getEnforcedConstraint())) {
            // no narrowing over what the scan already enforces
            return Result.empty();
        }

        if (newDomain.isNone()) {
            // the inlined predicate is equivalent to the original filter over the projection,
            // so an unsatisfiable domain means the filter matches no rows
            return Result.ofPlanNode(new ValuesNode(filterNode.getId(), filterNode.getOutputSymbols()));
        }

        Optional<ConstraintApplicationResult<TableHandle>> result =
                plannerContext.getMetadata().applyFilter(session, tableScan.getTable(), new Constraint(newDomain));
        if (result.isEmpty()) {
            return Result.empty();
        }

        TableHandle newTable = result.get().getHandle();
        TableProperties newTableProperties = plannerContext.getMetadata().getTableProperties(session, newTable);
        if (newTableProperties.getPredicate().isNone()) {
            return Result.ofPlanNode(new ValuesNode(filterNode.getId(), filterNode.getOutputSymbols()));
        }

        TupleDomain<ColumnHandle> newEnforcedConstraint = computeEnforced(newDomain, result.get().getRemainingFilter());
        if (newTable.equals(tableScan.getTable()) && newEnforcedConstraint.equals(tableScan.getEnforcedConstraint())) {
            return Result.empty();
        }

        verifyTablePartitioning(session, plannerContext.getMetadata(), tableScan, newTableProperties.getTablePartitioning());

        TableScanNode newScan = new TableScanNode(
                tableScan.getId(),
                newTable,
                tableScan.getOutputSymbols(),
                tableScan.getAssignments(),
                newEnforcedConstraint,
                // Unlike in PushPredicateIntoTableScan, the filter stays above the projection, so
                // deriving the scan estimate from the filter would apply its selectivity twice:
                // once in the precalculated scan statistics and once by the filter above.
                Optional.empty(),
                tableScan.isUpdateTarget(),
                tableScan.getUseConnectorNodePartitioning());

        // the filter and projection are kept as-is: row-level semantics (nulls, errors) must be preserved
        PlanNode newSource = newScan;
        if (residualFilter.isPresent()) {
            newSource = residualFilter.get().replaceChildren(ImmutableList.of(newScan));
        }
        PlanNode newProject = project.replaceChildren(ImmutableList.of(newSource));
        return Result.ofPlanNode(filterNode.replaceChildren(ImmutableList.of(newProject)));
    }
}
