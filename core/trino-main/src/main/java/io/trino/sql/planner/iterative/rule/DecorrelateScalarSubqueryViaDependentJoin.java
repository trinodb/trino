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
import com.google.common.collect.Sets;
import io.trino.metadata.Metadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.NodeAndMappings;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.optimizations.Cardinality;
import io.trino.sql.planner.optimizations.decorrelation.DependentJoinAlgebra;
import io.trino.sql.planner.optimizations.decorrelation.PlainJoinForms;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RowNumberNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.planner.LogicalPlanner.failFunction;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.correlationInFilter;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.correlationOnlyInFilters;
import static io.trino.sql.planner.optimizations.decorrelation.DependentJoinAlgebra.correlationIdentical;
import static io.trino.sql.planner.optimizations.decorrelation.DependentJoinAlgebra.correlationMapping;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.correlation;
import static io.trino.sql.planner.plan.Patterns.correlatedJoin;

/// A non-aggregate scalar subquery `(SELECT u.b … WHERE correlated)` wraps its body in an
/// `EnforceSingleRowNode` (exactly one row: NULL-filled if empty, error if >1), possibly
/// under projections (e.g. coercions of the scalar value). The projections are stripped and
/// re-applied above the decorrelated result, where the input columns are in scope — sound
/// even when they reference correlation, since the original evaluates them over the
/// enforced (possibly NULL-filled) row for every outer row alike.
public class DecorrelateScalarSubqueryViaDependentJoin
        extends DependentJoinFrameworkRule
{
    public DecorrelateScalarSubqueryViaDependentJoin(PlannerContext plannerContext)
    {
        super(plannerContext);
    }

    @Override
    protected Optional<PlanNode> apply(CorrelatedJoinNode node, DependentJoinAlgebra algebra, Context context)
    {
        return new Rewriter(plannerContext(), context, algebra).decorrelate(node);
    }

    private static final class Rewriter
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final Lookup lookup;
        private final Metadata metadata;
        private final DependentJoinAlgebra algebra;
        private final PlainJoinForms forms;

        private Rewriter(PlannerContext plannerContext, Context context, DependentJoinAlgebra algebra)
        {
            this.idAllocator = context.getIdAllocator();
            this.symbolAllocator = context.getSymbolAllocator();
            this.lookup = context.getLookup();
            this.metadata = plannerContext.getMetadata();
            this.algebra = algebra;
            this.forms = new PlainJoinForms(context.getIdAllocator(), context.getSymbolAllocator(), context.getLookup(), plannerContext);
        }

        private Optional<PlanNode> decorrelate(CorrelatedJoinNode correlatedJoin)
        {
            Set<Symbol> correlation = ImmutableSet.copyOf(correlatedJoin.getCorrelation());
            PlanNode input = correlatedJoin.getInput();

            List<ProjectNode> scalarProjections = new ArrayList<>();
            PlanNode scalarShape = lookup.resolve(correlatedJoin.getSubquery());
            while (scalarShape instanceof ProjectNode project) {
                scalarProjections.add(project);
                scalarShape = lookup.resolve(project.getSource());
            }
            if (!(scalarShape instanceof EnforceSingleRowNode enforce) || !correlatedJoin.getFilter().equals(TRUE)) {
                return Optional.empty();
            }
            // A scalar subquery that outputs a bare correlation symbol (e.g. `(SELECT o.col WHERE p(o.col))`)
            // AND filters on the correlation selects the outer value directly: rebinding aliases the inner
            // correlation to the outer symbol, so for outer rows the filter excludes it would return the
            // outer value instead of NULL. The plain-join forms have the same flaw: the output symbol is an
            // input column, never null-extended. Decline — the shape fails as unsupported (legacy
            // also rejects it). (A computed projection like `o.col * 2` gets a fresh output
            // symbol; a bare output with no
            // correlated filter — e.g. `(SELECT t.* FROM …)` — either always matches or errors with
            // "multiple rows", so neither is matched here.)
            if (!Sets.intersection(ImmutableSet.copyOf(enforce.getOutputSymbols()), correlation).isEmpty()
                    && correlationInFilter(enforce, correlation, lookup)) {
                return Optional.empty();
            }
            // Filter-only correlation takes the cardinality-aware legacy forms (a plain join when
            // the body is provably ≤1 row, one LEFT join + a MarkDistinct row-count check
            // otherwise), falling back to the magic-set + per-group row_number for bodies deeper
            // than those forms lift (e.g. a grouped aggregation or bound under the enforce); the
            // projection-correlated case (the scalar value references correlation, which legacy
            // can't decorrelate) goes to the magic-set directly.
            Optional<PlanNode> scalar = correlationOnlyInFilters(enforce, correlation, lookup)
                    ? decorrelateFilterOnlyScalar(enforce, input, correlation)
                      .or(() -> decorrelateScalarViaMagicSet(enforce, input, correlatedJoin.getCorrelation()))
                    : decorrelateScalarViaMagicSet(enforce, input, correlatedJoin.getCorrelation());
            return scalar
                    .map(plan -> algebra.reapplyProjections(plan, scalarProjections, input, TRUE))
                    .map(plan -> algebra.restrictOutputs(plan, correlatedJoin.getOutputSymbols()));
        }

        /// Filter-only-correlated non-aggregate scalar subquery — the cardinality-aware forms of the
        /// legacy `TransformCorrelatedScalarSubquery`. A body provably producing at most one row
        /// needs no runtime check: `EnforceSingleRowNode` degenerates to NULL-extending LEFT-join
        /// semantics, so the bare body goes through the full LEFT decorrelation pipeline (a plain
        /// cross-product first when it produces exactly one row). A general body takes one LEFT join of
        /// the uniquely-tagged input plus a `MarkDistinct`/fail check enforcing the per-outer-row
        /// single-row contract.
        private Optional<PlanNode> decorrelateFilterOnlyScalar(EnforceSingleRowNode enforce, PlanNode input, Set<Symbol> correlation)
        {
            PlanNode body = enforce.getSource();
            Cardinality cardinality = extractCardinality(body, lookup);
            if (cardinality.isAtMostScalar()) {
                if (cardinality.isScalar()) {
                    // Exactly one row per outer row — the enforce is a no-op and the dependent join is
                    // a plain (residual) product.
                    return Optional.of(algebra.dependentJoin(input, body, correlation));
                }
                return leftJoinBody(input, body, correlation);
            }
            PlanNode taggedInput = new AssignUniqueId(idAllocator.getNextId(), input, symbolAllocator.newSymbol("unique", BIGINT));
            Optional<PlanNode> joined = leftJoinBody(taggedInput, body, correlation);
            if (joined.isEmpty()) {
                return Optional.empty();
            }
            Symbol isDistinct = symbolAllocator.newSymbol("is_distinct", BOOLEAN);
            PlanNode marked = new MarkDistinctNode(idAllocator.getNextId(), joined.get(), isDistinct, taggedInput.getOutputSymbols());
            return Optional.of(new FilterNode(
                    idAllocator.getNextId(),
                    marked,
                    new Switch(
                            isDistinct.toSymbolReference(),
                            ImmutableList.of(new WhenClause(TRUE, TRUE)),
                            new Cast(failFunction(metadata, SUBQUERY_MULTIPLE_ROWS, "Scalar sub-query has returned multiple rows"), BOOLEAN))));
        }

        /// The LEFT decorrelation pipeline over a bare subquery body: the bound-below-join and
        /// plain-join forms first, the magic-set as the general fallback (which bails gracefully when
        /// the input carries a synthetic unique id, since `PlanCopier` cannot clone it).
        private Optional<PlanNode> leftJoinBody(PlanNode input, PlanNode body, Set<Symbol> correlation)
        {
            Optional<PlanNode> result = forms.tryPlainJoinLimit(JoinType.LEFT, input, body, correlation);
            if (result.isEmpty()) {
                result = forms.tryPlainLeftJoinDistinct(input, body, correlation);
            }
            if (result.isEmpty()) {
                result = forms.tryGroupedAggregationLeftJoin(input, body, correlation);
            }
            if (result.isEmpty()) {
                result = forms.tryPlainLeftJoin(input, body, correlation);
            }
            if (result.isEmpty()) {
                result = algebra.magicSetJoin(JoinType.LEFT, input, body, ImmutableList.copyOf(correlation), TRUE, body.getOutputSymbols());
            }
            return result;
        }

        /// Non-aggregate scalar subquery `(SELECT u.b … WHERE correlated)` — `EnforceSingleRowNode`
        /// over the body. Decorrelates the body per distinct correlation value, enforces ≤1 row per
        /// group (failing otherwise, like the original), then LEFT-joins the input back on the
        /// correlation columns so an empty group NULL-fills the scalar — matching
        /// `EnforceSingleRowNode`'s exactly-one-row semantics.
        private Optional<PlanNode> decorrelateScalarViaMagicSet(EnforceSingleRowNode enforce, PlanNode input, List<Symbol> correlation)
        {
            PlanNode source = enforce.getSource();

            Optional<NodeAndMappings> cloneOptional = algebra.copyInput(input, correlation);
            if (cloneOptional.isEmpty()) {
                return Optional.empty();
            }
            NodeAndMappings clone = cloneOptional.get();
            List<Symbol> clonedCorrelation = clone.getFields();
            PlanNode distinctCorrelation = singleAggregation(
                    idAllocator.getNextId(),
                    clone.getNode(),
                    ImmutableMap.of(),
                    singleGroupingSet(clonedCorrelation));
            Optional<PlanNode> reboundSource = algebra.rebindCorrelation(source, correlationMapping(correlation, clonedCorrelation));
            if (reboundSource.isEmpty()) {
                return Optional.empty();
            }
            PlanNode perD = algebra.dependentJoin(distinctCorrelation, reboundSource.get(), ImmutableSet.copyOf(clonedCorrelation));

            // Enforce ≤1 source row per correlation group; fail (at runtime) on the 2nd row.
            Symbol rowNumber = symbolAllocator.newSymbol("row_number", BIGINT);
            PlanNode numbered = new RowNumberNode(idAllocator.getNextId(), perD, clonedCorrelation, false, rowNumber, Optional.empty());
            PlanNode checked = new FilterNode(
                    idAllocator.getNextId(),
                    numbered,
                    new Switch(
                            new Comparison(LESS_THAN_OR_EQUAL, rowNumber.toSymbolReference(), new Constant(BIGINT, 1L)),
                            ImmutableList.of(new WhenClause(TRUE, TRUE)),
                            new Cast(failFunction(metadata, SUBQUERY_MULTIPLE_ROWS, "Scalar sub-query has returned multiple rows"), BOOLEAN)));

            List<Symbol> subqueryOutputs = enforce.getOutputSymbols();
            PlanNode joined = new JoinNode(
                    idAllocator.getNextId(),
                    JoinType.LEFT,
                    input,
                    checked,
                    ImmutableList.of(),
                    input.getOutputSymbols(),
                    subqueryOutputs,
                    false,
                    Optional.of(correlationIdentical(correlation, clonedCorrelation)),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of(),
                    Optional.empty());

            return Optional.of(joined);
        }
    }
}
