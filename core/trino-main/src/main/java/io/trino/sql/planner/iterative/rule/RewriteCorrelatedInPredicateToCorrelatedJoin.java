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
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.isUseLegacyDecorrelator;
import static io.trino.matching.Pattern.nonEmpty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.Patterns.Apply.correlation;
import static io.trino.sql.planner.plan.Patterns.applyNode;
import static java.util.Objects.requireNonNull;

/// Lowers a correlated `IN` [ApplyNode] into a [CorrelatedJoinNode] that the
/// dependent-join framework can decorrelate, so the framework owns correlated `IN`
/// end-to-end (rather than relying on the legacy `TransformCorrelatedInPredicateToJoin`).
/// Active only when the dependent-join framework is active (the default; `use_legacy_decorrelator = false`).
///
/// The three-valued `x IN (subquery)` result is computed per outer row from two counts over
/// the subquery rows `b`:
///
/// - `countMatches`     = count of rows where `x = b` (a `NULL` on either
///         side makes the comparison `NULL`, which the `FILTER` excludes);
/// - `countNullMatches` = count of rows where `x IS NULL OR b IS NULL`.
///
/// Then `IN = CASE WHEN countMatches > 0 THEN true WHEN countNullMatches > 0 THEN null ELSE false`. An empty subquery yields both counts `0` → `false` (`x IN ()` is
/// `false`, even for `NULL` `x`), which the framework's scalar global-aggregate
/// path produces via its `non_null` mask. `NOT IN` is the surrounding `NOT` over
/// this same three-valued result, so it needs no special handling here.
///
/// Transforms:
/// ```
/// - Apply (output: x IN b)
///      - input: A (producing x)
///      - subquery: B (producing b, using correlation from A)
/// ```
/// Into:
/// ```
/// - Project (output: A.*, in <- CASE …)
///      - CorrelatedJoin INNER, correlation(A's correlation ∪ {x})
///           - input: A
///           - subquery: Aggregate [global] countMatches <- count() filter (x = b)
///                                           countNullMatches <- count() filter (x IS NULL OR b IS NULL)
///                          - Project matchCond, nullMatchCond
///                              - B
/// ```
public class RewriteCorrelatedInPredicateToCorrelatedJoin
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode()
            .with(nonEmpty(correlation()));

    private final Metadata metadata;

    public RewriteCorrelatedInPredicateToCorrelatedJoin(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return !isUseLegacyDecorrelator(session);
    }

    @Override
    public Result apply(ApplyNode apply, Captures captures, Context context)
    {
        Map<Symbol, ApplyNode.SetExpression> assignments = apply.getSubqueryAssignments();
        if (assignments.size() != 1) {
            return Result.empty();
        }
        if (!(getOnlyElement(assignments.values()) instanceof ApplyNode.In inPredicate)) {
            return Result.empty();
        }
        Symbol inResult = getOnlyElement(assignments.keySet());
        Symbol probe = inPredicate.value();
        Symbol reference = inPredicate.reference();

        // The probe must be supplied by the input; it becomes a correlation reference of the
        // synthesized join (it is used inside the subquery's match conditions below).
        if (!apply.getInput().getOutputSymbols().contains(probe)) {
            return Result.empty();
        }

        Symbol matchCondition = context.getSymbolAllocator().newSymbol("matchCondition", BOOLEAN);
        Symbol nullMatchCondition = context.getSymbolAllocator().newSymbol("nullMatchCondition", BOOLEAN);
        ProjectNode conditions = new ProjectNode(
                context.getIdAllocator().getNextId(),
                apply.getSubquery(),
                Assignments.builder()
                        .put(matchCondition, new Comparison(Comparison.Operator.EQUAL, probe.toSymbolReference(), reference.toSymbolReference()))
                        .put(nullMatchCondition, or(new IsNull(probe.toSymbolReference()), new IsNull(reference.toSymbolReference())))
                        .build());

        Symbol countMatches = context.getSymbolAllocator().newSymbol("countMatches", BIGINT);
        Symbol countNullMatches = context.getSymbolAllocator().newSymbol("countNullMatches", BIGINT);
        AggregationNode counts = singleAggregation(
                context.getIdAllocator().getNextId(),
                conditions,
                ImmutableMap.of(
                        countMatches, countWithFilter(matchCondition),
                        countNullMatches, countWithFilter(nullMatchCondition)),
                singleGroupingSet(ImmutableList.of()));

        List<Symbol> joinCorrelation = ImmutableList.<Symbol>builder()
                .addAll(apply.getCorrelation())
                .addAll(apply.getCorrelation().contains(probe) ? ImmutableList.of() : ImmutableList.of(probe))
                .build();
        CorrelatedJoinNode correlatedJoin = new CorrelatedJoinNode(
                context.getIdAllocator().getNextId(),
                apply.getInput(),
                counts,
                joinCorrelation,
                JoinType.INNER,
                TRUE,
                apply.getOriginSubquery());

        Expression inExpression = new Case(
                ImmutableList.of(
                        new WhenClause(new Comparison(Comparison.Operator.GREATER_THAN, countMatches.toSymbolReference(), new Constant(BIGINT, 0L)), TRUE),
                        new WhenClause(new Comparison(Comparison.Operator.GREATER_THAN, countNullMatches.toSymbolReference(), new Constant(BIGINT, 0L)), new Constant(BOOLEAN, null))),
                FALSE);
        PlanNode result = new ProjectNode(
                context.getIdAllocator().getNextId(),
                correlatedJoin,
                Assignments.builder()
                        .putIdentities(apply.getInput().getOutputSymbols())
                        .put(inResult, inExpression)
                        .build());

        return Result.ofPlanNode(result);
    }

    private AggregationNode.Aggregation countWithFilter(Symbol filter)
    {
        return new AggregationNode.Aggregation(
                metadata.resolveBuiltinFunction("count", ImmutableList.of()),
                ImmutableList.of(),
                false,
                Optional.of(filter),
                Optional.empty(),
                Optional.empty());
    }
}
