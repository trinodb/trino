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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableSet;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.CorrelatedJoinNode;
import io.prestosql.sql.planner.plan.PlanNode;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Sets.intersection;
import static io.prestosql.sql.planner.SymbolsExtractor.extractUnique;
import static io.prestosql.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.prestosql.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static io.prestosql.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static io.prestosql.sql.planner.plan.CorrelatedJoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.CorrelatedJoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.CorrelatedJoinNode.Type.RIGHT;
import static io.prestosql.sql.planner.plan.Patterns.correlatedJoin;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;

/**
 * This rule restricts the outputs of CorrelatedJoinNode's input and subquery
 * based on which CorrelatedJoinNode's output symbols are referenced.
 * <p>
 * A symbol from input can be pruned, when
 * - it is not a referenced output symbol,
 * - it is not a correlation symbol,
 * - it is not present in join filter.
 * <p>
 * A symbol from subquery can be pruned, when
 * - it is not a referenced output symbol,
 * - it is not present in join filetr.
 * <p>
 * Note: this rule does not remove any symbols from the correlation list.
 * This is responsibility of PruneCorrelatedJoinCorrelation rule.
 * <p>
 * Transforms:
 * <pre>
 * - Project (a, c)
 *      - CorrelatedJoin
 *          correlation: [corr]
 *          filter: a > d
 *          - Input (a, b, corr)
 *          - Subquery (c, d, e)
 * </pre>
 * Into:
 * <pre>
 * - Project (a, c)
 *      - CorrelatedJoin
 *          correlation: [corr]
 *          filter: a > d
 *          - Project (a, corr)
 *              - Input (a, b, corr)
 *          - Project (c, d)
 *              - Subquery (c, d, e)
 * </pre>
 */
public class PruneCorrelatedJoinColumns
        extends ProjectOffPushDownRule<CorrelatedJoinNode>
{
    public PruneCorrelatedJoinColumns()
    {
        super(correlatedJoin());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(Context context, CorrelatedJoinNode correlatedJoinNode, Set<Symbol> referencedOutputs)
    {
        PlanNode input = correlatedJoinNode.getInput();
        PlanNode subquery = correlatedJoinNode.getSubquery();

        // remove unused correlated join node, retain input
        if (intersection(ImmutableSet.copyOf(subquery.getOutputSymbols()), referencedOutputs).isEmpty()) {
            // remove unused subquery of inner join
            if (correlatedJoinNode.getType() == INNER && isScalar(subquery, context.getLookup()) && correlatedJoinNode.getFilter().equals(TRUE_LITERAL)) {
                return Optional.of(input);
            }
            // remove unused subquery of left join
            if (correlatedJoinNode.getType() == LEFT && isAtMostScalar(subquery, context.getLookup())) {
                return Optional.of(input);
            }
        }

        Set<Symbol> referencedAndCorrelationSymbols = ImmutableSet.<Symbol>builder()
                .addAll(referencedOutputs)
                .addAll(correlatedJoinNode.getCorrelation())
                .build();

        // remove unused input node, retain subquery
        if (intersection(ImmutableSet.copyOf(input.getOutputSymbols()), referencedAndCorrelationSymbols).isEmpty()) {
            // remove unused input of inner join
            if (correlatedJoinNode.getType() == INNER && isScalar(input, context.getLookup()) && correlatedJoinNode.getFilter().equals(TRUE_LITERAL)) {
                return Optional.of(subquery);
            }
            // remove unused input of right join
            if (correlatedJoinNode.getType() == RIGHT && isAtMostScalar(input, context.getLookup())) {
                return Optional.of(subquery);
            }
        }

        Set<Symbol> filterSymbols = extractUnique(correlatedJoinNode.getFilter());

        Set<Symbol> referencedAndFilterSymbols = ImmutableSet.<Symbol>builder()
                .addAll(referencedOutputs)
                .addAll(filterSymbols)
                .build();

        Optional<PlanNode> newSubquery = restrictOutputs(context.getIdAllocator(), subquery, referencedAndFilterSymbols);

        Set<Symbol> referencedAndFilterAndCorrelationSymbols = ImmutableSet.<Symbol>builder()
                .addAll(referencedAndFilterSymbols)
                .addAll(correlatedJoinNode.getCorrelation())
                .build();

        Optional<PlanNode> newInput = restrictOutputs(context.getIdAllocator(), input, referencedAndFilterAndCorrelationSymbols);

        boolean pruned = newSubquery.isPresent() || newInput.isPresent();

        if (pruned) {
            return Optional.of(new CorrelatedJoinNode(
                    correlatedJoinNode.getId(),
                    newInput.orElse(input),
                    newSubquery.orElse(subquery),
                    correlatedJoinNode.getCorrelation(),
                    correlatedJoinNode.getType(),
                    correlatedJoinNode.getFilter(),
                    correlatedJoinNode.getOriginSubquery()));
        }

        return Optional.empty();
    }
}
