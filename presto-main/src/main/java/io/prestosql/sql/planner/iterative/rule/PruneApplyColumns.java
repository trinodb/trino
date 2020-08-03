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
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.tree.Expression;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Sets.intersection;
import static io.prestosql.sql.planner.SymbolsExtractor.extractUnique;
import static io.prestosql.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.prestosql.sql.planner.plan.Patterns.applyNode;

/**
 * This rule restricts the outputs of ApplyNode's input and subquery
 * based on which ApplyNode's output symbols are referenced.
 * <p>
 * A symbol from input source can be pruned, when
 * - it is not a referenced output symbol
 * - it is not a correlation symbol
 * - it is not referenced in subqueryAssignments
 * <p>
 * A symbol from subquery source can be pruned, when
 * it is not referenced in subqueryAssignments.
 * <p>
 * A subquery assignment can be removed, when
 * its key is not a referenced output symbol.
 * <p>
 * Note: this rule does not remove any symbols from the correlation list.
 * This is responsibility of PruneApplyCorrelation rule.
 * <p>
 * Transforms:
 * <pre>
 * - Project (i1, r1)
 *      - Apply
 *          correlation: [corr]
 *          assignments:
 *              r1 -> a in s1,
 *              r2 -> b in s2,
 *          - Input (a, b, corr)
 *          - Subquery (s1, s2)
 * </pre>
 * Into:
 * <pre>
 * - Project (i1, r1)
 *      - Apply
 *          correlation: [corr]
 *          assignments:
 *              r1 -> a in s1,
 *          - Project (a, corr)
 *              - Input (a, b, corr)
 *          - Project (s1)
 *              - Subquery (s1, s2)
 * </pre>
 */
public class PruneApplyColumns
        extends ProjectOffPushDownRule<ApplyNode>
{
    public PruneApplyColumns()
    {
        super(applyNode());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(
            Context context,
            ApplyNode applyNode,
            Set<Symbol> referencedOutputs)
    {
        // remove unused apply node
        if (intersection(applyNode.getSubqueryAssignments().getSymbols(), referencedOutputs).isEmpty()) {
            return Optional.of(applyNode.getInput());
        }

        // extract referenced assignments
        ImmutableSet.Builder<Symbol> requiredAssignmentsSymbols = ImmutableSet.builder();
        Assignments.Builder newSubqueryAssignments = Assignments.builder();
        for (Map.Entry<Symbol, Expression> entry : applyNode.getSubqueryAssignments().entrySet()) {
            if (referencedOutputs.contains(entry.getKey())) {
                requiredAssignmentsSymbols.addAll(extractUnique(entry.getValue()));
                newSubqueryAssignments.put(entry);
            }
        }

        // prune subquery symbols
        Optional<PlanNode> newSubquery = restrictOutputs(context.getIdAllocator(), applyNode.getSubquery(), requiredAssignmentsSymbols.build());

        // prune input symbols
        Set<Symbol> requiredInputSymbols = ImmutableSet.<Symbol>builder()
                .addAll(referencedOutputs)
                .addAll(applyNode.getCorrelation())
                .addAll(requiredAssignmentsSymbols.build())
                .build();

        Optional<PlanNode> newInput = restrictOutputs(context.getIdAllocator(), applyNode.getInput(), requiredInputSymbols);

        boolean pruned = newSubquery.isPresent()
                || newInput.isPresent()
                || newSubqueryAssignments.build().size() < applyNode.getSubqueryAssignments().size();

        if (pruned) {
            return Optional.of(new ApplyNode(
                    applyNode.getId(),
                    newInput.orElse(applyNode.getInput()),
                    newSubquery.orElse(applyNode.getSubquery()),
                    newSubqueryAssignments.build(),
                    applyNode.getCorrelation(),
                    applyNode.getOriginSubquery()));
        }

        return Optional.empty();
    }
}
