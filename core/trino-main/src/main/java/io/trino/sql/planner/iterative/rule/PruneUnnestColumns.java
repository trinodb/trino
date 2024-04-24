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

import com.google.common.collect.ImmutableSet;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.UnnestNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.plan.Patterns.unnest;

/**
 * Absorb pruning projection into the node.
 * Remove any unnecessary replicate symbols and ordinality symbol.
 * Symbol is considered unnecessary if it is:
 * - not referenced by the parent node
 * - not used in filter expression
 * Note: mappings are not eligible for pruning.
 * <p>
 * Transforms:
 * <pre>
 *  - Project (c)
 *      - Unnest:
 *          replicate (a, b, c, d)
 *          mappings (d -> d1)
 *          ordinality (ord)
 *          filter (a > 5)
 *          - Source (a, b, c d)
 * </pre>
 * into:
 * <pre>
 *  - Project (c)
 *      - Unnest:
 *          replicate (a, c)
 *          mappings (d -> d1)
 *          ordinality ()
 *          filter (a > 5)
 *          - Source (a, b, c, d)
 *  </pre>
 * Note: If, as a result of this rule, any of source's symbols became
 * unreferenced, it will be addressed by PruneUnnestSourceColumns rule.
 */
public class PruneUnnestColumns
        extends ProjectOffPushDownRule<UnnestNode>
{
    public PruneUnnestColumns()
    {
        super(unnest());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(Context context, UnnestNode unnestNode, Set<Symbol> referencedOutputs)
    {
        ImmutableSet.Builder<Symbol> referencedAndFilterSymbolsBuilder = ImmutableSet.<Symbol>builder()
                .addAll(referencedOutputs);
        Set<Symbol> referencedAndFilterSymbols = referencedAndFilterSymbolsBuilder.build();

        List<Symbol> prunedReplicateSymbols = unnestNode.getReplicateSymbols().stream()
                .filter(referencedAndFilterSymbols::contains)
                .collect(toImmutableList());

        Optional<Symbol> prunedOrdinalitySymbol = unnestNode.getOrdinalitySymbol()
                .filter(referencedAndFilterSymbols::contains);

        if (prunedReplicateSymbols.size() == unnestNode.getReplicateSymbols().size() && prunedOrdinalitySymbol.equals(unnestNode.getOrdinalitySymbol())) {
            return Optional.empty();
        }

        return Optional.of(new UnnestNode(
                unnestNode.getId(),
                unnestNode.getSource(),
                prunedReplicateSymbols,
                unnestNode.getMappings(),
                prunedOrdinalitySymbol,
                unnestNode.getJoinType()));
    }
}
