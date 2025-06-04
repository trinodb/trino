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
import com.google.common.collect.Maps;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.WindowNode;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.plan.Patterns.window;

public class PruneWindowColumns
        extends ProjectOffPushDownRule<WindowNode>
{
    public PruneWindowColumns()
    {
        super(window());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(Context context, WindowNode windowNode, Set<Symbol> referencedOutputs)
    {
        Map<Symbol, WindowNode.Function> referencedFunctions = Maps.filterKeys(
                windowNode.getWindowFunctions(),
                referencedOutputs::contains);

        if (referencedFunctions.isEmpty()) {
            return Optional.of(windowNode.getSource());
        }

        ImmutableSet.Builder<Symbol> referencedInputs = ImmutableSet.<Symbol>builder()
                .addAll(windowNode.getSource().getOutputSymbols().stream()
                        .filter(referencedOutputs::contains)
                        .iterator())
                .addAll(windowNode.getPartitionBy());

        windowNode.getOrderingScheme().ifPresent(
                orderingScheme -> orderingScheme
                        .orderBy()
                        .forEach(referencedInputs::add));
        windowNode.getHashSymbol().ifPresent(referencedInputs::add);

        for (WindowNode.Function windowFunction : referencedFunctions.values()) {
            windowFunction.getOrderingScheme().ifPresent(orderingScheme -> referencedInputs.addAll(orderingScheme.orderBy()));
            referencedInputs.addAll(SymbolsExtractor.extractUnique(windowFunction));
        }

        PlanNode prunedWindowNode = new WindowNode(
                windowNode.getId(),
                restrictOutputs(context.getIdAllocator(), windowNode.getSource(), referencedInputs.build())
                        .orElse(windowNode.getSource()),
                windowNode.getSpecification(),
                referencedFunctions,
                windowNode.getHashSymbol(),
                windowNode.getPrePartitionedInputs(),
                windowNode.getPreSortedOrderPrefix());

        if (prunedWindowNode.getOutputSymbols().size() == windowNode.getOutputSymbols().size()) {
            // Neither function pruning nor input pruning was successful.
            return Optional.empty();
        }

        return Optional.of(prunedWindowNode);
    }
}
