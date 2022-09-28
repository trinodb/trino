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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.spi.function.BoundSignature;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TopNRankingNode.RankingType;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.Expression;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.RANK;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.ROW_NUMBER;

final class Util
{
    private Util()
    {
    }

    /**
     * Prune the set of available inputs to those required by the given expressions.
     * <p>
     * If all inputs are used, return Optional.empty() to indicate that no pruning is necessary.
     */
    public static Optional<Set<Symbol>> pruneInputs(Collection<Symbol> availableInputs, Collection<Expression> expressions)
    {
        Set<Symbol> availableInputsSet = ImmutableSet.copyOf(availableInputs);
        Set<Symbol> prunedInputs = Sets.filter(availableInputsSet, SymbolsExtractor.extractUnique(expressions)::contains);

        if (prunedInputs.size() == availableInputsSet.size()) {
            return Optional.empty();
        }

        return Optional.of(prunedInputs);
    }

    /**
     * Transforms a plan like P->C->X to C->P->X
     */
    public static PlanNode transpose(PlanNode parent, PlanNode child)
    {
        return child.replaceChildren(ImmutableList.of(
                parent.replaceChildren(
                        child.getSources())));
    }

    /**
     * @return If the node has outputs not in permittedOutputs, returns an identity projection containing only those node outputs also in permittedOutputs.
     */
    public static Optional<PlanNode> restrictOutputs(PlanNodeIdAllocator idAllocator, PlanNode node, Set<Symbol> permittedOutputs)
    {
        List<Symbol> restrictedOutputs = node.getOutputSymbols().stream()
                .filter(permittedOutputs::contains)
                .collect(toImmutableList());

        if (restrictedOutputs.size() == node.getOutputSymbols().size()) {
            return Optional.empty();
        }

        return Optional.of(
                new ProjectNode(
                        idAllocator.getNextId(),
                        node,
                        Assignments.identity(restrictedOutputs)));
    }

    /**
     * @return The original node, with identity projections possibly inserted between node and each child, limiting the columns to those permitted.
     * Returns a present Optional iff at least one child was rewritten.
     */
    @SafeVarargs
    public static Optional<PlanNode> restrictChildOutputs(PlanNodeIdAllocator idAllocator, PlanNode node, Set<Symbol>... permittedChildOutputsArgs)
    {
        List<Set<Symbol>> permittedChildOutputs = ImmutableList.copyOf(permittedChildOutputsArgs);

        checkArgument(
                (node.getSources().size() == permittedChildOutputs.size()),
                "Mismatched child (%s) and permitted outputs (%s) sizes",
                node.getSources().size(),
                permittedChildOutputs.size());

        ImmutableList.Builder<PlanNode> newChildrenBuilder = ImmutableList.builder();
        boolean rewroteChildren = false;

        for (int i = 0; i < node.getSources().size(); ++i) {
            PlanNode oldChild = node.getSources().get(i);
            Optional<PlanNode> newChild = restrictOutputs(idAllocator, oldChild, permittedChildOutputs.get(i));
            rewroteChildren |= newChild.isPresent();
            newChildrenBuilder.add(newChild.orElse(oldChild));
        }

        if (!rewroteChildren) {
            return Optional.empty();
        }
        return Optional.of(node.replaceChildren(newChildrenBuilder.build()));
    }

    public static Optional<RankingType> toTopNRankingType(WindowNode node)
    {
        if (node.getWindowFunctions().size() != 1 || node.getOrderingScheme().isEmpty()) {
            return Optional.empty();
        }

        BoundSignature signature = getOnlyElement(node.getWindowFunctions().values()).getResolvedFunction().getSignature();
        if (!signature.getArgumentTypes().isEmpty()) {
            return Optional.empty();
        }
        if (signature.getName().equals("row_number")) {
            return Optional.of(ROW_NUMBER);
        }
        if (signature.getName().equals("rank")) {
            return Optional.of(RANK);
        }
        return Optional.empty();
    }
}
