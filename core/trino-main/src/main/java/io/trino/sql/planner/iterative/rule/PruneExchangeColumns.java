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
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.sql.planner.plan.Patterns.exchange;

/**
 * This rule restricts the outputs of ExchangeNode based on which
 * ExchangeNode's output symbols are either referenced by the
 * parent node or used for partitioning, ordering or as a hash
 * symbol by the ExchangeNode.
 * <p>
 * For each symbol removed from the output symbols list, the corresponding
 * input symbols are removed from ExchangeNode's inputs lists.
 * <p>
 * Transforms:
 * <pre>
 * - Project (o1)
 *      - Exchange:
 *        outputs [o1, o2, o3, h]
 *        partitioning by (o2)
 *        hash h
 *        inputs [[a1, a2, a3, h1], [b1, b2, b3, h2]]
 *          - source [a1, a2, a3, h1]
 *          - source [b1, b2, b3, h2]
 * </pre>
 * Into:
 * <pre>
 * - Project (o1)
 *      - Exchange:
 *        outputs [o1, o2, h]
 *        partitioning by (o2)
 *        hash h
 *        inputs [[a1, a2, h1], [b1, b2, h2]]
 *          - source [a1, a2, a3, h1]
 *          - source [b1, b2, b3, h2]
 * </pre>
 */
public class PruneExchangeColumns
        extends ProjectOffPushDownRule<ExchangeNode>
{
    public PruneExchangeColumns()
    {
        super(exchange());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(Context context, ExchangeNode exchangeNode, Set<Symbol> referencedOutputs)
    {
        // Extract output symbols referenced by parent node or used for partitioning, ordering or as a hash symbol of the Exchange
        ImmutableSet.Builder<Symbol> builder = ImmutableSet.builder();
        builder.addAll(referencedOutputs);
        builder.addAll(exchangeNode.getPartitioningScheme().getPartitioning().getColumns());
        exchangeNode.getPartitioningScheme().getHashColumn().ifPresent(builder::add);
        exchangeNode.getOrderingScheme().ifPresent(orderingScheme -> builder.addAll(orderingScheme.getOrderBy()));
        Set<Symbol> outputsToRetain = builder.build();

        if (outputsToRetain.size() == exchangeNode.getOutputSymbols().size()) {
            return Optional.empty();
        }

        ImmutableList.Builder<Symbol> newOutputs = ImmutableList.builder();
        List<List<Symbol>> newInputs = new ArrayList<>(exchangeNode.getInputs().size());
        for (int i = 0; i < exchangeNode.getInputs().size(); i++) {
            newInputs.add(new ArrayList<>());
        }

        // Retain used symbols from output list and corresponding symbols from all input lists
        for (int i = 0; i < exchangeNode.getOutputSymbols().size(); i++) {
            Symbol output = exchangeNode.getOutputSymbols().get(i);
            if (outputsToRetain.contains(output)) {
                newOutputs.add(output);
                for (int source = 0; source < exchangeNode.getInputs().size(); source++) {
                    newInputs.get(source).add(exchangeNode.getInputs().get(source).get(i));
                }
            }
        }

        // newOutputs contains all partition, sort and hash symbols so simply swap the output layout
        PartitioningScheme newPartitioningScheme = new PartitioningScheme(
                exchangeNode.getPartitioningScheme().getPartitioning(),
                newOutputs.build(),
                exchangeNode.getPartitioningScheme().getHashColumn(),
                exchangeNode.getPartitioningScheme().isReplicateNullsAndAny(),
                exchangeNode.getPartitioningScheme().getBucketToPartition(),
                exchangeNode.getPartitioningScheme().getPartitionCount());

        return Optional.of(new ExchangeNode(
                exchangeNode.getId(),
                exchangeNode.getType(),
                exchangeNode.getScope(),
                newPartitioningScheme,
                exchangeNode.getSources(),
                newInputs,
                exchangeNode.getOrderingScheme()));
    }
}
