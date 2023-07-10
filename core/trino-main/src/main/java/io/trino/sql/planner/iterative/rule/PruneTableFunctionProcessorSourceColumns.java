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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughColumn;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Maps.filterKeys;
import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.plan.Patterns.tableFunctionProcessor;

/**
 * This rule prunes unreferenced outputs of TableFunctionProcessorNode.
 * First, it extracts all symbols required for:
 * - pass-through
 * - table function computation
 * - partitioning and ordering (including the hashSymbol)
 * Next, a mapping of input symbols to marker symbols is updated
 * so that it only contains mappings for the required symbols.
 * Last, all the remaining marker symbols are added to the collection
 * of required symbols.
 * Any source output symbols not included in the required symbols
 * can be pruned.
 */
public class PruneTableFunctionProcessorSourceColumns
        implements Rule<TableFunctionProcessorNode>
{
    private static final Pattern<TableFunctionProcessorNode> PATTERN = tableFunctionProcessor();

    @Override
    public Pattern<TableFunctionProcessorNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableFunctionProcessorNode node, Captures captures, Context context)
    {
        if (node.getSource().isEmpty()) {
            return Result.empty();
        }

        ImmutableSet.Builder<Symbol> requiredInputs = ImmutableSet.builder();

        node.getPassThroughSpecifications().stream()
                .map(PassThroughSpecification::columns)
                .flatMap(Collection::stream)
                .map(PassThroughColumn::symbol)
                .forEach(requiredInputs::add);

        node.getRequiredSymbols().stream()
                .forEach(requiredInputs::addAll);

        node.getSpecification().ifPresent(specification -> {
            requiredInputs.addAll(specification.getPartitionBy());
            specification.getOrderingScheme().ifPresent(orderingScheme -> requiredInputs.addAll(orderingScheme.getOrderBy()));
        });

        node.getHashSymbol().ifPresent(requiredInputs::add);

        Optional<Map<Symbol, Symbol>> updatedMarkerSymbols = node.getMarkerSymbols()
                .map(mapping -> filterKeys(mapping, requiredInputs.build()::contains));

        updatedMarkerSymbols.ifPresent(mapping -> requiredInputs.addAll(mapping.values()));

        return restrictOutputs(context.getIdAllocator(), node.getSource().orElseThrow(), requiredInputs.build())
                .map(child -> Result.ofPlanNode(new TableFunctionProcessorNode(
                        node.getId(),
                        node.getName(),
                        node.getProperOutputs(),
                        Optional.of(child),
                        node.isPruneWhenEmpty(),
                        node.getPassThroughSpecifications(),
                        node.getRequiredSymbols(),
                        updatedMarkerSymbols,
                        node.getSpecification(),
                        node.getPrePartitioned(),
                        node.getPreSorted(),
                        node.getHashSymbol(),
                        node.getHandle())))
                .orElse(Result.empty());
    }
}
