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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.type.Type;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.plan.Patterns.exchange;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;

/**
 * Transforms:
 * <pre>
 *  Project(x = e1, y = e2)
 *    Exchange()
 *      Source(a, b, c)
 *  </pre>
 * to:
 * <pre>
 *  Exchange()
 *    Project(x = e1, y = e2)
 *      Source(a, b, c)
 *  </pre>
 * Or if Exchange needs symbols from Source for partitioning, ordering or as hash symbol to:
 * <pre>
 *  Project(x, y)
 *    Exchange()
 *      Project(x = e1, y = e2, a)
 *        Source(a, b, c)
 *  </pre>
 * To avoid looping this optimizer will not be fired if upper Project contains just symbol references.
 */
public class PushProjectionThroughExchange
        implements Rule<ProjectNode>
{
    private static final Capture<ExchangeNode> CHILD = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .matching(project -> !isSymbolToSymbolProjection(project))
            .with(source().matching(exchange().capturedAs(CHILD)));

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode project, Captures captures, Context context)
    {
        ExchangeNode exchange = captures.get(CHILD);
        Set<Symbol> partitioningColumns = exchange.getPartitioningScheme().getPartitioning().getColumns();

        ImmutableList.Builder<PlanNode> newSourceBuilder = ImmutableList.builder();
        ImmutableList.Builder<List<Symbol>> inputsBuilder = ImmutableList.builder();
        for (int i = 0; i < exchange.getSources().size(); i++) {
            Map<Symbol, Symbol> outputToInputMap = mapExchangeOutputToInput(exchange, i);

            Assignments.Builder projections = Assignments.builder();
            ImmutableList.Builder<Symbol> inputs = ImmutableList.builder();

            // Need to retain the partition keys for the exchange
            partitioningColumns.stream()
                    .map(outputToInputMap::get)
                    .forEach(inputSymbol -> {
                        projections.put(inputSymbol, inputSymbol.toSymbolReference());
                        inputs.add(inputSymbol);
                    });

            // Need to retain the hash symbol for the exchange
            exchange.getPartitioningScheme().getHashColumn()
                    .map(outputToInputMap::get)
                    .ifPresent(inputSymbol -> {
                        projections.put(inputSymbol, inputSymbol.toSymbolReference());
                        inputs.add(inputSymbol);
                    });

            if (exchange.getOrderingScheme().isPresent()) {
                // Need to retain ordering columns for the exchange
                exchange.getOrderingScheme().get().getOrderBy().stream()
                        // Do not duplicate symbols in inputs list
                        .filter(symbol -> !partitioningColumns.contains(symbol))
                        .map(outputToInputMap::get)
                        .forEach(inputSymbol -> {
                            projections.put(inputSymbol, inputSymbol.toSymbolReference());
                            inputs.add(inputSymbol);
                        });
            }

            ImmutableSet.Builder<Symbol> outputBuilder = ImmutableSet.builder();
            partitioningColumns.forEach(outputBuilder::add);
            exchange.getPartitioningScheme().getHashColumn().ifPresent(outputBuilder::add);
            exchange.getOrderingScheme().ifPresent(orderingScheme -> outputBuilder.addAll(orderingScheme.getOrderBy()));
            Set<Symbol> partitioningHashAndOrderingOutputs = outputBuilder.build();

            Map<Symbol, Expression> translationMap = outputToInputMap.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toSymbolReference()));

            for (Map.Entry<Symbol, Expression> projection : project.getAssignments().entrySet()) {
                // Skip identity projection if symbol is in outputs already
                if (partitioningHashAndOrderingOutputs.contains(projection.getKey())) {
                    continue;
                }
                Expression translatedExpression = inlineSymbols(translationMap, projection.getValue());
                Type type = context.getSymbolAllocator().getTypes().get(projection.getKey());
                Symbol symbol = context.getSymbolAllocator().newSymbol(translatedExpression, type);
                projections.put(symbol, translatedExpression);
                inputs.add(symbol);
            }
            newSourceBuilder.add(new ProjectNode(context.getIdAllocator().getNextId(), exchange.getSources().get(i), projections.build()));
            inputsBuilder.add(inputs.build());
        }

        // Construct the output symbols in the same order as the sources
        ImmutableList.Builder<Symbol> outputBuilder = ImmutableList.builder();
        partitioningColumns.forEach(outputBuilder::add);
        exchange.getPartitioningScheme().getHashColumn().ifPresent(outputBuilder::add);
        if (exchange.getOrderingScheme().isPresent()) {
            exchange.getOrderingScheme().get().getOrderBy().stream()
                    // Do not duplicate symbols in outputs list (for consistency with inputs lists)
                    .filter(symbol -> !partitioningColumns.contains(symbol))
                    .forEach(outputBuilder::add);
        }

        Set<Symbol> partitioningHashAndOrderingOutputs = ImmutableSet.copyOf(outputBuilder.build());

        for (Map.Entry<Symbol, Expression> projection : project.getAssignments().entrySet()) {
            // Do not add output for identity projection if symbol is in outputs already
            if (partitioningHashAndOrderingOutputs.contains(projection.getKey())) {
                continue;
            }
            outputBuilder.add(projection.getKey());
        }

        // outputBuilder contains all partition and hash symbols so simply swap the output layout
        PartitioningScheme partitioningScheme = new PartitioningScheme(
                exchange.getPartitioningScheme().getPartitioning(),
                outputBuilder.build(),
                exchange.getPartitioningScheme().getHashColumn(),
                exchange.getPartitioningScheme().isReplicateNullsAndAny(),
                exchange.getPartitioningScheme().getBucketToPartition());

        PlanNode result = new ExchangeNode(
                exchange.getId(),
                exchange.getType(),
                exchange.getScope(),
                partitioningScheme,
                newSourceBuilder.build(),
                inputsBuilder.build(),
                exchange.getOrderingScheme());

        // we need to strip unnecessary symbols (hash, partitioning columns).
        return Result.ofPlanNode(restrictOutputs(context.getIdAllocator(), result, ImmutableSet.copyOf(project.getOutputSymbols())).orElse(result));
    }

    private static boolean isSymbolToSymbolProjection(ProjectNode project)
    {
        return project.getAssignments().getExpressions().stream().allMatch(SymbolReference.class::isInstance);
    }

    private static Map<Symbol, Symbol> mapExchangeOutputToInput(ExchangeNode exchange, int sourceIndex)
    {
        ImmutableMap.Builder<Symbol, Symbol> outputToInputMap = ImmutableMap.builder();
        for (int i = 0; i < exchange.getOutputSymbols().size(); i++) {
            outputToInputMap.put(exchange.getOutputSymbols().get(i), exchange.getInputs().get(sourceIndex).get(i));
        }
        return outputToInputMap.buildOrThrow();
    }
}
