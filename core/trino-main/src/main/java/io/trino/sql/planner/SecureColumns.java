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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableSet;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsAndCosts;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.SecureExpression;
import io.trino.sql.ir.SecureExpressions;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.type.CharVarcharCoercion;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.preOrder;

/**
 * Domains derived from a {@link SecureExpression} are handed to connectors for enforcement only. They must not be
 * reported as a scan's constraint, rebuilt as a predicate, or reflected in the value ranges of recorded statistics.
 */
public final class SecureColumns
{
    private SecureColumns() {}

    /**
     * Symbols referenced by secure expressions in {@code expression}.
     */
    public static Set<Symbol> symbols(Expression expression)
    {
        return preOrder(expression)
                .filter(SecureExpression.class::isInstance)
                .flatMap(secure -> SymbolsExtractor.extractUnique(secure).stream())
                .collect(toImmutableSet());
    }

    /**
     * Symbols that carry values a secure expression constrains or computes: the symbols it references, the outputs of
     * secure masks, and whatever projections derive from either. Only aliases propagate backwards to the input,
     * since a derived expression does not make its independent inputs secure.
     * <p>
     * Per-fragment callers rely on a secure expression staying in its scan's fragment: {@code PushPredicateIntoTableScan}
     * keeps it as the residual directly above the scan, and fragments split only at remote exchanges.
     */
    public static Set<Symbol> symbols(PlanNode root)
    {
        Set<Symbol> secure = new HashSet<>();
        Map<Symbol, Expression> assignments = new HashMap<>();
        collect(root, secure, assignments);

        boolean changed = true;
        while (changed) {
            changed = false;
            for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
                Set<Symbol> inputs = SymbolsExtractor.extractUnique(assignment.getValue());
                if (secure.contains(assignment.getKey()) && assignment.getValue() instanceof Reference reference) {
                    changed |= secure.add(Symbol.from(reference));
                }
                if (!Collections.disjoint(inputs, secure)) {
                    changed |= secure.add(assignment.getKey());
                }
            }
        }
        return ImmutableSet.copyOf(secure);
    }

    private static void collect(PlanNode node, Set<Symbol> secure, Map<Symbol, Expression> assignments)
    {
        if (node instanceof GroupReference) {
            // Unresolved memo plans are only printed for debugging
            return;
        }
        ExpressionExtractor.extractExpressionsNonRecursive(node)
                .forEach(expression -> secure.addAll(symbols(expression)));
        if (node instanceof ProjectNode project) {
            project.getAssignments().forEach((output, expression) -> {
                assignments.putIfAbsent(output, expression);
                if (SecureExpressions.isPresent(expression)) {
                    // A secure mask's output is the policy's value
                    secure.add(output);
                }
            });
        }
        node.getSources().forEach(source -> collect(source, secure, assignments));
    }

    /**
     * Rebuilds {@code domain} as a predicate, wrapping the part over {@code secureSymbols} since it may derive from a
     * secure expression.
     */
    public static Expression toPredicate(DomainTranslator domainTranslator, CharVarcharCoercion charVarcharCoercion, TupleDomain<Symbol> domain, Set<Symbol> secureSymbols)
    {
        if (secureSymbols.isEmpty()) {
            return domainTranslator.toPredicate(charVarcharCoercion, domain);
        }
        Expression secure = domainTranslator.toPredicate(charVarcharCoercion, domain.filter((symbol, _) -> secureSymbols.contains(symbol)));
        return combineConjuncts(
                domainTranslator.toPredicate(charVarcharCoercion, domain.filter((symbol, _) -> !secureSymbols.contains(symbol))),
                TRUE.equals(secure) ? secure : new SecureExpression(secure));
    }

    /**
     * Drops value ranges from recorded statistics, since the range of any symbol downstream of a secure expression may
     * reproduce its bounds.
     */
    public static StatsAndCosts withoutValueRanges(StatsAndCosts statsAndCosts)
    {
        Map<PlanNodeId, PlanNodeStatsEstimate> stats = statsAndCosts.getStats().entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> withoutValueRanges(entry.getValue())));
        return new StatsAndCosts(stats, statsAndCosts.getCosts());
    }

    private static PlanNodeStatsEstimate withoutValueRanges(PlanNodeStatsEstimate estimate)
    {
        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.buildFrom(estimate);
        estimate.getSymbolStatistics().forEach((symbol, statistics) -> builder.addSymbolStatistics(
                symbol,
                SymbolStatsEstimate.buildFrom(statistics)
                        .setLowValue(Double.NEGATIVE_INFINITY)
                        .setHighValue(Double.POSITIVE_INFINITY)
                        .build()));
        return builder.build();
    }

    /**
     * Removes the domains of the scan columns that {@code secureSymbols} constrain.
     */
    public static TupleDomain<ColumnHandle> redact(TupleDomain<ColumnHandle> domain, TableScanNode scan, Set<Symbol> secureSymbols)
    {
        Set<ColumnHandle> secureColumns = scan.getAssignments().entrySet().stream()
                .filter(entry -> secureSymbols.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .collect(toImmutableSet());
        if (secureColumns.isEmpty()) {
            return domain;
        }
        return domain.filter((column, _) -> !secureColumns.contains(column));
    }
}
