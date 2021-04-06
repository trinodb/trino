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
import com.google.common.collect.Sets;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.partitioningBy;

/**
 * Replace filter predicate conjuncts with underlying project expressions.
 * <p>
 * Transforms:
 * <pre>
 * - Filter (a AND b > c)
 *     - Project
 *       a <- d IS NULL
 *       b <- b
 *       c <- c
 *         - source (b, c, d)
 * </pre>
 * into:
 * <pre>
 * - Project
 *   a <- TRUE
 *   b <- b
 *   c <- c
 *     - Filter (d IS NULL AND b > c)
 *         - Project
 *           b <- b
 *           c <- c
 *           d <- d
 *         - source (b, c, d)
 * </pre>
 * In the preceding example, filter predicate conjunct `a` is replaced with
 * project expression `d IS NULL`.
 * Additionally:
 * - an identity assignment `d <- d` is added to the underlying projection in order
 * to expose the symbol `d` used by the rewritten filter predicate,
 * - the inlined assignment `a <- d IS NULL` is removed from the underlying projection.
 * - another projection is added above the rewritten FilterNode, assigning
 * TRUE_LITERAL to the replaced symbol `a`. It is needed to restore the original
 * output of the FilterNode. If the symbol `a` is not referenced in the upstream plan,
 * the projection should be subsequently removed by other rules.
 * <p>
 * Note: project expressions are inlined only in case when the resulting symbols
 * are referenced exactly once in the filter predicate. Otherwise, the resulting
 * plan could be less efficient due to duplicated computation. Also, inlining the
 * expression multiple times is not correct in case of non-deterministic expressions.
 */
public class InlineProjectIntoFilter
        implements Rule<FilterNode>
{
    private static final Capture<ProjectNode> PROJECTION = newCapture();

    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(project().capturedAs(PROJECTION)));

    private final Metadata metadata;

    public InlineProjectIntoFilter(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        ProjectNode projectNode = captures.get(PROJECTION);

        List<Expression> filterConjuncts = extractConjuncts(node.getPredicate());

        Map<Boolean, List<Expression>> conjuncts = filterConjuncts.stream()
                .collect(partitioningBy(SymbolReference.class::isInstance));
        List<Expression> simpleConjuncts = conjuncts.get(true);
        List<Expression> complexConjuncts = conjuncts.get(false);

        // Do not inline expression if the symbol is used multiple times in simple conjuncts.
        Set<Expression> simpleUniqueConjuncts = simpleConjuncts.stream()
                .collect(Collectors.groupingBy(identity(), counting()))
                .entrySet().stream()
                .filter(entry -> entry.getValue() == 1)
                .map(Map.Entry::getKey)
                .collect(toImmutableSet());

        Set<Expression> complexConjunctSymbols = SymbolsExtractor.extractUnique(complexConjuncts).stream()
                .map(Symbol::toSymbolReference)
                .collect(toImmutableSet());

        // Do not inline expression if the symbol is used in complex conjuncts.
        Set<Expression> simpleConjunctsToInline = Sets.difference(simpleUniqueConjuncts, complexConjunctSymbols);

        if (simpleConjunctsToInline.isEmpty()) {
            return Result.empty();
        }

        ImmutableList.Builder<Expression> newConjuncts = ImmutableList.builder();
        Assignments.Builder newAssignments = Assignments.builder();
        Assignments.Builder postFilterAssignmentsBuilder = Assignments.builder();

        for (Expression conjunct : filterConjuncts) {
            if (simpleConjunctsToInline.contains(conjunct)) {
                Expression expression = projectNode.getAssignments().get(Symbol.from(conjunct));
                if (expression == null || expression instanceof SymbolReference) {
                    // expression == null -> The symbol is not produced by the underlying projection (i.e. it is a correlation symbol).
                    // expression instanceof SymbolReference -> Do not inline trivial projections.
                    newConjuncts.add(conjunct);
                }
                else {
                    newConjuncts.add(expression);
                    newAssignments.putIdentities(SymbolsExtractor.extractUnique(expression));
                    postFilterAssignmentsBuilder.put(Symbol.from(conjunct), TRUE_LITERAL);
                }
            }
            else {
                newConjuncts.add(conjunct);
            }
        }

        Assignments postFilterAssignments = postFilterAssignmentsBuilder.build();
        if (postFilterAssignments.isEmpty()) {
            return Result.empty();
        }

        Set<Symbol> postFilterSymbols = postFilterAssignments.getSymbols();
        // Remove inlined expressions from the underlying projection.
        newAssignments.putAll(projectNode.getAssignments().filter(symbol -> !postFilterSymbols.contains(symbol)));

        Map<Symbol, Expression> outputAssignments = new HashMap<>();
        outputAssignments.putAll(Assignments.identity(node.getOutputSymbols()).getMap());
        // Restore inlined symbols.
        outputAssignments.putAll(postFilterAssignments.getMap());

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                new FilterNode(
                        node.getId(),
                        new ProjectNode(
                                projectNode.getId(),
                                projectNode.getSource(),
                                newAssignments.build()),
                        combineConjuncts(metadata, newConjuncts.build())),
                Assignments.builder()
                        .putAll(outputAssignments)
                        .build()));
    }
}
