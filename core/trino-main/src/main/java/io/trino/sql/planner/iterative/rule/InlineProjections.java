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
import com.google.common.collect.Sets;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.util.AstUtils;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.stream.Collectors.toSet;

/**
 * Inlines expressions from a child project node into a parent project node
 * as long as they are simple constants, or they are referenced only once (to
 * avoid introducing duplicate computation) and the references don't appear
 * within a TRY block (to avoid changing semantics).
 */
public class InlineProjections
        implements Rule<ProjectNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(project().capturedAs(CHILD)));

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode parent, Captures captures, Context context)
    {
        ProjectNode child = captures.get(CHILD);

        return inlineProjections(parent, child)
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }

    static Optional<ProjectNode> inlineProjections(ProjectNode parent, ProjectNode child)
    {
        Set<Symbol> targets = extractInliningTargets(parent, child);
        if (targets.isEmpty()) {
            return Optional.empty();
        }

        // inline the expressions
        Assignments assignments = child.getAssignments().filter(targets::contains);
        Map<Symbol, Expression> parentAssignments = parent.getAssignments()
                .entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> inlineReferences(entry.getValue(), assignments)));

        // Synthesize identity assignments for the inputs of expressions that were inlined
        // to place in the child projection.
        Set<Symbol> inputs = child.getAssignments()
                .entrySet().stream()
                .filter(entry -> targets.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .flatMap(entry -> SymbolsExtractor.extractAll(entry).stream())
                .collect(toSet());

        Assignments.Builder newChildAssignmentsBuilder = Assignments.builder();
        for (Map.Entry<Symbol, Expression> assignment : child.getAssignments().entrySet()) {
            if (!targets.contains(assignment.getKey())) {
                newChildAssignmentsBuilder.put(assignment);
            }
        }
        for (Symbol input : inputs) {
            newChildAssignmentsBuilder.putIdentity(input);
        }

        Assignments newChildAssignments = newChildAssignmentsBuilder.build();
        PlanNode newChild;
        if (newChildAssignments.isIdentity()) {
            newChild = child.getSource();
        }
        else {
            newChild = new ProjectNode(
                    child.getId(),
                    child.getSource(),
                    newChildAssignments);
        }

        return Optional.of(
                new ProjectNode(
                        parent.getId(),
                        newChild,
                        Assignments.copyOf(parentAssignments)));
    }

    private static Expression inlineReferences(Expression expression, Assignments assignments)
    {
        Function<Symbol, Expression> mapping = symbol -> {
            Expression result = assignments.get(symbol);
            if (result != null) {
                return result;
            }

            return symbol.toSymbolReference();
        };

        return inlineSymbols(mapping, expression);
    }

    private static Set<Symbol> extractInliningTargets(ProjectNode parent, ProjectNode child)
    {
        // candidates for inlining are
        //   1. references to simple constants or symbol references
        //   2. references to complex expressions that
        //      a. are not inputs to try() expressions
        //      b. appear only once across all expressions
        //      c. are not identity projections
        // which come from the child, as opposed to an enclosing scope.

        Set<Symbol> childOutputSet = ImmutableSet.copyOf(child.getOutputSymbols());

        Map<Symbol, Long> dependencies = parent.getAssignments()
                .getExpressions().stream()
                .flatMap(expression -> SymbolsExtractor.extractAll(expression).stream())
                .filter(childOutputSet::contains)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        // find references to simple constants or symbol references
        Set<Symbol> basicReferences = dependencies.keySet().stream()
                .filter(input -> child.getAssignments().get(input) instanceof Literal || child.getAssignments().get(input) instanceof SymbolReference)
                .filter(input -> !child.getAssignments().isIdentity(input)) // skip identities, otherwise, this rule will keep firing forever
                .collect(toSet());

        // exclude any complex inputs to TRY expressions. Inlining them would potentially
        // change the semantics of those expressions
        Set<Symbol> tryArguments = parent.getAssignments()
                .getExpressions().stream()
                .flatMap(expression -> extractTryArguments(expression).stream())
                .collect(toSet());

        Set<Symbol> singletons = dependencies.entrySet().stream()
                .filter(entry -> entry.getValue() == 1) // reference appears just once across all expressions in parent project node
                .filter(entry -> !tryArguments.contains(entry.getKey())) // they are not inputs to TRY. Otherwise, inlining might change semantics
                .filter(entry -> !child.getAssignments().isIdentity(entry.getKey())) // skip identities, otherwise, this rule will keep firing forever
                .filter(entry -> !(child.getAssignments().get(entry.getKey()) instanceof DereferenceExpression)) // skip dereferences, otherwise, inlining can cause conflicts with PushdownDereferences
                .map(Map.Entry::getKey)
                .collect(toSet());

        return Sets.union(singletons, basicReferences);
    }

    private static Set<Symbol> extractTryArguments(Expression expression)
    {
        return AstUtils.preOrder(expression)
                .filter(TryExpression.class::isInstance)
                .map(TryExpression.class::cast)
                .flatMap(tryExpression -> SymbolsExtractor.extractAll(tryExpression).stream())
                .collect(toSet());
    }
}
