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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

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
        // squash identity projections
        if (parent.isIdentity() && child.isIdentity()) {
            return Optional.of((ProjectNode) parent.replaceChildren(ImmutableList.of(child.getSource())));
        }

        Set<Symbol> targets = extractInliningTargets(parent, child);
        if (targets.isEmpty()) {
            return Optional.empty();
        }

        // inline the expressions
        Assignments assignments = child.getAssignments().filter(targets::contains);
        Assignments.Builder parentAssignments = Assignments.builder();
        for (Map.Entry<Symbol, Expression> assignment : parent.getAssignments().entrySet()) {
            parentAssignments.put(assignment.getKey(), inlineReferences(assignment.getValue(), assignments));
        }

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
                // If this is not an identity assignment, remove the symbol from inputs, as we don't want to reset the expression
                if (!isSymbolReference(assignment.getKey(), assignment.getValue())) {
                    inputs.remove(assignment.getKey());
                }
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
                        parentAssignments.build()));
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
                .filter(input -> child.getAssignments().get(input) instanceof Constant || child.getAssignments().get(input) instanceof Reference)
                .filter(input -> !child.getAssignments().isIdentity(input)) // skip identities, otherwise, this rule will keep firing forever
                .collect(toSet());

        Set<Symbol> singletons = dependencies.entrySet().stream()
                .filter(entry -> entry.getValue() == 1) // reference appears just once across all expressions in parent project node
                .filter(entry -> !child.getAssignments().isIdentity(entry.getKey())) // skip identities, otherwise, this rule will keep firing forever
                .filter(entry -> {
                    // skip dereferences, otherwise, inlining can cause conflicts with PushdownDereferences
                    Expression assignment = child.getAssignments().get(entry.getKey());

                    if (assignment instanceof FieldReference) {
                        return !(((FieldReference) assignment).base().type() instanceof RowType);
                    }

                    return true;
                })
                .map(Map.Entry::getKey)
                .collect(toSet());

        return Sets.union(singletons, basicReferences);
    }

    private static boolean isSymbolReference(Symbol symbol, Expression expression)
    {
        return expression instanceof Reference reference && reference.name().equals(symbol.name());
    }
}
