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

import io.trino.Session;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.SystemSessionProperties.isFunctionPreimagesEnabled;
import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.trino.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.canonicalizeExpression;
import static io.trino.sql.planner.iterative.rule.UnwrapCastInComparison.unwrapCasts;
import static io.trino.sql.planner.iterative.rule.UnwrapFunctionInComparison.unwrap;

/// Substitution of a projection's assignments into a predicate above it, shared by predicate
/// pushdown, its enforcement analysis and scan constraint derivation.
public final class ProjectionInlining
{
    private ProjectionInlining() {}

    /// Whether `expression` can be rewritten in terms of the projection's inputs without
    /// duplicating the evaluation of a complex assignment.
    public static boolean isInliningCandidate(Expression expression, ProjectNode node)
    {
        // candidate symbols for inlining are
        //   1. references to simple constants or symbol references
        //   2. references to complex expressions that appear only once
        // which come from the node, as opposed to an enclosing scope.
        Map<Symbol, Expression> assignments = node.getAssignments().assignments();
        Map<Symbol, Long> dependencies = SymbolsExtractor.extractAll(expression).stream()
                .filter(assignments::containsKey)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        return dependencies.entrySet().stream()
                .allMatch(entry -> entry.getValue() == 1
                        || node.getAssignments().get(entry.getKey()) instanceof Constant
                        || node.getAssignments().get(entry.getKey()) instanceof Reference);
    }

    /// Rewrites `predicate` in terms of the projection's inputs and unwraps the resulting comparisons.
    public static Expression inlinePredicate(Session session, PlannerContext plannerContext, SymbolAllocator symbolAllocator, ProjectNode node, Expression predicate)
    {
        Expression inlined = inlineSymbols(node.getAssignments().assignments(), predicate);
        // normalize expressions to a form that comparison preimages understand
        Expression canonical = canonicalizeExpression(inlined, plannerContext, getCharVarcharCoercion(session));
        if (isFunctionPreimagesEnabled(session)) {
            return unwrap(plannerContext, session, symbolAllocator, canonical);
        }
        return unwrapCasts(session, plannerContext, symbolAllocator, canonical);
    }
}
