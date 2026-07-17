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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableList;
import io.trino.sql.jsonpath.tree.ArrayAccessor;
import io.trino.sql.jsonpath.tree.ContextVariable;
import io.trino.sql.jsonpath.tree.JsonPath;
import io.trino.sql.jsonpath.tree.MemberAccessor;
import io.trino.sql.jsonpath.tree.PathNode;
import io.trino.sql.jsonpath.tree.SqlValueLiteral;
import io.trino.sql.tree.ArrayWildcardSubscript;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.SubscriptExpression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/// A SQL:2023 §6.36 simplified accessor like `j.foo[3].bar.*`, split into
/// two pieces the analyzer needs separately:
///
///   1. `[#prefix()]` — the leading identifiers that might form a JSON
///      column name. For `j.foo[3].bar.*` that's `[j, foo]`.
///   2. `[#outerSteps()]` — the accessor steps left over. For
///      `j.foo[3].bar.*` that's `[[3], .bar, .*]`. These never join the
///      column-name candidate; they always end up in the JSON path that
///      the column is projected through.
///
/// To analyze a chain the caller tries `[#prefix()]` lengths against the
/// scope (longest first). If a length-`k` prefix matches a JSON column,
/// the JSON path becomes the remaining `k..prefix.size()` identifiers
/// followed by `[#outerSteps()]`.
///
/// For outer-scope correlated subqueries the caller also needs the
/// sub-expression of the user's input that corresponds to a matched
/// prefix — that's what `[#matchedPrefixExpression(int)]` returns, backed
/// by `[#visitedNodes()]` which records the [Expression] visited at each
/// step of `[#walk(Expression)]`.
///
/// `[#buildPath(List)]` produces the [JsonPath] tree directly (no
/// path-string round-trip). Per SQL:2023 §6.36 NOTE 198 / NOTE 199,
/// member-accessor identifiers are emitted verbatim (no implicit case
/// folding).
///
/// @param prefix the leading identifiers of the chain. Some leading subsequence of them
///         resolves to the JSON column reference (possibly qualified); the identifiers
///         beyond that subsequence are JSON member names emitted into the path.
/// @param outerSteps accessor steps left over after `prefix`; always
///         emitted into the JSON path.
/// @param visitedNodes the [Expression] visited at each step of `walk`,
///         used by `matchedPrefixExpression` to recover the
///         sub-expression for a matched prefix length. Empty for chains
///         constructed from a qualified name rather than walked from an
///         expression tree.
record JsonAccessorChain(List<Identifier> prefix, List<AccessorStep> outerSteps, List<Expression> visitedNodes)
{
    JsonAccessorChain
    {
        prefix = ImmutableList.copyOf(prefix);
        outerSteps = ImmutableList.copyOf(outerSteps);
        visitedNodes = ImmutableList.copyOf(visitedNodes);
    }

    /// Returns the sub-[Expression] of the user's input that corresponds
    /// to a matched prefix length.
    ///
    /// @param prefixLength number of leading `prefix()` identifiers that
    ///         resolved to a column.
    /// @return the matching input sub-expression, or [Optional#empty()]
    ///         for a chain with no visited nodes.
    Optional<Expression> matchedPrefixExpression(int prefixLength)
    {
        if (visitedNodes.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(visitedNodes.get(visitedNodes.size() - prefixLength));
    }

    /// Returns a chain for a wildcard receiver, accepting both a bare
    /// [Identifier] (a single-step column reference such as `j` in
    /// `SELECT j.*`) and a longer accessor chain that [#walk(Expression)]
    /// can peel.
    ///
    /// `walk` rejects a chain with no steps; this wraps that case so callers
    /// don't have to duplicate the bare-identifier branch.
    ///
    /// @param target the wildcard receiver expression.
    /// @return the chain when `target` is either a bare `Identifier` or a
    ///         walkable accessor; empty otherwise.
    static Optional<JsonAccessorChain> walkForWildcard(Expression target)
    {
        if (target instanceof Identifier root) {
            return Optional.of(new JsonAccessorChain(ImmutableList.of(root), ImmutableList.of(), ImmutableList.of(target)));
        }
        return walk(target);
    }

    static Optional<JsonAccessorChain> walk(Expression target)
    {
        List<AccessorStep> stepsReversed = new ArrayList<>();
        List<Expression> visited = new ArrayList<>();
        visited.add(target);
        Expression node = target;
        boolean progressed = true;
        while (progressed) {
            WalkStep walkStep = switch (node) {
                case DereferenceExpression dereference -> new WalkStep(
                        dereference.getField().<AccessorStep>map(AccessorStep.Member::new).orElse(AccessorStep.WildcardMember.INSTANCE),
                        dereference.getBase());
                case SubscriptExpression subscript when subscript.getIndex() instanceof LongLiteral literal -> new WalkStep(
                        new AccessorStep.Index(literal.getParsedValue()),
                        subscript.getBase());
                case ArrayWildcardSubscript wildcard -> new WalkStep(
                        AccessorStep.WildcardArray.INSTANCE,
                        wildcard.getBase());
                case null, default -> null;
            };
            if (walkStep == null) {
                progressed = false;
            }
            else {
                stepsReversed.add(walkStep.step());
                node = walkStep.nextNode();
                visited.add(node);
            }
        }
        if (stepsReversed.isEmpty()) {
            return Optional.empty();
        }
        Collections.reverse(stepsReversed);

        // the loop peels every DereferenceExpression, so the remaining node is the
        // chain's root: only a bare identifier can anchor a column-reference prefix
        if (!(node instanceof Identifier root)) {
            return Optional.empty();
        }

        ImmutableList.Builder<Identifier> prefix = ImmutableList.<Identifier>builder().add(root);
        int outerStart = 0;
        while (outerStart < stepsReversed.size() && stepsReversed.get(outerStart) instanceof AccessorStep.Member member) {
            prefix.add(member.name());
            outerStart++;
        }
        return Optional.of(new JsonAccessorChain(prefix.build(), stepsReversed.subList(outerStart, stepsReversed.size()), visited));
    }

    private record WalkStep(AccessorStep step, Expression nextNode) {}

    /// Builds the [JsonPath] tree (`lax $.<…>`) that represents the given
    /// accessor steps.
    ///
    /// The result is the same tree the JSON path parser would produce for
    /// the canonical string form, but skips the path-string round-trip
    /// (which would be re-parsed during planning — a form of AST synthesis
    /// the accessor avoids).
    ///
    /// Per SQL:2023 §6.36 NOTE 198 / NOTE 199, no implicit case folding is
    /// performed: unquoted identifiers contribute their literal value as
    /// the member-accessor key.
    ///
    /// @param steps the accessor steps in source order.
    /// @return a lax [JsonPath] rooted at a [ContextVariable].
    static JsonPath buildPath(List<AccessorStep> steps)
    {
        PathNode root = new ContextVariable();
        for (AccessorStep step : steps) {
            root = switch (step) {
                case AccessorStep.Member member -> new MemberAccessor(root, Optional.of(member.name().getValue()));
                case AccessorStep.Index index -> new ArrayAccessor(root, ImmutableList.of(
                        new ArrayAccessor.Subscript(new SqlValueLiteral(new LongLiteral(Long.toString(index.value()))))));
                case AccessorStep.WildcardMember _ -> new MemberAccessor(root, Optional.empty());
                case AccessorStep.WildcardArray _ -> new ArrayAccessor(root, ImmutableList.of());
            };
        }
        return new JsonPath(true, root);
    }
}
