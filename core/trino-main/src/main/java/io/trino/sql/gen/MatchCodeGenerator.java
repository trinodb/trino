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
package io.trino.sql.gen;

import io.airlift.bytecode.BytecodeNode;
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/// Generates bytecode for [Match] as a searched [Case] over the operand bound by a [Let]: each
/// clause's lambda body becomes a condition with the parameter resolved to the binding.
/// Single-evaluation of the operand is the whole reason [Match] exists separately from a searched
/// [Case]; naively substituting the operand into each clause's predicate would re-evaluate it N
/// times and change semantics for non-deterministic operands such as `random()`.
///
/// For SQL three-valued logic, [Let] restores the operand's null state to `wasNull` on every read,
/// so `WHEN IS NULL` and other null-aware predicates see the operand as null, and [Case] selects a
/// clause only when its predicate is true, not null.
public class MatchCodeGenerator
        implements BytecodeGenerator
{
    private final Expression value;
    private final List<MatchClause> clauses;
    private final Expression defaultValue;

    public MatchCodeGenerator(Match matchExpression)
    {
        requireNonNull(matchExpression, "matchExpression is null");
        value = matchExpression.operand();
        clauses = matchExpression.clauses();
        defaultValue = matchExpression.defaultValue();
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generatorContext)
    {
        // Matches can be nested, so the name has to differ from the enclosing bindings
        Symbol operand = new Symbol(value.type(), "$$MATCH_OPERAND$$" + generatorContext.lets().size());
        List<WhenClause> whenClauses = clauses.stream()
                .map(clause -> new WhenClause(inlineBody(clause, operand.toSymbolReference()), clause.result()))
                .collect(toImmutableList());

        return generatorContext.generate(new Let(operand, value, new Case(whenClauses, defaultValue)));
    }

    /// Return the clause's lambda body with the operand parameter substituted by `operandReference`
    /// (so the body reads the once-evaluated operand) and any capture-desugared captured-parameters
    /// substituted back to their original outer references (so the body reads the outer scope
    /// directly). The result is suitable for inline bytecode generation in the surrounding method
    /// — the lambda interface call is dropped entirely.
    private static Expression inlineBody(MatchClause clause, Reference operandReference)
    {
        Lambda lambda = clause.lambda();
        Bind bind = clause.bind();
        List<Symbol> arguments = lambda.arguments();
        int captureCount = bind == null ? 0 : bind.values().size();
        // After LambdaCaptureDesugaringRewriter, the first captureCount args are captured-symbol
        // placeholders bound to the corresponding values in Bind.values; the remaining (always one
        // here, since Match's lambdas take a single operand parameter) is the operand parameter.
        Symbol operandParameter = arguments.getLast();

        Map<String, Expression> substitutions = new HashMap<>();
        substitutions.put(operandParameter.name(), operandReference);
        for (int i = 0; i < captureCount; i++) {
            substitutions.put(arguments.get(i).name(), bind.values().get(i));
        }

        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteReference(Reference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Expression replacement = substitutions.get(node.name());
                return replacement != null ? replacement : null;
            }

            @Override
            public Expression rewriteLambda(Lambda node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                // Don't recurse into nested lambdas that shadow any substituted name; if none of
                // the nested lambda's arguments shadows ours, fall through to keep recursing.
                if (node.arguments().stream().anyMatch(arg -> substitutions.containsKey(arg.name()))) {
                    return node;
                }
                return null;
            }
        }, lambda.body());
    }
}
