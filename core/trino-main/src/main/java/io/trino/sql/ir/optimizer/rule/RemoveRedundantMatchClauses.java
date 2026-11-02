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
package io.trino.sql.ir.optimizer.rule;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.OperatorType;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;

/// Remove duplicated and redundant equality clauses in Match. E.g.,
///
/// - `Match(x, [When(=a, r1), When(=b, r2), When(=a, r3)], d) -> Match(x, [When(=a, r1), When(=b, r2)], d)`
/// - `Match(x, [When(=a, r1), When(=x, r2), When(=b, r3)], d) -> Match(x, [When(=a, r1), When(=x, r2)], d)`
/// - `Match(1, [When(=1, r)], d) -> r`
///
/// Only equality-shaped clauses (lambdas of the form `(p) -> p = expr`) are removed; clauses with
/// other predicate bodies are opaque and preserved as-is, but they don't stop the clauses after them
/// from being analyzed.
public class RemoveRedundantMatchClauses
        implements IrOptimizerRule
{
    private final Metadata metadata;
    private final InterpretedFunctionInvoker functionInvoker;

    public RemoveRedundantMatchClauses(PlannerContext context)
    {
        metadata = context.getMetadata();
        functionInvoker = new InterpretedFunctionInvoker(context.getFunctionManager());
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Match(Expression operand, List<MatchClause> clauses, Expression defaultValue))) {
            return Optional.empty();
        }

        if (!isDeterministic(operand)) {
            return Optional.empty();
        }

        List<MatchClause> newClauses = new ArrayList<>();
        Expression newDefault = defaultValue;

        ResolvedFunction equals = metadata.resolveOperator(getCharVarcharCoercion(session), OperatorType.EQUAL, ImmutableList.of(operand.type(), operand.type()));

        Set<Expression> seen = new HashSet<>();
        boolean selfComparisonSeen = false;
        boolean changed = false;

        for (MatchClause clause : clauses) {
            Optional<Expression> equalityValue = extractEqualityValue(clause);
            if (equalityValue.isEmpty()) {
                // Non-equality predicate is opaque, so it can match regardless of what the equality
                // clauses around it compare the operand against. Keep it.
                newClauses.add(clause);
                continue;
            }

            Expression candidate = equalityValue.get();

            if (selfComparisonSeen) {
                // `operand = value` can only be TRUE when `operand = operand` is TRUE, and that
                // comparison was already made and didn't match, so this clause never matches either.
                // Note: the eliminated predicate can be fallible.
                changed = true;
            }
            else if (seen.contains(candidate)) {
                // deterministic and already seen, so previous match clause always wins
                changed = true;
            }
            else if (operand instanceof Constant constantOperand && candidate instanceof Constant constantCandidate) {
                changed = true;
                Boolean equal = (Boolean) functionInvoker.invoke(equals, session.toConnectorSession(), constantOperand.value(), constantCandidate.value());
                if (Boolean.TRUE.equals(equal)) {
                    newDefault = clause.result();
                    break;
                }
            }
            else if (operand.equals(candidate)) {
                // `operand = operand` is not necessarily TRUE: it's NULL when the operand is null or
                // a composite value containing a null, and FALSE for NaN. So the clause has to stay,
                // but no equality clause after it can match.
                newClauses.add(clause);
                selfComparisonSeen = true;
            }
            else {
                newClauses.add(clause);

                if (isDeterministic(candidate)) {
                    seen.add(candidate);
                }
            }
        }

        if (!changed) {
            return Optional.empty();
        }

        if (newClauses.isEmpty()) {
            return Optional.of(newDefault);
        }

        return Optional.of(new Match(operand, newClauses, newDefault));
    }

    private static Optional<Expression> extractEqualityValue(MatchClause clause)
    {
        // A Bind wrapper would shift the operand parameter past captured-symbol placeholders, and
        // the rewritten body would reference those placeholders rather than the original outer
        // symbols — neither shape is what this rule's equality dedupe expects, so bail.
        if (clause.bind() != null) {
            return Optional.empty();
        }
        Lambda predicate = clause.lambda();
        Symbol parameter = getOnlyElement(predicate.arguments());
        if (!(matchComparison(predicate.body()) instanceof Comparison.Equal(Expression left, Expression right))) {
            return Optional.empty();
        }
        if (left instanceof Reference reference && reference.name().equals(parameter.name())) {
            return Optional.of(right);
        }
        if (right instanceof Reference reference && reference.name().equals(parameter.name())) {
            return Optional.of(left);
        }
        return Optional.empty();
    }
}
