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
package io.trino.typesolver;

import io.trino.typesolver.Expression.Application;
import io.trino.typesolver.Expression.Literal;
import io.trino.typesolver.Expression.Symbol;
import io.trino.typesolver.Expression.Variable;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/// First-order structural unification over [Expression].
///
/// Takes two expressions (possibly containing free variables) and either returns a
/// substitution that makes them equal ([Success]) or declares them incompatible
/// ([Failure]).
///
/// Standard algorithm:
///
/// 1. Maintain a queue of pairs to unify.
/// 2. For each pair, substitute current bindings first (so both sides reflect anything
///    learned about their variables already).
/// 3. Cases:
///
///    - identical — done;
///    - variable on either side — bind it (after an occurs check via [#contains]);
///    - both [Application], matching arity — recurse into head and arguments;
///    - both [Expression.Row], matching arity — recurse into field types;
///    - [Expression.AnyRow] on either side against a [Expression.Row] — treated
///      as equal (AnyRow is a row-family wildcard);
///    - anything else — fail.
///
/// Used by [Domain#constrain] (intersecting alternative sets) and
/// [PatternCoercion] (unifying logical-variable occurrences across the two pattern
/// sides).
public class Unifier
{
    private Unifier() {}

    public static Result unify(Expression left, Expression right)
    {
        ResolutionBudget.consume();
        if (left.equals(right)) {
            return new Success(Map.of());
        }
        if (left instanceof Variable(String variable)) {
            return contains(right, variable) ? new Failure(left, right) : new Success(Map.of(variable, right));
        }
        if (right instanceof Variable(String variable)) {
            return contains(left, variable) ? new Failure(left, right) : new Success(Map.of(variable, left));
        }
        if (left instanceof Symbol || right instanceof Symbol || left instanceof Literal || right instanceof Literal) {
            return new Failure(left, right);
        }
        if (left instanceof Application(Symbol leftHead, List<Expression> leftArguments) &&
                right instanceof Application(Symbol rightHead, List<Expression> rightArguments)) {
            if (leftArguments.size() != rightArguments.size()) {
                return new Failure(left, right);
            }
            if (!leftHead.equals(rightHead)) {
                return new Failure(leftHead, rightHead);
            }
            if (leftArguments.stream().allMatch(Unifier::isLeaf) && rightArguments.stream().allMatch(Unifier::isLeaf)) {
                return unifyFlatArguments(leftArguments, rightArguments);
            }
        }
        Map<String, Expression> bindings = new HashMap<>();

        Queue<Entry> queue = new ArrayDeque<>();
        queue.add(new Entry(left, right));

        while (!queue.isEmpty()) {
            ResolutionBudget.consume();
            Entry entry = queue.poll();

            // Re-substitute both sides with everything learned so far before dispatching.
            // This is what lets cascading information flow through — e.g. after binding X=int,
            // a pending (X, bigint) pair becomes (int, bigint), which then fails cleanly.
            left = Expression.substitute(entry.left, bindings);
            right = Expression.substitute(entry.right, bindings);

            Map<String, Expression> newBindings = new HashMap<>();
            switch (new Entry(left, right)) {
                case Entry(Expression leftExpression, Expression rightExpression) when leftExpression.equals(rightExpression) -> {
                    continue;
                }
                case Entry(Variable(String variable), Expression expression) when !contains(expression, variable) -> newBindings.put(variable, expression);
                case Entry(Expression expression, Variable(String variable)) when !contains(expression, variable) -> newBindings.put(variable, expression);
                case Entry(Application(Expression leftHead, List<Expression> leftArguments), Application(Expression rightHead, List<Expression> rightArguments)) -> {
                    if (leftArguments.size() != rightArguments.size()) {
                        return new Failure(left, right);
                    }
                    queue.add(new Entry(leftHead, rightHead));
                    for (int i = 0; i < leftArguments.size(); i++) {
                        queue.add(new Entry(leftArguments.get(i), rightArguments.get(i)));
                    }
                }
                case Entry(Expression.Row(List<Expression.RowField> leftFields), Expression.Row(List<Expression.RowField> rightFields)) -> {
                    if (leftFields.size() != rightFields.size()) {
                        return new Failure(left, right);
                    }
                    for (int i = 0; i < leftFields.size(); i++) {
                        queue.add(new Entry(leftFields.get(i).type(), rightFields.get(i).type()));
                    }
                }
                case Entry(Expression.AnyRow _, Expression.AnyRow _) -> {
                    continue;
                }
                case Entry(Expression.AnyRow _, Expression.Row _), Entry(Expression.Row _, Expression.AnyRow _) -> {
                    continue;
                }
                case Entry(Symbol _, Symbol _), Entry(Literal _, Literal _) -> {
                    return new Failure(left, right);
                }
                default -> {
                    return new Failure(left, right);
                }
            }

            if (!newBindings.isEmpty()) {
                combine(bindings, newBindings);
            }
        }

        return new Success(bindings);
    }

    private static boolean isLeaf(Expression expression)
    {
        return expression instanceof Variable || expression instanceof Symbol || expression instanceof Literal;
    }

    private static Result unifyFlatArguments(List<Expression> leftArguments, List<Expression> rightArguments)
    {
        Map<String, Expression> bindings = new HashMap<>();
        for (int index = 0; index < leftArguments.size(); index++) {
            ResolutionBudget.consume();
            Expression leftArgument = Expression.substitute(leftArguments.get(index), bindings);
            Expression rightArgument = Expression.substitute(rightArguments.get(index), bindings);
            if (leftArgument.equals(rightArgument)) {
                continue;
            }
            // Leaves cannot contain the bound variable. Resolve aliases before comparing
            // each pair, then flatten the resulting substitution once at the end.
            if (leftArgument instanceof Variable(String name)) {
                bindings.put(name, rightArgument);
            }
            else if (rightArgument instanceof Variable(String name)) {
                bindings.put(name, leftArgument);
            }
            else {
                return new Failure(leftArgument, rightArgument);
            }
        }
        bindings.replaceAll((_, value) -> Expression.substitute(value, bindings));
        return new Success(bindings);
    }

    /// Fold a new batch of bindings into the accumulated map, making sure existing bindings are
    /// rewritten through the new ones (so that e.g. `X → Y` followed by `Y → int`
    /// collapses `X` all the way to `int`).
    private static void combine(Map<String, Expression> bindings, Map<String, Expression> newBindings)
    {
        for (Map.Entry<String, Expression> entry : bindings.entrySet()) {
            Expression substituted = Expression.substitute(entry.getValue(), newBindings);
            entry.setValue(substituted);
        }
        bindings.putAll(newBindings);
    }

    /// Occurs check — prevents creating an infinite type by binding `variable` to an
    /// expression that references `variable`.
    private static boolean contains(Expression expression, String variable)
    {
        return switch (expression) {
            case Variable(String name) -> name.equals(variable);
            case Literal _, Symbol _, Expression.AnyRow _ -> false;
            case Application(Expression head, List<Expression> arguments) -> contains(head, variable) || arguments.stream().anyMatch(argument -> contains(argument, variable));
            case Expression.Row(List<Expression.RowField> fields) -> fields.stream().anyMatch(field -> contains(field.type(), variable));
            case Expression.BinaryOperation(Expression.BinaryOperator _, Expression left, Expression right) -> contains(left, variable) || contains(right, variable);
            case Expression.Conditional(Expression.BinaryOperation condition, Expression ifTrue, Expression ifFalse) -> contains(condition, variable) || contains(ifTrue, variable) || contains(ifFalse, variable);
            case Expression.FunctionType functionType -> functionType.parameterTypes().stream().anyMatch(parameter -> contains(parameter, variable))
                    || functionType.variadicParameterType().map(parameter -> contains(parameter, variable)).orElse(false)
                    || contains(functionType.returnType(), variable);
        };
    }

    private record Entry(Expression left, Expression right) {}

    public sealed interface Result
            permits Success, Failure {}

    public record Success(Map<String, Expression> bindings)
            implements Result {}

    public record Failure(Expression left, Expression right)
            implements Result {}
}
