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
package org.weakref.solver;

import org.weakref.solver.Expression.Application;
import org.weakref.solver.Expression.Literal;
import org.weakref.solver.Expression.Symbol;
import org.weakref.solver.Expression.Variable;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * First-order structural unification over {@link Expression}.
 * <p>
 * Takes two expressions (possibly containing free variables) and either returns a
 * substitution that makes them equal ({@link Success}) or declares them incompatible
 * ({@link Failure}).
 * <p>
 * Standard algorithm:
 * <ol>
 *   <li>Maintain a queue of pairs to unify.</li>
 *   <li>For each pair, substitute current bindings first (so both sides reflect anything
 *       learned about their variables already).</li>
 *   <li>Cases:
 *     <ul>
 *       <li>identical — done;</li>
 *       <li>variable on either side — bind it (after an occurs check via {@link #contains});</li>
 *       <li>both {@link Application}, matching arity — recurse into head and arguments;</li>
 *       <li>both {@link Expression.Row}, matching arity — recurse into field types;</li>
 *       <li>{@link Expression.AnyRow} on either side against a {@link Expression.Row} — treated
 *           as equal (AnyRow is a row-family wildcard);</li>
 *       <li>anything else — fail.</li>
 *     </ul>
 *   </li>
 * </ol>
 * Used by {@link Domain#constrain} (intersecting alternative sets) and
 * {@link PatternCoercion} (unifying logical-variable occurrences across the two pattern
 * sides).
 */
public class Unifier
{
    private Unifier() {}

    public static Result unify(Expression left, Expression right)
    {
        Map<String, Expression> bindings = new HashMap<>();

        Queue<Entry> queue = new ArrayDeque<>();
        queue.add(new Entry(left, right));

        while (!queue.isEmpty()) {
            Entry entry = queue.poll();

            // Re-substitute both sides with everything learned so far before dispatching.
            // This is what lets cascading information flow through — e.g. after binding @X=int,
            // a pending (@X, bigint) pair becomes (int, bigint), which then fails cleanly.
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

    /**
     * Fold a new batch of bindings into the accumulated map, making sure existing bindings are
     * rewritten through the new ones (so that e.g. {@code @X → @Y} followed by {@code @Y → int}
     * collapses {@code @X} all the way to {@code int}).
     */
    private static void combine(Map<String, Expression> bindings, Map<String, Expression> newBindings)
    {
        for (Map.Entry<String, Expression> entry : bindings.entrySet()) {
            Expression substituted = Expression.substitute(entry.getValue(), newBindings);
            entry.setValue(substituted);
        }
        bindings.putAll(newBindings);
    }

    /**
     * Occurs check — prevents creating an infinite type by binding {@code variable} to an
     * expression that references {@code variable}.
     */
    private static boolean contains(Expression expression, String variable)
    {
        return switch (expression) {
            case Variable(String name) -> name.equals(variable);
            case Literal _, Symbol _, Expression.AnyRow _ -> false;
            case Application(Expression head, List<Expression> arguments) -> contains(head, variable) || arguments.stream().anyMatch(argument -> contains(argument, variable));
            case Expression.Row(List<Expression.RowField> fields) -> fields.stream().anyMatch(field -> contains(field.type(), variable));
            case Expression.BinaryOperation(Expression.BinaryOperator _, Expression left, Expression right) -> contains(left, variable) || contains(right, variable);
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
