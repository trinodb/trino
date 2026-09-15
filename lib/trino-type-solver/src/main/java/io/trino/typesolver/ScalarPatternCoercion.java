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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/// Prepared scalar rules keep immutable parameter slots and guard templates in the type
/// system. Each match owns a binding array; aliases and unsupported inputs use the original
/// rule. The public pattern representation and its matching API remain unchanged.
final class ScalarPatternCoercion
        implements CoercionRule
{
    private final PatternCoercion original;
    private final ScalarPattern fromPattern;
    private final ScalarPattern toPattern;
    private final Set<String> names;
    private final List<Constraint> constraints;
    private final List<NumericTemplate> guards;
    private final String ruleId;

    private ScalarPatternCoercion(PatternCoercion original, ScalarPattern fromPattern, ScalarPattern toPattern, Set<String> names, List<Constraint> constraints, List<NumericTemplate> guards)
    {
        this.original = original;
        this.fromPattern = fromPattern;
        this.toPattern = toPattern;
        this.names = names;
        this.constraints = constraints;
        this.guards = guards;
        this.ruleId = original.ruleId();
    }

    static CoercionRule prepare(CoercionRule rule)
    {
        if (!(rule instanceof PatternCoercion pattern)) {
            return rule;
        }
        Map<String, Integer> slots = new LinkedHashMap<>();
        ScalarPattern from = ScalarPattern.compile(pattern.fromPattern(), slots);
        ScalarPattern to = ScalarPattern.compile(pattern.toPattern(), slots);
        // Fresh allocator names must not be captured by another logical parameter.
        if (from == null || to == null || slots.keySet().stream().anyMatch(name -> name.startsWith("$v"))) {
            return rule;
        }
        List<Constraint> constraints = List.copyOf(pattern.constraints());
        List<NumericTemplate> guards = new ArrayList<>();
        for (Constraint constraint : constraints) {
            if (!(constraint instanceof NumericRelation(Expression.BinaryOperation operation))) {
                return rule;
            }
            NumericTemplate guard = compileNumeric(operation, slots, 0);
            if (guard == null) {
                return rule;
            }
            guards.add(guard);
        }
        return new ScalarPatternCoercion(pattern, from, to, Set.copyOf(slots.keySet()), constraints, List.copyOf(guards));
    }

    @Override
    public String ruleId()
    {
        return ruleId;
    }

    PatternCoercion original()
    {
        return original;
    }

    @Override
    public Optional<Match> matches(VariableAllocator allocator, Expression from, Expression to)
    {
        return match(allocator, from, to, true);
    }

    Optional<Set<Constraint>> matchConstraints(VariableAllocator allocator, Expression from, Expression to)
    {
        return match(allocator, from, to, false).map(Match::constraints);
    }

    private Optional<Match> match(VariableAllocator allocator, Expression from, Expression to, boolean includePlan)
    {
        // PatternCoercion accepts caller-owned constraint lists. If one changes after
        // registration, its prepared guards no longer describe the original rule.
        if (!original.constraints().equals(constraints)) {
            return fallback(allocator, from, to, includePlan);
        }
        boolean fromScalar = isScalar(from);
        boolean toScalar = isScalar(to);
        boolean fromGround = fromScalar && Expression.isGround(from);
        boolean toGround = toScalar && Expression.isGround(to);
        Expression[] bindings = new Expression[names.size()];
        ExactType witness = null;
        if (fromGround && to instanceof Expression.Variable variable) {
            if (!fromPattern.bind(from, bindings)) {
                return Optional.empty();
            }
            allocator.reserveThrough(VariableAllocator.variableId(variable.name()));
            witness = new ExactType(variable.name(), toPattern.instantiate(bindings, allocator));
        }
        else if (toGround && from instanceof Expression.Variable variable) {
            if (!toPattern.bind(to, bindings)) {
                return Optional.empty();
            }
            allocator.reserveThrough(VariableAllocator.variableId(variable.name()));
            witness = new ExactType(variable.name(), fromPattern.instantiate(bindings, allocator));
        }
        else if (fromScalar && toScalar) {
            if (!fromPattern.bind(from, bindings) || !toPattern.bind(to, bindings)) {
                return fromGround && toGround ? Optional.empty() : fallback(allocator, from, to, includePlan);
            }
            for (Expression binding : bindings) {
                if (binding instanceof Expression.Variable(String name) && names.contains(name)) {
                    return fallback(allocator, from, to, includePlan);
                }
            }
        }
        else {
            return fallback(allocator, from, to, includePlan);
        }
        List<Constraint> conditions = new ArrayList<>(guards.size());
        for (NumericTemplate guard : guards) {
            conditions.add(new NumericRelation((Expression.BinaryOperation) guard.instantiate(bindings)));
        }
        Set<Constraint> obligations = new HashSet<>(conditions);
        if (witness != null) {
            obligations.add(witness);
        }
        if (!includePlan) {
            return Optional.of(new Match(obligations));
        }
        CoercionPlan plan = from.equals(to)
                ? CoercionPlan.exact(from, to)
                : CoercionPlan.directSteps(from, to, List.of(new CoercionPlan.DirectRule(ruleId, conditions)));
        return Optional.of(new Match(obligations, plan));
    }

    private Optional<Match> fallback(VariableAllocator allocator, Expression from, Expression to, boolean includePlan)
    {
        return includePlan ? original.matches(allocator, from, to) : original.matchConstraints(allocator, from, to).map(Match::new);
    }

    private static boolean isScalar(Expression expression)
    {
        if (expression instanceof Expression.Symbol) {
            return true;
        }
        if (!(expression instanceof Expression.Application(Expression.Symbol _, List<Expression> arguments))) {
            return false;
        }
        for (Expression argument : arguments) {
            if (!(argument instanceof Expression.Variable || argument instanceof Expression.Literal)) {
                return false;
            }
        }
        return true;
    }

    private record ScalarPattern(Expression expression, int[] slots)
    {
        static ScalarPattern compile(Expression expression, Map<String, Integer> names)
        {
            if (!isScalar(expression)) {
                return null;
            }
            if (!(expression instanceof Expression.Application(_, List<Expression> arguments))) {
                return new ScalarPattern(expression, new int[0]);
            }
            int[] slots = new int[arguments.size()];
            for (int index = 0; index < arguments.size(); index++) {
                slots[index] = arguments.get(index) instanceof Expression.Variable(String name)
                        ? names.computeIfAbsent(name, _ -> names.size())
                        : -1;
            }
            return new ScalarPattern(expression, slots);
        }

        boolean bind(Expression input, Expression[] bindings)
        {
            ResolutionBudget.consume();
            if (expression instanceof Expression.Symbol) {
                return expression.equals(input);
            }
            Expression.Application pattern = (Expression.Application) expression;
            if (!(input instanceof Expression.Application(Expression head, List<Expression> arguments)) ||
                    !pattern.head().equals(head) || arguments.size() != slots.length) {
                return false;
            }
            for (int index = 0; index < slots.length; index++) {
                ResolutionBudget.consume();
                Expression argument = arguments.get(index);
                int slot = slots[index];
                if (slot < 0) {
                    if (!pattern.arguments().get(index).equals(argument)) {
                        return false;
                    }
                }
                else if (bindings[slot] == null) {
                    bindings[slot] = argument;
                }
                else if (!bindings[slot].equals(argument)) {
                    return false;
                }
            }
            return true;
        }

        Expression instantiate(Expression[] bindings, VariableAllocator allocator)
        {
            ResolutionBudget.consume();
            if (!(expression instanceof Expression.Application(Expression head, List<Expression> arguments))) {
                return expression;
            }
            List<Expression> instantiated = new ArrayList<>(slots.length);
            for (int index = 0; index < slots.length; index++) {
                ResolutionBudget.consume();
                int slot = slots[index];
                if (slot < 0) {
                    instantiated.add(arguments.get(index));
                    continue;
                }
                if (bindings[slot] == null) {
                    bindings[slot] = Expression.variable(allocator.newVariable());
                }
                instantiated.add(bindings[slot]);
            }
            return new Expression.Application(head, instantiated);
        }
    }

    private interface NumericTemplate
    {
        Expression instantiate(Expression[] bindings);
    }

    private static NumericTemplate compileNumeric(Expression expression, Map<String, Integer> slots, int depth)
    {
        // Preparation is optional. Deep or unsupported guards keep the general path
        // and its existing validation and resource-limit behavior.
        if (depth > 32) {
            return null;
        }
        if (expression instanceof Expression.Literal) {
            return _ -> {
                ResolutionBudget.consume();
                return expression;
            };
        }
        if (expression instanceof Expression.Variable(String name)) {
            Integer slot = slots.get(name);
            if (slot == null) {
                return null;
            }
            return bindings -> {
                ResolutionBudget.consume();
                return bindings[slot];
            };
        }
        if (expression instanceof Expression.BinaryOperation(Expression.BinaryOperator operator, Expression left, Expression right)) {
            NumericTemplate leftTemplate = compileNumeric(left, slots, depth + 1);
            NumericTemplate rightTemplate = compileNumeric(right, slots, depth + 1);
            if (leftTemplate == null || rightTemplate == null) {
                return null;
            }
            return bindings -> {
                ResolutionBudget.consume();
                return Expression.operation(operator, leftTemplate.instantiate(bindings), rightTemplate.instantiate(bindings));
            };
        }
        return null;
    }
}
