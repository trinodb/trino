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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Unified AST for everything the solver works over.
 * <p>
 * The same interface covers both type-level expressions ({@code bigint},
 * {@code array(integer)}, {@code row(varchar, @T)}, etc.) and the numeric-parameter
 * sublanguage used inside parametric types and guard relations ({@code @p - @s},
 * {@code 38}, {@code @n1 + @n2}). This is deliberate — a decimal's precision and
 * an integer literal live in the same AST so the solver can reason about them with
 * a single set of machinery.
 * <p>
 * The variants split into:
 * <ul>
 *   <li><b>Type-level</b>: {@link Symbol}, {@link Application}, {@link Row},
 *       {@link AnyRow}, {@link FunctionType}</li>
 *   <li><b>Value-level (integer)</b>: {@link Literal}, {@link BinaryOperation}</li>
 *   <li><b>Cross-cutting</b>: {@link Variable} (can stand for either kind)</li>
 * </ul>
 * {@link RowField} is a helper for {@link Row} and isn't an {@code Expression} itself.
 * <p>
 * This interface is constructor-agnostic: nothing in here knows about {@code integer},
 * {@code decimal}, or any SQL-specific type name. Those only appear at registration
 * time as {@code Symbol} values.
 */
public sealed interface Expression
{
    /**
     * Variable with name beginning with '@'.
     * Can represent a type variable or a parameter variable (numeric, string, etc.)
     */
    record Variable(String name)
            implements Expression
    {
        public Variable
        {
            if (!name.startsWith("@")) {
                throw new IllegalArgumentException("Variable name must start with '@': " + name);
            }
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    /**
     * Atomic symbol (e.g., "bigint", "decimal", "row", "function").
     */
    record Symbol(String name)
            implements Expression
    {
        @Override
        public String toString()
        {
            return name;
        }
    }

    /**
     * Application: head applied to arguments.
     * Supports variadic arguments.
     * Can represent type application (e.g., array(integer)) or operation application.
     */
    record Application(Expression head, List<Expression> arguments)
            implements Expression
    {
        public Application
        {
            arguments = List.copyOf(arguments);
        }

        @Override
        public String toString()
        {
            return head +
                    arguments.stream()
                            .map(Expression::toString)
                            .collect(Collectors.joining(", ", "(", ")"));
        }
    }

    record Row(List<RowField> fields)
            implements Expression
    {
        public Row
        {
            fields = List.copyOf(fields);
        }

        @Override
        public String toString()
        {
            return fields.stream()
                    .map(RowField::toString)
                    .collect(Collectors.joining(", ", "row(", ")"));
        }
    }

    record AnyRow()
            implements Expression
    {
        @Override
        public String toString()
        {
            return "row(*)";
        }
    }

    record RowField(Optional<String> name, Expression type)
    {
        public RowField
        {
            name = name == null ? Optional.empty() : name;
        }

        @Override
        public String toString()
        {
            return name.map(value -> value + ":" + type)
                    .orElse(type.toString());
        }
    }

    /**
     * Function type with parameter types and return type.
     * This is a type-level construct representing function types.
     * <p>
     * Variadic semantics:
     * - If isVariadic is true, the last parameter type repeats zero or more times
     * - variadicParameterType contains the element type for the variadic parameter
     */
    record FunctionType(
            List<Expression> parameterTypes,
            boolean isVariadic,
            Optional<Expression> variadicParameterType,
            Expression returnType)
            implements Expression
    {
        public FunctionType
        {
            parameterTypes = List.copyOf(parameterTypes);
            if (isVariadic && variadicParameterType.isEmpty()) {
                throw new IllegalArgumentException("Variadic function must have variadicParameterType");
            }
            if (!isVariadic && variadicParameterType.isPresent()) {
                throw new IllegalArgumentException("Non-variadic function cannot have variadicParameterType");
            }
        }

        @Override
        public String toString()
        {
            return "(" +
                    parameterTypes.stream()
                            .map(Expression::toString)
                            .collect(Collectors.joining(", ", "(", isVariadic ? ", ..." : ")")) +
                    ")->" + returnType;
        }
    }

    /**
     * Literal numeric value (integer).
     */
    record Literal(int value)
            implements Expression
    {
        @Override
        public String toString()
        {
            return Integer.toString(value);
        }
    }

    /**
     * Binary operation over expressions.
     * Can represent arithmetic operations (for numeric parameters) or other operations.
     */
    record BinaryOperation(BinaryOperator operator, Expression left, Expression right)
            implements Expression
    {
        @Override
        public String toString()
        {
            String operator = switch (this.operator) {
                case ADD -> "+";
                case SUBTRACT -> "-";
                case MULTIPLY -> "*";
                case DIVIDE -> "/";
                case MIN -> "min";
                case MAX -> "max";
                case LESS_THAN -> "<";
                case LESS_THAN_OR_EQUAL -> "<=";
                case GREATER_THAN -> ">";
                case GREATER_THAN_OR_EQUAL -> ">=";
                case EQUAL -> "==";
                case NOT_EQUAL -> "!=";
            };
            if (this.operator == BinaryOperator.MIN || this.operator == BinaryOperator.MAX) {
                return operator + "(" + left + ", " + right + ")";
            }
            return "(" + left + " " + operator + " " + right + ")";
        }
    }

    /**
     * Conditional numeric expression: chooses between two numeric expressions based on a comparison.
     * The condition must be a {@link BinaryOperation} over a comparison operator. A conditional
     * evaluates away once its condition is ground; it never participates in structural unification
     * (it appears only in numeric parameter positions, which bind whole expressions to variables).
     */
    record Conditional(BinaryOperation condition, Expression ifTrue, Expression ifFalse)
            implements Expression
    {
        @Override
        public String toString()
        {
            return "if(" + condition + ", " + ifTrue + ", " + ifFalse + ")";
        }
    }

    enum BinaryOperator
    {
        // Arithmetic
        ADD,
        SUBTRACT,
        MULTIPLY,
        DIVIDE,
        MIN,
        MAX,

        // Comparison (for relations)
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        EQUAL,
        NOT_EQUAL,
    }

    record Instantiation(Expression expression, Map<String, String> mapping) {}

    /**
     * Instantiate the given expression with fresh variables
     *
     * @return the instantiated expression together with the fresh variable bindings
     */
    static Instantiation instantiate(Expression expression, VariableAllocator allocator)
    {
        Map<String, String> mappings = new HashMap<>();

        return new Instantiation(instantiate(expression, allocator, mappings), mappings);
    }

    private static Expression instantiate(Expression expression, VariableAllocator allocator, Map<String, String> mappings)
    {
        // Hot path (coercion-rule instantiation): build child lists with a plain loop rather than a
        // stream, to avoid the per-node stream-pipeline allocation.
        return switch (expression) {
            case Application application -> new Application(
                    instantiate(application.head(), allocator, mappings),
                    instantiateAll(application.arguments(), allocator, mappings));
            case Row row -> {
                List<RowField> fields = new ArrayList<>(row.fields().size());
                for (RowField field : row.fields()) {
                    fields.add(new RowField(field.name(), instantiate(field.type(), allocator, mappings)));
                }
                yield new Row(fields);
            }
            case AnyRow anyRow -> anyRow;
            case BinaryOperation(BinaryOperator operator, Expression left, Expression right) -> operation(operator, instantiate(left, allocator, mappings), instantiate(right, allocator, mappings));
            case Conditional(BinaryOperation condition, Expression ifTrue, Expression ifFalse) -> new Conditional(
                    (BinaryOperation) instantiate(condition, allocator, mappings),
                    instantiate(ifTrue, allocator, mappings),
                    instantiate(ifFalse, allocator, mappings));
            case Literal literal -> literal;
            case Symbol symbol -> symbol;
            case Variable variable -> new Variable(mappings.computeIfAbsent(variable.name(), _ -> allocator.newVariable()));
            case FunctionType functionType -> new FunctionType(
                    instantiateAll(functionType.parameterTypes(), allocator, mappings),
                    functionType.isVariadic(),
                    functionType.variadicParameterType().map(parameter -> instantiate(parameter, allocator, mappings)),
                    instantiate(functionType.returnType(), allocator, mappings));
        };
    }

    private static List<Expression> instantiateAll(List<Expression> expressions, VariableAllocator allocator, Map<String, String> mappings)
    {
        Expression[] instantiated = new Expression[expressions.size()];
        for (int i = 0; i < expressions.size(); i++) {
            instantiated[i] = instantiate(expressions.get(i), allocator, mappings);
        }
        return List.of(instantiated);
    }

    static Expression rewrite(Expression expression, Map<String, Expression> mappings)
    {
        return switch (expression) {
            case Application application -> new Application(
                    rewrite(application.head(), mappings),
                    application.arguments().stream()
                            .map(argument -> rewrite(argument, mappings))
                            .toList());
            case Row row -> new Row(row.fields().stream()
                    .map(field -> new RowField(field.name(), rewrite(field.type(), mappings)))
                    .toList());
            case AnyRow anyRow -> anyRow;
            case Literal literal -> literal;
            case Symbol symbol -> symbol;
            case Variable variable -> {
                Expression mapped = mappings.get(variable.name());
                if (mapped == null) {
                    throw new IllegalArgumentException("No mapping for variable: " + variable.name());
                }
                yield mapped;
            }
            case BinaryOperation binary -> operation(binary.operator(), rewrite(binary.left(), mappings), rewrite(binary.right(), mappings));
            case Conditional(BinaryOperation condition, Expression ifTrue, Expression ifFalse) -> new Conditional(
                    (BinaryOperation) rewrite(condition, mappings),
                    rewrite(ifTrue, mappings),
                    rewrite(ifFalse, mappings));
            case FunctionType functionType -> new FunctionType(
                    functionType.parameterTypes().stream()
                            .map(parameter -> rewrite(parameter, mappings))
                            .toList(),
                    functionType.isVariadic(),
                    functionType.variadicParameterType().map(parameter -> rewrite(parameter, mappings)),
                    rewrite(functionType.returnType(), mappings));
        };
    }

    static Expression substitute(Expression expression, Map<String, Expression> substitutions)
    {
        if (substitutions.isEmpty()) {
            return expression;
        }
        return substitute(expression, substitutions, null);
    }

    // Identity comparisons are deliberate: an unchanged subtree is returned as-is to avoid
    // reallocating equal structures on the substitution hot path
    @SuppressWarnings("ReferenceEquality")
    private static Expression substitute(Expression expression, Map<String, Expression> substitutions, Set<String> activeVariables)
    {
        // Each arm returns the original expression unchanged when no nested substitution applied, so
        // unaffected subtrees (the common case — most variables in the map aren't in a given subtree)
        // are not needlessly reallocated.
        return switch (expression) {
            case Application application -> {
                Expression head = substitute(application.head(), substitutions, activeVariables);
                List<Expression> arguments = substituteAll(application.arguments(), substitutions, activeVariables);
                yield head == application.head() && arguments == application.arguments()
                        ? application
                        : new Application(head, arguments);
            }
            case Row row -> {
                List<RowField> fields = null;
                for (int i = 0; i < row.fields().size(); i++) {
                    RowField field = row.fields().get(i);
                    Expression type = substitute(field.type(), substitutions, activeVariables);
                    if (type != field.type() && fields == null) {
                        fields = new ArrayList<>(row.fields().subList(0, i));
                    }
                    if (fields != null) {
                        fields.add(type == field.type() ? field : new RowField(field.name(), type));
                    }
                }
                yield fields == null ? row : new Row(fields);
            }
            case AnyRow anyRow -> anyRow;
            case Literal literal -> literal;
            case Symbol symbol -> symbol;
            case Variable variable -> {
                Expression replacement = substitutions.get(variable.name());
                if (replacement == null || replacement.equals(variable)) {
                    yield variable;
                }
                // A ground replacement contains no variables, so applying the map to it is a no-op and
                // no cycle is possible — skip the recursion and the cycle-guard set entirely. This is
                // the overwhelmingly common case (a variable resolved to a concrete type or literal),
                // so the guard set is allocated lazily only for chained, variable-bearing replacements.
                if (isGround(replacement)) {
                    yield replacement;
                }
                Set<String> active = activeVariables == null ? new HashSet<>() : activeVariables;
                if (!active.add(variable.name())) {
                    throw new IllegalArgumentException("Cyclic substitution involving " + variable.name());
                }
                Expression substituted = substitute(replacement, substitutions, active);
                active.remove(variable.name());
                yield substituted;
            }
            case BinaryOperation binary -> {
                Expression left = substitute(binary.left(), substitutions, activeVariables);
                Expression right = substitute(binary.right(), substitutions, activeVariables);
                yield left == binary.left() && right == binary.right() ? binary : operation(binary.operator(), left, right);
            }
            case Conditional conditional -> {
                Expression condition = substitute(conditional.condition(), substitutions, activeVariables);
                Expression ifTrue = substitute(conditional.ifTrue(), substitutions, activeVariables);
                Expression ifFalse = substitute(conditional.ifFalse(), substitutions, activeVariables);
                yield condition == conditional.condition() && ifTrue == conditional.ifTrue() && ifFalse == conditional.ifFalse()
                        ? conditional
                        : new Conditional((BinaryOperation) condition, ifTrue, ifFalse);
            }
            case FunctionType functionType -> {
                List<Expression> parameters = substituteAll(functionType.parameterTypes(), substitutions, activeVariables);
                Optional<Expression> variadic = functionType.variadicParameterType().map(parameter -> substitute(parameter, substitutions, activeVariables));
                Expression returnType = substitute(functionType.returnType(), substitutions, activeVariables);
                yield parameters == functionType.parameterTypes()
                        && variadic.equals(functionType.variadicParameterType())
                        && returnType == functionType.returnType()
                        ? functionType
                        : new FunctionType(parameters, functionType.isVariadic(), variadic, returnType);
            }
        };
    }

    private static List<Expression> substituteAll(List<Expression> expressions, Map<String, Expression> substitutions, Set<String> activeVariables)
    {
        List<Expression> result = null;
        for (int i = 0; i < expressions.size(); i++) {
            Expression original = expressions.get(i);
            Expression substituted = substitute(original, substitutions, activeVariables);
            if (substituted != original && result == null) {
                result = new ArrayList<>(expressions.subList(0, i));
            }
            if (result != null) {
                result.add(substituted);
            }
        }
        return result == null ? expressions : result;
    }

    static Variable variable(String name)
    {
        return new Variable(name);
    }

    /// Merge two structurally-unified expressions' row field names: a name survives only where
    /// both sides carry it identically, the way the engine computes row supertypes — comparing a
    /// named row column against an anonymous ROW constructor yields the anonymous shape. The
    /// expressions must have the same structure (one unified against the other); positions where
    /// they differ (a variable against a type) keep the left side.
    static Expression mergeRowFieldNames(Expression left, Expression right)
    {
        if (left instanceof Row(List<RowField> leftFields) &&
                right instanceof Row(List<RowField> rightFields) &&
                leftFields.size() == rightFields.size()) {
            List<RowField> merged = new ArrayList<>(leftFields.size());
            for (int index = 0; index < leftFields.size(); index++) {
                RowField leftField = leftFields.get(index);
                RowField rightField = rightFields.get(index);
                Optional<String> name = leftField.name().equals(rightField.name()) ? leftField.name() : Optional.empty();
                merged.add(new RowField(name, mergeRowFieldNames(leftField.type(), rightField.type())));
            }
            return new Row(merged);
        }
        if (left instanceof Application(Expression leftHead, List<Expression> leftArguments) &&
                right instanceof Application(Expression rightHead, List<Expression> rightArguments) &&
                leftHead.equals(rightHead) &&
                leftArguments.size() == rightArguments.size()) {
            List<Expression> merged = new ArrayList<>(leftArguments.size());
            for (int index = 0; index < leftArguments.size(); index++) {
                merged.add(mergeRowFieldNames(leftArguments.get(index), rightArguments.get(index)));
            }
            return new Application(leftHead, merged);
        }
        return left;
    }

    static Symbol symbol(String name)
    {
        return new Symbol(name);
    }

    static Application apply(String constructorName, List<Expression> arguments)
    {
        return new Application(symbol(constructorName), arguments);
    }

    static Application apply(String constructorName, Expression... arguments)
    {
        return apply(constructorName, List.of(arguments));
    }

    static Literal literal(int value)
    {
        return new Literal(value);
    }

    static RowField field(String name, Expression type)
    {
        return new RowField(Optional.of(name), type);
    }

    static RowField anonymousField(Expression type)
    {
        return new RowField(Optional.empty(), type);
    }

    static Row row(RowField... fields)
    {
        return new Row(List.of(fields));
    }

    static AnyRow anyRow()
    {
        return new AnyRow();
    }

    static BinaryOperation operation(BinaryOperator operator, Expression left, Expression right)
    {
        return new BinaryOperation(operator, left, right);
    }

    static Conditional conditional(BinaryOperation condition, Expression ifTrue, Expression ifFalse)
    {
        return new Conditional(condition, ifTrue, ifFalse);
    }

    static boolean isGround(Expression expression)
    {
        return switch (expression) {
            case Variable _ -> false;
            case Symbol _, Literal _, AnyRow _ -> true;
            case Application(Expression head, List<Expression> arguments) -> isGround(head) && arguments.stream().allMatch(Expression::isGround);
            case Row(List<RowField> fields) -> fields.stream().allMatch(field -> isGround(field.type()));
            case BinaryOperation(BinaryOperator _, Expression left, Expression right) -> isGround(left) && isGround(right);
            case Conditional(BinaryOperation condition, Expression ifTrue, Expression ifFalse) -> isGround(condition) && isGround(ifTrue) && isGround(ifFalse);
            case FunctionType functionType -> functionType.parameterTypes().stream().allMatch(Expression::isGround)
                    && functionType.variadicParameterType().map(Expression::isGround).orElse(true)
                    && isGround(functionType.returnType());
        };
    }

    static Expression evaluate(Expression expression)
    {
        return switch (expression) {
            case BinaryOperation(BinaryOperator operator, Expression left, Expression right) -> {
                Expression leftEvaluated = evaluate(left);
                Expression rightEvaluated = evaluate(right);
                if (leftEvaluated instanceof Literal(int l) && rightEvaluated instanceof Literal(int r)) {
                    // Fold in 64-bit and saturate to the int range, mirroring Trino's TypeCalculation:
                    // calculated type literals (e.g. varchar lengths) are computed as longs and the growth
                    // formulas clamp with min(2147483647, ...), so a saturated 32-bit fold reaches the same
                    // bound instead of wrapping to a bogus negative length.
                    Expression reduced = switch (operator) {
                        case ADD -> literal(saturateToInt((long) l + r));
                        case SUBTRACT -> literal(saturateToInt((long) l - r));
                        case MULTIPLY -> literal(saturateToInt((long) l * r));
                        case DIVIDE -> r == 0 ? null : literal(l / r);
                        case MIN -> literal(Math.min(l, r));
                        case MAX -> literal(Math.max(l, r));
                        case LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, EQUAL, NOT_EQUAL -> null;
                    };
                    if (reduced != null) {
                        yield reduced;
                    }
                }
                yield new BinaryOperation(operator, leftEvaluated, rightEvaluated);
            }
            case Conditional(BinaryOperation condition, Expression ifTrue, Expression ifFalse) -> {
                Expression conditionLeft = evaluate(condition.left());
                Expression conditionRight = evaluate(condition.right());
                if (conditionLeft instanceof Literal(int l) && conditionRight instanceof Literal(int r)) {
                    boolean holds = switch (condition.operator()) {
                        case LESS_THAN -> l < r;
                        case LESS_THAN_OR_EQUAL -> l <= r;
                        case GREATER_THAN -> l > r;
                        case GREATER_THAN_OR_EQUAL -> l >= r;
                        case EQUAL -> l == r;
                        case NOT_EQUAL -> l != r;
                        case ADD, SUBTRACT, MULTIPLY, DIVIDE, MIN, MAX -> throw new IllegalArgumentException("Conditional condition must be a comparison: " + condition);
                    };
                    yield evaluate(holds ? ifTrue : ifFalse);
                }
                yield new Conditional(new BinaryOperation(condition.operator(), conditionLeft, conditionRight), evaluate(ifTrue), evaluate(ifFalse));
            }
            case Application(Expression head, List<Expression> arguments) -> new Application(
                    head,
                    arguments.stream().map(Expression::evaluate).toList());
            case Row(List<RowField> fields) -> new Row(fields.stream()
                    .map(field -> new RowField(field.name(), evaluate(field.type())))
                    .toList());
            case FunctionType functionType -> new FunctionType(
                    functionType.parameterTypes().stream().map(Expression::evaluate).toList(),
                    functionType.isVariadic(),
                    functionType.variadicParameterType().map(Expression::evaluate),
                    evaluate(functionType.returnType()));
            case AnyRow _, Literal _, Symbol _, Variable _ -> expression;
        };
    }

    static int saturateToInt(long value)
    {
        return (int) Math.max(Integer.MIN_VALUE, Math.min(Integer.MAX_VALUE, value));
    }

    static FunctionType function(List<Expression> parameterTypes, Expression returnType)
    {
        return new FunctionType(parameterTypes, false, Optional.empty(), returnType);
    }

    static FunctionType variadicFunction(
            List<Expression> parameterTypes,
            Expression variadicParameterType,
            Expression returnType)
    {
        return new FunctionType(parameterTypes, true, Optional.of(variadicParameterType), returnType);
    }
}
