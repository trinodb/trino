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
        return switch (expression) {
            case Application application -> new Application(instantiate(application.head(), allocator, mappings), application.arguments().stream()
                    .map(argument -> instantiate(argument, allocator, mappings))
                    .toList());
            case Row row -> new Row(row.fields().stream()
                    .map(field -> new RowField(field.name(), instantiate(field.type(), allocator, mappings)))
                    .toList());
            case AnyRow anyRow -> anyRow;
            case BinaryOperation(BinaryOperator operator, Expression left, Expression right) -> operation(operator, instantiate(left, allocator, mappings), instantiate(right, allocator, mappings));
            case Literal literal -> literal;
            case Symbol symbol -> symbol;
            case Variable variable -> new Variable(mappings.computeIfAbsent(variable.name(), _ -> allocator.newVariable()));
            case FunctionType functionType -> new FunctionType(
                    functionType.parameterTypes().stream()
                            .map(parameter -> instantiate(parameter, allocator, mappings))
                            .toList(),
                    functionType.isVariadic(),
                    functionType.variadicParameterType().map(parameter -> instantiate(parameter, allocator, mappings)),
                    instantiate(functionType.returnType(), allocator, mappings));
        };
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
        return substitute(expression, substitutions, new HashSet<>());
    }

    private static Expression substitute(Expression expression, Map<String, Expression> substitutions, Set<String> activeVariables)
    {
        return switch (expression) {
            case Application application -> new Application(
                    substitute(application.head(), substitutions, activeVariables),
                    application.arguments().stream()
                            .map(argument -> substitute(argument, substitutions, activeVariables))
                            .toList());
            case Row row -> new Row(row.fields().stream()
                    .map(field -> new RowField(field.name(), substitute(field.type(), substitutions, activeVariables)))
                    .toList());
            case AnyRow anyRow -> anyRow;
            case Literal literal -> literal;
            case Symbol symbol -> symbol;
            case Variable variable -> {
                Expression replacement = substitutions.get(variable.name());
                if (replacement == null) {
                    yield variable;
                }
                if (replacement.equals(variable)) {
                    yield variable;
                }
                if (!activeVariables.add(variable.name())) {
                    throw new IllegalArgumentException("Cyclic substitution involving " + variable.name());
                }
                Expression substituted = substitute(replacement, substitutions, activeVariables);
                activeVariables.remove(variable.name());
                yield substituted;
            }
            case BinaryOperation binary -> operation(binary.operator(), substitute(binary.left(), substitutions, activeVariables), substitute(binary.right(), substitutions, activeVariables));
            case FunctionType functionType -> new FunctionType(
                    functionType.parameterTypes().stream()
                            .map(parameter -> substitute(parameter, substitutions, activeVariables))
                            .toList(),
                    functionType.isVariadic(),
                    functionType.variadicParameterType().map(parameter -> substitute(parameter, substitutions, activeVariables)),
                    substitute(functionType.returnType(), substitutions, activeVariables));
        };
    }

    static Variable variable(String name)
    {
        return new Variable(name);
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

    static boolean isGround(Expression expression)
    {
        return switch (expression) {
            case Variable _ -> false;
            case Symbol _, Literal _, AnyRow _ -> true;
            case Application(Expression head, List<Expression> arguments) -> isGround(head) && arguments.stream().allMatch(Expression::isGround);
            case Row(List<RowField> fields) -> fields.stream().allMatch(field -> isGround(field.type()));
            case BinaryOperation(BinaryOperator _, Expression left, Expression right) -> isGround(left) && isGround(right);
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
                    Expression reduced = switch (operator) {
                        case ADD -> literal(l + r);
                        case SUBTRACT -> literal(l - r);
                        case MULTIPLY -> literal(l * r);
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
