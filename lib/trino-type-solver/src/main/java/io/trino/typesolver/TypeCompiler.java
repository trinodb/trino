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

import io.trino.spi.type.NumericExpression;
import io.trino.spi.type.TemplateParameter;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeParameter;
import io.trino.spi.type.TypeTemplate;
import io.trino.spi.type.VarcharType;
import io.trino.typesolver.Expression.BinaryOperator;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.TypeTemplates.fromTypeDescriptor;
import static io.trino.typesolver.Expression.anonymousField;
import static io.trino.typesolver.Expression.apply;
import static io.trino.typesolver.Expression.conditional;
import static io.trino.typesolver.Expression.field;
import static io.trino.typesolver.Expression.function;
import static io.trino.typesolver.Expression.literal;
import static io.trino.typesolver.Expression.operation;
import static io.trino.typesolver.Expression.row;
import static io.trino.typesolver.Expression.variable;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ROOT;
import static java.util.stream.Collectors.joining;

/// Translates Trino's structural type representations into the solver's constraint terms.
///
/// The mapping is intentionally total over the types the Trino preset claims to
/// support; an unsupported type surfaces as an [IllegalArgumentException]
/// rather than a silent skip, so coverage gaps are visible.
public final class TypeCompiler
{
    private static final Map<String, String> MULTI_WORD_BASES = Map.of(
            "timestamp_with_time_zone", "timestamp with time zone",
            "time_with_time_zone", "time with time zone",
            "interval_day_to_second", "interval day to second",
            "interval_year_to_month", "interval year to month");

    private TypeCompiler() {}

    /// Convert a ground descriptor to a solver term by first lifting it into Trino's canonical
    /// [TypeTemplate] structure. This covers scalar, parametric, function, and named row types.
    public static Expression compile(TypeDescriptor descriptor)
    {
        return ResolutionBudget.withBudget(() -> {
            ArrayDeque<DescriptorVisit> pending = new ArrayDeque<>();
            pending.add(new DescriptorVisit(descriptor, 0));
            while (!pending.isEmpty()) {
                DescriptorVisit visit = pending.removeLast();
                ResolutionBudget.checkDepth(visit.depth());
                for (TypeParameter parameter : visit.descriptor().getParameters()) {
                    if (parameter instanceof TypeParameter.Type(_, TypeDescriptor type)) {
                        pending.add(new DescriptorVisit(type, visit.depth() + 1));
                    }
                }
            }
            return compile(fromTypeDescriptor(descriptor));
        });
    }

    private record DescriptorVisit(TypeDescriptor descriptor, int depth) {}

    /// Convert Trino's canonical open type term to the solver's constraint term.
    public static Expression compile(TypeTemplate template)
    {
        return compile(template, Map.of());
    }

    /// Compile a type template while expanding calculated numeric variables from a function
    /// signature. The map is keyed case-insensitively by normalized lower-case names.
    static Expression compile(TypeTemplate template, Map<String, NumericExpression> numericVariables)
    {
        return ResolutionBudget.nested(() -> compileTemplate(template, numericVariables));
    }

    private static Expression compileTemplate(TypeTemplate template, Map<String, NumericExpression> numericVariables)
    {
        return switch (template) {
            case TypeTemplate.TypeVariable(String name) -> variable(solverVariable(name));
            case TypeTemplate.TypeApplication(String base, List<TemplateParameter> parameters) -> {
                String symbol = symbolName(base);
                if (parameters.isEmpty()) {
                    yield Expression.symbol(symbol);
                }
                if (symbol.equals("varchar")
                        && parameters.equals(List.of(new TemplateParameter.NumericArgument(new NumericExpression.Literal(VarcharType.UNBOUNDED_LENGTH))))) {
                    yield Expression.symbol(symbol);
                }
                if (symbol.equals("function")) {
                    List<Expression> components = parameters.stream()
                            .map(parameter -> compile(parameter, numericVariables))
                            .toList();
                    yield function(components.subList(0, components.size() - 1), components.getLast());
                }
                if (symbol.equals("row")) {
                    yield row(parameters.stream()
                            .map(TemplateParameter.TypeArgument.class::cast)
                            .map(argument -> argument.name()
                                    .map(name -> field(name, compile(argument.type(), numericVariables)))
                                    .orElseGet(() -> anonymousField(compile(argument.type(), numericVariables))))
                            .toArray(Expression.RowField[]::new));
                }
                yield apply(symbol, parameters.stream()
                        .map(parameter -> compile(parameter, numericVariables))
                        .toArray(Expression[]::new));
            }
        };
    }

    private static Expression compile(TemplateParameter parameter, Map<String, NumericExpression> numericVariables)
    {
        return switch (parameter) {
            case TemplateParameter.TypeArgument(_, TypeTemplate type) -> compile(type, numericVariables);
            case TemplateParameter.NumericArgument(NumericExpression value) -> compile(value, numericVariables);
        };
    }

    private static Expression compile(NumericExpression expression, Map<String, NumericExpression> numericVariables)
    {
        return ResolutionBudget.nested(() -> compileNumeric(expression, numericVariables));
    }

    private static Expression compileNumeric(NumericExpression expression, Map<String, NumericExpression> numericVariables)
    {
        return switch (expression) {
            case NumericExpression.Literal(long value) -> literal(toSolverInteger(value));
            case NumericExpression.Variable(String name) -> {
                String normalized = solverVariable(name);
                NumericExpression definition = numericVariables.get(normalized);
                if (definition == null || (definition instanceof NumericExpression.Variable(String defined) && defined.equalsIgnoreCase(name))) {
                    yield variable(normalized);
                }
                yield compile(definition, numericVariables);
            }
            case NumericExpression.Operation(NumericExpression.Operator operator, NumericExpression left, NumericExpression right) -> operation(
                    toBinaryOperator(operator),
                    compile(left, numericVariables),
                    compile(right, numericVariables));
            case NumericExpression.Conditional(NumericExpression.Comparison comparison, NumericExpression ifTrue, NumericExpression ifFalse) -> conditional(
                    operation(
                            toBinaryOperator(comparison.operator()),
                            compile(comparison.left(), numericVariables),
                            compile(comparison.right(), numericVariables)),
                    compile(ifTrue, numericVariables),
                    compile(ifFalse, numericVariables));
        };
    }

    private static int toSolverInteger(long value)
    {
        return toIntExact(value);
    }

    private static BinaryOperator toBinaryOperator(NumericExpression.Operator operator)
    {
        return BinaryOperator.valueOf(operator.name());
    }

    private static BinaryOperator toBinaryOperator(NumericExpression.ComparisonOperator operator)
    {
        return BinaryOperator.valueOf(operator.name());
    }

    private static String solverVariable(String name)
    {
        return name.toLowerCase(ROOT);
    }

    /// Convert a ground solver expression back to a Trino type descriptor — the inverse of
    /// [#compile(TypeDescriptor)] for resolution results that must become engine types. Multi-word
    /// bases reverse the underscore encoding (`timestamp_with_time_zone(3)` is the engine's
    /// `timestamp(3) with time zone`).
    public static TypeDescriptor toTypeDescriptor(Expression expression)
    {
        return switch (expression) {
            case Expression.Symbol(String name) -> new TypeDescriptor(trinoBase(name));
            case Expression.Application(Expression.Symbol(String name), List<Expression> arguments) -> new TypeDescriptor(
                    trinoBase(name),
                    arguments.stream().map(TypeCompiler::toParameter).toList());
            case Expression.Row(List<Expression.RowField> fields) -> TypeDescriptor.rowType(fields.stream()
                    .map(field -> field.name()
                            .map(name -> TypeParameter.namedField(name, toTypeDescriptor(field.type())))
                            .orElseGet(() -> TypeParameter.anonymousField(toTypeDescriptor(field.type()))))
                    .toList());
            case Expression.FunctionType functionType -> {
                List<TypeDescriptor> components = new ArrayList<>(functionType.parameterTypes().stream()
                        .map(TypeCompiler::toTypeDescriptor)
                        .toList());
                components.add(toTypeDescriptor(functionType.returnType()));
                yield TypeDescriptor.functionType(components.getFirst(), components.subList(1, components.size()).toArray(TypeDescriptor[]::new));
            }
            default -> throw new IllegalArgumentException("Unsupported type expression: " + expression);
        };
    }

    private static TypeParameter toParameter(Expression expression)
    {
        if (expression instanceof Expression.Literal(int value)) {
            return TypeParameter.numericParameter(value);
        }
        return TypeParameter.typeParameter(toTypeDescriptor(expression));
    }

    private static String trinoBase(String name)
    {
        return MULTI_WORD_BASES.getOrDefault(name, name);
    }

    private static String symbolName(String base)
    {
        // Bases map lower-cased (programmatically declared bases like JoniRegExp keep their case
        // in the descriptor, parsed ones are already lower case); multi-word names
        // (interval / with time zone) use underscores in the solver preset.
        String name = base.toLowerCase(ROOT).replace(' ', '_');
        return name.equals("int") ? "integer" : name;
    }

    /// Render a solver expression as a canonical type string for reporting and
    /// for comparing common-supertype results across the two engines.
    ///
    /// @return a human-readable, structurally faithful rendering of the expression
    public static String render(Expression expression)
    {
        return switch (expression) {
            case Expression.Symbol symbol -> symbol.name();
            case Expression.Literal value -> Integer.toString(value.value());
            // varchar(2147483647) is Trino's unbounded varchar (length == Integer.MAX_VALUE); a calculated
            // length that saturates to the max denotes the same unbounded type, so canonicalize it.
            case Expression.Application(Expression.Symbol(String name), List<Expression> arguments)
            when name.equals("varchar") && arguments.equals(List.of(literal(Integer.MAX_VALUE))) -> "varchar";
            case Expression.Application(Expression head, List<Expression> arguments) -> render(head) + "(" + arguments.stream().map(TypeCompiler::render).collect(joining(", ")) + ")";
            case Expression.Row(List<Expression.RowField> fields) -> "row(" + fields.stream()
                    .map(rowField -> rowField.name().map(name -> name + " ").orElse("") + render(rowField.type()))
                    .collect(joining(", ")) + ")";
            // Mirror the solver's own function-type format, but render the components through
            // this method so nested canonicalizations (the unbounded varchar sentinel) apply
            case Expression.FunctionType functionType -> "((" + functionType.parameterTypes().stream()
                    .map(TypeCompiler::render)
                    .collect(joining(", ")) + "))->" + render(functionType.returnType());
            default -> expression.toString();
        };
    }
}
