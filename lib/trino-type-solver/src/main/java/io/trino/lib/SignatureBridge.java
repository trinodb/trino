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
package io.trino.lib;

import io.trino.spi.function.NumericVariableConstraint;
import io.trino.spi.function.Signature;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.function.VariableDeclaration;
import io.trino.spi.type.NumericExpression;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TemplateParameter;
import io.trino.spi.type.TypeTemplate;
import io.trino.spi.type.VarcharType;
import org.weakref.solver.Constraint;
import org.weakref.solver.Expression;
import org.weakref.solver.Expression.BinaryOperator;
import org.weakref.solver.RequireComparable;
import org.weakref.solver.RequireOrderable;
import org.weakref.solver.TypeScheme;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Locale.ROOT;
import static org.weakref.solver.Expression.anonymousField;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.conditional;
import static org.weakref.solver.Expression.field;
import static org.weakref.solver.Expression.function;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.operation;
import static org.weakref.solver.Expression.row;
import static org.weakref.solver.Expression.symbol;
import static org.weakref.solver.Expression.variable;
import static org.weakref.solver.Expression.variadicFunction;

/**
 * Translates a Trino {@link Signature} (as carried by every registered function) into the solver's
 * {@link TypeScheme}. This is the Phase 2a "live catalog" bridge: instead of a hand-written preset,
 * the solver can be fed straight from {@code GlobalFunctionCatalog}.
 * <p>
 * A signature's argument and return types are open {@link TypeTemplate}s. A {@link TypeTemplate.TypeVariable}
 * maps to a solver variable; a {@link TypeTemplate.TypeApplication} maps to a symbol (parameterless) or an
 * application of its {@link TemplateParameter}s, with {@code row} translated structurally. Comparable/orderable
 * type-variable constraints become {@link RequireComparable}/{@link RequireOrderable}.
 * <p>
 * Numeric arguments carry a {@link NumericExpression}. Derived numeric variables (varchar length, decimal
 * precision/scale) are declared with a defining expression and substituted in place wherever they appear, so
 * a bridged signature resolves to the same concrete result type Trino computes; the input numeric variables
 * stay free solver variables.
 */
public final class SignatureBridge
{
    private SignatureBridge() {}

    public static boolean isCalculated(Signature signature)
    {
        return signature.getVariables().stream()
                .anyMatch(VariableDeclaration.NumericVariable.class::isInstance);
    }

    public static TypeScheme toTypeScheme(Signature signature)
    {
        // Trino lowercases type-variable references inside templates (array(E) -> array(e)) but keeps the
        // original case in the constraint name; normalize everything to lower case so the same variable is
        // one name throughout.
        Set<String> typeVariables = new LinkedHashSet<>();
        signature.getTypeVariableConstraints().forEach(constraint -> typeVariables.add(constraint.getName().toLowerCase(ROOT)));

        // Numeric variables declared with a defining expression (decimal result precision/scale, calculated
        // varchar length) are substituted wherever they are referenced, so a bridged signature resolves to
        // the same concrete result type Trino computes. An input numeric variable defines itself and stays a
        // free solver variable.
        Map<String, NumericExpression> numericVariables = new HashMap<>();
        for (VariableDeclaration variable : signature.getVariables()) {
            if (variable instanceof VariableDeclaration.NumericVariable(NumericVariableConstraint constraint)) {
                numericVariables.put(constraint.getName().toLowerCase(ROOT), constraint.getExpression());
            }
        }

        List<Expression> argumentTypes = signature.getArgumentTypes().stream()
                .map(type -> toExpression(type, typeVariables, numericVariables))
                .toList();
        Expression returnType = toExpression(signature.getReturnType(), typeVariables, numericVariables);

        Set<String> variableNames = new LinkedHashSet<>();
        collectVariables(returnType, variableNames);
        argumentTypes.forEach(type -> collectVariables(type, variableNames));

        List<Expression.Variable> parameters = variableNames.stream().map(Expression::variable).toList();

        List<Constraint> constraints = new ArrayList<>();
        for (TypeVariableConstraint constraint : signature.getTypeVariableConstraints()) {
            String name = "@" + constraint.getName().toLowerCase(ROOT);
            if (constraint.isComparableRequired()) {
                constraints.add(new RequireComparable(name));
            }
            if (constraint.isOrderableRequired()) {
                constraints.add(new RequireOrderable(name));
            }
        }

        Expression.FunctionType functionType;
        if (signature.isVariableArity() && !argumentTypes.isEmpty()) {
            // The last declared argument repeats zero or more times — concat_ws(varchar, varchar)
            // accepts a lone separator — so only the preceding arguments are fixed
            functionType = variadicFunction(argumentTypes.subList(0, argumentTypes.size() - 1), argumentTypes.getLast(), returnType);
        }
        else {
            functionType = function(argumentTypes, returnType);
        }

        // Carry the declared parameter names so a named-argument call can be mapped onto this
        // scheme's positions; signatures that declare none stay positional
        List<Optional<String>> argumentNames = signature.getArguments().stream()
                .map(Signature.Argument::name)
                .toList();
        return new TypeScheme(parameters, constraints, functionType, argumentNames);
    }

    private static Expression toExpression(TypeTemplate template, Set<String> typeVariables, Map<String, NumericExpression> numericVariables)
    {
        if (template instanceof TypeTemplate.TypeVariable(String name)) {
            return variable("@" + name.toLowerCase(ROOT));
        }
        TypeTemplate.TypeApplication application = (TypeTemplate.TypeApplication) template;
        String base = application.base();
        List<TemplateParameter> parameters = application.parameters();

        if (parameters.isEmpty()) {
            // A parameterless application whose base names a declared type variable is a bare type variable
            // (e.g. E in array(E)), not a concrete type. Trino is inconsistent about lowercasing these
            // references, so match case-insensitively and emit the canonical lower-case name.
            String variableName = base.toLowerCase(ROOT);
            if (typeVariables.contains(variableName)) {
                return variable("@" + variableName);
            }
            return symbol(symbolName(base));
        }

        String symbolBase = symbolName(base);
        // Unbounded varchar is encoded by Trino as varchar(Integer.MAX_VALUE) (the UNBOUNDED_LENGTH
        // sentinel), but the solver's preset models it as a distinct parameterless type. Map the sentinel
        // back so bounded and unbounded varchar stay separate kinds in the solver.
        if (base.equals(StandardTypes.VARCHAR)
                && parameters.size() == 1
                && parameters.getFirst() instanceof TemplateParameter.NumericArgument(NumericExpression.Literal(long length))
                && length == VarcharType.UNBOUNDED_LENGTH) {
            return symbol(symbolBase);
        }
        // A function type's last parameter is its return type. The solver models function types as
        // a distinct node, not an application — lambda-taking signatures must unify against the
        // function types call sites construct for lambda arguments.
        if (base.equals("function")) {
            List<Expression> types = parameters.stream()
                    .map(parameter -> toParameter(parameter, typeVariables, numericVariables))
                    .toList();
            return function(types.subList(0, types.size() - 1), types.getLast());
        }
        if (base.equals("row")) {
            Expression.RowField[] fields = parameters.stream()
                    .map(parameter -> {
                        TemplateParameter.TypeArgument typeField = (TemplateParameter.TypeArgument) parameter;
                        Expression fieldType = toExpression(typeField.type(), typeVariables, numericVariables);
                        return typeField.name().map(name -> field(name, fieldType)).orElseGet(() -> anonymousField(fieldType));
                    })
                    .toArray(Expression.RowField[]::new);
            return row(fields);
        }
        Expression[] arguments = parameters.stream()
                .map(parameter -> toParameter(parameter, typeVariables, numericVariables))
                .toArray(Expression[]::new);
        return apply(symbolBase, arguments);
    }

    /// Canonical solver symbol for a Trino type base: lower case (annotation-declared bases are
    /// lowercased by parsing, programmatic ones — JoniRegExp, CodePoints — are not), with
    /// multi-word names (interval / with time zone) underscored.
    private static String symbolName(String base)
    {
        return base.toLowerCase(ROOT).replace(' ', '_');
    }

    private static Expression toParameter(TemplateParameter parameter, Set<String> typeVariables, Map<String, NumericExpression> numericVariables)
    {
        return switch (parameter) {
            case TemplateParameter.TypeArgument(var _, TypeTemplate type) -> toExpression(type, typeVariables, numericVariables);
            case TemplateParameter.NumericArgument(NumericExpression value) -> toExpression(value, numericVariables);
        };
    }

    private static Expression toExpression(NumericExpression expression, Map<String, NumericExpression> numericVariables)
    {
        return switch (expression) {
            case NumericExpression.Literal(long value) -> literal((int) Math.min(value, Integer.MAX_VALUE));
            case NumericExpression.Variable(String name) -> {
                NumericExpression definition = numericVariables.get(name.toLowerCase(ROOT));
                // A derived numeric variable substitutes its defining expression in place; an input variable
                // defines itself (and any name not declared as a variable) stays a free solver variable.
                if (definition == null || (definition instanceof NumericExpression.Variable(String defined) && defined.equalsIgnoreCase(name))) {
                    yield variable("@" + name.toLowerCase(ROOT));
                }
                yield toExpression(definition, numericVariables);
            }
            case NumericExpression.Operation(NumericExpression.Operator operator, NumericExpression left, NumericExpression right) -> operation(toBinaryOperator(operator), toExpression(left, numericVariables), toExpression(right, numericVariables));
            case NumericExpression.Conditional(NumericExpression.Comparison comparison, NumericExpression ifTrue, NumericExpression ifFalse) -> conditional(
                    operation(toComparisonOperator(comparison.operator()), toExpression(comparison.left(), numericVariables), toExpression(comparison.right(), numericVariables)),
                    toExpression(ifTrue, numericVariables),
                    toExpression(ifFalse, numericVariables));
        };
    }

    private static BinaryOperator toComparisonOperator(NumericExpression.ComparisonOperator operator)
    {
        return switch (operator) {
            case GREATER_THAN -> BinaryOperator.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL -> BinaryOperator.GREATER_THAN_OR_EQUAL;
            case LESS_THAN -> BinaryOperator.LESS_THAN;
            case LESS_THAN_OR_EQUAL -> BinaryOperator.LESS_THAN_OR_EQUAL;
            case EQUAL -> BinaryOperator.EQUAL;
            case NOT_EQUAL -> BinaryOperator.NOT_EQUAL;
        };
    }

    private static BinaryOperator toBinaryOperator(NumericExpression.Operator operator)
    {
        return switch (operator) {
            case ADD -> BinaryOperator.ADD;
            case SUBTRACT -> BinaryOperator.SUBTRACT;
            case MULTIPLY -> BinaryOperator.MULTIPLY;
            case DIVIDE -> BinaryOperator.DIVIDE;
            case MIN -> BinaryOperator.MIN;
            case MAX -> BinaryOperator.MAX;
        };
    }

    private static void collectVariables(Expression expression, Set<String> names)
    {
        switch (expression) {
            case Expression.Variable(String name) -> names.add(name);
            case Expression.Application(Expression head, List<Expression> arguments) -> {
                collectVariables(head, names);
                arguments.forEach(argument -> collectVariables(argument, names));
            }
            case Expression.Row(List<Expression.RowField> fields) -> fields.forEach(field -> collectVariables(field.type(), names));
            case Expression.BinaryOperation(Expression.BinaryOperator _, Expression left, Expression right) -> {
                collectVariables(left, names);
                collectVariables(right, names);
            }
            case Expression.Conditional(Expression.BinaryOperation condition, Expression ifTrue, Expression ifFalse) -> {
                collectVariables(condition, names);
                collectVariables(ifTrue, names);
                collectVariables(ifFalse, names);
            }
            default -> {}
        }
    }
}
