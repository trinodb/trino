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

import io.trino.spi.function.NumericVariableConstraint;
import io.trino.spi.function.Signature;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.function.VariableDeclaration;
import io.trino.spi.type.NumericExpression;
import io.trino.spi.type.TypeTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.typesolver.Expression.function;
import static io.trino.typesolver.Expression.variadicFunction;
import static java.util.Locale.ROOT;

/// Compiles Trino's authoritative [Signature], [TypeTemplate], and [NumericExpression] model into
/// the solver's internal constraint representation. Derived numeric variables are expanded while
/// input variables remain free; type-variable requirements become solver constraints.
public final class SignatureCompiler
{
    private SignatureCompiler() {}

    public static boolean isCalculated(Signature signature)
    {
        return signature.getVariables().stream()
                .anyMatch(VariableDeclaration.NumericVariable.class::isInstance);
    }

    public static CompiledSignature compile(Signature signature)
    {
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
                .map(type -> TypeCompiler.compile(type, numericVariables))
                .toList();
        Expression returnType = TypeCompiler.compile(signature.getReturnType(), numericVariables);

        Set<String> variableNames = new LinkedHashSet<>();
        collectVariables(returnType, variableNames);
        argumentTypes.forEach(type -> collectVariables(type, variableNames));

        List<Expression.Variable> parameters = variableNames.stream().map(Expression::variable).toList();

        List<Constraint> constraints = new ArrayList<>();
        for (TypeVariableConstraint constraint : signature.getTypeVariableConstraints()) {
            String name = constraint.getName().toLowerCase(ROOT);
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
        return new CompiledSignature(parameters, constraints, functionType, argumentNames);
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
            case Expression.FunctionType function -> {
                function.parameterTypes().forEach(type -> collectVariables(type, names));
                function.variadicParameterType().ifPresent(type -> collectVariables(type, names));
                collectVariables(function.returnType(), names);
            }
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
