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
package io.trino.spi.expression;

import io.trino.spi.type.FunctionType;
import io.trino.spi.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

public final class Lambda
        extends ConnectorExpression
{
    private final List<Variable> arguments;
    private final ConnectorExpression body;

    public Lambda(Type type, List<Variable> arguments, ConnectorExpression body)
    {
        super(type);
        arguments = List.copyOf(requireNonNull(arguments, "arguments is null"));
        requireNonNull(body, "body is null");

        if (!(type instanceof FunctionType functionType)) {
            throw new IllegalArgumentException("type must be a function type: " + type);
        }

        long uniqueNames = arguments.stream()
                .map(Variable::getName)
                .distinct()
                .count();
        if (uniqueNames != arguments.size()) {
            throw new IllegalArgumentException("lambda argument names must be unique: " + arguments);
        }

        if (functionType.getArgumentTypes().size() != arguments.size()) {
            throw new IllegalArgumentException("type argument count does not match lambda arguments: " + type);
        }
        if (!functionType.getArgumentTypes().equals(arguments.stream()
                .map(Variable::getType)
                .toList())) {
            throw new IllegalArgumentException("type argument types do not match lambda arguments: " + type);
        }

        if (!functionType.getReturnType().equals(body.getType())) {
            throw new IllegalArgumentException("type return type does not match lambda body: " + type);
        }

        validateArgumentReferences(body, arguments.stream()
                .collect(toMap(Variable::getName, Variable::getType)));

        this.arguments = arguments;
        this.body = body;
    }

    private static void validateArgumentReferences(ConnectorExpression expression, Map<String, Type> argumentTypes)
    {
        switch (expression) {
            case Variable variable -> {
                Type argumentType = argumentTypes.get(variable.getName());
                if (argumentType != null && !argumentType.equals(variable.getType())) {
                    throw new IllegalArgumentException("lambda argument reference type does not match argument declaration: " + variable);
                }
            }
            case Lambda lambda -> {
                Map<String, Type> nestedArgumentTypes = new HashMap<>(argumentTypes);
                lambda.getArguments().stream()
                        .map(Variable::getName)
                        .forEach(nestedArgumentTypes::remove);
                validateArgumentReferences(lambda.getBody(), nestedArgumentTypes);
            }
            default -> expression.getChildren().forEach(child ->
                    validateArgumentReferences(child, argumentTypes));
        }
    }

    public List<Variable> getArguments()
    {
        return arguments;
    }

    public ConnectorExpression getBody()
    {
        return body;
    }

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        // Lambda bodies introduce a local scope.
        // Code that traverses into the body must handle lambda arguments explicitly.
        return emptyList();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Lambda lambda = (Lambda) o;
        return Objects.equals(arguments, lambda.arguments) &&
                Objects.equals(body, lambda.body) &&
                Objects.equals(getType(), lambda.getType());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(arguments, body, getType());
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", "Lambda[", "]")
                .add("arguments=" + arguments.stream()
                        .map(Variable::toString)
                        .collect(joining(", ", "[", "]")))
                .add("body=" + body)
                .toString();
    }
}
