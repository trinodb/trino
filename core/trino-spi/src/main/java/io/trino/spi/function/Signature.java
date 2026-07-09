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
package io.trino.spi.function;

import io.trino.spi.type.NumericExpression;
import io.trino.spi.type.TemplateParameter;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeTemplate;
import io.trino.spi.type.TypeTemplates;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Collections.unmodifiableSet;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Stream.concat;

public class Signature
{
    public record Argument(TypeTemplate type, Optional<String> name)
    {
        public static Argument of(TypeTemplate type)
        {
            return new Argument(type, Optional.empty());
        }

        public static Argument of(TypeTemplate type, String name)
        {
            return new Argument(type, Optional.of(name));
        }
    }

    private final Set<VariableDeclaration> variables;
    private final TypeTemplate returnType;
    private final List<Argument> arguments;
    private final boolean variableArity;

    private Signature(
            List<VariableDeclaration> variables,
            TypeTemplate returnType,
            List<Argument> arguments,
            boolean variableArity)
    {
        this.variables = unmodifiableSet(new LinkedHashSet<>(variables));
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.arguments = List.copyOf(arguments);
        this.variableArity = variableArity;
    }

    public TypeTemplate getReturnType()
    {
        return returnType;
    }

    public List<Argument> getArguments()
    {
        return arguments;
    }

    public List<TypeTemplate> getArgumentTypes()
    {
        return arguments.stream()
                .map(Argument::type)
                .collect(toUnmodifiableList());
    }

    public boolean isVariableArity()
    {
        return variableArity;
    }

    /**
     * Only parametric types with type-kinded parameters are considered "generic".
     */
    public boolean isGeneric()
    {
        return variables.stream().anyMatch(VariableDeclaration.TypeVariable.class::isInstance);
    }

    /// The type and numeric variables declared by this signature.
    public List<VariableDeclaration> getVariables()
    {
        return List.copyOf(variables);
    }

    /// The type-variable constraints, as a view over [#getVariables()].
    public List<TypeVariableConstraint> getTypeVariableConstraints()
    {
        return variables.stream()
                .filter(VariableDeclaration.TypeVariable.class::isInstance)
                .map(variable -> ((VariableDeclaration.TypeVariable) variable).constraint())
                .collect(toUnmodifiableList());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(variables, returnType, arguments, variableArity);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Signature other)) {
            return false;
        }
        return Objects.equals(this.variables, other.variables) &&
                Objects.equals(this.returnType, other.returnType) &&
                Objects.equals(this.arguments, other.arguments) &&
                this.variableArity == other.variableArity;
    }

    @Override
    public String toString()
    {
        String constraints = variables.stream()
                .map(variable -> switch (variable) {
                    case VariableDeclaration.TypeVariable typeVariable -> typeVariable.constraint().toString();
                    case VariableDeclaration.NumericVariable numericVariable -> numericVariable.constraint().toString();
                })
                .collect(joining(",", "<", ">"));

        return (variables.isEmpty() ? "" : constraints) +
                arguments.stream().map(Argument::type).map(TypeTemplate::render).collect(joining(",", "(", ")")) +
                ":" + returnType.render();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(Signature base)
    {
        Builder builder = new Builder()
                .variables(base.variables)
                .returnType(base.returnType)
                .arguments(base.arguments);
        if (base.variableArity) {
            builder.variableArity();
        }
        return builder;
    }

    public static final class Builder
    {
        private final List<VariableDeclaration> variables = new ArrayList<>();
        private TypeTemplate returnType;
        private final List<Argument> arguments = new ArrayList<>();
        private boolean variableArity;

        private Builder() {}

        public Builder typeVariable(String name)
        {
            return typeVariableConstraint(TypeVariableConstraint.builder(name).build());
        }

        public Builder comparableTypeParameter(String name)
        {
            return typeVariableConstraint(TypeVariableConstraint.builder(name)
                    .comparableRequired()
                    .build());
        }

        public Builder orderableTypeParameter(String name)
        {
            return typeVariableConstraint(TypeVariableConstraint.builder(name)
                    .orderableRequired()
                    .build());
        }

        public Builder castableToTypeParameter(String name, TypeTemplate toType)
        {
            return typeVariableConstraint(TypeVariableConstraint.builder(name)
                    .castableTo(toType)
                    .build());
        }

        public Builder castableToTypeParameter(String name, TypeDescriptor toType)
        {
            return castableToTypeParameter(name, TypeTemplates.fromTypeDescriptor(toType));
        }

        public Builder castableFromTypeParameter(String name, TypeTemplate fromType)
        {
            return typeVariableConstraint(TypeVariableConstraint.builder(name)
                    .castableFrom(fromType)
                    .build());
        }

        public Builder castableFromTypeParameter(String name, TypeDescriptor fromType)
        {
            return castableFromTypeParameter(name, TypeTemplates.fromTypeDescriptor(fromType));
        }

        public Builder rowTypeParameter(String name)
        {
            return typeVariableConstraint(TypeVariableConstraint.builder(name)
                    .rowType()
                    .build());
        }

        public Builder typeVariableConstraint(TypeVariableConstraint typeVariableConstraint)
        {
            variables.add(new VariableDeclaration.TypeVariable(typeVariableConstraint));
            return this;
        }

        public Builder typeVariableConstraints(List<TypeVariableConstraint> typeVariableConstraints)
        {
            requireNonNull(typeVariableConstraints, "typeVariableConstraints is null").forEach(this::typeVariableConstraint);
            return this;
        }

        public Builder returnType(Type returnType)
        {
            return returnType(returnType.getTypeDescriptor());
        }

        public Builder returnType(TypeTemplate returnType)
        {
            this.returnType = requireNonNull(returnType, "returnType is null");
            return this;
        }

        public Builder returnType(TypeDescriptor returnType)
        {
            return returnType(TypeTemplates.fromTypeDescriptor(returnType));
        }

        public Builder numericVariable(String name, NumericExpression expression)
        {
            variables.add(new VariableDeclaration.NumericVariable(new NumericVariableConstraint(name, expression)));
            return this;
        }

        public Builder numericVariable(String name)
        {
            return numericVariable(name, new NumericExpression.Variable(name));
        }

        public Builder numericVariableConstraints(List<NumericVariableConstraint> numericVariableConstraints)
        {
            numericVariableConstraints.forEach(constraint -> variables.add(new VariableDeclaration.NumericVariable(constraint)));
            return this;
        }

        public Builder variable(VariableDeclaration variable)
        {
            variables.add(requireNonNull(variable, "variable is null"));
            return this;
        }

        public Builder variables(Collection<VariableDeclaration> variables)
        {
            this.variables.addAll(requireNonNull(variables, "variables is null"));
            return this;
        }

        public Builder argumentType(Type type)
        {
            return argumentType(type.getTypeDescriptor());
        }

        public Builder argumentType(TypeTemplate type)
        {
            arguments.add(Argument.of(type));
            return this;
        }

        public Builder argumentType(TypeTemplate type, String name)
        {
            arguments.add(Argument.of(type, name));
            return this;
        }

        public Builder argumentTypes(TypeTemplate... argumentTypes)
        {
            for (TypeTemplate argumentType : argumentTypes) {
                argumentType(argumentType);
            }
            return this;
        }

        public Builder argumentTypes(Collection<TypeTemplate> argumentTypes)
        {
            for (TypeTemplate argumentType : argumentTypes) {
                argumentType(argumentType);
            }
            return this;
        }

        public Builder argumentType(TypeDescriptor type)
        {
            return argumentType(TypeTemplates.fromTypeDescriptor(type));
        }

        public Builder argumentType(TypeDescriptor type, String name)
        {
            return argumentType(TypeTemplates.fromTypeDescriptor(type), name);
        }

        public Builder argumentType(Type type, String name)
        {
            return argumentType(type.getTypeDescriptor(), name);
        }

        public Builder argumentTypes(List<TypeDescriptor> argumentTypes)
        {
            for (TypeDescriptor argumentType : argumentTypes) {
                argumentType(argumentType);
            }
            return this;
        }

        public Builder argumentTypes(TypeDescriptor... argumentTypes)
        {
            for (TypeDescriptor argumentType : argumentTypes) {
                argumentType(argumentType);
            }
            return this;
        }

        public Builder arguments(List<Argument> arguments)
        {
            this.arguments.clear();
            this.arguments.addAll(arguments);
            return this;
        }

        public Builder variableArity()
        {
            this.variableArity = true;
            return this;
        }

        public Signature build()
        {
            // Canonicalize the declaration order: type variables before numeric variables, each kind
            // sorted by name. Equality and hashCode are set-based, so they do not depend on the order
            // the builder methods were called in; sorting here makes toString -- and the FunctionId
            // derived from it -- independent of that order too, so equal signatures always render alike.
            List<VariableDeclaration> orderedVariables = concat(
                    variables.stream().filter(VariableDeclaration.TypeVariable.class::isInstance).sorted(comparing(VariableDeclaration::name)),
                    variables.stream().filter(VariableDeclaration.NumericVariable.class::isInstance).sorted(comparing(VariableDeclaration::name)))
                    .collect(toUnmodifiableList());

            // A declared type variable must be referenced as a TypeVariable node. A parameterless
            // application with the variable's name would bind by base name but compare unequal to the
            // canonical form and survive variable substitution unresolved -- a construction-site bug
            // this catches at registration rather than deep in query processing.
            Set<String> typeVariableNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            orderedVariables.stream()
                    .filter(VariableDeclaration.TypeVariable.class::isInstance)
                    .map(VariableDeclaration::name)
                    .forEach(typeVariableNames::add);
            for (Argument argument : arguments) {
                verifyVariablesAreStructural(argument.type(), typeVariableNames);
            }
            verifyVariablesAreStructural(returnType, typeVariableNames);

            return new Signature(orderedVariables, returnType, arguments, variableArity);
        }

        private static void verifyVariablesAreStructural(TypeTemplate template, Set<String> typeVariableNames)
        {
            if (template instanceof TypeTemplate.TypeApplication(String base, List<TemplateParameter> parameters)) {
                if (parameters.isEmpty() && typeVariableNames.contains(base)) {
                    throw new IllegalArgumentException("Type variable '%s' must be referenced as a type variable, not as a parameterless type".formatted(base));
                }
                for (TemplateParameter parameter : parameters) {
                    if (parameter instanceof TemplateParameter.TypeArgument(Optional<String> _, TypeTemplate type)) {
                        verifyVariablesAreStructural(type, typeVariableNames);
                    }
                }
            }
        }
    }
}
