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

import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Stream.concat;

public class Signature
{
    public record Argument(TypeSignature type, Optional<String> name)
    {
        public static Argument of(TypeSignature type)
        {
            return new Argument(type, Optional.empty());
        }

        public static Argument of(TypeSignature type, String name)
        {
            return new Argument(type, Optional.of(name));
        }
    }

    private final List<TypeVariableConstraint> typeVariableConstraints;
    private final List<LongVariableConstraint> longVariableConstraints;
    private final TypeSignature returnType;
    private final List<Argument> arguments;
    private final boolean variableArity;

    private Signature(
            List<TypeVariableConstraint> typeVariableConstraints,
            List<LongVariableConstraint> longVariableConstraints,
            TypeSignature returnType,
            List<Argument> arguments,
            boolean variableArity)
    {
        this.typeVariableConstraints = List.copyOf(typeVariableConstraints);
        this.longVariableConstraints = List.copyOf(longVariableConstraints);
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.arguments = List.copyOf(arguments);
        this.variableArity = variableArity;
    }

    public TypeSignature getReturnType()
    {
        return returnType;
    }

    public List<Argument> getArguments()
    {
        return arguments;
    }

    public List<TypeSignature> getArgumentTypes()
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
        return !typeVariableConstraints.isEmpty();
    }

    public List<TypeVariableConstraint> getTypeVariableConstraints()
    {
        return typeVariableConstraints;
    }

    public List<LongVariableConstraint> getLongVariableConstraints()
    {
        return longVariableConstraints;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(typeVariableConstraints, longVariableConstraints, returnType, arguments, variableArity);
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
        return Objects.equals(this.typeVariableConstraints, other.typeVariableConstraints) &&
                Objects.equals(this.longVariableConstraints, other.longVariableConstraints) &&
                Objects.equals(this.returnType, other.returnType) &&
                Objects.equals(this.arguments, other.arguments) &&
                this.variableArity == other.variableArity;
    }

    @Override
    public String toString()
    {
        List<String> allConstraints = concat(
                typeVariableConstraints.stream().map(TypeVariableConstraint::toString),
                longVariableConstraints.stream().map(LongVariableConstraint::toString))
                .collect(Collectors.toList());

        return (allConstraints.isEmpty() ? "" : allConstraints.stream().collect(joining(",", "<", ">"))) +
                arguments.stream().map(Argument::type).map(Objects::toString).collect(joining(",", "(", ")")) +
                ":" + returnType;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(Signature base)
    {
        Builder builder = new Builder()
                .typeVariableConstraints(base.typeVariableConstraints)
                .longVariableConstraints(base.longVariableConstraints)
                .returnType(base.returnType)
                .arguments(base.arguments);
        if (base.variableArity) {
            builder.variableArity();
        }
        return builder;
    }

    public static final class Builder
    {
        private final List<TypeVariableConstraint> typeVariableConstraints = new ArrayList<>();
        private final List<LongVariableConstraint> longVariableConstraints = new ArrayList<>();
        private TypeSignature returnType;
        private final List<Argument> arguments = new ArrayList<>();
        private boolean variableArity;

        private Builder() {}

        public Builder typeVariable(String name)
        {
            typeVariableConstraints.add(TypeVariableConstraint.builder(name).build());
            return this;
        }

        public Builder comparableTypeParameter(String name)
        {
            typeVariableConstraints.add(TypeVariableConstraint.builder(name)
                    .comparableRequired()
                    .build());
            return this;
        }

        public Builder orderableTypeParameter(String name)
        {
            typeVariableConstraints.add(TypeVariableConstraint.builder(name)
                    .orderableRequired()
                    .build());
            return this;
        }

        public Builder castableToTypeParameter(String name, TypeSignature toType)
        {
            typeVariableConstraints.add(TypeVariableConstraint.builder(name)
                    .castableTo(toType)
                    .build());
            return this;
        }

        public Builder castableFromTypeParameter(String name, TypeSignature fromType)
        {
            typeVariableConstraints.add(TypeVariableConstraint.builder(name)
                    .castableFrom(fromType)
                    .build());
            return this;
        }

        public Builder rowTypeParameter(String name)
        {
            typeVariableConstraints.add(TypeVariableConstraint.builder(name)
                    .rowType()
                    .build());
            return this;
        }

        public Builder typeVariableConstraint(TypeVariableConstraint typeVariableConstraint)
        {
            this.typeVariableConstraints.add(requireNonNull(typeVariableConstraint, "typeVariableConstraint is null"));
            return this;
        }

        public Builder typeVariableConstraints(List<TypeVariableConstraint> typeVariableConstraints)
        {
            this.typeVariableConstraints.addAll(requireNonNull(typeVariableConstraints, "typeVariableConstraints is null"));
            return this;
        }

        public Builder returnType(Type returnType)
        {
            return returnType(returnType.getTypeSignature());
        }

        public Builder returnType(TypeSignature returnType)
        {
            this.returnType = requireNonNull(returnType, "returnType is null");
            return this;
        }

        public Builder longVariable(String name, String expression)
        {
            this.longVariableConstraints.add(new LongVariableConstraint(name, expression));
            return this;
        }

        public Builder longVariable(String name)
        {
            this.longVariableConstraints.add(new LongVariableConstraint(name, name));
            return this;
        }

        public Builder longVariableConstraints(List<LongVariableConstraint> longVariableConstraints)
        {
            this.longVariableConstraints.addAll(longVariableConstraints);
            return this;
        }

        public Builder argumentType(Type type)
        {
            return argumentType(type.getTypeSignature());
        }

        public Builder argumentType(TypeSignature type)
        {
            arguments.add(Argument.of(type));
            return this;
        }

        public Builder argumentType(TypeSignature type, String name)
        {
            arguments.add(Argument.of(type, name));
            return this;
        }

        public Builder argumentType(Type type, String name)
        {
            return argumentType(type.getTypeSignature(), name);
        }

        public Builder argumentTypes(List<TypeSignature> argumentTypes)
        {
            for (TypeSignature argumentType : argumentTypes) {
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
            return new Signature(typeVariableConstraints, longVariableConstraints, returnType, arguments, variableArity);
        }
    }
}
