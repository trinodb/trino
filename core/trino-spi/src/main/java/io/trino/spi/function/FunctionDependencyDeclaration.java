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

import io.trino.spi.Experimental;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

@Experimental(eta = "2022-10-31")
public class FunctionDependencyDeclaration
{
    public static final FunctionDependencyDeclaration NO_DEPENDENCIES = builder().build();

    private final Set<TypeSignature> typeDependencies;
    private final Set<FunctionDependency> functionDependencies;
    private final Set<OperatorDependency> operatorDependencies;
    private final Set<CastDependency> castDependencies;

    public static FunctionDependencyDeclarationBuilder builder()
    {
        return new FunctionDependencyDeclarationBuilder();
    }

    private FunctionDependencyDeclaration(
            Set<TypeSignature> typeDependencies,
            Set<FunctionDependency> functionDependencies,
            Set<OperatorDependency> operatorDependencies,
            Set<CastDependency> castDependencies)
    {
        this.typeDependencies = Set.copyOf(requireNonNull(typeDependencies, "typeDependencies is null"));
        this.functionDependencies = Set.copyOf(requireNonNull(functionDependencies, "functionDependencies is null"));
        this.operatorDependencies = Set.copyOf(requireNonNull(operatorDependencies, "operatorDependencies is null"));
        this.castDependencies = Set.copyOf(requireNonNull(castDependencies, "castDependencies is null"));
    }

    public Set<TypeSignature> getTypeDependencies()
    {
        return typeDependencies;
    }

    public Set<FunctionDependency> getFunctionDependencies()
    {
        return functionDependencies;
    }

    public Set<OperatorDependency> getOperatorDependencies()
    {
        return operatorDependencies;
    }

    public Set<CastDependency> getCastDependencies()
    {
        return castDependencies;
    }

    public static final class FunctionDependencyDeclarationBuilder
    {
        private final Set<TypeSignature> typeDependencies = new LinkedHashSet<>();
        private final Set<FunctionDependency> functionDependencies = new LinkedHashSet<>();
        private final Set<OperatorDependency> operatorDependencies = new LinkedHashSet<>();
        private final Set<CastDependency> castDependencies = new LinkedHashSet<>();

        private FunctionDependencyDeclarationBuilder() {}

        public FunctionDependencyDeclarationBuilder addType(TypeSignature typeSignature)
        {
            typeDependencies.add(typeSignature);
            return this;
        }

        public FunctionDependencyDeclarationBuilder addFunction(QualifiedFunctionName name, List<Type> parameterTypes)
        {
            functionDependencies.add(new FunctionDependency(name, parameterTypes.stream()
                    .map(Type::getTypeSignature)
                    .collect(toUnmodifiableList()), false));
            return this;
        }

        public FunctionDependencyDeclarationBuilder addFunctionSignature(QualifiedFunctionName name, List<TypeSignature> parameterTypes)
        {
            functionDependencies.add(new FunctionDependency(name, parameterTypes, false));
            return this;
        }

        public FunctionDependencyDeclarationBuilder addOptionalFunction(QualifiedFunctionName name, List<Type> parameterTypes)
        {
            functionDependencies.add(new FunctionDependency(
                    name,
                    parameterTypes.stream()
                            .map(Type::getTypeSignature)
                            .collect(toUnmodifiableList()),
                    true));
            return this;
        }

        public FunctionDependencyDeclarationBuilder addOptionalFunctionSignature(QualifiedFunctionName name, List<TypeSignature> parameterTypes)
        {
            functionDependencies.add(new FunctionDependency(name, parameterTypes, true));
            return this;
        }

        public FunctionDependencyDeclarationBuilder addOperator(OperatorType operatorType, List<Type> parameterTypes)
        {
            operatorDependencies.add(new OperatorDependency(operatorType, parameterTypes.stream()
                    .map(Type::getTypeSignature)
                    .collect(toUnmodifiableList()), false));
            return this;
        }

        public FunctionDependencyDeclarationBuilder addOperatorSignature(OperatorType operatorType, List<TypeSignature> parameterTypes)
        {
            operatorDependencies.add(new OperatorDependency(operatorType, parameterTypes, false));
            return this;
        }

        public FunctionDependencyDeclarationBuilder addOptionalOperator(OperatorType operatorType, List<Type> parameterTypes)
        {
            operatorDependencies.add(new OperatorDependency(
                    operatorType,
                    parameterTypes.stream()
                            .map(Type::getTypeSignature)
                            .collect(toUnmodifiableList()),
                    true));
            return this;
        }

        public FunctionDependencyDeclarationBuilder addOptionalOperatorSignature(OperatorType operatorType, List<TypeSignature> parameterTypes)
        {
            operatorDependencies.add(new OperatorDependency(operatorType, parameterTypes, true));
            return this;
        }

        public FunctionDependencyDeclarationBuilder addCast(Type fromType, Type toType)
        {
            castDependencies.add(new CastDependency(fromType.getTypeSignature(), toType.getTypeSignature(), false));
            return this;
        }

        public FunctionDependencyDeclarationBuilder addCastSignature(TypeSignature fromType, TypeSignature toType)
        {
            castDependencies.add(new CastDependency(fromType, toType, false));
            return this;
        }

        public FunctionDependencyDeclarationBuilder addOptionalCast(Type fromType, Type toType)
        {
            castDependencies.add(new CastDependency(fromType.getTypeSignature(), toType.getTypeSignature(), true));
            return this;
        }

        public FunctionDependencyDeclarationBuilder addOptionalCastSignature(TypeSignature fromType, TypeSignature toType)
        {
            castDependencies.add(new CastDependency(fromType, toType, true));
            return this;
        }

        public FunctionDependencyDeclaration build()
        {
            return new FunctionDependencyDeclaration(
                    typeDependencies,
                    functionDependencies,
                    operatorDependencies,
                    castDependencies);
        }
    }

    public static final class FunctionDependency
    {
        private final QualifiedFunctionName name;
        private final List<TypeSignature> argumentTypes;
        private final boolean optional;

        private FunctionDependency(QualifiedFunctionName name, List<TypeSignature> argumentTypes, boolean optional)
        {
            this.name = requireNonNull(name, "name is null");
            this.argumentTypes = List.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
            this.optional = optional;
        }

        public QualifiedFunctionName getName()
        {
            return name;
        }

        public List<TypeSignature> getArgumentTypes()
        {
            return argumentTypes;
        }

        public boolean isOptional()
        {
            return optional;
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
            FunctionDependency that = (FunctionDependency) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(argumentTypes, that.argumentTypes);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, argumentTypes);
        }

        @Override
        public String toString()
        {
            return name + argumentTypes.stream()
                    .map(TypeSignature::toString)
                    .collect(Collectors.joining(", ", "(", ")"));
        }
    }

    public static final class OperatorDependency
    {
        private final OperatorType operatorType;
        private final List<TypeSignature> argumentTypes;
        private final boolean optional;

        private OperatorDependency(OperatorType operatorType, List<TypeSignature> argumentTypes, boolean optional)
        {
            this.operatorType = requireNonNull(operatorType, "operatorType is null");
            this.argumentTypes = List.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
            this.optional = optional;
        }

        public OperatorType getOperatorType()
        {
            return operatorType;
        }

        public List<TypeSignature> getArgumentTypes()
        {
            return argumentTypes;
        }

        public boolean isOptional()
        {
            return optional;
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
            OperatorDependency that = (OperatorDependency) o;
            return operatorType == that.operatorType &&
                    Objects.equals(argumentTypes, that.argumentTypes);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(operatorType, argumentTypes);
        }

        @Override
        public String toString()
        {
            return operatorType + argumentTypes.stream()
                    .map(TypeSignature::toString)
                    .collect(Collectors.joining(", ", "(", ")"));
        }
    }

    public static final class CastDependency
    {
        private final TypeSignature fromType;
        private final TypeSignature toType;
        private final boolean optional;

        private CastDependency(TypeSignature fromType, TypeSignature toType, boolean optional)
        {
            this.fromType = fromType;
            this.toType = toType;
            this.optional = optional;
        }

        public TypeSignature getFromType()
        {
            return fromType;
        }

        public TypeSignature getToType()
        {
            return toType;
        }

        public boolean isOptional()
        {
            return optional;
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
            CastDependency that = (CastDependency) o;
            return Objects.equals(fromType, that.fromType) &&
                    Objects.equals(toType, that.toType);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(fromType, toType);
        }

        @Override
        public String toString()
        {
            return format("cast(%s, %s)", fromType, toType);
        }
    }
}
