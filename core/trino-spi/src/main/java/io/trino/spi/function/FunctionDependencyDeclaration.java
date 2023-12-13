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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.DoNotCall;
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

    @JsonProperty
    public Set<TypeSignature> getTypeDependencies()
    {
        return typeDependencies;
    }

    @JsonProperty
    public Set<FunctionDependency> getFunctionDependencies()
    {
        return functionDependencies;
    }

    @JsonProperty
    public Set<OperatorDependency> getOperatorDependencies()
    {
        return operatorDependencies;
    }

    @JsonProperty
    public Set<CastDependency> getCastDependencies()
    {
        return castDependencies;
    }

    @JsonCreator
    @DoNotCall // For JSON deserialization only
    public static FunctionDependencyDeclaration fromJson(
            @JsonProperty Set<TypeSignature> typeDependencies,
            @JsonProperty Set<FunctionDependency> functionDependencies,
            @JsonProperty Set<OperatorDependency> operatorDependencies,
            @JsonProperty Set<CastDependency> castDependencies)
    {
        return new FunctionDependencyDeclaration(typeDependencies, functionDependencies, operatorDependencies, castDependencies);
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

        public FunctionDependencyDeclarationBuilder addFunction(CatalogSchemaFunctionName name, List<Type> parameterTypes)
        {
            functionDependencies.add(new FunctionDependency(name, parameterTypes.stream()
                    .map(Type::getTypeSignature)
                    .toList(), false));
            return this;
        }

        public FunctionDependencyDeclarationBuilder addFunctionSignature(CatalogSchemaFunctionName name, List<TypeSignature> parameterTypes)
        {
            functionDependencies.add(new FunctionDependency(name, parameterTypes, false));
            return this;
        }

        public FunctionDependencyDeclarationBuilder addOptionalFunction(CatalogSchemaFunctionName name, List<Type> parameterTypes)
        {
            functionDependencies.add(new FunctionDependency(
                    name,
                    parameterTypes.stream()
                            .map(Type::getTypeSignature)
                            .toList(),
                    true));
            return this;
        }

        public FunctionDependencyDeclarationBuilder addOptionalFunctionSignature(CatalogSchemaFunctionName name, List<TypeSignature> parameterTypes)
        {
            functionDependencies.add(new FunctionDependency(name, parameterTypes, true));
            return this;
        }

        public FunctionDependencyDeclarationBuilder addOperator(OperatorType operatorType, List<Type> parameterTypes)
        {
            operatorDependencies.add(new OperatorDependency(operatorType, parameterTypes.stream()
                    .map(Type::getTypeSignature)
                    .toList(), false));
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
                            .toList(),
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
        private final CatalogSchemaFunctionName name;
        private final List<TypeSignature> argumentTypes;
        private final boolean optional;

        private FunctionDependency(CatalogSchemaFunctionName name, List<TypeSignature> argumentTypes, boolean optional)
        {
            this.name = requireNonNull(name, "name is null");
            this.argumentTypes = List.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
            this.optional = optional;
        }

        @JsonProperty
        public CatalogSchemaFunctionName getName()
        {
            return name;
        }

        @JsonProperty
        public List<TypeSignature> getArgumentTypes()
        {
            return argumentTypes;
        }

        @JsonProperty
        public boolean isOptional()
        {
            return optional;
        }

        @JsonCreator
        public static FunctionDependency fromJson(
                @JsonProperty CatalogSchemaFunctionName name,
                @JsonProperty List<TypeSignature> argumentTypes,
                @JsonProperty boolean optional)
        {
            return new FunctionDependency(name, argumentTypes, optional);
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

        @JsonProperty
        public OperatorType getOperatorType()
        {
            return operatorType;
        }

        @JsonProperty
        public List<TypeSignature> getArgumentTypes()
        {
            return argumentTypes;
        }

        @JsonProperty
        public boolean isOptional()
        {
            return optional;
        }

        @JsonCreator
        public static OperatorDependency fromJson(
                @JsonProperty OperatorType operatorType,
                @JsonProperty List<TypeSignature> argumentTypes,
                @JsonProperty boolean optional)
        {
            return new OperatorDependency(operatorType, argumentTypes, optional);
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

        @JsonProperty
        public TypeSignature getFromType()
        {
            return fromType;
        }

        @JsonProperty
        public TypeSignature getToType()
        {
            return toType;
        }

        @JsonProperty
        public boolean isOptional()
        {
            return optional;
        }

        @JsonCreator
        public static CastDependency fromJson(
                @JsonProperty TypeSignature fromType,
                @JsonProperty TypeSignature toType,
                @JsonProperty boolean optional)
        {
            return new CastDependency(fromType, toType, optional);
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
