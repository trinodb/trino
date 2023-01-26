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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import io.trino.operator.annotations.CastImplementationDependency;
import io.trino.operator.annotations.FunctionImplementationDependency;
import io.trino.operator.annotations.ImplementationDependency;
import io.trino.operator.annotations.LiteralImplementationDependency;
import io.trino.operator.annotations.OperatorImplementationDependency;
import io.trino.operator.annotations.TypeImplementationDependency;
import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.ParameterKind;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

class TypeSignatureMapping
{
    private final Map<String, String> mapping;

    public TypeSignatureMapping(Map<String, String> mapping)
    {
        this.mapping = ImmutableSortedMap.<String, String>orderedBy(String.CASE_INSENSITIVE_ORDER)
                .putAll(mapping)
                .build();
    }

    public Set<String> getTypeParameters()
    {
        return ImmutableSet.copyOf(mapping.keySet());
    }

    public ImplementationDependency mapTypes(ImplementationDependency dependency)
    {
        if (mapping.isEmpty()) {
            return dependency;
        }
        if (dependency instanceof TypeImplementationDependency typeDependency) {
            return new TypeImplementationDependency(mapTypeSignature(typeDependency.getSignature()));
        }
        if (dependency instanceof LiteralImplementationDependency) {
            return dependency;
        }
        if (dependency instanceof FunctionImplementationDependency functionDependency) {
            return new FunctionImplementationDependency(
                    functionDependency.getFullyQualifiedName(),
                    functionDependency.getArgumentTypes().stream()
                            .map(this::mapTypeSignature)
                            .collect(toImmutableList()),
                    functionDependency.getInvocationConvention(),
                    functionDependency.getType());
        }
        if (dependency instanceof OperatorImplementationDependency operatorDependency) {
            return new OperatorImplementationDependency(
                    operatorDependency.getOperator(),
                    operatorDependency.getArgumentTypes().stream()
                            .map(this::mapTypeSignature)
                            .collect(toImmutableList()),
                    operatorDependency.getInvocationConvention(),
                    operatorDependency.getType());
        }
        if (dependency instanceof CastImplementationDependency castDependency) {
            return new CastImplementationDependency(
                    mapTypeSignature(castDependency.getFromType()),
                    mapTypeSignature(castDependency.getToType()),
                    castDependency.getInvocationConvention(),
                    castDependency.getType());
        }
        throw new IllegalArgumentException("Unsupported dependency " + dependency);
    }

    public TypeSignature mapTypeSignature(TypeSignature typeSignature)
    {
        if (mapping.isEmpty()) {
            return typeSignature;
        }
        if (mapping.containsKey(typeSignature.getBase())) {
            checkArgument(typeSignature.getParameters().isEmpty(), "Type variable can not have type parameters: %s", typeSignature);
            return new TypeSignature(mapping.get(typeSignature.getBase()));
        }
        return new TypeSignature(
                typeSignature.getBase(),
                typeSignature.getParameters().stream()
                        .map(this::mapTypeSignatureParameter)
                        .collect(toImmutableList()));
    }

    private TypeSignatureParameter mapTypeSignatureParameter(TypeSignatureParameter parameter)
    {
        if (parameter.getKind() == ParameterKind.TYPE) {
            return TypeSignatureParameter.typeParameter(mapTypeSignature(parameter.getTypeSignature()));
        }
        if (parameter.getKind() == ParameterKind.NAMED_TYPE) {
            NamedTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
            return TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(
                    namedTypeSignature.getFieldName(),
                    mapTypeSignature(namedTypeSignature.getTypeSignature())));
        }
        return parameter;
    }
}
