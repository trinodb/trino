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
package io.trino.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ResolvedFunction
{
    private final BoundSignature signature;
    private final CatalogHandle catalogHandle;
    private final FunctionId functionId;
    private final FunctionKind functionKind;
    private final boolean deterministic;
    private final FunctionNullability functionNullability;
    private final Map<TypeSignature, Type> typeDependencies;
    private final Set<ResolvedFunction> functionDependencies;

    @JsonCreator
    public ResolvedFunction(
            @JsonProperty("signature") BoundSignature signature,
            @JsonProperty("catalogHandle") CatalogHandle catalogHandle,
            @JsonProperty("id") FunctionId functionId,
            @JsonProperty("functionKind") FunctionKind functionKind,
            @JsonProperty("deterministic") boolean deterministic,
            @JsonProperty("functionNullability") FunctionNullability functionNullability,
            @JsonProperty("typeDependencies") Map<TypeSignature, Type> typeDependencies,
            @JsonProperty("functionDependencies") Set<ResolvedFunction> functionDependencies)
    {
        this.signature = requireNonNull(signature, "signature is null");
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.functionId = requireNonNull(functionId, "functionId is null");
        this.functionKind = requireNonNull(functionKind, "functionKind is null");
        this.deterministic = deterministic;
        this.functionNullability = requireNonNull(functionNullability, "functionNullability is null");
        this.typeDependencies = ImmutableMap.copyOf(requireNonNull(typeDependencies, "typeDependencies is null"));
        this.functionDependencies = ImmutableSet.copyOf(requireNonNull(functionDependencies, "functionDependencies is null"));
        checkArgument(functionNullability.getArgumentNullable().size() == signature.getArgumentTypes().size(), "signature and functionNullability must have same argument count");
    }

    @JsonProperty
    public BoundSignature getSignature()
    {
        return signature;
    }

    @JsonProperty
    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
    }

    @JsonProperty("id")
    public FunctionId getFunctionId()
    {
        return functionId;
    }

    @JsonProperty("functionKind")
    public FunctionKind getFunctionKind()
    {
        return functionKind;
    }

    @JsonProperty
    public boolean isDeterministic()
    {
        return deterministic;
    }

    @JsonProperty
    public FunctionNullability getFunctionNullability()
    {
        return functionNullability;
    }

    @JsonProperty
    public Map<TypeSignature, Type> getTypeDependencies()
    {
        return typeDependencies;
    }

    @JsonProperty
    public Set<ResolvedFunction> getFunctionDependencies()
    {
        return functionDependencies;
    }

    public CatalogSchemaFunctionName getName()
    {
        return getSignature().getName();
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
        ResolvedFunction that = (ResolvedFunction) o;
        return Objects.equals(signature, that.signature) &&
                Objects.equals(catalogHandle, that.catalogHandle) &&
                Objects.equals(functionId, that.functionId) &&
                functionKind == that.functionKind &&
                deterministic == that.deterministic &&
                Objects.equals(functionNullability, that.functionNullability) &&
                Objects.equals(typeDependencies, that.typeDependencies) &&
                Objects.equals(functionDependencies, that.functionDependencies);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(signature, catalogHandle, functionId, functionKind, deterministic, functionNullability, typeDependencies, functionDependencies);
    }

    @Override
    public String toString()
    {
        return signature.toString();
    }
}
