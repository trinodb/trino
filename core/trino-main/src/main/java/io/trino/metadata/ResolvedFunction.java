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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.connector.CatalogHandle;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record ResolvedFunction(
        BoundSignature signature,
        CatalogHandle catalogHandle,
        FunctionId functionId,
        FunctionKind functionKind,
        boolean deterministic,
        boolean neverFails,
        FunctionNullability functionNullability,
        Map<TypeDescriptor, Type> typeDependencies,
        Set<ResolvedFunction> functionDependencies)
{
    public ResolvedFunction
    {
        requireNonNull(signature, "signature is null");
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(functionId, "functionId is null");
        requireNonNull(functionKind, "functionKind is null");
        requireNonNull(functionNullability, "functionNullability is null");
        typeDependencies = ImmutableMap.copyOf(requireNonNull(typeDependencies, "typeDependencies is null"));
        functionDependencies = ImmutableSet.copyOf(requireNonNull(functionDependencies, "functionDependencies is null"));
        checkArgument(functionNullability.getArgumentNullable().size() == signature.getArgumentTypes().size(), "signature and functionNullability must have same argument count");
    }

    public CatalogSchemaFunctionName name()
    {
        return signature().getName();
    }

    @Override
    public int hashCode()
    {
        // equal instances share the function id, which is cheap to hash
        return functionId.hashCode();
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        ResolvedFunction other = (ResolvedFunction) object;
        return functionId.equals(other.functionId) &&
                deterministic == other.deterministic &&
                neverFails == other.neverFails &&
                functionKind == other.functionKind &&
                signature.equals(other.signature) &&
                catalogHandle.equals(other.catalogHandle) &&
                functionNullability.equals(other.functionNullability) &&
                typeDependencies.equals(other.typeDependencies) &&
                functionDependencies.equals(other.functionDependencies);
    }

    @Override
    public String toString()
    {
        return signature.toString();
    }
}
