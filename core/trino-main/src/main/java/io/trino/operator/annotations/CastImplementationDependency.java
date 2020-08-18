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
package io.trino.operator.annotations;

import io.trino.metadata.FunctionBinding;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.type.TypeSignature;

import java.util.Objects;

import static io.trino.metadata.SignatureBinder.applyBoundVariables;
import static java.util.Objects.requireNonNull;

public final class CastImplementationDependency
        extends ScalarImplementationDependency
{
    private final TypeSignature fromType;
    private final TypeSignature toType;

    public CastImplementationDependency(TypeSignature fromType, TypeSignature toType, InvocationConvention invocationConvention, Class<?> type)
    {
        super(invocationConvention, type);
        this.fromType = requireNonNull(fromType, "fromType is null");
        this.toType = requireNonNull(toType, "toType is null");
    }

    public TypeSignature getFromType()
    {
        return fromType;
    }

    public TypeSignature getToType()
    {
        return toType;
    }

    @Override
    public void declareDependencies(FunctionDependencyDeclarationBuilder builder)
    {
        builder.addCastSignature(fromType, toType);
    }

    @Override
    protected ScalarFunctionImplementation getImplementation(FunctionBinding functionBinding, FunctionDependencies functionDependencies, InvocationConvention invocationConvention)
    {
        TypeSignature from = applyBoundVariables(fromType, functionBinding);
        TypeSignature to = applyBoundVariables(toType, functionBinding);
        return functionDependencies.getCastImplementationSignature(from, to, invocationConvention);
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
        CastImplementationDependency that = (CastImplementationDependency) o;
        return Objects.equals(fromType, that.fromType) &&
                Objects.equals(toType, that.toType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fromType, toType);
    }
}
