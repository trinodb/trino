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
import io.trino.spi.type.TypeSignature;

import java.util.Objects;

import static io.trino.metadata.SignatureBinder.applyBoundVariables;
import static java.util.Objects.requireNonNull;

public final class TypeImplementationDependency
        implements ImplementationDependency
{
    private final TypeSignature signature;

    public TypeImplementationDependency(TypeSignature signature)
    {
        this.signature = requireNonNull(signature, "signature is null");
    }

    public TypeSignature getSignature()
    {
        return signature;
    }

    @Override
    public void declareDependencies(FunctionDependencyDeclarationBuilder builder)
    {
        builder.addType(signature);
    }

    @Override
    public Object resolve(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        return functionDependencies.getType(applyBoundVariables(signature, functionBinding));
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
        TypeImplementationDependency that = (TypeImplementationDependency) o;
        return Objects.equals(signature, that.signature);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(signature);
    }
}
