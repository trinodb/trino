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
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder;
import io.trino.metadata.FunctionInvoker;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.tree.QualifiedName;

import java.util.List;
import java.util.Objects;

import static io.trino.metadata.SignatureBinder.applyBoundVariables;
import static java.util.Objects.requireNonNull;

public final class FunctionImplementationDependency
        extends ScalarImplementationDependency
{
    private final QualifiedName fullyQualifiedName;
    private final List<TypeSignature> argumentTypes;

    public FunctionImplementationDependency(QualifiedName fullyQualifiedName, List<TypeSignature> argumentTypes, InvocationConvention invocationConvention, Class<?> type)
    {
        super(invocationConvention, type);
        this.fullyQualifiedName = requireNonNull(fullyQualifiedName, "fullyQualifiedName is null");
        this.argumentTypes = requireNonNull(argumentTypes, "argumentTypes is null");
    }

    @Override
    public void declareDependencies(FunctionDependencyDeclarationBuilder builder)
    {
        builder.addFunctionSignature(fullyQualifiedName, argumentTypes);
    }

    @Override
    protected FunctionInvoker getInvoker(FunctionBinding functionBinding, FunctionDependencies functionDependencies, InvocationConvention invocationConvention)
    {
        List<TypeSignature> types = applyBoundVariables(argumentTypes, functionBinding);
        return functionDependencies.getFunctionSignatureInvoker(fullyQualifiedName, types, invocationConvention);
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
        FunctionImplementationDependency that = (FunctionImplementationDependency) o;
        return Objects.equals(fullyQualifiedName, that.fullyQualifiedName) &&
                Objects.equals(argumentTypes, that.argumentTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fullyQualifiedName, argumentTypes);
    }
}
