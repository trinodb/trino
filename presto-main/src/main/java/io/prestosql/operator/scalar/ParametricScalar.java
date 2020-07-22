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
package io.prestosql.operator.scalar;

import com.google.common.annotations.VisibleForTesting;
import io.prestosql.metadata.FunctionBinding;
import io.prestosql.metadata.FunctionDependencies;
import io.prestosql.metadata.FunctionDependencyDeclaration;
import io.prestosql.metadata.FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.operator.ParametricImplementationsGroup;
import io.prestosql.operator.annotations.ImplementationDependency;
import io.prestosql.operator.scalar.annotations.ParametricScalarImplementation;
import io.prestosql.operator.scalar.annotations.ParametricScalarImplementation.ParametricScalarImplementationChoice;
import io.prestosql.spi.PrestoException;

import java.util.Collection;
import java.util.Optional;

import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_IMPLEMENTATION;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static io.prestosql.util.Failures.checkCondition;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ParametricScalar
        extends SqlScalarFunction
{
    private final ParametricImplementationsGroup<ParametricScalarImplementation> implementations;

    public ParametricScalar(
            Signature signature,
            ScalarHeader details,
            ParametricImplementationsGroup<ParametricScalarImplementation> implementations,
            boolean deprecated)
    {
        super(new FunctionMetadata(
                signature,
                implementations.isNullable(),
                implementations.getArgumentDefinitions(),
                details.isHidden(),
                details.isDeterministic(),
                details.getDescription().orElse(""),
                SCALAR,
                deprecated));
        this.implementations = requireNonNull(implementations);
    }

    @VisibleForTesting
    public ParametricImplementationsGroup<ParametricScalarImplementation> getImplementations()
    {
        return implementations;
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        FunctionDependencyDeclarationBuilder builder = FunctionDependencyDeclaration.builder();
        declareDependencies(builder, implementations.getExactImplementations().values());
        declareDependencies(builder, implementations.getSpecializedImplementations());
        declareDependencies(builder, implementations.getGenericImplementations());
        return builder.build();
    }

    private static void declareDependencies(FunctionDependencyDeclarationBuilder builder,
            Collection<ParametricScalarImplementation> implementations)
    {
        for (ParametricScalarImplementation implementation : implementations) {
            for (ParametricScalarImplementationChoice choice : implementation.getChoices()) {
                for (ImplementationDependency dependency : choice.getDependencies()) {
                    dependency.declareDependencies(builder);
                }
                for (ImplementationDependency dependency : choice.getConstructorDependencies()) {
                    dependency.declareDependencies(builder);
                }
            }
        }
    }

    @Override
    public ScalarFunctionImplementation specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        Signature boundSignature = functionBinding.getBoundSignature().toSignature();
        if (implementations.getExactImplementations().containsKey(boundSignature)) {
            ParametricScalarImplementation implementation = implementations.getExactImplementations().get(boundSignature);
            Optional<ScalarFunctionImplementation> scalarFunctionImplementation = implementation.specialize(functionBinding, functionDependencies);
            checkCondition(scalarFunctionImplementation.isPresent(), FUNCTION_IMPLEMENTATION_ERROR, format("Exact implementation of %s do not match expected java types.", boundSignature.getName()));
            return scalarFunctionImplementation.get();
        }

        ScalarFunctionImplementation selectedImplementation = null;
        for (ParametricScalarImplementation implementation : implementations.getSpecializedImplementations()) {
            Optional<ScalarFunctionImplementation> scalarFunctionImplementation = implementation.specialize(functionBinding, functionDependencies);
            if (scalarFunctionImplementation.isPresent()) {
                checkCondition(selectedImplementation == null, AMBIGUOUS_FUNCTION_IMPLEMENTATION, "Ambiguous implementation for %s with bindings %s", getFunctionMetadata().getSignature(), functionBinding.getBoundSignature());
                selectedImplementation = scalarFunctionImplementation.get();
            }
        }
        if (selectedImplementation != null) {
            return selectedImplementation;
        }
        for (ParametricScalarImplementation implementation : implementations.getGenericImplementations()) {
            Optional<ScalarFunctionImplementation> scalarFunctionImplementation = implementation.specialize(functionBinding, functionDependencies);
            if (scalarFunctionImplementation.isPresent()) {
                checkCondition(selectedImplementation == null, AMBIGUOUS_FUNCTION_IMPLEMENTATION, "Ambiguous implementation for %s with bindings %s", getFunctionMetadata().getSignature(), functionBinding.getBoundSignature());
                selectedImplementation = scalarFunctionImplementation.get();
            }
        }
        if (selectedImplementation != null) {
            return selectedImplementation;
        }

        throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("Unsupported binding %s for signature %s", functionBinding.getBoundSignature(), getFunctionMetadata().getSignature()));
    }
}
