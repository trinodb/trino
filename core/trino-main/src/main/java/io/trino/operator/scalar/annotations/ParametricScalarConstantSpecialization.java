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
package io.trino.operator.scalar.annotations;

import io.trino.metadata.FunctionBinding;
import io.trino.operator.ParametricImplementationsGroup;
import io.trino.operator.annotations.ImplementationDependency;
import io.trino.operator.scalar.ConstantSpecializedSqlScalarFunction.Specialization;
import io.trino.operator.scalar.annotations.ParametricScalarImplementation.ConstantSpecializedFunction;
import io.trino.operator.scalar.annotations.ParametricScalarImplementation.ParametricScalarImplementationChoice;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder;
import io.trino.spi.function.FunctionNullability;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.primitives.Primitives.wrap;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_IMPLEMENTATION;
import static io.trino.util.Failures.checkCondition;
import static java.util.Objects.requireNonNull;

public final class ParametricScalarConstantSpecialization
{
    private final Set<Integer> consumedArguments;
    private final ParametricImplementationsGroup<ParametricScalarImplementation> implementations;
    private final FunctionNullability canonicalNullability;

    public ParametricScalarConstantSpecialization(
            Set<Integer> consumedArguments,
            ParametricImplementationsGroup<ParametricScalarImplementation> implementations,
            FunctionNullability canonicalNullability)
    {
        this.consumedArguments = Set.copyOf(requireNonNull(consumedArguments, "consumedArguments is null"));
        this.implementations = requireNonNull(implementations, "implementations is null");
        this.canonicalNullability = requireNonNull(canonicalNullability, "canonicalNullability is null");
    }

    public void declareDependencies(FunctionDependencyDeclarationBuilder builder)
    {
        declareDependencies(builder, implementations.getExactImplementations().values());
        declareDependencies(builder, implementations.getSpecializedImplementations());
        declareDependencies(builder, implementations.getGenericImplementations());
    }

    private static void declareDependencies(FunctionDependencyDeclarationBuilder builder, Collection<ParametricScalarImplementation> implementations)
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

    public Optional<Specialization> specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        BoundSignature boundSignature = functionBinding.getBoundSignature();
        List<io.trino.spi.type.Type> residualArgumentTypes = new ArrayList<>();
        for (int index = 0; index < boundSignature.getArgumentTypes().size(); index++) {
            if (!consumedArguments.contains(index)) {
                residualArgumentTypes.add(boundSignature.getArgumentTypes().get(index));
            }
        }
        BoundSignature residualBoundSignature = new BoundSignature(boundSignature.getName(), boundSignature.getReturnType(), residualArgumentTypes);

        ParametricScalarImplementation exactImplementation = implementations.getExactImplementations().get(residualBoundSignature.toSignature());
        if (exactImplementation != null) {
            Optional<ConstantSpecializedFunction> implementation = exactImplementation.specializeConstant(functionBinding, functionDependencies, residualBoundSignature);
            if (implementation.isEmpty()) {
                return Optional.empty();
            }
            ConstantSpecializedFunction specializedFunction = implementation.orElseThrow();
            validateConstructorTypes(boundSignature, specializedFunction);
            return Optional.of(new Specialization(consumedArguments, specializedFunction.boundSignature(), specializedFunction.choices(), specializedFunction.constructorConstantArguments()));
        }

        ConstantSpecializedFunction selectedImplementation = selectImplementation(
                implementations.getSpecializedImplementations(),
                functionBinding,
                functionDependencies,
                residualBoundSignature);
        if (selectedImplementation == null) {
            selectedImplementation = selectImplementation(
                    implementations.getGenericImplementations(),
                    functionBinding,
                    functionDependencies,
                    residualBoundSignature);
        }
        if (selectedImplementation == null) {
            return Optional.empty();
        }
        validateConstructorTypes(boundSignature, selectedImplementation);
        return Optional.of(new Specialization(consumedArguments, selectedImplementation.boundSignature(), selectedImplementation.choices(), selectedImplementation.constructorConstantArguments()));
    }

    private void validateConstructorTypes(BoundSignature boundSignature, ConstantSpecializedFunction implementation)
    {
        implementation.choices().forEach(choice -> {
            for (int index = 0; index < implementation.constructorConstantArguments().size(); index++) {
                int argument = implementation.constructorConstantArguments().get(index);
                Class<?> expectedType = boundSignature.getArgumentTypes().get(argument).getJavaType();
                if (canonicalNullability.isArgumentNullable(argument)) {
                    expectedType = wrap(expectedType);
                }
                checkCondition(
                        choice.instanceFactory().type().parameterType(index).isAssignableFrom(expectedType),
                        StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR,
                        "@ConstantArgument(%s) constructor parameter must accept %s, but is %s",
                        argument,
                        expectedType.getName(),
                        choice.instanceFactory().type().parameterType(index).getName());
            }
        });
    }

    private static ConstantSpecializedFunction selectImplementation(
            List<ParametricScalarImplementation> implementations,
            FunctionBinding functionBinding,
            FunctionDependencies functionDependencies,
            BoundSignature residualBoundSignature)
    {
        ConstantSpecializedFunction selectedImplementation = null;
        for (ParametricScalarImplementation implementation : implementations) {
            Optional<ConstantSpecializedFunction> candidate = implementation.specializeConstant(functionBinding, functionDependencies, residualBoundSignature);
            if (candidate.isPresent()) {
                checkCondition(selectedImplementation == null, AMBIGUOUS_FUNCTION_IMPLEMENTATION, "Ambiguous constant specialization for %s", residualBoundSignature);
                selectedImplementation = candidate.orElseThrow();
            }
        }
        return selectedImplementation;
    }
}
