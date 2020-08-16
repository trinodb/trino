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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.metadata.AggregationFunctionMetadata;
import io.trino.metadata.AggregationFunctionMetadata.AggregationFunctionMetadataBuilder;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionDependencyDeclaration;
import io.trino.metadata.FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Signature;
import io.trino.metadata.SignatureBinder;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.ParametricImplementationsGroup;
import io.trino.operator.aggregation.AggregationFromAnnotationsParser.AccumulatorStateDetails;
import io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind;
import io.trino.operator.aggregation.AggregationImplementation.AccumulatorStateDescriptor;
import io.trino.operator.annotations.ImplementationDependency;
import io.trino.spi.TrinoException;

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.ParametricFunctionHelpers.bindDependencies;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.normalizeInputMethod;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_CALL;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ParametricAggregation
        extends SqlAggregationFunction
{
    private final ParametricImplementationsGroup<ParametricAggregationImplementation> implementations;
    private final List<AccumulatorStateDetails<?>> stateDetails;

    public ParametricAggregation(
            Signature signature,
            AggregationHeader details,
            List<AccumulatorStateDetails<?>> stateDetails,
            ParametricImplementationsGroup<ParametricAggregationImplementation> implementations)
    {
        super(
                createFunctionMetadata(signature, details, implementations.getFunctionNullability()),
                createAggregationFunctionMetadata(details, stateDetails));
        this.stateDetails = ImmutableList.copyOf(requireNonNull(stateDetails, "stateDetails is null"));
        checkArgument(implementations.getFunctionNullability().isReturnNullable(), "currently aggregates are required to be nullable");
        this.implementations = requireNonNull(implementations, "implementations is null");
    }

    private static FunctionMetadata createFunctionMetadata(Signature signature, AggregationHeader details, FunctionNullability functionNullability)
    {
        FunctionMetadata.Builder functionMetadata = FunctionMetadata.aggregateBuilder()
                .signature(signature)
                .canonicalName(details.getName());

        if (details.getDescription().isPresent()) {
            functionMetadata.description(details.getDescription().get());
        }
        else {
            functionMetadata.noDescription();
        }

        if (details.isHidden()) {
            functionMetadata.hidden();
        }
        if (details.isDeprecated()) {
            functionMetadata.deprecated();
        }

        if (functionNullability.isReturnNullable()) {
            functionMetadata.nullable();
        }
        functionMetadata.argumentNullability(functionNullability.getArgumentNullable());

        return functionMetadata.build();
    }

    private static AggregationFunctionMetadata createAggregationFunctionMetadata(AggregationHeader details, List<AccumulatorStateDetails<?>> stateDetails)
    {
        AggregationFunctionMetadataBuilder builder = AggregationFunctionMetadata.builder();
        if (details.isOrderSensitive()) {
            builder.orderSensitive();
        }
        if (details.isDecomposable()) {
            for (AccumulatorStateDetails<?> stateDetail : stateDetails) {
                builder.intermediateType(stateDetail.getSerializedType());
            }
        }
        return builder.build();
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        FunctionDependencyDeclarationBuilder builder = FunctionDependencyDeclaration.builder();
        declareDependencies(builder, implementations.getExactImplementations().values());
        declareDependencies(builder, implementations.getSpecializedImplementations());
        declareDependencies(builder, implementations.getGenericImplementations());
        for (AccumulatorStateDetails<?> stateDetail : stateDetails) {
            for (ImplementationDependency dependency : stateDetail.getDependencies()) {
                dependency.declareDependencies(builder);
            }
        }
        return builder.build();
    }

    private static void declareDependencies(FunctionDependencyDeclarationBuilder builder, Collection<ParametricAggregationImplementation> implementations)
    {
        for (ParametricAggregationImplementation implementation : implementations) {
            for (ImplementationDependency dependency : implementation.getInputDependencies()) {
                dependency.declareDependencies(builder);
            }
            for (ImplementationDependency dependency : implementation.getCombineDependencies()) {
                dependency.declareDependencies(builder);
            }
            for (ImplementationDependency dependency : implementation.getOutputDependencies()) {
                dependency.declareDependencies(builder);
            }
        }
    }

    @Override
    public AggregationImplementation specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        // Find implementation matching arguments
        ParametricAggregationImplementation concreteImplementation = findMatchingImplementation(boundSignature);

        // Build state factory and serializer
        FunctionMetadata metadata = getFunctionMetadata();
        FunctionBinding functionBinding = SignatureBinder.bindFunction(metadata.getFunctionId(), metadata.getSignature(), boundSignature);
        List<AccumulatorStateDescriptor<?>> accumulatorStateDescriptors = stateDetails.stream()
                .map(state -> state.createAccumulatorStateDescriptor(functionBinding, functionDependencies))
                .collect(toImmutableList());

        // Bind provided dependencies to aggregation method handlers
        MethodHandle inputHandle = bindDependencies(concreteImplementation.getInputFunction(), concreteImplementation.getInputDependencies(), functionBinding, functionDependencies);
        Optional<MethodHandle> removeInputHandle = concreteImplementation.getRemoveInputFunction().map(
                removeInputFunction -> bindDependencies(removeInputFunction, concreteImplementation.getRemoveInputDependencies(), functionBinding, functionDependencies));

        Optional<MethodHandle> combineHandle = concreteImplementation.getCombineFunction();
        if (getAggregationMetadata().isDecomposable()) {
            checkArgument(combineHandle.isPresent(), "Decomposable method %s does not have a combine method", boundSignature.getName());
            combineHandle = combineHandle.map(combineFunction -> bindDependencies(combineFunction, concreteImplementation.getCombineDependencies(), functionBinding, functionDependencies));
        }
        else {
            checkArgument(concreteImplementation.getCombineFunction().isEmpty(), "Decomposable method %s does not have a combine method", boundSignature.getName());
        }

        MethodHandle outputHandle = bindDependencies(concreteImplementation.getOutputFunction(), concreteImplementation.getOutputDependencies(), functionBinding, functionDependencies);

        List<AggregationParameterKind> inputParameterKinds = concreteImplementation.getInputParameterKinds();
        inputHandle = normalizeInputMethod(inputHandle, boundSignature, inputParameterKinds);
        removeInputHandle = removeInputHandle.map(function -> normalizeInputMethod(function, boundSignature, inputParameterKinds));

        return new AggregationImplementation(
                inputHandle,
                removeInputHandle,
                combineHandle,
                outputHandle,
                accumulatorStateDescriptors);
    }

    @VisibleForTesting
    public List<AccumulatorStateDetails<?>> getStateDetails()
    {
        return stateDetails;
    }

    @VisibleForTesting
    public ParametricImplementationsGroup<ParametricAggregationImplementation> getImplementations()
    {
        return implementations;
    }

    private ParametricAggregationImplementation findMatchingImplementation(BoundSignature boundSignature)
    {
        Signature signature = boundSignature.toSignature();
        Optional<ParametricAggregationImplementation> foundImplementation = Optional.empty();
        if (implementations.getExactImplementations().containsKey(signature)) {
            foundImplementation = Optional.of(implementations.getExactImplementations().get(signature));
        }
        else {
            for (ParametricAggregationImplementation candidate : implementations.getGenericImplementations()) {
                if (candidate.areTypesAssignable(boundSignature)) {
                    if (foundImplementation.isPresent()) {
                        throw new TrinoException(AMBIGUOUS_FUNCTION_CALL, format("Ambiguous function call (%s) for %s", boundSignature, getFunctionMetadata().getSignature()));
                    }
                    foundImplementation = Optional.of(candidate);
                }
            }
        }

        if (foundImplementation.isEmpty()) {
            throw new TrinoException(FUNCTION_IMPLEMENTATION_MISSING, format("Unsupported type parameters (%s) for %s", boundSignature, getFunctionMetadata().getSignature()));
        }
        return foundImplementation.get();
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", ParametricAggregation.class.getSimpleName() + "[", "]")
                .add("signature=" + implementations.getSignature())
                .toString();
    }
}
