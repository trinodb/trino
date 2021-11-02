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
import io.airlift.bytecode.DynamicClassLoader;
import io.trino.metadata.AggregationFunctionMetadata;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionDependencyDeclaration;
import io.trino.metadata.FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.ParametricImplementationsGroup;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata;
import io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.operator.annotations.ImplementationDependency;
import io.trino.spi.TrinoException;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.operator.ParametricFunctionHelpers.bindDependencies;
import static io.trino.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.trino.operator.aggregation.state.StateCompiler.generateStateSerializer;
import static io.trino.operator.aggregation.state.StateCompiler.getSerializedType;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_CALL;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ParametricAggregation
        extends SqlAggregationFunction
{
    private final ParametricImplementationsGroup<AggregationImplementation> implementations;
    private final Class<?> stateClass;

    public ParametricAggregation(
            Signature signature,
            AggregationHeader details,
            Class<?> stateClass,
            ParametricImplementationsGroup<AggregationImplementation> implementations)
    {
        super(
                new FunctionMetadata(
                        signature,
                        details.getName(),
                        true,
                        implementations.getArgumentDefinitions(),
                        details.isHidden(),
                        true,
                        details.getDescription().orElse(""),
                        AGGREGATE,
                        details.isDeprecated()),
                new AggregationFunctionMetadata(
                        details.isOrderSensitive(),
                        details.isDecomposable() ? Optional.of(getSerializedType(stateClass).getTypeSignature()) : Optional.empty()));
        this.stateClass = requireNonNull(stateClass, "stateClass is null");
        checkArgument(implementations.isNullable(), "currently aggregates are required to be nullable");
        this.implementations = requireNonNull(implementations, "implementations is null");
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

    private static void declareDependencies(FunctionDependencyDeclarationBuilder builder, Collection<AggregationImplementation> implementations)
    {
        for (AggregationImplementation implementation : implementations) {
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
    public InternalAggregationFunction specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        // Bind variables
        Signature signature = getFunctionMetadata().getSignature();

        // Find implementation matching arguments
        AggregationImplementation concreteImplementation = findMatchingImplementation(functionBinding.getBoundSignature());

        // Build argument and return Types from signatures
        List<Type> inputTypes = functionBinding.getBoundSignature().getArgumentTypes();
        Type outputType = functionBinding.getBoundSignature().getReturnType();

        // Create classloader for additional aggregation dependencies
        Class<?> definitionClass = concreteImplementation.getDefinitionClass();
        DynamicClassLoader classLoader = new DynamicClassLoader(definitionClass.getClassLoader(), getClass().getClassLoader());

        // Build state factory and serializer
        AccumulatorStateSerializer<?> stateSerializer = generateStateSerializer(stateClass, classLoader);
        AccumulatorStateFactory<?> stateFactory = StateCompiler.generateStateFactory(stateClass, classLoader);

        // Bind provided dependencies to aggregation method handlers
        MethodHandle inputHandle = bindDependencies(concreteImplementation.getInputFunction(), concreteImplementation.getInputDependencies(), functionBinding, functionDependencies);
        Optional<MethodHandle> removeInputHandle = concreteImplementation.getRemoveInputFunction().map(
                removeInputFunction -> bindDependencies(removeInputFunction, concreteImplementation.getRemoveInputDependencies(), functionBinding, functionDependencies));
        MethodHandle combineHandle = bindDependencies(concreteImplementation.getCombineFunction(), concreteImplementation.getCombineDependencies(), functionBinding, functionDependencies);
        MethodHandle outputHandle = bindDependencies(concreteImplementation.getOutputFunction(), concreteImplementation.getOutputDependencies(), functionBinding, functionDependencies);

        // Build metadata of input parameters
        List<ParameterMetadata> parametersMetadata = buildParameterMetadata(concreteImplementation.getInputParameterMetadataTypes(), inputTypes);

        // Generate Aggregation name
        String aggregationName = generateAggregationName(signature.getName(), outputType.getTypeSignature(), signaturesFromTypes(inputTypes));

        // Collect all collected data in Metadata
        AggregationMetadata aggregationMetadata = new AggregationMetadata(
                aggregationName,
                parametersMetadata,
                inputHandle,
                removeInputHandle,
                combineHandle,
                outputHandle,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateClass,
                        stateSerializer,
                        stateFactory)),
                outputType);

        // Create specialized InternalAggregregationFunction for Trino
        return new InternalAggregationFunction(
                signature.getName(),
                inputTypes,
                ImmutableList.of(stateSerializer.getSerializedType()),
                outputType,
                new LazyAccumulatorFactoryBinder(aggregationMetadata, classLoader));
    }

    public Class<?> getStateClass()
    {
        return stateClass;
    }

    @VisibleForTesting
    public ParametricImplementationsGroup<AggregationImplementation> getImplementations()
    {
        return implementations;
    }

    private AggregationImplementation findMatchingImplementation(BoundSignature boundSignature)
    {
        Signature signature = boundSignature.toSignature();
        Optional<AggregationImplementation> foundImplementation = Optional.empty();
        if (implementations.getExactImplementations().containsKey(signature)) {
            foundImplementation = Optional.of(implementations.getExactImplementations().get(signature));
        }
        else {
            for (AggregationImplementation candidate : implementations.getGenericImplementations()) {
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

    private static List<TypeSignature> signaturesFromTypes(List<Type> types)
    {
        return types
                .stream()
                .map(Type::getTypeSignature)
                .collect(toImmutableList());
    }

    private static List<ParameterMetadata> buildParameterMetadata(List<ParameterType> parameterMetadataTypes, List<Type> inputTypes)
    {
        ImmutableList.Builder<ParameterMetadata> builder = ImmutableList.builder();
        int inputId = 0;

        for (ParameterType parameterMetadataType : parameterMetadataTypes) {
            switch (parameterMetadataType) {
                case STATE:
                case BLOCK_INDEX:
                    builder.add(new ParameterMetadata(parameterMetadataType));
                    break;
                case INPUT_CHANNEL:
                case BLOCK_INPUT_CHANNEL:
                case NULLABLE_BLOCK_INPUT_CHANNEL:
                    builder.add(new ParameterMetadata(parameterMetadataType, inputTypes.get(inputId++)));
                    break;
            }
        }

        return builder.build();
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", ParametricAggregation.class.getSimpleName() + "[", "]")
                .add("signature=" + implementations.getSignature())
                .toString();
    }
}
