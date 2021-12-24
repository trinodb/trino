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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MoreCollectors;
import io.airlift.log.Logger;
import io.trino.metadata.Signature;
import io.trino.operator.ParametricImplementationsGroup;
import io.trino.operator.annotations.FunctionsParserHelper;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.FunctionDependency;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.RemoveInputFunction;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.TypeSignature;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.aggregation.AggregationImplementation.Parser.parseImplementation;
import static io.trino.operator.annotations.FunctionsParserHelper.parseDescription;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public final class AggregationFromAnnotationsParser
{
    private static final Logger log = Logger.get(AggregationFromAnnotationsParser.class);

    private AggregationFromAnnotationsParser() {}

    public static List<ParametricAggregation> parseFunctionDefinitions(Class<?> aggregationDefinition)
    {
        AggregationFunction aggregationAnnotation = aggregationDefinition.getAnnotation(AggregationFunction.class);
        requireNonNull(aggregationAnnotation, "aggregationAnnotation is null");

        ImmutableList.Builder<ParametricAggregation> functions = ImmutableList.builder();

        // There must be a single state class and combine function
        AccumulatorStateDetails stateDetails = getStateDetails(aggregationDefinition);
        Optional<Method> combineFunction = getCombineFunction(aggregationDefinition, stateDetails);

        // Each output function defines a new aggregation function
        for (Method outputFunction : getOutputFunctions(aggregationDefinition, stateDetails)) {
            AggregationHeader header = parseHeader(aggregationDefinition, outputFunction);
            if (header.isDecomposable()) {
                checkArgument(combineFunction.isPresent(), "Decomposable method %s does not have a combine method", header.getName());
            }
            else if (combineFunction.isPresent()) {
                log.warn("Aggregation function %s is not decomposable, but has combine method", header.getName());
            }

            // Input functions can have either an exact signature, or generic/calculate signature
            List<AggregationImplementation> exactImplementations = new ArrayList<>();
            List<AggregationImplementation> nonExactImplementations = new ArrayList<>();
            for (Method inputFunction : getInputFunctions(aggregationDefinition, stateDetails)) {
                Optional<Method> removeInputFunction = getRemoveInputFunction(aggregationDefinition, inputFunction);
                AggregationImplementation implementation = parseImplementation(
                        aggregationDefinition,
                        header.getName(),
                        inputFunction,
                        removeInputFunction,
                        outputFunction,
                        combineFunction.filter(function -> header.isDecomposable()));
                if (isGenericOrCalculated(implementation.getSignature())) {
                    exactImplementations.add(implementation);
                }
                else {
                    nonExactImplementations.add(implementation);
                }
            }

            // register a set functions for the canonical name, and each alias
            functions.addAll(buildFunctions(header.getName(), header, stateDetails, exactImplementations, nonExactImplementations));
            for (String alias : getAliases(aggregationDefinition.getAnnotation(AggregationFunction.class), outputFunction)) {
                functions.addAll(buildFunctions(alias, header, stateDetails, exactImplementations, nonExactImplementations));
            }
        }

        return functions.build();
    }

    private static List<ParametricAggregation> buildFunctions(
            String name,
            AggregationHeader header,
            AccumulatorStateDetails stateDetails,
            List<AggregationImplementation> exactImplementations,
            List<AggregationImplementation> nonExactImplementations)
    {
        ImmutableList.Builder<ParametricAggregation> functions = ImmutableList.builder();

        // create a separate function for each exact implementation
        for (AggregationImplementation exactImplementation : exactImplementations) {
            functions.add(new ParametricAggregation(
                    exactImplementation.getSignature().withName(name),
                    header,
                    stateDetails,
                    ParametricImplementationsGroup.of(exactImplementation).withAlias(name)));
        }

        // if there are non-exact functions, create a single generic/calculated function using these implementations
        if (!nonExactImplementations.isEmpty()) {
            ParametricImplementationsGroup.Builder<AggregationImplementation> implementationsBuilder = ParametricImplementationsGroup.builder();
            nonExactImplementations.forEach(implementationsBuilder::addImplementation);
            ParametricImplementationsGroup<AggregationImplementation> implementations = implementationsBuilder.build();
            functions.add(new ParametricAggregation(
                    implementations.getSignature().withName(name),
                    header,
                    stateDetails,
                    implementations.withAlias(name)));
        }

        return functions.build();
    }

    private static boolean isGenericOrCalculated(Signature signature)
    {
        return signature.getTypeVariableConstraints().isEmpty()
                && signature.getArgumentTypes().stream().noneMatch(TypeSignature::isCalculated)
                && !signature.getReturnType().isCalculated();
    }

    private static AggregationHeader parseHeader(AnnotatedElement aggregationDefinition, AnnotatedElement outputFunction)
    {
        AggregationFunction aggregationAnnotation = aggregationDefinition.getAnnotation(AggregationFunction.class);
        requireNonNull(aggregationAnnotation, "aggregationAnnotation is null");
        String name = getName(aggregationAnnotation, outputFunction);
        return new AggregationHeader(
                name,
                parseDescription(aggregationDefinition, outputFunction),
                aggregationAnnotation.decomposable(),
                aggregationAnnotation.isOrderSensitive(),
                aggregationAnnotation.hidden(),
                aggregationDefinition.getAnnotationsByType(Deprecated.class).length > 0);
    }

    private static String getName(AggregationFunction aggregationAnnotation, AnnotatedElement outputFunction)
    {
        AggregationFunction annotation = outputFunction.getAnnotation(AggregationFunction.class);
        if (annotation != null && !annotation.value().isEmpty()) {
            return emptyToNull(annotation.value());
        }
        return emptyToNull(aggregationAnnotation.value());
    }

    private static List<String> getAliases(AggregationFunction aggregationAnnotation, AnnotatedElement outputFunction)
    {
        AggregationFunction annotation = outputFunction.getAnnotation(AggregationFunction.class);
        if (annotation != null && annotation.alias().length > 0) {
            return ImmutableList.copyOf(annotation.alias());
        }
        return ImmutableList.copyOf(aggregationAnnotation.alias());
    }

    private static Optional<Method> getCombineFunction(Class<?> clazz, AccumulatorStateDetails stateDetails)
    {
        List<Method> combineFunctions = FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, CombineFunction.class);
        for (Method combineFunction : combineFunctions) {
            // verify parameter types
            List<Class<?>> parameterTypes = getNonDependencyParameterTypes(combineFunction);
            List<Class<?>> expectedParameterTypes = nCopies(2, stateDetails.getStateClass());
            checkArgument(parameterTypes.equals(expectedParameterTypes), "Expected combine function non-dependency parameters to be %s: %s", expectedParameterTypes, combineFunction);
        }
        checkArgument(combineFunctions.size() <= 1, "There must be only one @CombineFunction in class %s for the @AggregationState %s", clazz.toGenericString(), stateDetails.getStateClass().toGenericString());
        return combineFunctions.stream().findFirst();
    }

    private static List<Method> getOutputFunctions(Class<?> clazz, AccumulatorStateDetails stateDetails)
    {
        List<Method> outputFunctions = FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, OutputFunction.class);
        for (Method outputFunction : outputFunctions) {
            // verify parameter types
            List<Class<?>> parameterTypes = getNonDependencyParameterTypes(outputFunction);
            List<Class<?>> expectedParameterTypes = ImmutableList.<Class<?>>builder()
                    .add(stateDetails.getStateClass())
                    .add(BlockBuilder.class)
                    .build();
            checkArgument(parameterTypes.equals(expectedParameterTypes),
                    "Expected output function non-dependency parameters to be %s: %s",
                    expectedParameterTypes.stream().map(Class::getSimpleName).collect(toImmutableList()),
                    outputFunction);
        }
        checkArgument(!outputFunctions.isEmpty(), "Aggregation has no output functions");
        return outputFunctions;
    }

    private static List<Method> getInputFunctions(Class<?> clazz, AccumulatorStateDetails stateDetails)
    {
        List<Method> inputFunctions = FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class);
        for (Method inputFunction : inputFunctions) {
            // verify state parameter is first non-dependency parameter
            Class<?> actualStateType = getNonDependencyParameterTypes(inputFunction).get(0);
            checkArgument(stateDetails.getStateClass().equals(actualStateType),
                    "Expected input function non-dependency parameters to begin with state type %s: %s",
                    stateDetails.getStateClass().getSimpleName(),
                    inputFunction);
        }

        checkArgument(!inputFunctions.isEmpty(), "Aggregation has no input functions");
        return inputFunctions;
    }

    private static IntStream getNonDependencyParameters(Method function)
    {
        Annotation[][] parameterAnnotations = function.getParameterAnnotations();
        return IntStream.range(0, function.getParameterCount())
                .filter(i -> Arrays.stream(parameterAnnotations[i]).noneMatch(TypeParameter.class::isInstance))
                .filter(i -> Arrays.stream(parameterAnnotations[i]).noneMatch(LiteralParameter.class::isInstance))
                .filter(i -> Arrays.stream(parameterAnnotations[i]).noneMatch(OperatorDependency.class::isInstance))
                .filter(i -> Arrays.stream(parameterAnnotations[i]).noneMatch(FunctionDependency.class::isInstance));
    }

    private static List<Class<?>> getNonDependencyParameterTypes(Method function)
    {
        Class<?>[] parameterTypes = function.getParameterTypes();
        return getNonDependencyParameters(function)
                .mapToObj(index -> parameterTypes[index])
                .collect(toImmutableList());
    }

    private static Optional<Method> getRemoveInputFunction(Class<?> clazz, Method inputFunction)
    {
        // Only include methods which take the same parameters as the corresponding input function
        return FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, RemoveInputFunction.class).stream()
                .filter(method -> Arrays.equals(method.getParameterTypes(), inputFunction.getParameterTypes()))
                .filter(method -> Arrays.deepEquals(method.getParameterAnnotations(), inputFunction.getParameterAnnotations()))
                .collect(MoreCollectors.toOptional());
    }

    private static AccumulatorStateDetails getStateDetails(Class<?> clazz)
    {
        ImmutableSet.Builder<AccumulatorStateDetails> builder = ImmutableSet.builder();
        for (Method inputFunction : FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class)) {
            checkArgument(inputFunction.getParameterTypes().length > 0, "Input function has no parameters");
            int aggregationStateParamIndex = AggregationImplementation.Parser.findAggregationStateParamId(inputFunction);
            Class<? extends AccumulatorState> stateClass = inputFunction.getParameterTypes()[aggregationStateParamIndex].asSubclass(AccumulatorState.class);

            Optional<TypeSignature> stateType = Arrays.stream(inputFunction.getParameterAnnotations()[aggregationStateParamIndex])
                    .filter(AggregationState.class::isInstance)
                    .map(AggregationState.class::cast)
                    .findFirst()
                    .map(AggregationState::value)
                    .filter(type -> !type.isEmpty())
                    .map(TypeSignature::new);

            builder.add(new AccumulatorStateDetails(stateClass, stateType));
        }
        Set<AccumulatorStateDetails> stateClasses = builder.build();
        checkArgument(!stateClasses.isEmpty(), "No input functions found");
        checkArgument(stateClasses.size() == 1, "There must be exactly one @AccumulatorState in class %s", clazz.toGenericString());

        return getOnlyElement(stateClasses);
    }

    public static class AccumulatorStateDetails
    {
        private final Class<? extends AccumulatorState> stateClass;
        private final Optional<TypeSignature> type;

        public AccumulatorStateDetails(Class<? extends AccumulatorState> stateClass, Optional<TypeSignature> type)
        {
            this.stateClass = requireNonNull(stateClass, "stateClass is null");
            this.type = requireNonNull(type, "type is null");
        }

        public Class<? extends AccumulatorState> getStateClass()
        {
            return stateClass;
        }

        public Optional<TypeSignature> getStateType()
        {
            return type;
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
            AccumulatorStateDetails that = (AccumulatorStateDetails) o;
            return Objects.equals(stateClass, that.stateClass) && Objects.equals(type, that.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(stateClass, type);
        }
    }
}
