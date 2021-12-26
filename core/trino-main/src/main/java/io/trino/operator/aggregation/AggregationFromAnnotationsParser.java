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
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.aggregation.AggregationImplementation.Parser.parseImplementation;
import static io.trino.operator.annotations.FunctionsParserHelper.parseDescription;
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

        // There must be a single set of state classes and a single combine function
        List<AccumulatorStateDetails> stateDetails = getStateDetails(aggregationDefinition);
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
            List<AccumulatorStateDetails> stateDetails,
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

    private static Optional<Method> getCombineFunction(Class<?> clazz, List<AccumulatorStateDetails> stateDetails)
    {
        List<Method> combineFunctions = FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, CombineFunction.class);
        if (combineFunctions.isEmpty()) {
            return Optional.empty();
        }
        checkArgument(combineFunctions.size() == 1, "There must be only one @CombineFunction in class %s", clazz.toGenericString());
        Method combineFunction = getOnlyElement(combineFunctions);

        // verify parameter types
        List<Class<?>> parameterTypes = getNonDependencyParameterTypes(combineFunction);
        List<Class<?>> expectedParameterTypes = Stream.concat(stateDetails.stream(), stateDetails.stream())
                .map(AccumulatorStateDetails::getStateClass)
                .collect(toImmutableList());
        checkArgument(parameterTypes.equals(expectedParameterTypes),
                "Expected combine function non-dependency parameters to be %s: %s",
                expectedParameterTypes,
                combineFunction);

        // legacy combine functions did not require parameters to be fully annotated
        if (stateDetails.size() > 1) {
            List<List<Annotation>> parameterAnnotations = getNonDependencyParameterAnnotations(combineFunction);
            List<AccumulatorStateDetails> actualStateDetails = new ArrayList<>();
            for (int parameterIndex = 0; parameterIndex < parameterTypes.size(); parameterIndex++) {
                actualStateDetails.add(toAccumulatorStateDetails(parameterTypes.get(parameterIndex), parameterAnnotations.get(parameterIndex), combineFunction, true));
            }
            List<AccumulatorStateDetails> expectedStateDetails = ImmutableList.<AccumulatorStateDetails>builder().addAll(stateDetails).addAll(stateDetails).build();
            checkArgument(actualStateDetails.equals(expectedStateDetails), "Expected combine function to have state parameters %s, but has %s", stateDetails, expectedStateDetails);
        }
        return Optional.of(combineFunction);
    }

    private static List<Method> getOutputFunctions(Class<?> clazz, List<AccumulatorStateDetails> stateDetails)
    {
        List<Method> outputFunctions = FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, OutputFunction.class);
        for (Method outputFunction : outputFunctions) {
            // verify parameter types
            List<Class<?>> parameterTypes = getNonDependencyParameterTypes(outputFunction);
            List<Class<?>> expectedParameterTypes = ImmutableList.<Class<?>>builder()
                    .addAll(stateDetails.stream().map(AccumulatorStateDetails::getStateClass).collect(toImmutableList()))
                    .add(BlockBuilder.class)
                    .build();
            checkArgument(parameterTypes.equals(expectedParameterTypes),
                    "Expected output function non-dependency parameters to be %s: %s",
                    expectedParameterTypes.stream().map(Class::getSimpleName).collect(toImmutableList()),
                    outputFunction);

            // legacy output functions did not require parameters to be fully annotated
            if (stateDetails.size() > 1) {
                List<List<Annotation>> parameterAnnotations = getNonDependencyParameterAnnotations(outputFunction);

                List<AccumulatorStateDetails> actualStateDetails = new ArrayList<>();
                for (int parameterIndex = 0; parameterIndex < stateDetails.size(); parameterIndex++) {
                    actualStateDetails.add(toAccumulatorStateDetails(parameterTypes.get(parameterIndex), parameterAnnotations.get(parameterIndex), outputFunction, true));
                }
                checkArgument(actualStateDetails.equals(stateDetails), "Expected output function to have state parameters %s, but has %s", stateDetails, actualStateDetails);
            }
        }
        checkArgument(!outputFunctions.isEmpty(), "Aggregation has no output functions");
        return outputFunctions;
    }

    private static List<Method> getInputFunctions(Class<?> clazz, List<AccumulatorStateDetails> stateDetails)
    {
        List<Method> inputFunctions = FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class);
        for (Method inputFunction : inputFunctions) {
            // verify state parameter types
            List<Class<?>> parameterTypes = getNonDependencyParameterTypes(inputFunction)
                    .subList(0, stateDetails.size());
            List<Class<?>> expectedParameterTypes = ImmutableList.<Class<?>>builder()
                    .addAll(stateDetails.stream().map(AccumulatorStateDetails::getStateClass).collect(toImmutableList()))
                    .build()
                    .subList(0, stateDetails.size());
            checkArgument(parameterTypes.equals(expectedParameterTypes),
                    "Expected input function non-dependency parameters to begin with state types %s: %s",
                    expectedParameterTypes.stream().map(Class::getSimpleName).collect(toImmutableList()),
                    inputFunction);

            // g input functions did not require parameters to be fully annotated
            if (stateDetails.size() > 1) {
                List<List<Annotation>> parameterAnnotations = getNonDependencyParameterAnnotations(inputFunction)
                        .subList(0, stateDetails.size());

                List<AccumulatorStateDetails> actualStateDetails = new ArrayList<>();
                for (int parameterIndex = 0; parameterIndex < stateDetails.size(); parameterIndex++) {
                    actualStateDetails.add(toAccumulatorStateDetails(parameterTypes.get(parameterIndex), parameterAnnotations.get(parameterIndex), inputFunction, false));
                }
                checkArgument(actualStateDetails.equals(stateDetails), "Expected input function to have state parameters %s, but has %s", stateDetails, actualStateDetails);
            }
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

    private static List<List<Annotation>> getNonDependencyParameterAnnotations(Method function)
    {
        Annotation[][] parameterAnnotations = function.getParameterAnnotations();
        return getNonDependencyParameters(function)
                .mapToObj(index -> ImmutableList.copyOf(parameterAnnotations[index]))
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

    private static List<AccumulatorStateDetails> getStateDetails(Class<?> clazz)
    {
        ImmutableSet.Builder<List<AccumulatorStateDetails>> builder = ImmutableSet.builder();
        for (Method inputFunction : FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class)) {
            List<Class<?>> parameterTypes = getNonDependencyParameterTypes(inputFunction);
            checkArgument(!parameterTypes.isEmpty(), "Input function has no parameters");
            List<List<Annotation>> parameterAnnotations = getNonDependencyParameterAnnotations(inputFunction);

            ImmutableList.Builder<AccumulatorStateDetails> stateParameters = ImmutableList.builder();
            for (int parameterIndex = 0; parameterIndex < parameterTypes.size(); parameterIndex++) {
                Class<?> parameterType = parameterTypes.get(parameterIndex);
                if (!AccumulatorState.class.isAssignableFrom(parameterType)) {
                    continue;
                }

                stateParameters.add(toAccumulatorStateDetails(parameterType, parameterAnnotations.get(parameterIndex), inputFunction, false));
            }
            List<AccumulatorStateDetails> states = stateParameters.build();
            checkArgument(!states.isEmpty(), "Input function must have at least one state parameter");
            builder.add(states);
        }
        Set<List<AccumulatorStateDetails>> functionStateClasses = builder.build();
        checkArgument(!functionStateClasses.isEmpty(), "No input functions found");
        checkArgument(functionStateClasses.size() == 1, "There must be exactly one set of @AccumulatorState in class %s", clazz.toGenericString());

        return getOnlyElement(functionStateClasses);
    }

    private static AccumulatorStateDetails toAccumulatorStateDetails(Class<?> parameterType, List<Annotation> parameterAnnotations, Method method, boolean requireAnnotation)
    {
        Optional<AggregationState> state = parameterAnnotations.stream()
                .filter(AggregationState.class::isInstance)
                .map(AggregationState.class::cast)
                .findFirst();

        if (requireAnnotation) {
            checkArgument(state.isPresent(), "AggregationState must be present on AccumulatorState parameters: %s", method);
        }

        Optional<TypeSignature> stateSqlType = state.map(AggregationState::value)
                .filter(type -> !type.isEmpty())
                .map(TypeSignature::new);

        AccumulatorStateDetails accumulatorStateDetails = new AccumulatorStateDetails(parameterType.asSubclass(AccumulatorState.class), stateSqlType);
        return accumulatorStateDetails;
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
