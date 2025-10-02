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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.metadata.FunctionBinding;
import io.trino.operator.ParametricImplementationsGroup;
import io.trino.operator.aggregation.state.InOutStateSerializer;
import io.trino.operator.annotations.FunctionsParserHelper;
import io.trino.operator.annotations.ImplementationDependency;
import io.trino.operator.annotations.TypeImplementationDependency;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateMetadata;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationImplementation.AccumulatorStateDescriptor;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.InOut;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.Signature;
import io.trino.spi.function.WindowAccumulator;
import io.trino.spi.type.TypeSignature;

import java.lang.annotation.Annotation;
import java.lang.reflect.AccessFlag;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.aggregation.ParametricAggregationImplementation.Parser.parseImplementation;
import static io.trino.operator.aggregation.state.StateCompiler.generateInOutStateFactory;
import static io.trino.operator.aggregation.state.StateCompiler.generateStateFactory;
import static io.trino.operator.aggregation.state.StateCompiler.generateStateSerializer;
import static io.trino.operator.aggregation.state.StateCompiler.getMetadataAnnotation;
import static io.trino.operator.annotations.FunctionsParserHelper.parseDescription;
import static io.trino.operator.annotations.ImplementationDependency.Factory.createDependency;
import static io.trino.operator.annotations.ImplementationDependency.getImplementationDependencyAnnotation;
import static io.trino.operator.annotations.ImplementationDependency.validateImplementationDependencyAnnotation;
import static io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

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
        List<AccumulatorStateDetails<?>> stateDetails = getStateDetails(aggregationDefinition);
        Optional<Method> combineFunction = getCombineFunction(aggregationDefinition, stateDetails);

        // Each output function defines a new aggregation function
        for (Method outputFunction : getOutputFunctions(aggregationDefinition, stateDetails)) {
            AggregationHeader header = parseHeader(aggregationDefinition, outputFunction);
            if (header.decomposable()) {
                checkArgument(combineFunction.isPresent(), "Decomposable method %s does not have a combine method", header.name());
            }
            else if (combineFunction.isPresent()) {
                log.warn("Aggregation function %s is not decomposable, but has combine method", header.name());
            }

            // Input functions can have either an exact signature, or generic/calculate signature
            List<ParametricAggregationImplementation> exactImplementations = new ArrayList<>();
            List<ParametricAggregationImplementation> nonExactImplementations = new ArrayList<>();
            for (Method inputFunction : getInputFunctions(aggregationDefinition, stateDetails)) {
                ParametricAggregationImplementation implementation = parseImplementation(
                        aggregationDefinition,
                        stateDetails,
                        inputFunction,
                        outputFunction,
                        combineFunction.filter(function -> header.decomposable()),
                        header.windowAccumulator());
                if (isGenericOrCalculated(implementation.getSignature())) {
                    exactImplementations.add(implementation);
                }
                else {
                    nonExactImplementations.add(implementation);
                }
            }

            functions.addAll(buildFunctions(header, stateDetails, exactImplementations, nonExactImplementations));
        }

        return functions.build();
    }

    private static List<ParametricAggregation> buildFunctions(
            AggregationHeader header,
            List<AccumulatorStateDetails<?>> stateDetails,
            List<ParametricAggregationImplementation> exactImplementations,
            List<ParametricAggregationImplementation> nonExactImplementations)
    {
        ImmutableList.Builder<ParametricAggregation> functions = ImmutableList.builder();

        // create a separate function for each exact implementation
        for (ParametricAggregationImplementation exactImplementation : exactImplementations) {
            functions.add(new ParametricAggregation(
                    exactImplementation.getSignature(),
                    header,
                    stateDetails,
                    ParametricImplementationsGroup.of(exactImplementation)));
        }

        // if there are non-exact functions, create a single generic/calculated function using these implementations
        if (!nonExactImplementations.isEmpty()) {
            ParametricImplementationsGroup.Builder<ParametricAggregationImplementation> implementationsBuilder = ParametricImplementationsGroup.builder();
            nonExactImplementations.forEach(implementationsBuilder::addImplementation);
            ParametricImplementationsGroup<ParametricAggregationImplementation> implementations = implementationsBuilder.build();
            functions.add(new ParametricAggregation(
                    implementations.getSignature(),
                    header,
                    stateDetails,
                    implementations));
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
        return new AggregationHeader(
                getName(aggregationAnnotation, outputFunction),
                getAliases(aggregationAnnotation, outputFunction),
                parseDescription(aggregationDefinition, outputFunction),
                aggregationAnnotation.decomposable(),
                aggregationAnnotation.isOrderSensitive(),
                aggregationAnnotation.hidden(),
                aggregationDefinition.getAnnotationsByType(Deprecated.class).length > 0,
                getWindowAccumulator(aggregationAnnotation));
    }

    private static String getName(AggregationFunction aggregationAnnotation, AnnotatedElement outputFunction)
    {
        AggregationFunction annotation = outputFunction.getAnnotation(AggregationFunction.class);
        if (annotation != null && !annotation.value().isEmpty()) {
            return emptyToNull(annotation.value());
        }
        return emptyToNull(aggregationAnnotation.value());
    }

    private static Set<String> getAliases(AggregationFunction aggregationAnnotation, AnnotatedElement outputFunction)
    {
        AggregationFunction annotation = outputFunction.getAnnotation(AggregationFunction.class);
        if (annotation != null && annotation.alias().length > 0) {
            return ImmutableSet.copyOf(annotation.alias());
        }
        return ImmutableSet.copyOf(aggregationAnnotation.alias());
    }

    private static Optional<Class<? extends WindowAccumulator>> getWindowAccumulator(AggregationFunction aggregationAnnotation)
    {
        Class<? extends WindowAccumulator> windowAccumulator = aggregationAnnotation.windowAccumulator();
        if (windowAccumulator.equals(WindowAccumulator.class)) {
            return Optional.empty();
        }
        Set<AccessFlag> accessFlags = windowAccumulator.accessFlags();
        checkArgument(accessFlags.contains(AccessFlag.PUBLIC), "Window accumulator must be public: %s", windowAccumulator);
        checkArgument(accessFlags.contains(AccessFlag.STATIC), "Window accumulator must be static: %s", windowAccumulator);
        boolean hasValidConstructor = Arrays.stream(windowAccumulator.getConstructors())
                .map(Constructor::getParameterTypes)
                .map(List::of)
                .anyMatch(parameters -> parameters.isEmpty() || parameters.equals(List.of(List.class)));
        checkArgument(hasValidConstructor, "Window accumulator must have a public no-arg constructor or a constructor with a single List parameter: %s", windowAccumulator);
        return Optional.of(windowAccumulator);
    }

    private static Optional<Method> getCombineFunction(Class<?> clazz, List<AccumulatorStateDetails<?>> stateDetails)
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
            List<AccumulatorStateDetails<?>> actualStateDetails = new ArrayList<>();
            for (int parameterIndex = 0; parameterIndex < parameterTypes.size(); parameterIndex++) {
                actualStateDetails.add(toAccumulatorStateDetails(parameterTypes.get(parameterIndex).asSubclass(AccumulatorState.class), parameterAnnotations.get(parameterIndex), combineFunction, true));
            }
            List<AccumulatorStateDetails<?>> expectedStateDetails = ImmutableList.<AccumulatorStateDetails<?>>builder().addAll(stateDetails).addAll(stateDetails).build();
            checkArgument(actualStateDetails.equals(expectedStateDetails), "Expected combine function to have state parameters %s, but has %s", stateDetails, expectedStateDetails);
        }
        return Optional.of(combineFunction);
    }

    private static List<Method> getOutputFunctions(Class<?> clazz, List<AccumulatorStateDetails<?>> stateDetails)
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

                List<AccumulatorStateDetails<?>> actualStateDetails = new ArrayList<>();
                for (int parameterIndex = 0; parameterIndex < stateDetails.size(); parameterIndex++) {
                    actualStateDetails.add(toAccumulatorStateDetails(parameterTypes.get(parameterIndex).asSubclass(AccumulatorState.class), parameterAnnotations.get(parameterIndex), outputFunction, true));
                }
                checkArgument(actualStateDetails.equals(stateDetails), "Expected output function to have state parameters %s, but has %s", stateDetails, actualStateDetails);
            }
        }
        checkArgument(!outputFunctions.isEmpty(), "Aggregation has no output functions");
        return outputFunctions;
    }

    private static List<Method> getInputFunctions(Class<?> clazz, List<AccumulatorStateDetails<?>> stateDetails)
    {
        List<Method> inputFunctions = FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class);
        for (Method inputFunction : inputFunctions) {
            // verify state parameter types
            List<Class<?>> parameterTypes = getNonDependencyParameterTypes(inputFunction)
                    .subList(0, stateDetails.size());
            List<Class<?>> expectedParameterTypes = stateDetails.stream()
                    .map(AccumulatorStateDetails::getStateClass)
                    .collect(toImmutableList());
            checkArgument(parameterTypes.equals(expectedParameterTypes),
                    "Expected input function non-dependency parameters to begin with state types %s: %s",
                    expectedParameterTypes.stream().map(Class::getSimpleName).collect(toImmutableList()),
                    inputFunction);

            // input functions did not require parameters to be fully annotated
            if (stateDetails.size() > 1) {
                List<List<Annotation>> parameterAnnotations = getNonDependencyParameterAnnotations(inputFunction)
                        .subList(0, stateDetails.size());

                List<AccumulatorStateDetails<?>> actualStateDetails = new ArrayList<>();
                for (int parameterIndex = 0; parameterIndex < stateDetails.size(); parameterIndex++) {
                    actualStateDetails.add(toAccumulatorStateDetails(parameterTypes.get(parameterIndex).asSubclass(AccumulatorState.class), parameterAnnotations.get(parameterIndex), inputFunction, false));
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
                .filter(i -> Arrays.stream(parameterAnnotations[i]).noneMatch(ImplementationDependency::isImplementationDependencyAnnotation));
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

    private static List<AccumulatorStateDetails<?>> getStateDetails(Class<?> clazz)
    {
        ImmutableSet.Builder<List<AccumulatorStateDetails<?>>> builder = ImmutableSet.builder();
        for (Method inputFunction : FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class)) {
            List<Class<?>> parameterTypes = getNonDependencyParameterTypes(inputFunction);
            checkArgument(!parameterTypes.isEmpty(), "Input function has no parameters");
            List<List<Annotation>> parameterAnnotations = getNonDependencyParameterAnnotations(inputFunction);

            ImmutableList.Builder<AccumulatorStateDetails<?>> stateParameters = ImmutableList.builder();
            for (int parameterIndex = 0; parameterIndex < parameterTypes.size(); parameterIndex++) {
                Class<?> parameterType = parameterTypes.get(parameterIndex);
                if (!AccumulatorState.class.isAssignableFrom(parameterType)) {
                    continue;
                }

                stateParameters.add(toAccumulatorStateDetails(parameterType.asSubclass(AccumulatorState.class), parameterAnnotations.get(parameterIndex), inputFunction, false));
            }
            List<AccumulatorStateDetails<?>> states = stateParameters.build();
            checkArgument(!states.isEmpty(), "Input function must have at least one state parameter");
            builder.add(states);
        }
        Set<List<AccumulatorStateDetails<?>>> functionStateClasses = builder.build();
        checkArgument(!functionStateClasses.isEmpty(), "No input functions found");
        checkArgument(functionStateClasses.size() == 1, "There must be exactly one set of @AccumulatorState in class %s", clazz.toGenericString());

        return getOnlyElement(functionStateClasses);
    }

    private static <T extends AccumulatorState> AccumulatorStateDetails<T> toAccumulatorStateDetails(
            Class<T> stateClass,
            List<Annotation> parameterAnnotations,
            Method method,
            boolean requireAnnotation)
    {
        Optional<AggregationState> state = parameterAnnotations.stream()
                .filter(AggregationState.class::isInstance)
                .map(AggregationState.class::cast)
                .findFirst();

        if (requireAnnotation) {
            checkArgument(state.isPresent(), "AggregationState must be present on AccumulatorState parameters: %s", method);
        }

        List<String> declaredTypeParameters = state.map(AggregationState::value)
                .map(ImmutableList::copyOf)
                .orElse(ImmutableList.of());

        return toAccumulatorStateDetails(stateClass, declaredTypeParameters);
    }

    @VisibleForTesting
    public static <T extends AccumulatorState> AccumulatorStateDetails<T> toAccumulatorStateDetails(Class<T> stateClass, List<String> declaredTypeParameters)
    {
        StateMetadata metadata = StateMetadata.create(getMetadataAnnotation(stateClass));
        // Generic state classes have their own type variables, that must be mapped to the aggregation's type variables
        TypeSignatureMapping typeParameterMapping = getTypeParameterMapping(stateClass, declaredTypeParameters, metadata);

        if (stateClass.equals(InOut.class)) {
            String typeVariable = typeParameterMapping.mapTypeSignature(new TypeSignature("T")).toString();
            @SuppressWarnings("unchecked") AccumulatorStateDetails<T> stateDetails = (AccumulatorStateDetails<T>) getInOutAccumulatorStateDetails(typeVariable);
            return stateDetails;
        }

        List<ImplementationDependency> allDependencies = new ArrayList<>();

        BiFunction<FunctionBinding, FunctionDependencies, AccumulatorStateSerializer<T>> serializerGenerator;
        if (metadata.stateSerializerClass().isPresent()) {
            Constructor<?> constructor = getOnlyConstructor(metadata.stateSerializerClass().get());
            List<ImplementationDependency> dependencies = parseImplementationDependencies(typeParameterMapping, constructor);
            serializerGenerator = new TypedFactory<>(constructor, dependencies);
            allDependencies.addAll(dependencies);
        }
        else {
            serializerGenerator = (functionBinding, functionDependencies) -> generateStateSerializer(stateClass);
        }

        TypeSignature serializedType;
        if (metadata.serializedType().isPresent()) {
            serializedType = typeParameterMapping.mapTypeSignature(parseTypeSignature(metadata.serializedType().get(), ImmutableSet.of()));
        }
        else {
            // serialized type is not explicit declared, so we must construct it to get the
            // type, but this will only work if there are no dependencies
            checkArgument(allDependencies.isEmpty(), "serializedType must be set for state %s with dependencies", stateClass);
            AccumulatorStateSerializer<T> serializer = serializerGenerator.apply(null, null);
            serializedType = serializer.getSerializedType().getTypeSignature();
            // since there are no dependencies, the same serializer can be used for all
            serializerGenerator = (functionBinding, functionDependencies) -> serializer;
        }

        BiFunction<FunctionBinding, FunctionDependencies, AccumulatorStateFactory<T>> factoryGenerator;
        if (metadata.stateFactoryClass().isPresent()) {
            Constructor<?> constructor = getOnlyConstructor(metadata.stateFactoryClass().get());
            List<ImplementationDependency> dependencies = parseImplementationDependencies(typeParameterMapping, constructor);
            factoryGenerator = new TypedFactory<>(constructor, dependencies);
            allDependencies.addAll(dependencies);
        }
        else {
            factoryGenerator = (functionBinding, functionDependencies) -> generateStateFactory(stateClass);
        }

        return new AccumulatorStateDetails<>(
                stateClass,
                declaredTypeParameters,
                serializedType,
                serializerGenerator,
                factoryGenerator,
                allDependencies);
    }

    private static Constructor<?> getOnlyConstructor(Class<?> clazz)
    {
        Constructor<?>[] constructors = clazz.getConstructors();
        checkArgument(constructors.length == 1, "Expected %s to have only one public constructor", clazz.getSimpleName());
        return constructors[0];
    }

    private static AccumulatorStateDetails<InOut> getInOutAccumulatorStateDetails(String typeVariable)
    {
        TypeSignature serializedType = parseTypeSignature(typeVariable, ImmutableSet.of());
        return new AccumulatorStateDetails<>(
                InOut.class,
                ImmutableList.of(typeVariable),
                serializedType,
                (functionBinding, functionDependencies) -> new InOutStateSerializer(functionBinding.getTypeVariable(typeVariable)),
                (functionBinding, functionDependencies) -> generateInOutStateFactory(functionBinding.getTypeVariable(typeVariable)),
                ImmutableList.of(new TypeImplementationDependency(parseTypeSignature(typeVariable, ImmutableSet.of()))));
    }

    private static TypeSignatureMapping getTypeParameterMapping(Class<?> stateClass, List<String> declaredTypeParameters, StateMetadata metadata)
    {
        List<String> expectedTypeParameters = metadata.typeParameters();
        if (expectedTypeParameters.isEmpty()) {
            return new TypeSignatureMapping(ImmutableMap.of());
        }
        checkArgument(declaredTypeParameters.size() == expectedTypeParameters.size(), "AggregationState %s requires %s type parameters", stateClass, expectedTypeParameters.size());

        ImmutableMap.Builder<String, String> mapping = ImmutableMap.builder();
        for (int parameterIndex = 0; parameterIndex < declaredTypeParameters.size(); parameterIndex++) {
            String declaredTypeParameter = declaredTypeParameters.get(parameterIndex);
            String expectedTypeParameter = expectedTypeParameters.get(parameterIndex);
            mapping.put(expectedTypeParameter, declaredTypeParameter);
        }
        return new TypeSignatureMapping(mapping.buildOrThrow());
    }

    private static List<ImplementationDependency> parseImplementationDependencies(TypeSignatureMapping typeSignatureMapping, Executable inputFunction)
    {
        ImmutableList.Builder<ImplementationDependency> builder = ImmutableList.builder();

        for (Parameter parameter : inputFunction.getParameters()) {
            getImplementationDependencyAnnotation(parameter).ifPresent(annotation -> {
                // check if only declared typeParameters and literalParameters are used
                validateImplementationDependencyAnnotation(
                        inputFunction,
                        annotation,
                        typeSignatureMapping.getTypeParameters(),
                        ImmutableSet.of());
                ImplementationDependency dependency = createDependency(annotation, ImmutableSet.of(), parameter.getType());
                dependency = typeSignatureMapping.mapTypes(dependency);
                builder.add(dependency);
            });
        }
        return builder.build();
    }

    private record StateMetadata(
            Optional<Class<? extends AccumulatorStateSerializer<?>>> stateSerializerClass,
            Optional<Class<? extends AccumulatorStateFactory<?>>> stateFactoryClass,
            List<String> typeParameters,
            Optional<String> serializedType)
    {
        public static StateMetadata create(AccumulatorStateMetadata metadata)
        {
            if (metadata == null) {
                return new StateMetadata(Optional.empty(), Optional.empty(), ImmutableList.of(), Optional.empty());
            }
            else {
                //noinspection unchecked
                return new StateMetadata(
                        Optional.of(metadata.stateSerializerClass())
                                .filter(not(AccumulatorStateSerializer.class::equals))
                                .map(type -> (Class<? extends AccumulatorStateSerializer<?>>) type),
                        Optional.of(metadata.stateFactoryClass())
                                .filter(not(AccumulatorStateFactory.class::equals))
                                .map(type -> (Class<? extends AccumulatorStateFactory<?>>) type),
                        ImmutableList.copyOf(metadata.typeParameters()),
                        Optional.of(metadata.serializedType())
                                .filter(not(String::isEmpty)));
            }
        }
    }

    public static class AccumulatorStateDetails<T extends AccumulatorState>
    {
        private final Class<T> stateClass;
        private final List<String> typeParameters;
        private final TypeSignature serializedType;
        private final BiFunction<FunctionBinding, FunctionDependencies, AccumulatorStateSerializer<T>> serializerGenerator;
        private final BiFunction<FunctionBinding, FunctionDependencies, AccumulatorStateFactory<T>> factoryGenerator;
        private final List<ImplementationDependency> dependencies;

        public AccumulatorStateDetails(
                Class<T> stateClass,
                List<String> typeParameters,
                TypeSignature serializedType,
                BiFunction<FunctionBinding, FunctionDependencies, AccumulatorStateSerializer<T>> serializerGenerator,
                BiFunction<FunctionBinding, FunctionDependencies, AccumulatorStateFactory<T>> factoryGenerator,
                List<ImplementationDependency> dependencies)
        {
            this.stateClass = requireNonNull(stateClass, "stateClass is null");
            this.typeParameters = ImmutableList.copyOf(requireNonNull(typeParameters, "typeParameters is null"));
            this.serializedType = requireNonNull(serializedType, "serializedType is null");
            this.serializerGenerator = requireNonNull(serializerGenerator, "serializerGenerator is null");
            this.factoryGenerator = requireNonNull(factoryGenerator, "factoryGenerator is null");
            this.dependencies = ImmutableList.copyOf(requireNonNull(dependencies, "dependencies is null"));
        }

        public Class<T> getStateClass()
        {
            return stateClass;
        }

        public TypeSignature getSerializedType()
        {
            return serializedType;
        }

        public List<ImplementationDependency> getDependencies()
        {
            return dependencies;
        }

        public AccumulatorStateDescriptor<T> createAccumulatorStateDescriptor(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
        {
            return AccumulatorStateDescriptor.builder(stateClass)
                    .serializer(serializerGenerator.apply(functionBinding, functionDependencies))
                    .factory(factoryGenerator.apply(functionBinding, functionDependencies))
                    .build();
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
            AccumulatorStateDetails<?> that = (AccumulatorStateDetails<?>) o;
            return Objects.equals(stateClass, that.stateClass) && Objects.equals(typeParameters, that.typeParameters);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(stateClass, typeParameters);
        }
    }

    private static class TypedFactory<T>
            implements BiFunction<FunctionBinding, FunctionDependencies, T>
    {
        private final Constructor<T> constructor;
        private final List<ImplementationDependency> dependencies;

        public TypedFactory(Constructor<?> constructor, List<ImplementationDependency> dependencies)
        {
            //noinspection unchecked
            this.constructor = (Constructor<T>) constructor;
            this.dependencies = dependencies;
        }

        @Override
        public T apply(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
        {
            List<Object> values = dependencies.stream()
                    .map(dependency -> dependency.resolve(functionBinding, functionDependencies))
                    .collect(toImmutableList());

            try {
                return constructor.newInstance(values.toArray());
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
