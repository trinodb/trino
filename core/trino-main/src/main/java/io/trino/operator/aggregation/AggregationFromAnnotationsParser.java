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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MoreCollectors;
import io.trino.metadata.Signature;
import io.trino.operator.ParametricImplementationsGroup;
import io.trino.operator.annotations.FunctionsParserHelper;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.RemoveInputFunction;
import io.trino.spi.type.TypeSignature;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.ParametricFunctionHelpers.signatureWithName;
import static io.trino.operator.aggregation.AggregationImplementation.Parser.parseImplementation;
import static io.trino.operator.annotations.FunctionsParserHelper.parseDescription;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class AggregationFromAnnotationsParser
{
    private AggregationFromAnnotationsParser() {}

    // This function should only be used for function matching for testing purposes.
    // General purpose function matching is done through FunctionRegistry.
    @VisibleForTesting
    public static ParametricAggregation parseFunctionDefinitionWithTypesConstraint(Class<?> clazz, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(argumentTypes, "argumentTypes is null");
        for (ParametricAggregation aggregation : parseFunctionDefinitions(clazz)) {
            Signature signature = aggregation.getFunctionMetadata().getSignature();
            if (signature.getReturnType().equals(returnType) &&
                    signature.getArgumentTypes().equals(argumentTypes)) {
                return aggregation;
            }
        }
        throw new IllegalArgumentException(format("No method with return type %s and arguments %s", returnType, argumentTypes));
    }

    public static List<ParametricAggregation> parseFunctionDefinitions(Class<?> aggregationDefinition)
    {
        AggregationFunction aggregationAnnotation = aggregationDefinition.getAnnotation(AggregationFunction.class);
        requireNonNull(aggregationAnnotation, "aggregationAnnotation is null");

        ImmutableList.Builder<ParametricAggregation> functions = ImmutableList.builder();

        // There must be a single state class and combine function
        Class<?> stateClass = getStateClass(aggregationDefinition);
        Method combineFunction = getCombineFunction(aggregationDefinition, stateClass);

        // Each output function defines a new aggregation function
        for (Method outputFunction : getOutputFunctions(aggregationDefinition, stateClass)) {
            AggregationHeader header = parseHeader(aggregationDefinition, outputFunction);

            // Input functions can have either an exact signature, or generic/calculate signature
            List<AggregationImplementation> exactImplementations = new ArrayList<>();
            List<AggregationImplementation> nonExactImplementations = new ArrayList<>();
            for (Method inputFunction : getInputFunctions(aggregationDefinition, stateClass)) {
                Optional<Method> removeInputFunction = getRemoveInputFunction(aggregationDefinition, inputFunction);
                AggregationImplementation implementation = parseImplementation(aggregationDefinition, header.getName(), inputFunction, removeInputFunction, outputFunction, combineFunction);
                if (isGenericOrCalculated(implementation.getSignature())) {
                    exactImplementations.add(implementation);
                }
                else {
                    nonExactImplementations.add(implementation);
                }
            }

            // register a set functions for the canonical name, and each alias
            functions.addAll(buildFunctions(header.getName(), header, stateClass, exactImplementations, nonExactImplementations));
            for (String alias : getAliases(aggregationDefinition.getAnnotation(AggregationFunction.class), outputFunction)) {
                functions.addAll(buildFunctions(alias, header, stateClass, exactImplementations, nonExactImplementations));
            }
        }

        return functions.build();
    }

    private static List<ParametricAggregation> buildFunctions(
            String name,
            AggregationHeader header,
            Class<?> stateClass,
            List<AggregationImplementation> exactImplementations,
            List<AggregationImplementation> nonExactImplementations)
    {
        ImmutableList.Builder<ParametricAggregation> functions = ImmutableList.builder();

        // create a separate function for each exact implementation
        for (AggregationImplementation exactImplementation : exactImplementations) {
            functions.add(new ParametricAggregation(
                    signatureWithName(name, exactImplementation.getSignature()),
                    header,
                    stateClass,
                    ParametricImplementationsGroup.of(exactImplementation).withAlias(name)));
        }

        // if there are non-exact functions, create a single generic/calculated function using these implementations
        if (!nonExactImplementations.isEmpty()) {
            ParametricImplementationsGroup.Builder<AggregationImplementation> implementationsBuilder = ParametricImplementationsGroup.builder();
            nonExactImplementations.forEach(implementationsBuilder::addImplementation);
            ParametricImplementationsGroup<AggregationImplementation> implementations = implementationsBuilder.build();
            functions.add(new ParametricAggregation(
                    signatureWithName(name, implementations.getSignature()),
                    header,
                    stateClass,
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

    private static Method getCombineFunction(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> combineFunctions = FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, CombineFunction.class).stream()
                .filter(method -> method.getParameterTypes()[AggregationImplementation.Parser.findAggregationStateParamId(method, 0)] == stateClass)
                .filter(method -> method.getParameterTypes()[AggregationImplementation.Parser.findAggregationStateParamId(method, 1)] == stateClass)
                .collect(toImmutableList());

        checkArgument(combineFunctions.size() == 1, "There must be exactly one @CombineFunction in class %s for the @AggregationState %s", clazz.toGenericString(), stateClass.toGenericString());
        return getOnlyElement(combineFunctions);
    }

    private static List<Method> getOutputFunctions(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> outputFunctions = FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, OutputFunction.class).stream()
                .filter(method -> method.getParameterTypes()[AggregationImplementation.Parser.findAggregationStateParamId(method)] == stateClass)
                .collect(toImmutableList());

        checkArgument(!outputFunctions.isEmpty(), "Aggregation has no output functions");
        return outputFunctions;
    }

    private static List<Method> getInputFunctions(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> inputFunctions = FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class).stream()
                .filter(method -> (method.getParameterTypes()[AggregationImplementation.Parser.findAggregationStateParamId(method)] == stateClass))
                .collect(toImmutableList());

        checkArgument(!inputFunctions.isEmpty(), "Aggregation has no input functions");
        return inputFunctions;
    }

    private static Optional<Method> getRemoveInputFunction(Class<?> clazz, Method inputFunction)
    {
        // Only include methods which take the same parameters as the corresponding input function
        return FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, RemoveInputFunction.class).stream()
                .filter(method -> Arrays.equals(method.getParameterTypes(), inputFunction.getParameterTypes()))
                .filter(method -> Arrays.deepEquals(method.getParameterAnnotations(), inputFunction.getParameterAnnotations()))
                .collect(MoreCollectors.toOptional());
    }

    private static Class<?> getStateClass(Class<?> clazz)
    {
        ImmutableSet.Builder<Class<?>> builder = ImmutableSet.builder();
        for (Method inputFunction : FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class)) {
            checkArgument(inputFunction.getParameterTypes().length > 0, "Input function has no parameters");
            Class<?> stateClass = AggregationImplementation.Parser.findAggregationStateParamType(inputFunction);

            checkArgument(AccumulatorState.class.isAssignableFrom(stateClass), "stateClass is not a subclass of AccumulatorState");
            builder.add(stateClass);
        }
        ImmutableSet<Class<?>> stateClasses = builder.build();
        checkArgument(!stateClasses.isEmpty(), "No input functions found");
        checkArgument(stateClasses.size() == 1, "There must be exactly one @AccumulatorState in class %s", clazz.toGenericString());

        return getOnlyElement(stateClasses);
    }
}
