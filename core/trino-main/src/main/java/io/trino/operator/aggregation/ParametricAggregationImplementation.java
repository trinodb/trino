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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.trino.operator.ParametricImplementation;
import io.trino.operator.aggregation.AggregationFromAnnotationsParser.AccumulatorStateDetails;
import io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind;
import io.trino.operator.annotations.ImplementationDependency;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.Signature;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.TypeSignature;
import io.trino.util.Reflection;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.BLOCK_INDEX;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.NULLABLE_BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.STATE;
import static io.trino.operator.annotations.FunctionsParserHelper.containsAnnotation;
import static io.trino.operator.annotations.FunctionsParserHelper.createTypeVariableConstraints;
import static io.trino.operator.annotations.FunctionsParserHelper.parseLiteralParameters;
import static io.trino.operator.annotations.FunctionsParserHelper.parseLongVariableConstraints;
import static io.trino.operator.annotations.ImplementationDependency.Factory.createDependency;
import static io.trino.operator.annotations.ImplementationDependency.getImplementationDependencyAnnotation;
import static io.trino.operator.annotations.ImplementationDependency.isImplementationDependencyAnnotation;
import static io.trino.operator.annotations.ImplementationDependency.validateImplementationDependencyAnnotation;
import static io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;
import static io.trino.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class ParametricAggregationImplementation
        implements ParametricImplementation
{
    public static class AggregateNativeContainerType
    {
        private final Class<?> javaType;
        private final boolean isBlockPosition;

        public AggregateNativeContainerType(Class<?> javaType, boolean isBlockPosition)
        {
            this.javaType = javaType;
            this.isBlockPosition = isBlockPosition;
        }

        public Class<?> getJavaType()
        {
            return javaType;
        }

        public boolean isBlockPosition()
        {
            return isBlockPosition;
        }
    }

    private final Signature signature;

    private final Class<?> definitionClass;
    private final MethodHandle inputFunction;
    private final Optional<MethodHandle> removeInputFunction;
    private final MethodHandle outputFunction;
    private final Optional<MethodHandle> combineFunction;
    private final List<AggregateNativeContainerType> argumentNativeContainerTypes;
    private final List<ImplementationDependency> inputDependencies;
    private final List<ImplementationDependency> removeInputDependencies;
    private final List<ImplementationDependency> combineDependencies;
    private final List<ImplementationDependency> outputDependencies;
    private final List<AggregationParameterKind> inputParameterKinds;
    private final FunctionNullability functionNullability;

    public ParametricAggregationImplementation(
            Signature signature,
            Class<?> definitionClass,
            MethodHandle inputFunction,
            Optional<MethodHandle> removeInputFunction,
            MethodHandle outputFunction,
            Optional<MethodHandle> combineFunction,
            List<AggregateNativeContainerType> argumentNativeContainerTypes,
            List<ImplementationDependency> inputDependencies,
            List<ImplementationDependency> removeInputDependencies,
            List<ImplementationDependency> combineDependencies,
            List<ImplementationDependency> outputDependencies,
            List<AggregationParameterKind> inputParameterKinds)
    {
        this.signature = requireNonNull(signature, "signature cannot be null");
        this.definitionClass = requireNonNull(definitionClass, "definition class cannot be null");
        this.inputFunction = requireNonNull(inputFunction, "inputFunction cannot be null");
        this.removeInputFunction = requireNonNull(removeInputFunction, "removeInputFunction cannot be null");
        this.outputFunction = requireNonNull(outputFunction, "outputFunction cannot be null");
        this.combineFunction = requireNonNull(combineFunction, "combineFunction cannot be null");
        this.argumentNativeContainerTypes = requireNonNull(argumentNativeContainerTypes, "argumentNativeContainerTypes cannot be null");
        this.inputDependencies = requireNonNull(inputDependencies, "inputDependencies cannot be null");
        this.removeInputDependencies = requireNonNull(removeInputDependencies, "removeInputDependencies cannot be null");
        this.outputDependencies = requireNonNull(outputDependencies, "outputDependencies cannot be null");
        this.combineDependencies = requireNonNull(combineDependencies, "combineDependencies cannot be null");
        this.inputParameterKinds = requireNonNull(inputParameterKinds, "inputParameterKinds cannot be null");
        this.functionNullability = new FunctionNullability(
                true,
                inputParameterKinds.stream()
                        .filter(parameterType -> parameterType != BLOCK_INDEX && parameterType != STATE)
                        .map(NULLABLE_BLOCK_INPUT_CHANNEL::equals)
                        .collect(toImmutableList()));
    }

    @Override
    public Signature getSignature()
    {
        return signature;
    }

    @Override
    public boolean hasSpecializedTypeParameters()
    {
        return false;
    }

    @Override
    public FunctionNullability getFunctionNullability()
    {
        return functionNullability;
    }

    public Class<?> getDefinitionClass()
    {
        return definitionClass;
    }

    public MethodHandle getInputFunction()
    {
        return inputFunction;
    }

    public Optional<MethodHandle> getRemoveInputFunction()
    {
        return removeInputFunction;
    }

    public MethodHandle getOutputFunction()
    {
        return outputFunction;
    }

    public Optional<MethodHandle> getCombineFunction()
    {
        return combineFunction;
    }

    public List<ImplementationDependency> getInputDependencies()
    {
        return inputDependencies;
    }

    public List<ImplementationDependency> getRemoveInputDependencies()
    {
        return removeInputDependencies;
    }

    public List<ImplementationDependency> getOutputDependencies()
    {
        return outputDependencies;
    }

    public List<ImplementationDependency> getCombineDependencies()
    {
        return combineDependencies;
    }

    public List<AggregationParameterKind> getInputParameterKinds()
    {
        return inputParameterKinds;
    }

    public boolean areTypesAssignable(BoundSignature boundSignature)
    {
        checkState(argumentNativeContainerTypes.size() == boundSignature.getArgumentTypes().size(), "Number of argument assigned to AggregationImplementation is different than number parsed from annotations.");

        // TODO specialized functions variants support is missing here
        for (int i = 0; i < boundSignature.getArgumentTypes().size(); i++) {
            Class<?> argumentType = boundSignature.getArgumentTypes().get(i).getJavaType();
            Class<?> methodDeclaredType = argumentNativeContainerTypes.get(i).getJavaType();
            boolean isCurrentBlockPosition = argumentNativeContainerTypes.get(i).isBlockPosition();

            // block and position works for any type, but if block is annotated with SqlType nativeContainerType, then only types with the
            // specified container type match
            if (isCurrentBlockPosition && methodDeclaredType.isAssignableFrom(Block.class)) {
                continue;
            }
            if (methodDeclaredType.isAssignableFrom(argumentType)) {
                continue;
            }
            return false;
        }

        return true;
    }

    @Override
    public ParametricImplementation withAlias(String alias)
    {
        return new ParametricAggregationImplementation(
                signature.withName(alias),
                definitionClass,
                inputFunction,
                removeInputFunction,
                outputFunction,
                combineFunction,
                argumentNativeContainerTypes,
                inputDependencies,
                removeInputDependencies,
                combineDependencies,
                outputDependencies,
                inputParameterKinds);
    }

    public static final class Parser
    {
        private final Class<?> aggregationDefinition;
        private final MethodHandle inputHandle;
        private final Optional<MethodHandle> removeInputHandle;
        private final MethodHandle outputHandle;
        private final Optional<MethodHandle> combineHandle;
        private final List<AggregateNativeContainerType> argumentNativeContainerTypes;
        private final List<ImplementationDependency> inputDependencies;
        private final List<ImplementationDependency> removeInputDependencies;
        private final List<ImplementationDependency> combineDependencies;
        private final List<ImplementationDependency> outputDependencies;
        private final List<AggregationParameterKind> inputParameterKinds;

        private final Signature.Builder signatureBuilder = Signature.builder();

        private final Set<String> literalParameters;
        private final List<TypeParameter> typeParameters;

        private Parser(
                Class<?> aggregationDefinition,
                String name,
                List<AccumulatorStateDetails<?>> stateDetails,
                Method inputFunction,
                Optional<Method> removeInputFunction,
                Method outputFunction,
                Optional<Method> combineFunction)
        {
            // rewrite data passed directly
            this.aggregationDefinition = aggregationDefinition;
            signatureBuilder.name(name);

            // parse declared literal and type parameters
            // it is required to declare all literal and type parameters in input function
            literalParameters = parseLiteralParameters(inputFunction);
            typeParameters = Arrays.asList(inputFunction.getAnnotationsByType(TypeParameter.class));

            // parse dependencies
            inputDependencies = parseImplementationDependencies(inputFunction);
            removeInputDependencies = removeInputFunction.map(this::parseImplementationDependencies).orElse(ImmutableList.of());
            outputDependencies = parseImplementationDependencies(outputFunction);
            combineDependencies = combineFunction.map(this::parseImplementationDependencies).orElse(ImmutableList.of());

            // parse input parameters
            inputParameterKinds = parseInputParameterKinds(inputFunction);

            // parse constraints
            parseLongVariableConstraints(inputFunction, signatureBuilder);
            List<ImplementationDependency> allDependencies =
                    Stream.of(
                            stateDetails.stream().map(AccumulatorStateDetails::getDependencies).flatMap(Collection::stream),
                            inputDependencies.stream(),
                            removeInputDependencies.stream(),
                            outputDependencies.stream(),
                            combineDependencies.stream())
                            .reduce(Stream::concat)
                            .orElseGet(Stream::empty)
                            .collect(toImmutableList());
            createTypeVariableConstraints(typeParameters, allDependencies)
                    .forEach(signatureBuilder::typeVariableConstraint);

            // parse native types of arguments
            argumentNativeContainerTypes = parseSignatureArgumentsTypes(inputFunction);

            // determine TypeSignatures of function declaration
            signatureBuilder.argumentTypes(getInputTypesSignatures(inputFunction));
            signatureBuilder.returnType(parseTypeSignature(outputFunction.getAnnotation(OutputFunction.class).value(), literalParameters));

            inputHandle = methodHandle(inputFunction);
            removeInputHandle = removeInputFunction.map(Reflection::methodHandle);
            combineHandle = combineFunction.map(Reflection::methodHandle);
            outputHandle = methodHandle(outputFunction);
        }

        private ParametricAggregationImplementation get()
        {
            return new ParametricAggregationImplementation(
                    signatureBuilder.build(),
                    aggregationDefinition,
                    inputHandle,
                    removeInputHandle,
                    outputHandle,
                    combineHandle,
                    argumentNativeContainerTypes,
                    inputDependencies,
                    removeInputDependencies,
                    combineDependencies,
                    outputDependencies,
                    inputParameterKinds);
        }

        public static ParametricAggregationImplementation parseImplementation(
                Class<?> aggregationDefinition,
                String name,
                List<AccumulatorStateDetails<?>> stateDetails,
                Method inputFunction,
                Optional<Method> removeInputFunction,
                Method outputFunction,
                Optional<Method> combineFunction)
        {
            return new Parser(aggregationDefinition, name, stateDetails, inputFunction, removeInputFunction, outputFunction, combineFunction).get();
        }

        private static List<AggregationParameterKind> parseInputParameterKinds(Method method)
        {
            ImmutableList.Builder<AggregationParameterKind> builder = ImmutableList.builder();

            Annotation[][] annotations = method.getParameterAnnotations();
            String methodName = method.getDeclaringClass() + "." + method.getName();

            checkArgument(method.getParameterCount() > 0, "At least @AggregationState argument is required for each of aggregation functions.");

            int i = 0;
            if (annotations[0].length == 0) {
                // Backward compatibility - first argument without annotations is interpreted as State argument
                builder.add(STATE);
                i++;
            }

            for (; i < annotations.length; ++i) {
                Annotation baseTypeAnnotation = baseTypeAnnotation(annotations[i], methodName);
                if (isImplementationDependencyAnnotation(baseTypeAnnotation)) {
                    // Implementation dependencies are bound in specializing phase.
                    // For that reason there are omitted in parameter metadata, as they
                    // are no longer visible while processing aggregations.
                }
                else if (baseTypeAnnotation instanceof AggregationState) {
                    builder.add(STATE);
                }
                else if (baseTypeAnnotation instanceof SqlType) {
                    boolean isParameterBlock = isParameterBlock(annotations[i]);
                    boolean isParameterNullable = isParameterNullable(annotations[i]);
                    builder.add(getInputParameterKind(isParameterNullable, isParameterBlock, methodName));
                }
                else if (baseTypeAnnotation instanceof BlockIndex) {
                    builder.add(BLOCK_INDEX);
                }
                else {
                    throw new VerifyException("Unhandled annotation: " + baseTypeAnnotation);
                }
            }
            return builder.build();
        }

        static AggregationParameterKind getInputParameterKind(boolean isNullable, boolean isBlock, String methodName)
        {
            if (isBlock) {
                if (isNullable) {
                    return NULLABLE_BLOCK_INPUT_CHANNEL;
                }
                return BLOCK_INPUT_CHANNEL;
            }
            if (isNullable) {
                throw new IllegalArgumentException(methodName + " contains a parameter with @NullablePosition that is not @BlockPosition");
            }
            return INPUT_CHANNEL;
        }

        private static Annotation baseTypeAnnotation(Annotation[] annotations, String methodName)
        {
            List<Annotation> baseTypes = Arrays.asList(annotations).stream()
                    .filter(annotation -> isAggregationMetaAnnotation(annotation) || annotation instanceof SqlType)
                    .collect(toImmutableList());

            checkArgument(baseTypes.size() == 1, "Parameter of %s must have exactly one of @SqlType, @BlockIndex", methodName);

            boolean nullable = isParameterNullable(annotations);
            boolean isBlock = isParameterBlock(annotations);

            Annotation annotation = baseTypes.get(0);
            checkArgument((!isBlock && !nullable) || (annotation instanceof SqlType),
                    "%s contains a parameter with @BlockPosition and/or @NullablePosition that is not @SqlType", methodName);

            return annotation;
        }

        public static List<AggregateNativeContainerType> parseSignatureArgumentsTypes(Method inputFunction)
        {
            ImmutableList.Builder<AggregateNativeContainerType> builder = ImmutableList.builder();

            for (int i = 0; i < inputFunction.getParameterCount(); i++) {
                Class<?> parameterType = inputFunction.getParameterTypes()[i];
                Annotation[] annotations = inputFunction.getParameterAnnotations()[i];

                // Skip injected parameters
                if (parameterType == ConnectorSession.class) {
                    continue;
                }

                if (containsAnnotation(annotations, Parser::isAggregationMetaAnnotation)) {
                    continue;
                }

                Optional<Class<?>> nativeContainerType = Arrays.stream(annotations)
                        .filter(SqlType.class::isInstance)
                        .map(SqlType.class::cast)
                        .findFirst()
                        .map(SqlType::nativeContainerType);
                // Note: this cannot be done as a chain due to strange generic type mismatches
                if (nativeContainerType.isPresent() && !nativeContainerType.get().equals(Object.class)) {
                    parameterType = nativeContainerType.get();
                }
                builder.add(new AggregateNativeContainerType(parameterType, isParameterBlock(annotations)));
            }

            return builder.build();
        }

        public List<ImplementationDependency> parseImplementationDependencies(Method inputFunction)
        {
            ImmutableList.Builder<ImplementationDependency> builder = ImmutableList.builder();

            for (Parameter parameter : inputFunction.getParameters()) {
                Class<?> parameterType = parameter.getType();

                // Skip injected parameters
                if (parameterType == ConnectorSession.class) {
                    continue;
                }

                getImplementationDependencyAnnotation(parameter).ifPresent(annotation -> {
                    // check if only declared typeParameters and literalParameters are used
                    validateImplementationDependencyAnnotation(
                            inputFunction,
                            annotation,
                            typeParameters.stream()
                                    .map(TypeParameter::value)
                                    .collect(toImmutableSet()),
                            literalParameters);
                    builder.add(createDependency(annotation, literalParameters, parameter.getType()));
                });
            }
            return builder.build();
        }

        public static boolean isParameterNullable(Annotation[] annotations)
        {
            return containsAnnotation(annotations, annotation -> annotation instanceof NullablePosition);
        }

        public static boolean isParameterBlock(Annotation[] annotations)
        {
            return containsAnnotation(annotations, annotation -> annotation instanceof BlockPosition);
        }

        public List<TypeSignature> getInputTypesSignatures(Method inputFunction)
        {
            ImmutableList.Builder<TypeSignature> builder = ImmutableList.builder();

            Annotation[][] parameterAnnotations = inputFunction.getParameterAnnotations();
            for (Annotation[] annotations : parameterAnnotations) {
                for (Annotation annotation : annotations) {
                    if (annotation instanceof SqlType) {
                        String typeName = ((SqlType) annotation).value();
                        builder.add(parseTypeSignature(typeName, literalParameters));
                    }
                }
            }

            return builder.build();
        }

        public static int findAggregationStateParamId(Method method)
        {
            return findAggregationStateParamId(method, 0);
        }

        public static int findAggregationStateParamId(Method method, int id)
        {
            int currentParamId = 0;
            int found = 0;
            for (Annotation[] annotations : method.getParameterAnnotations()) {
                for (Annotation annotation : annotations) {
                    if (annotation instanceof AggregationState) {
                        if (found++ == id) {
                            return currentParamId;
                        }
                    }
                }
                currentParamId++;
            }

            // backward compatibility @AggregationState annotation didn't exists before
            // some third party aggregates may assume that State will be id-th parameter
            return id;
        }

        private static boolean isAggregationMetaAnnotation(Annotation annotation)
        {
            return annotation instanceof BlockIndex || annotation instanceof AggregationState || isImplementationDependencyAnnotation(annotation);
        }
    }
}
