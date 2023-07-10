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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import io.trino.metadata.FunctionBinding;
import io.trino.operator.ParametricImplementation;
import io.trino.operator.annotations.FunctionsParserHelper;
import io.trino.operator.annotations.ImplementationDependency;
import io.trino.operator.scalar.ChoicesSpecializedSqlScalarFunction;
import io.trino.operator.scalar.ChoicesSpecializedSqlScalarFunction.ScalarImplementationChoice;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.InOut;
import io.trino.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.trino.spi.function.InvocationConvention.InvocationReturnConvention;
import io.trino.spi.function.IsNull;
import io.trino.spi.function.Signature;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.type.FunctionType;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.ImmutableSortedSet.toImmutableSortedSet;
import static io.trino.operator.ParametricFunctionHelpers.bindDependencies;
import static io.trino.operator.annotations.FunctionsParserHelper.containsImplementationDependencyAnnotation;
import static io.trino.operator.annotations.FunctionsParserHelper.containsLegacyNullable;
import static io.trino.operator.annotations.FunctionsParserHelper.createTypeVariableConstraints;
import static io.trino.operator.annotations.FunctionsParserHelper.getDeclaredSpecializedTypeParameters;
import static io.trino.operator.annotations.FunctionsParserHelper.parseLiteralParameters;
import static io.trino.operator.annotations.FunctionsParserHelper.parseLongVariableConstraints;
import static io.trino.operator.annotations.ImplementationDependency.Factory.createDependency;
import static io.trino.operator.annotations.ImplementationDependency.checkTypeParameters;
import static io.trino.operator.annotations.ImplementationDependency.getImplementationDependencyAnnotation;
import static io.trino.operator.annotations.ImplementationDependency.validateImplementationDependencyAnnotation;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.IN_OUT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.Reflection.constructorMethodHandle;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.Objects.requireNonNull;

public class ParametricScalarImplementation
        implements ParametricImplementation
{
    private final Signature signature;
    private final List<Optional<Class<?>>> argumentNativeContainerTypes; // argument native container type is Optional.empty() for function type
    private final Map<String, Class<?>> specializedTypeParameters;
    private final Class<?> returnNativeContainerType;
    private final List<ParametricScalarImplementationChoice> choices;
    private final FunctionNullability functionNullability;

    private ParametricScalarImplementation(
            Signature signature,
            List<Optional<Class<?>>> argumentNativeContainerTypes,
            Map<String, Class<?>> specializedTypeParameters,
            List<ParametricScalarImplementationChoice> choices,
            Class<?> returnContainerType)
    {
        this.signature = requireNonNull(signature, "signature is null");
        this.argumentNativeContainerTypes = ImmutableList.copyOf(requireNonNull(argumentNativeContainerTypes, "argumentNativeContainerTypes is null"));
        this.specializedTypeParameters = ImmutableMap.copyOf(requireNonNull(specializedTypeParameters, "specializedTypeParameters is null"));
        this.choices = requireNonNull(choices, "choices is null");
        checkArgument(!choices.isEmpty(), "choices is empty");
        this.returnNativeContainerType = requireNonNull(returnContainerType, "returnContainerType is null");

        for (Class<?> specializedJavaType : specializedTypeParameters.values()) {
            checkArgument(!Primitives.isWrapperType(specializedJavaType), "specializedTypeParameter must not contain boxed primitive types");
        }

        ParametricScalarImplementationChoice defaultChoice = choices.get(0);
        boolean hasBlockPositionArgument = defaultChoice.getArgumentConventions().stream()
                .noneMatch(argumentConvention -> BLOCK_POSITION == argumentConvention || BLOCK_POSITION_NOT_NULL == argumentConvention);
        checkArgument(hasBlockPositionArgument, "default choice can not use the block and position calling convention: %s", signature);

        boolean returnNullability = defaultChoice.getReturnConvention().isNullable();
        checkArgument(choices.stream().allMatch(choice -> choice.getReturnConvention().isNullable() == returnNullability), "all choices must have the same nullable flag: %s", signature);

        List<Boolean> argumentNullability = defaultChoice.getArgumentConventions().stream()
                .map(InvocationArgumentConvention::isNullable)
                .collect(toImmutableList());
        functionNullability = new FunctionNullability(returnNullability, argumentNullability);

        checkArgument(
                choices.stream().allMatch(choice -> matches(argumentNullability, choice.getArgumentConventions())),
                "all choices must have the same nullable parameter flags: %s",
                signature);
    }

    @Override
    public FunctionNullability getFunctionNullability()
    {
        return functionNullability;
    }

    public Optional<SpecializedSqlScalarFunction> specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        List<ScalarImplementationChoice> implementationChoices = new ArrayList<>();
        for (Map.Entry<String, Class<?>> entry : specializedTypeParameters.entrySet()) {
            if (!entry.getValue().isAssignableFrom(functionBinding.getTypeVariable(entry.getKey()).getJavaType())) {
                return Optional.empty();
            }
        }

        BoundSignature boundSignature = functionBinding.getBoundSignature();
        if (returnNativeContainerType != Object.class && returnNativeContainerType != boundSignature.getReturnType().getJavaType()) {
            return Optional.empty();
        }

        for (int i = 0; i < boundSignature.getArgumentTypes().size(); i++) {
            if (boundSignature.getArgumentTypes().get(i) instanceof FunctionType) {
                if (argumentNativeContainerTypes.get(i).isPresent()) {
                    return Optional.empty();
                }
            }
            else {
                if (argumentNativeContainerTypes.get(i).isEmpty()) {
                    return Optional.empty();
                }

                Class<?> argumentType = boundSignature.getArgumentTypes().get(i).getJavaType();
                Class<?> argumentNativeContainerType = argumentNativeContainerTypes.get(i).get();
                if (argumentNativeContainerType != Object.class && argumentNativeContainerType != argumentType) {
                    return Optional.empty();
                }
            }
        }

        for (ParametricScalarImplementationChoice choice : choices) {
            MethodHandle boundMethodHandle = bindDependencies(choice.getMethodHandle(), choice.getDependencies(), functionBinding, functionDependencies);
            Optional<MethodHandle> boundConstructor = choice.getConstructor().map(constructor -> {
                MethodHandle result = bindDependencies(constructor, choice.getConstructorDependencies(), functionBinding, functionDependencies);
                checkCondition(
                        result.type().parameterList().isEmpty(),
                        FUNCTION_IMPLEMENTATION_ERROR,
                        "All parameters of a constructor in a function definition class must be Dependencies. Signature: %s",
                        boundSignature);
                return result;
            });

            implementationChoices.add(new ScalarImplementationChoice(
                    choice.getReturnConvention(),
                    choice.getArgumentConventions(),
                    choice.getLambdaInterfaces(),
                    boundMethodHandle.asType(javaMethodType(choice, boundSignature)),
                    boundConstructor));
        }
        return Optional.of(new ChoicesSpecializedSqlScalarFunction(boundSignature, implementationChoices));
    }

    @Override
    public boolean hasSpecializedTypeParameters()
    {
        return !specializedTypeParameters.isEmpty();
    }

    @Override
    public Signature getSignature()
    {
        return signature;
    }

    @VisibleForTesting
    public List<ParametricScalarImplementationChoice> getChoices()
    {
        return choices;
    }

    @Override
    public ParametricScalarImplementation withAlias(String alias)
    {
        return new ParametricScalarImplementation(
                signature.withName(alias),
                argumentNativeContainerTypes,
                specializedTypeParameters,
                choices,
                returnNativeContainerType);
    }

    private static MethodType javaMethodType(ParametricScalarImplementationChoice choice, BoundSignature signature)
    {
        // This method accomplishes two purposes:
        // * Assert that the method signature is as expected.
        //   This catches errors that would otherwise surface during bytecode generation and class loading.
        // * Adapt the method signature when necessary (for example, when the parameter type or return type is declared as Object).
        ImmutableList.Builder<Class<?>> methodHandleParameterTypes = ImmutableList.builder();
        if (choice.getConstructor().isPresent()) {
            methodHandleParameterTypes.add(Object.class);
        }
        if (choice.hasConnectorSession()) {
            methodHandleParameterTypes.add(ConnectorSession.class);
        }

        List<InvocationArgumentConvention> argumentConventions = choice.getArgumentConventions();
        int lambdaArgumentIndex = 0;
        for (int i = 0; i < argumentConventions.size(); i++) {
            InvocationArgumentConvention argumentConvention = argumentConventions.get(i);
            Type signatureType = signature.getArgumentTypes().get(i);
            switch (argumentConvention) {
                case NEVER_NULL:
                    methodHandleParameterTypes.add(signatureType.getJavaType());
                    break;
                case NULL_FLAG:
                    methodHandleParameterTypes.add(signatureType.getJavaType());
                    methodHandleParameterTypes.add(boolean.class);
                    break;
                case BOXED_NULLABLE:
                    methodHandleParameterTypes.add(Primitives.wrap(signatureType.getJavaType()));
                    break;
                case BLOCK_POSITION_NOT_NULL:
                case BLOCK_POSITION:
                    methodHandleParameterTypes.add(Block.class);
                    methodHandleParameterTypes.add(int.class);
                    break;
                case IN_OUT:
                    methodHandleParameterTypes.add(InOut.class);
                    break;
                case FUNCTION:
                    methodHandleParameterTypes.add(choice.getLambdaInterfaces().get(lambdaArgumentIndex));
                    lambdaArgumentIndex++;
                    break;
                default:
                    throw new UnsupportedOperationException("unknown argument convention: " + argumentConvention);
            }
        }

        Class<?> methodHandleReturnType = signature.getReturnType().getJavaType();
        if (choice.getReturnConvention().isNullable()) {
            methodHandleReturnType = Primitives.wrap(methodHandleReturnType);
        }

        return MethodType.methodType(methodHandleReturnType, methodHandleParameterTypes.build());
    }

    private static boolean matches(List<Boolean> argumentNullability, List<InvocationArgumentConvention> argumentConventions)
    {
        if (argumentNullability.size() != argumentConventions.size()) {
            return false;
        }
        for (int i = 0; i < argumentNullability.size(); i++) {
            boolean expectedNullable = argumentNullability.get(i);
            InvocationArgumentConvention argumentConvention = argumentConventions.get(i);
            if (argumentConvention == FUNCTION) {
                // functions are never null
                if (expectedNullable) {
                    return false;
                }
            }
            else if (expectedNullable != argumentConvention.isNullable()) {
                return false;
            }
        }
        return true;
    }

    public static final class Builder
    {
        private final Signature signature;
        private final List<Optional<Class<?>>> argumentNativeContainerTypes; // argument native container type is Optional.empty() for function type
        private final Map<String, Class<?>> specializedTypeParameters;
        private final Class<?> returnNativeContainerType;
        private final List<ParametricScalarImplementationChoice> choices;

        public Builder(
                Signature signature,
                List<Optional<Class<?>>> argumentNativeContainerTypes,
                Map<String, Class<?>> specializedTypeParameters,
                Class<?> returnNativeContainerType)
        {
            this.signature = requireNonNull(signature, "signature is null");
            this.argumentNativeContainerTypes = ImmutableList.copyOf(requireNonNull(argumentNativeContainerTypes, "argumentNativeContainerTypes is null"));
            this.specializedTypeParameters = ImmutableMap.copyOf(requireNonNull(specializedTypeParameters, "specializedTypeParameters is null"));
            this.choices = new ArrayList<>();
            this.returnNativeContainerType = requireNonNull(returnNativeContainerType, "returnNativeContainerType is null");
        }

        void addChoice(ParametricScalarImplementationChoice choice)
        {
            this.choices.add(choice);
        }

        public ParametricScalarImplementation build()
        {
            choices.sort(ParametricScalarImplementationChoice::compareTo);
            return new ParametricScalarImplementation(signature, argumentNativeContainerTypes, specializedTypeParameters, choices, returnNativeContainerType);
        }
    }

    public static final class ParametricScalarImplementationChoice
            implements Comparable<ParametricScalarImplementationChoice>
    {
        private final InvocationReturnConvention returnConvention;
        private final List<InvocationArgumentConvention> argumentConventions;
        private final List<Class<?>> lambdaInterfaces;
        private final MethodHandle methodHandle;
        private final Optional<MethodHandle> constructor;
        private final List<ImplementationDependency> dependencies;
        private final List<ImplementationDependency> constructorDependencies;
        private final int numberOfBlockPositionArguments;
        private final boolean hasConnectorSession;

        private ParametricScalarImplementationChoice(
                InvocationReturnConvention returnConvention,
                boolean hasConnectorSession,
                List<InvocationArgumentConvention> argumentConventions,
                List<Class<?>> lambdaInterfaces,
                MethodHandle methodHandle,
                Optional<MethodHandle> constructor,
                List<ImplementationDependency> dependencies,
                List<ImplementationDependency> constructorDependencies)
        {
            this.returnConvention = requireNonNull(returnConvention, "returnConvention is null");
            this.hasConnectorSession = hasConnectorSession;
            this.argumentConventions = ImmutableList.copyOf(requireNonNull(argumentConventions, "argumentConventions is null"));
            this.lambdaInterfaces = ImmutableList.copyOf(requireNonNull(lambdaInterfaces, "lambdaInterfaces is null"));
            this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
            this.constructor = requireNonNull(constructor, "constructor is null");
            this.dependencies = ImmutableList.copyOf(requireNonNull(dependencies, "dependencies is null"));
            this.constructorDependencies = ImmutableList.copyOf(requireNonNull(constructorDependencies, "constructorDependencies is null"));

            this.numberOfBlockPositionArguments = (int) argumentConventions.stream()
                    .filter(argumentConvention -> BLOCK_POSITION == argumentConvention || BLOCK_POSITION_NOT_NULL == argumentConvention)
                    .count();
        }

        public InvocationReturnConvention getReturnConvention()
        {
            return returnConvention;
        }

        public boolean hasConnectorSession()
        {
            return hasConnectorSession;
        }

        public MethodHandle getMethodHandle()
        {
            return methodHandle;
        }

        @VisibleForTesting
        public List<ImplementationDependency> getDependencies()
        {
            return dependencies;
        }

        public List<InvocationArgumentConvention> getArgumentConventions()
        {
            return argumentConventions;
        }

        public List<Class<?>> getLambdaInterfaces()
        {
            return lambdaInterfaces;
        }

        public boolean checkDependencies()
        {
            for (int i = 1; i < getDependencies().size(); i++) {
                if (!getDependencies().get(i).equals(getDependencies().get(0))) {
                    return false;
                }
            }
            return true;
        }

        @VisibleForTesting
        public List<ImplementationDependency> getConstructorDependencies()
        {
            return constructorDependencies;
        }

        public Optional<MethodHandle> getConstructor()
        {
            return constructor;
        }

        @Override
        public int compareTo(ParametricScalarImplementationChoice choice)
        {
            if (choice.numberOfBlockPositionArguments < this.numberOfBlockPositionArguments) {
                return 1;
            }
            return -1;
        }
    }

    public static final class SpecializedSignature
    {
        private final Signature signature;
        private final List<Optional<Class<?>>> argumentNativeContainerTypes;
        private final Map<String, Class<?>> specializedTypeParameters;
        private final Class<?> returnNativeContainerType;

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SpecializedSignature that = (SpecializedSignature) o;
            return Objects.equals(signature, that.signature) &&
                    Objects.equals(argumentNativeContainerTypes, that.argumentNativeContainerTypes) &&
                    Objects.equals(specializedTypeParameters, that.specializedTypeParameters) &&
                    Objects.equals(returnNativeContainerType, that.returnNativeContainerType);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(signature, argumentNativeContainerTypes, specializedTypeParameters, returnNativeContainerType);
        }

        private SpecializedSignature(
                Signature signature,
                List<Optional<Class<?>>> argumentNativeContainerTypes,
                Map<String, Class<?>> specializedTypeParameters,
                Class<?> returnNativeContainerType)
        {
            this.signature = signature;
            this.argumentNativeContainerTypes = argumentNativeContainerTypes;
            this.specializedTypeParameters = specializedTypeParameters;
            this.returnNativeContainerType = returnNativeContainerType;
        }
    }

    public static final class Parser
    {
        private final Signature signature;
        private final List<InvocationArgumentConvention> argumentConventions = new ArrayList<>();
        private final List<Class<?>> lambdaInterfaces = new ArrayList<>();
        private final List<Optional<Class<?>>> argumentNativeContainerTypes = new ArrayList<>();
        private final MethodHandle methodHandle;
        private final Set<TypeParameter> typeParameters;
        private final Set<String> literalParameters;
        private final Set<String> typeParameterNames;
        private final Map<String, Class<?>> specializedTypeParameters;
        private final Class<?> returnNativeContainerType;
        private boolean hasConnectorSession;

        private final ParametricScalarImplementationChoice choice;

        Parser(String functionName, Method method, Optional<Constructor<?>> constructor)
        {
            Signature.Builder signatureBuilder = Signature.builder();
            signatureBuilder.name(requireNonNull(functionName, "functionName is null"));
            boolean nullable = method.getAnnotation(SqlNullable.class) != null;
            checkArgument(nullable || !containsLegacyNullable(method.getAnnotations()), "Method [%s] is annotated with @Nullable but not @SqlNullable", method);

            typeParameters = ImmutableSet.copyOf(method.getAnnotationsByType(TypeParameter.class));

            literalParameters = parseLiteralParameters(method);
            typeParameterNames = typeParameters.stream()
                    .map(TypeParameter::value)
                    .collect(toImmutableSortedSet(CASE_INSENSITIVE_ORDER));

            SqlType returnType = method.getAnnotation(SqlType.class);
            checkArgument(returnType != null, "Method [%s] is missing @SqlType annotation", method);
            signatureBuilder.returnType(parseTypeSignature(returnType.value(), literalParameters));

            Class<?> actualReturnType = method.getReturnType();
            this.returnNativeContainerType = Primitives.unwrap(actualReturnType);

            if (Primitives.isWrapperType(actualReturnType)) {
                checkArgument(nullable, "Method [%s] has wrapper return type %s but is missing @SqlNullable", method, actualReturnType.getSimpleName());
            }
            else if (actualReturnType.isPrimitive()) {
                checkArgument(!nullable, "Method [%s] annotated with @SqlNullable has primitive return type %s", method, actualReturnType.getSimpleName());
            }

            parseLongVariableConstraints(method, signatureBuilder);

            this.specializedTypeParameters = getDeclaredSpecializedTypeParameters(method, typeParameters);

            for (TypeParameter typeParameter : typeParameters) {
                checkArgument(
                        typeParameter.value().matches("[A-Z][A-Z0-9]*"),
                        "Expected type parameter to only contain A-Z and 0-9 (starting with A-Z), but got %s on method [%s]", typeParameter.value(), method);
            }

            inferSpecialization(method, actualReturnType, returnType.value());

            List<ImplementationDependency> dependencies = new ArrayList<>();
            parseArguments(method, signatureBuilder, dependencies);

            List<ImplementationDependency> constructorDependencies = new ArrayList<>();
            Optional<MethodHandle> constructorMethodHandle = getConstructor(method, constructor, constructorDependencies);

            this.methodHandle = getMethodHandle(method, dependencies);

            this.choice = new ParametricScalarImplementationChoice(
                    nullable ? NULLABLE_RETURN : FAIL_ON_NULL,
                    hasConnectorSession,
                    argumentConventions,
                    lambdaInterfaces,
                    methodHandle,
                    constructorMethodHandle,
                    dependencies,
                    constructorDependencies);

            createTypeVariableConstraints(typeParameters, dependencies)
                    .forEach(signatureBuilder::typeVariableConstraint);
            signature = signatureBuilder.build();
        }

        private void parseArguments(Method method, Signature.Builder signatureBuilder, List<ImplementationDependency> dependencies)
        {
            boolean encounteredNonDependencyAnnotation = false;
            int parameterIndex = 0;
            while (parameterIndex < method.getParameterCount()) {
                Parameter parameter = method.getParameters()[parameterIndex];
                Class<?> parameterType = parameter.getType();

                // Skip injected parameters
                if (parameterType == ConnectorSession.class) {
                    checkCondition(!hasConnectorSession, FUNCTION_IMPLEMENTATION_ERROR, "Method [%s] has more than 1 ConnectorSession in the parameter list", method);
                    hasConnectorSession = true;
                    parameterIndex++;
                    continue;
                }

                Optional<Annotation> implementationDependency = getImplementationDependencyAnnotation(parameter);
                if (implementationDependency.isPresent()) {
                    checkCondition(!encounteredNonDependencyAnnotation, FUNCTION_IMPLEMENTATION_ERROR, "Method [%s] has parameters annotated with Dependency annotations that appears after other parameters", method);

                    // check if only declared typeParameters and literalParameters are used
                    validateImplementationDependencyAnnotation(method, implementationDependency.get(), typeParameterNames, literalParameters);
                    dependencies.add(createDependency(implementationDependency.get(), literalParameters, parameterType));

                    parameterIndex++;
                }
                else {
                    encounteredNonDependencyAnnotation = true;

                    Annotation[] annotations = parameter.getAnnotations();
                    checkArgument(Stream.of(annotations).noneMatch(IsNull.class::isInstance), "Method [%s] has @IsNull parameter that does not follow a @SqlType parameter", method);

                    SqlType type = Stream.of(annotations)
                            .filter(SqlType.class::isInstance)
                            .map(SqlType.class::cast)
                            .findFirst()
                            .orElseThrow(() -> new IllegalArgumentException(format("Method [%s] is missing @SqlType annotation for parameter", method)));
                    TypeSignature typeSignature = parseTypeSignature(type.value(), literalParameters);
                    signatureBuilder.argumentType(typeSignature);

                    if (typeSignature.getBase().equals(FunctionType.NAME)) {
                        // function type
                        checkCondition(parameterType.isAnnotationPresent(FunctionalInterface.class), FUNCTION_IMPLEMENTATION_ERROR, "argument %s is marked as lambda but the function interface class is not annotated: %s", parameterIndex, methodHandle);
                        argumentConventions.add(FUNCTION);
                        lambdaInterfaces.add(parameterType);
                        argumentNativeContainerTypes.add(Optional.empty());
                        parameterIndex++;
                    }
                    else {
                        // value type
                        InvocationArgumentConvention argumentConvention;
                        if (Stream.of(annotations).anyMatch(BlockPosition.class::isInstance)) {
                            checkState(method.getParameterCount() > (parameterIndex + 1));
                            checkState(parameterType == Block.class);

                            argumentConvention = Stream.of(annotations).anyMatch(SqlNullable.class::isInstance) ? BLOCK_POSITION : BLOCK_POSITION_NOT_NULL;
                            Annotation[] parameterAnnotations = method.getParameterAnnotations()[parameterIndex + 1];
                            checkState(Stream.of(parameterAnnotations).anyMatch(BlockIndex.class::isInstance));
                        }
                        else if (Stream.of(annotations).anyMatch(SqlNullable.class::isInstance)) {
                            checkCondition(!parameterType.isPrimitive(), FUNCTION_IMPLEMENTATION_ERROR, "Method [%s] has parameter with primitive type %s annotated with @SqlNullable", method, parameterType.getSimpleName());

                            argumentConvention = BOXED_NULLABLE;
                        }
                        else if (parameterType.equals(InOut.class)) {
                            argumentConvention = IN_OUT;
                        }
                        else {
                            // USE_NULL_FLAG or RETURN_NULL_ON_NULL
                            checkCondition(parameterType == Void.class || !Primitives.isWrapperType(parameterType), FUNCTION_IMPLEMENTATION_ERROR, "A parameter with USE_NULL_FLAG or RETURN_NULL_ON_NULL convention must not use wrapper type. Found in method [%s]", method);

                            boolean useNullFlag = false;
                            if (method.getParameterCount() > (parameterIndex + 1)) {
                                Annotation[] parameterAnnotations = method.getParameterAnnotations()[parameterIndex + 1];
                                if (Stream.of(parameterAnnotations).anyMatch(IsNull.class::isInstance)) {
                                    Class<?> isNullType = method.getParameterTypes()[parameterIndex + 1];

                                    checkArgument(Stream.of(parameterAnnotations).filter(FunctionsParserHelper::isTrinoAnnotation).allMatch(IsNull.class::isInstance), "Method [%s] has @IsNull parameter that has other annotations", method);
                                    checkArgument(isNullType == boolean.class, "Method [%s] has non-boolean parameter with @IsNull", method);
                                    checkArgument((parameterType == Void.class) || !Primitives.isWrapperType(parameterType), "Method [%s] uses @IsNull following a parameter with boxed primitive type: %s", method, parameterType.getSimpleName());

                                    useNullFlag = true;
                                }
                            }

                            if (useNullFlag) {
                                argumentConvention = NULL_FLAG;
                            }
                            else {
                                argumentConvention = NEVER_NULL;
                            }
                        }

                        if (argumentConvention == BLOCK_POSITION || argumentConvention == BLOCK_POSITION_NOT_NULL) {
                            argumentNativeContainerTypes.add(Optional.of(type.nativeContainerType()));
                        }
                        else {
                            inferSpecialization(method, parameterType, type.value());

                            checkCondition(type.nativeContainerType().equals(Object.class), FUNCTION_IMPLEMENTATION_ERROR, "@SqlType can only contain an explicitly specified nativeContainerType when using @BlockPosition");
                            argumentNativeContainerTypes.add(Optional.of(Primitives.unwrap(parameterType)));
                        }

                        argumentConventions.add(argumentConvention);
                        parameterIndex += argumentConvention.getParameterCount();
                    }
                }
            }
        }

        private void inferSpecialization(Method method, Class<?> parameterType, String typeParameterName)
        {
            if (typeParameterNames.contains(typeParameterName) && parameterType != Object.class) {
                // Infer specialization on this type parameter.
                // We don't do this for Object because it could match any type.
                Class<?> specialization = specializedTypeParameters.get(typeParameterName);
                Class<?> nativeParameterType = Primitives.unwrap(parameterType);
                checkArgument(specialization == null || specialization.equals(nativeParameterType), "Method [%s] type %s has conflicting specializations %s and %s", method, typeParameterName, specialization, nativeParameterType);
                specializedTypeParameters.put(typeParameterName, nativeParameterType);
            }
        }

        // Find matching constructor, if this is an instance method, and populate constructorDependencies
        private Optional<MethodHandle> getConstructor(Method method, Optional<Constructor<?>> optionalConstructor, List<ImplementationDependency> constructorDependencies)
        {
            if (isStatic(method.getModifiers())) {
                return Optional.empty();
            }

            checkArgument(optionalConstructor.isPresent(), "Method [%s] is an instance method. It must be in a class annotated with @ScalarFunction or @ScalarOperator, and the class is required to have a public constructor.", method);
            Constructor<?> constructor = optionalConstructor.get();
            Set<TypeParameter> constructorTypeParameters = Stream.of(constructor.getAnnotationsByType(TypeParameter.class))
                    .collect(toImmutableSet());
            checkArgument(constructorTypeParameters.containsAll(typeParameters), "Method [%s] is an instance method and requires a public constructor containing all type parameters: %s", method, typeParameters);

            for (int i = 0; i < constructor.getParameterCount(); i++) {
                Annotation[] annotations = constructor.getParameterAnnotations()[i];
                checkArgument(containsImplementationDependencyAnnotation(annotations), "Constructors may only have meta parameters [%s]", constructor);
                checkArgument(annotations.length == 1, "Meta parameters may only have a single annotation [%s]", constructor);
                Annotation annotation = annotations[0];
                if (annotation instanceof TypeParameter) {
                    checkTypeParameters(parseTypeSignature(((TypeParameter) annotation).value(), ImmutableSet.of()), typeParameterNames, method);
                }
                constructorDependencies.add(createDependency(annotation, literalParameters, constructor.getParameterTypes()[i]));
            }
            MethodHandle result = constructorMethodHandle(FUNCTION_IMPLEMENTATION_ERROR, constructor);
            // Change type of return value to Object to make sure callers won't have classloader issues
            return Optional.of(result.asType(result.type().changeReturnType(Object.class)));
        }

        private static MethodHandle getMethodHandle(Method method, List<ImplementationDependency> dependencies)
        {
            MethodHandle methodHandle = methodHandle(FUNCTION_IMPLEMENTATION_ERROR, method);
            if (!isStatic(method.getModifiers())) {
                // Change type of "this" argument to Object to make sure callers won't have classloader issues
                methodHandle = methodHandle.asType(methodHandle.type().changeParameterType(0, Object.class));
                // Re-arrange the parameters, so that the "this" parameter is after the meta parameters
                int[] permutedIndices = new int[methodHandle.type().parameterCount()];
                permutedIndices[0] = dependencies.size();
                MethodType newType = methodHandle.type().changeParameterType(dependencies.size(), methodHandle.type().parameterType(0));
                for (int i = 0; i < dependencies.size(); i++) {
                    permutedIndices[i + 1] = i;
                    newType = newType.changeParameterType(i, methodHandle.type().parameterType(i + 1));
                }
                for (int i = dependencies.size() + 1; i < permutedIndices.length; i++) {
                    permutedIndices[i] = i;
                }
                methodHandle = permuteArguments(methodHandle, newType, permutedIndices);
            }
            return methodHandle;
        }

        public List<Optional<Class<?>>> getArgumentNativeContainerTypes()
        {
            return argumentNativeContainerTypes;
        }

        public Map<String, Class<?>> getSpecializedTypeParameters()
        {
            return specializedTypeParameters;
        }

        public Class<?> getReturnNativeContainerType()
        {
            return returnNativeContainerType;
        }

        public ParametricScalarImplementationChoice getChoice()
        {
            return choice;
        }

        public SpecializedSignature getSpecializedSignature()
        {
            return new SpecializedSignature(
                    getSignature(),
                    argumentNativeContainerTypes,
                    specializedTypeParameters,
                    returnNativeContainerType);
        }

        public Signature getSignature()
        {
            return signature;
        }
    }
}
