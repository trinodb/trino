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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.FormatMethod;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.SignatureBinder;
import io.trino.operator.annotations.ImplementationDependency;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.ColumnarScalarFunctionImplementation;
import io.trino.spi.function.ColumnarScalarImplementation;
import io.trino.spi.function.ColumnarScalarSpecializer;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.IsNull;
import io.trino.spi.function.Signature;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.TypeTemplate;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSortedSet.toImmutableSortedSet;
import static io.trino.operator.ParametricFunctionHelpers.bindDependencies;
import static io.trino.operator.annotations.FunctionsParserHelper.createTypeVariableConstraints;
import static io.trino.operator.annotations.FunctionsParserHelper.getDeclaredSpecializedTypeParameters;
import static io.trino.operator.annotations.FunctionsParserHelper.parseLiteralParameters;
import static io.trino.operator.annotations.FunctionsParserHelper.parseNumericVariableConstraints;
import static io.trino.operator.annotations.ImplementationDependency.Factory.createDependency;
import static io.trino.operator.annotations.ImplementationDependency.getImplementationDependencyAnnotation;
import static io.trino.operator.annotations.ImplementationDependency.validateImplementationDependencyAnnotation;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.parseTypeTemplate;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.filterArguments;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.lang.invoke.MethodType.methodType;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.Objects.requireNonNull;

public final class ColumnarScalarImplementationParser
{
    private static final MethodHandle GET_BLOCK = methodHandle(SourcePage.class, "getBlock", int.class);

    private ColumnarScalarImplementationParser() {}

    public static Optional<Implementations> parse(List<Method> methods, Signature scalarSignature)
    {
        requireNonNull(methods, "methods is null");
        requireNonNull(scalarSignature, "scalarSignature is null");

        List<Method> directMethods = methods.stream()
                .filter(method -> method.isAnnotationPresent(ColumnarScalarImplementation.class))
                .toList();
        List<Method> specializerMethods = methods.stream()
                .filter(method -> method.isAnnotationPresent(ColumnarScalarSpecializer.class))
                .toList();

        checkImplementation(directMethods.isEmpty() || specializerMethods.isEmpty(),
                "Scalar function %s has both @ColumnarScalarImplementation and @ColumnarScalarSpecializer methods",
                scalarSignature);
        if (directMethods.isEmpty() && specializerMethods.isEmpty()) {
            return Optional.empty();
        }

        if (!specializerMethods.isEmpty()) {
            checkImplementation(specializerMethods.size() == 1, "Scalar function %s has more than one @ColumnarScalarSpecializer method", scalarSignature);
            return Optional.of(Implementations.forSpecializer(parseSpecializer(specializerMethods.getFirst())));
        }

        ImmutableList.Builder<DirectImplementation> implementations = ImmutableList.builder();
        for (Method method : directMethods) {
            DirectImplementation implementation = new DirectImplementation(method);
            if (implementation.signature().equals(scalarSignature)) {
                implementations.add(implementation);
            }
        }
        List<DirectImplementation> matchingImplementations = implementations.build();
        if (matchingImplementations.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(Implementations.forDirect(matchingImplementations, scalarSignature));
    }

    public static boolean matchesDirectImplementation(Method method, Signature scalarSignature)
    {
        checkImplementation(method.isAnnotationPresent(ColumnarScalarImplementation.class), "Method [%s] is not annotated with @ColumnarScalarImplementation", method);
        return new DirectImplementation(method).signature().equals(scalarSignature);
    }

    public static BiFunction<BoundSignature, FunctionDependencies, Optional<ColumnarScalarFunctionImplementation>> parseSpecializer(Method method)
    {
        validatePublicStatic(method, ColumnarScalarSpecializer.class);
        checkImplementation(method.getReturnType() == Optional.class,
                "Method [%s] annotated with @ColumnarScalarSpecializer must return Optional",
                method);
        checkImplementation(method.getGenericReturnType() instanceof ParameterizedType returnType &&
                        Arrays.equals(returnType.getActualTypeArguments(), new Object[] {ColumnarScalarFunctionImplementation.class}),
                "Method [%s] annotated with @ColumnarScalarSpecializer must return Optional<ColumnarScalarFunctionImplementation>",
                method);
        checkImplementation(Arrays.equals(method.getParameterTypes(), new Class<?>[] {BoundSignature.class, FunctionDependencies.class}),
                "Method [%s] annotated with @ColumnarScalarSpecializer must accept (BoundSignature, FunctionDependencies)",
                method);
        MethodHandle handle = methodHandle(FUNCTION_IMPLEMENTATION_ERROR, method);
        return (signature, dependencies) -> invokeSpecializer(handle, signature, dependencies);
    }

    private static Optional<ColumnarScalarFunctionImplementation> invokeSpecializer(MethodHandle handle, BoundSignature signature, FunctionDependencies dependencies)
    {
        try {
            @SuppressWarnings("unchecked")
            Optional<ColumnarScalarFunctionImplementation> implementation = (Optional<ColumnarScalarFunctionImplementation>) handle.invokeExact(signature, dependencies);
            return requireNonNull(implementation, "columnar scalar specializer returned null");
        }
        catch (Throwable throwable) {
            throw propagate(throwable);
        }
    }

    private static void validatePublicStatic(Method method, Class<? extends Annotation> annotation)
    {
        checkImplementation(isPublic(method.getModifiers()) && isStatic(method.getModifiers()),
                "Method [%s] annotated with @%s must be public and static",
                method,
                annotation.getSimpleName());
    }

    public static final class Implementations
    {
        private final List<DirectImplementation> directImplementations;
        private final Optional<BiFunction<BoundSignature, FunctionDependencies, Optional<ColumnarScalarFunctionImplementation>>> specializer;
        private final Signature signature;

        private Implementations(
                List<DirectImplementation> directImplementations,
                Optional<BiFunction<BoundSignature, FunctionDependencies, Optional<ColumnarScalarFunctionImplementation>>> specializer,
                Signature signature)
        {
            this.directImplementations = ImmutableList.copyOf(directImplementations);
            this.specializer = requireNonNull(specializer, "specializer is null");
            this.signature = signature;
        }

        private static Implementations forDirect(List<DirectImplementation> implementations, Signature signature)
        {
            return new Implementations(implementations, Optional.empty(), requireNonNull(signature, "signature is null"));
        }

        private static Implementations forSpecializer(BiFunction<BoundSignature, FunctionDependencies, Optional<ColumnarScalarFunctionImplementation>> specializer)
        {
            return new Implementations(ImmutableList.of(), Optional.of(specializer), null);
        }

        public Optional<ColumnarScalarFunctionImplementation> specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
        {
            if (specializer.isPresent()) {
                return specializer.get().apply(boundSignature, functionDependencies);
            }

            FunctionBinding functionBinding = SignatureBinder.bindFunction(FunctionId.toFunctionId(boundSignature.getName().functionName(), signature), signature, boundSignature);
            Optional<ColumnarScalarFunctionImplementation> specialized = selectImplementation(
                    directImplementations.stream()
                            .filter(DirectImplementation::hasSpecializedTypeParameters)
                            .toList(),
                    functionBinding,
                    functionDependencies,
                    boundSignature);
            if (specialized.isPresent()) {
                return specialized;
            }
            return selectImplementation(
                    directImplementations.stream()
                            .filter(implementation -> !implementation.hasSpecializedTypeParameters())
                            .toList(),
                    functionBinding,
                    functionDependencies,
                    boundSignature);
        }

        private static Optional<ColumnarScalarFunctionImplementation> selectImplementation(
                List<DirectImplementation> implementations,
                FunctionBinding functionBinding,
                FunctionDependencies functionDependencies,
                BoundSignature boundSignature)
        {
            ColumnarScalarFunctionImplementation selected = null;
            for (DirectImplementation implementation : implementations) {
                Optional<ColumnarScalarFunctionImplementation> candidate = implementation.specialize(functionBinding, functionDependencies);
                if (candidate.isPresent()) {
                    checkImplementation(selected == null, "Ambiguous columnar scalar implementation for %s", boundSignature);
                    selected = candidate.get();
                }
            }
            return Optional.ofNullable(selected);
        }

        public void declareDependencies(FunctionDependencyDeclarationBuilder builder)
        {
            directImplementations.stream()
                    .flatMap(implementation -> implementation.dependencies().stream())
                    .forEach(dependency -> dependency.declareDependencies(builder));
        }
    }

    private record DirectImplementation(
            Signature signature,
            Map<String, Class<?>> specializedTypeParameters,
            List<ImplementationDependency> dependencies,
            MethodHandle methodHandle,
            boolean hasConnectorSession,
            int argumentCount)
    {
        private DirectImplementation(Method method)
        {
            this(parseDirectMethod(method));
        }

        private DirectImplementation(ParsedDirectMethod parsed)
        {
            this(parsed.signature(), parsed.specializedTypeParameters(), parsed.dependencies(), parsed.methodHandle(), parsed.hasConnectorSession(), parsed.argumentCount());
        }

        private Optional<ColumnarScalarFunctionImplementation> specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
        {
            for (Map.Entry<String, Class<?>> entry : specializedTypeParameters.entrySet()) {
                if (!entry.getValue().isAssignableFrom(functionBinding.variables().getTypeVariable(entry.getKey()).getJavaType())) {
                    return Optional.empty();
                }
            }

            MethodHandle handle = bindDependencies(methodHandle, dependencies, functionBinding, functionDependencies);
            if (!hasConnectorSession) {
                handle = dropArguments(handle, 0, ConnectorSession.class);
            }
            for (int argument = 0; argument < argumentCount; argument++) {
                MethodHandle getBlock = insertArguments(GET_BLOCK, 1, argument);
                handle = filterArguments(handle, argument + 1, getBlock);
            }
            int[] reorder = new int[argumentCount + 1];
            reorder[0] = 0;
            Arrays.fill(reorder, 1, reorder.length, 1);
            handle = permuteArguments(handle, methodType(Block.class, ConnectorSession.class, SourcePage.class), reorder);
            MethodHandle implementation = handle;
            return Optional.of((session, arguments) -> invokeDirect(implementation, session, arguments));
        }

        private boolean hasSpecializedTypeParameters()
        {
            return !specializedTypeParameters.isEmpty();
        }
    }

    private static ParsedDirectMethod parseDirectMethod(Method method)
    {
        validatePublicStatic(method, ColumnarScalarImplementation.class);
        checkImplementation(method.getReturnType() == Block.class,
                "Method [%s] annotated with @ColumnarScalarImplementation must return Block",
                method);
        checkImplementation(!method.isAnnotationPresent(SqlNullable.class),
                "Method [%s] annotated with @ColumnarScalarImplementation cannot use @SqlNullable",
                method);

        Set<TypeParameter> typeParameters = ImmutableSet.copyOf(method.getAnnotationsByType(TypeParameter.class));
        Set<String> literalParameters = parseLiteralParameters(method);
        Set<String> typeParameterNames = typeParameters.stream()
                .map(TypeParameter::value)
                .collect(toImmutableSortedSet(CASE_INSENSITIVE_ORDER));

        Signature.Builder signature = Signature.builder();
        SqlType returnType = method.getAnnotation(SqlType.class);
        checkImplementation(returnType != null, "Method [%s] is missing @SqlType annotation", method);
        signature.returnType(parseTypeTemplate(returnType.value(), typeParameterNames, literalParameters));
        parseNumericVariableConstraints(method, signature);

        List<ImplementationDependency> dependencies = new ArrayList<>();
        boolean hasConnectorSession = false;
        boolean encounteredSqlArgument = false;
        int argumentCount = 0;
        for (Parameter parameter : method.getParameters()) {
            if (parameter.getType() == ConnectorSession.class) {
                checkImplementation(!hasConnectorSession, "Method [%s] has more than one ConnectorSession parameter", method);
                checkImplementation(!encounteredSqlArgument, "Method [%s] has ConnectorSession after SQL arguments", method);
                checkImplementation(parameter.getAnnotations().length == 0, "ConnectorSession parameter in method [%s] must not be annotated", method);
                hasConnectorSession = true;
                continue;
            }

            Optional<Annotation> dependency = getImplementationDependencyAnnotation(parameter);
            if (dependency.isPresent()) {
                checkImplementation(!encounteredSqlArgument, "Method [%s] has an injected dependency after SQL arguments", method);
                validateImplementationDependencyAnnotation(method, dependency.get(), typeParameterNames, literalParameters);
                dependencies.add(createDependency(dependency.get(), typeParameterNames, literalParameters, parameter.getType()));
                continue;
            }

            encounteredSqlArgument = true;
            checkImplementation(parameter.getType() == Block.class,
                    "SQL argument in method [%s] annotated with @ColumnarScalarImplementation must have type Block",
                    method);
            checkImplementation(!hasScalarArgumentAnnotation(parameter),
                    "SQL argument in method [%s] annotated with @ColumnarScalarImplementation cannot use scalar null or block-position annotations",
                    method);
            SqlType sqlType = parameter.getAnnotation(SqlType.class);
            checkImplementation(sqlType != null, "Method [%s] is missing @SqlType annotation for parameter", method);
            TypeTemplate type = parseTypeTemplate(sqlType.value(), typeParameterNames, literalParameters);
            checkImplementation(!(type instanceof TypeTemplate.TypeApplication application && application.base().equals(FunctionType.NAME)),
                    "Method [%s] annotated with @ColumnarScalarImplementation cannot accept function arguments",
                    method);
            signature.argumentType(type);
            argumentCount++;
        }
        checkImplementation(argumentCount > 0, "Method [%s] annotated with @ColumnarScalarImplementation must have at least one SQL argument", method);

        createTypeVariableConstraints(typeParameters, dependencies).forEach(signature::typeVariableConstraint);
        return new ParsedDirectMethod(
                signature.build(),
                getDeclaredSpecializedTypeParameters(method, typeParameters),
                ImmutableList.copyOf(dependencies),
                methodHandle(FUNCTION_IMPLEMENTATION_ERROR, method),
                hasConnectorSession,
                argumentCount);
    }

    private static boolean hasScalarArgumentAnnotation(Parameter parameter)
    {
        return Stream.of(parameter.getAnnotations()).anyMatch(annotation ->
                annotation instanceof SqlNullable ||
                        annotation instanceof BlockPosition ||
                        annotation instanceof BlockIndex ||
                        annotation instanceof IsNull);
    }

    private static Block invokeDirect(MethodHandle handle, ConnectorSession session, SourcePage arguments)
    {
        try {
            return (Block) handle.invokeExact(session, arguments);
        }
        catch (Throwable throwable) {
            throw propagate(throwable);
        }
    }

    private static RuntimeException propagate(Throwable throwable)
    {
        if (throwable instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (throwable instanceof Error error) {
            throw error;
        }
        return new RuntimeException(throwable);
    }

    @FormatMethod
    private static void checkImplementation(boolean condition, String message, Object... arguments)
    {
        if (!condition) {
            throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, message.formatted(arguments));
        }
    }

    private record ParsedDirectMethod(
            Signature signature,
            Map<String, Class<?>> specializedTypeParameters,
            List<ImplementationDependency> dependencies,
            MethodHandle methodHandle,
            boolean hasConnectorSession,
            int argumentCount) {}
}
