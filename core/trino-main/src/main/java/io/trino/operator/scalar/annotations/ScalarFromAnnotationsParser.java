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
import io.trino.metadata.SqlScalarFunction;
import io.trino.operator.ParametricImplementationsGroup;
import io.trino.operator.annotations.FunctionsParserHelper;
import io.trino.operator.scalar.ParametricScalar;
import io.trino.operator.scalar.ScalarHeader;
import io.trino.operator.scalar.annotations.ParametricScalarImplementation.SpecializedSignature;
import io.trino.spi.function.ColumnarScalarImplementation;
import io.trino.spi.function.ColumnarScalarSpecializer;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.Signature;
import io.trino.spi.function.SqlType;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.scalar.annotations.OperatorValidator.validateOperator;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.util.Failures.checkCondition;
import static java.util.Objects.requireNonNull;

public final class ScalarFromAnnotationsParser
{
    private ScalarFromAnnotationsParser() {}

    public static List<SqlScalarFunction> parseFunctionDefinition(Class<?> clazz)
    {
        ImmutableList.Builder<SqlScalarFunction> builder = ImmutableList.builder();
        boolean deprecated = clazz.getAnnotationsByType(Deprecated.class).length > 0;
        for (ScalarHeaderAndMethods scalar : findScalarsInFunctionDefinitionClass(clazz)) {
            builder.add(parseParametricScalar(scalar, FunctionsParserHelper.findConstructor(clazz), deprecated));
        }
        return builder.build();
    }

    public static List<SqlScalarFunction> parseFunctionDefinitions(Class<?> clazz)
    {
        ImmutableList.Builder<SqlScalarFunction> builder = ImmutableList.builder();
        for (ScalarHeaderAndMethods methods : findScalarsInFunctionSetClass(clazz)) {
            boolean deprecated = methods.methods().iterator().next().getAnnotationsByType(Deprecated.class).length > 0;
            // Non-static function only makes sense in classes annotated with @ScalarFunction or @ScalarOperator.
            builder.add(parseParametricScalar(methods, FunctionsParserHelper.findConstructor(clazz), deprecated));
        }
        List<SqlScalarFunction> functions = builder.build();
        for (Method method : findColumnarMethods(clazz)) {
            List<ScalarHeader> headers = ScalarHeader.fromAnnotatedElement(method);
            checkCondition(functions.stream().anyMatch(function ->
                            headers.stream().anyMatch(header -> function.getFunctionMetadata().getNames().contains(header.getName())) &&
                                    matchesColumnarImplementation(method, function.getFunctionMetadata().getSignature())),
                    FUNCTION_IMPLEMENTATION_ERROR,
                    "Columnar method [%s] does not match a scalar overload",
                    method);
        }
        return functions;
    }

    private static List<ScalarHeaderAndMethods> findScalarsInFunctionDefinitionClass(Class<?> annotated)
    {
        ImmutableList.Builder<ScalarHeaderAndMethods> builder = ImmutableList.builder();
        List<ScalarHeader> classHeaders = ScalarHeader.fromAnnotatedElement(annotated);
        checkArgument(!classHeaders.isEmpty(), "Class [%s] that defines function must be annotated with @ScalarFunction or @ScalarOperator", annotated.getName());

        for (ScalarHeader header : classHeaders) {
            List<Method> methods = FunctionsParserHelper.findPublicMethodsWithAnnotation(annotated, SqlType.class, ScalarFunction.class, ScalarOperator.class).stream()
                    .filter(method -> !isColumnarMethod(method))
                    .toList();
            List<Method> columnarMethods = findColumnarMethods(annotated);
            checkCondition(!methods.isEmpty(), FUNCTION_IMPLEMENTATION_ERROR, "Parametric class [%s] does not have any annotated methods", annotated.getName());
            for (Method method : methods) {
                checkArgument(method.getAnnotation(ScalarFunction.class) == null, "Parametric class method [%s] is annotated with @ScalarFunction", method);
                checkArgument(method.getAnnotation(ScalarOperator.class) == null, "Parametric class method [%s] is annotated with @ScalarOperator", method);
            }
            builder.add(new ScalarHeaderAndMethods(header, methods, columnarMethods, false));
        }

        return builder.build();
    }

    private static List<ScalarHeaderAndMethods> findScalarsInFunctionSetClass(Class<?> annotated)
    {
        ImmutableList.Builder<ScalarHeaderAndMethods> builder = ImmutableList.builder();
        List<Method> columnarMethods = findColumnarMethods(annotated);
        for (Method columnarMethod : columnarMethods) {
            checkCondition(columnarMethod.getAnnotation(ScalarFunction.class) != null || columnarMethod.getAnnotation(ScalarOperator.class) != null,
                    FUNCTION_IMPLEMENTATION_ERROR,
                    "Columnar method [%s] in a function set is missing @ScalarFunction or @ScalarOperator",
                    columnarMethod);
        }
        for (Method method : FunctionsParserHelper.findPublicMethodsWithAnnotation(annotated, ScalarFunction.class, ScalarOperator.class, SqlType.class).stream()
                .filter(candidate -> !isColumnarMethod(candidate))
                .toList()) {
            checkCondition((method.getAnnotation(ScalarFunction.class) != null) || (method.getAnnotation(ScalarOperator.class) != null),
                    FUNCTION_IMPLEMENTATION_ERROR,
                    "Method [%s] annotated with @SqlType is missing @ScalarFunction or @ScalarOperator",
                    method);
            for (ScalarHeader header : ScalarHeader.fromAnnotatedElement(method)) {
                List<Method> matchingColumnarMethods = columnarMethods.stream()
                        .filter(columnarMethod -> ScalarHeader.fromAnnotatedElement(columnarMethod).stream()
                                .anyMatch(columnarHeader -> columnarHeader.getName().equals(header.getName()) && columnarHeader.getOperatorType().equals(header.getOperatorType())))
                        .toList();
                builder.add(new ScalarHeaderAndMethods(header, ImmutableList.of(method), matchingColumnarMethods, true));
            }
        }
        List<ScalarHeaderAndMethods> methods = builder.build();
        checkArgument(!methods.isEmpty(), "Class [%s] does not have any methods annotated with @ScalarFunction or @ScalarOperator", annotated.getName());
        return methods;
    }

    private static SqlScalarFunction parseParametricScalar(ScalarHeaderAndMethods scalar, Optional<Constructor<?>> constructor, boolean deprecated)
    {
        Map<SpecializedSignature, ParametricScalarImplementation.Builder> signatures = new HashMap<>();
        for (Method method : scalar.methods()) {
            ParametricScalarImplementation.Parser implementation = new ParametricScalarImplementation.Parser(method, constructor);
            if (!signatures.containsKey(implementation.getSpecializedSignature())) {
                ParametricScalarImplementation.Builder builder = new ParametricScalarImplementation.Builder(
                        implementation.getSignature(),
                        implementation.getArgumentNativeContainerTypes(),
                        implementation.getSpecializedTypeParameters(),
                        implementation.getReturnNativeContainerType());
                signatures.put(implementation.getSpecializedSignature(), builder);
                builder.addChoice(implementation.getChoice());
            }
            else {
                ParametricScalarImplementation.Builder builder = signatures.get(implementation.getSpecializedSignature());
                builder.addChoice(implementation.getChoice());
            }
        }

        ParametricImplementationsGroup.Builder<ParametricScalarImplementation> implementationsBuilder = ParametricImplementationsGroup.builder();
        for (ParametricScalarImplementation.Builder implementation : signatures.values()) {
            implementationsBuilder.addImplementation(implementation.build());
        }
        ParametricImplementationsGroup<ParametricScalarImplementation> implementations = implementationsBuilder.build();
        Signature scalarSignature = implementations.getSignature();
        Optional<ColumnarScalarImplementationParser.Implementations> columnarImplementations =
                ColumnarScalarImplementationParser.parse(scalar.columnarMethods(), scalarSignature);
        checkCondition(scalar.allowUnmatchedColumnarMethods() || scalar.columnarMethods().isEmpty() || columnarImplementations.isPresent(),
                FUNCTION_IMPLEMENTATION_ERROR,
                "Columnar implementation does not match scalar signature %s",
                scalarSignature);

        scalar.header().getOperatorType().ifPresent(operatorType ->
                validateOperator(
                        operatorType,
                        scalarSignature.getReturnType(),
                        scalarSignature.getArgumentTypes()));

        return new ParametricScalar(scalarSignature, scalar.header(), implementations, columnarImplementations, deprecated);
    }

    private static List<Method> findColumnarMethods(Class<?> annotated)
    {
        return FunctionsParserHelper.findPublicMethodsWithAnnotation(annotated, ColumnarScalarImplementation.class, ColumnarScalarSpecializer.class);
    }

    private static boolean isColumnarMethod(Method method)
    {
        return method.isAnnotationPresent(ColumnarScalarImplementation.class) || method.isAnnotationPresent(ColumnarScalarSpecializer.class);
    }

    private static boolean matchesColumnarImplementation(Method method, Signature scalarSignature)
    {
        return method.isAnnotationPresent(ColumnarScalarSpecializer.class) ||
                ColumnarScalarImplementationParser.matchesDirectImplementation(method, scalarSignature);
    }

    private record ScalarHeaderAndMethods(ScalarHeader header, List<Method> methods, List<Method> columnarMethods, boolean allowUnmatchedColumnarMethods)
    {
        private ScalarHeaderAndMethods
        {
            requireNonNull(header);
            requireNonNull(methods);
            requireNonNull(columnarMethods);
        }
    }
}
