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
import io.trino.spi.function.ConstantArgument;
import io.trino.spi.function.ConstantSpecialization;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.NonConstantImplementation;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.Signature;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.TypeTemplate;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
        List<Class<?>> constantSpecializations = Arrays.stream(clazz.getDeclaredClasses())
                .filter(nestedClass -> nestedClass.isAnnotationPresent(ConstantSpecialization.class))
                .sorted(Comparator.comparing(Class::getName))
                .toList();
        if (!constantSpecializations.isEmpty()) {
            List<Class<?>> rowImplementations = Arrays.stream(clazz.getDeclaredClasses())
                    .filter(nestedClass -> nestedClass.isAnnotationPresent(NonConstantImplementation.class))
                    .toList();
            checkArgument(rowImplementations.size() == 1, "Class [%s] with constant specializations must declare exactly one nested @NonConstantImplementation class", clazz.getName());
            Class<?> rowImplementation = rowImplementations.getFirst();
            validateNestedImplementationClass(rowImplementation);
            constantSpecializations.forEach(ScalarFromAnnotationsParser::validateNestedImplementationClass);

            List<ScalarHeader> headers = ScalarHeader.fromAnnotatedElement(clazz);
            checkArgument(!headers.isEmpty(), "Class [%s] that defines function must be annotated with @ScalarFunction or @ScalarOperator", clazz.getName());
            List<Method> rowMethods = findImplementationMethods(rowImplementation);
            for (ScalarHeader header : headers) {
                builder.add(parseParametricScalar(
                        new ScalarHeaderAndMethods(header, rowMethods),
                        FunctionsParserHelper.findConstructor(rowImplementation),
                        constantSpecializations,
                        deprecated));
            }
            return builder.build();
        }
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
        return builder.build();
    }

    private static List<ScalarHeaderAndMethods> findScalarsInFunctionDefinitionClass(Class<?> annotated)
    {
        ImmutableList.Builder<ScalarHeaderAndMethods> builder = ImmutableList.builder();
        List<ScalarHeader> classHeaders = ScalarHeader.fromAnnotatedElement(annotated);
        checkArgument(!classHeaders.isEmpty(), "Class [%s] that defines function must be annotated with @ScalarFunction or @ScalarOperator", annotated.getName());

        for (ScalarHeader header : classHeaders) {
            List<Method> methods = FunctionsParserHelper.findPublicMethodsWithAnnotation(annotated, SqlType.class, ScalarFunction.class, ScalarOperator.class);
            checkCondition(!methods.isEmpty(), FUNCTION_IMPLEMENTATION_ERROR, "Parametric class [%s] does not have any annotated methods", annotated.getName());
            for (Method method : methods) {
                checkArgument(method.getAnnotation(ScalarFunction.class) == null, "Parametric class method [%s] is annotated with @ScalarFunction", method);
                checkArgument(method.getAnnotation(ScalarOperator.class) == null, "Parametric class method [%s] is annotated with @ScalarOperator", method);
            }
            builder.add(new ScalarHeaderAndMethods(header, methods));
        }

        return builder.build();
    }

    private static List<ScalarHeaderAndMethods> findScalarsInFunctionSetClass(Class<?> annotated)
    {
        ImmutableList.Builder<ScalarHeaderAndMethods> builder = ImmutableList.builder();
        for (Method method : FunctionsParserHelper.findPublicMethodsWithAnnotation(annotated, ScalarFunction.class, ScalarOperator.class, SqlType.class)) {
            checkCondition((method.getAnnotation(ScalarFunction.class) != null) || (method.getAnnotation(ScalarOperator.class) != null),
                    FUNCTION_IMPLEMENTATION_ERROR,
                    "Method [%s] annotated with @SqlType is missing @ScalarFunction or @ScalarOperator",
                    method);
            for (ScalarHeader header : ScalarHeader.fromAnnotatedElement(method)) {
                builder.add(new ScalarHeaderAndMethods(header, ImmutableList.of(method)));
            }
        }
        List<ScalarHeaderAndMethods> methods = builder.build();
        checkArgument(!methods.isEmpty(), "Class [%s] does not have any methods annotated with @ScalarFunction or @ScalarOperator", annotated.getName());
        return methods;
    }

    private static SqlScalarFunction parseParametricScalar(ScalarHeaderAndMethods scalar, Optional<Constructor<?>> constructor, boolean deprecated)
    {
        return parseParametricScalar(scalar, constructor, List.of(), deprecated);
    }

    private static SqlScalarFunction parseParametricScalar(
            ScalarHeaderAndMethods scalar,
            Optional<Constructor<?>> constructor,
            List<Class<?>> constantSpecializationClasses,
            boolean deprecated)
    {
        ParametricImplementationsGroup<ParametricScalarImplementation> implementations = parseImplementations(scalar.methods(), constructor, false);
        Signature scalarSignature = implementations.getSignature();

        List<ParametricScalarConstantSpecialization> constantSpecializations = constantSpecializationClasses.stream()
                .map(specializationClass -> parseConstantSpecialization(specializationClass, scalarSignature, implementations.getFunctionNullability()))
                .toList();
        Set<Set<Integer>> distinctConstantPositions = new HashSet<>();
        for (ParametricScalarConstantSpecialization specialization : constantSpecializations) {
            checkArgument(distinctConstantPositions.add(specialization.getConstantPositions()), "Function [%s] has multiple @ConstantSpecialization classes for constant arguments %s", scalar.header().getName(), specialization.getConstantPositions());
        }

        scalar.header().getOperatorType().ifPresent(operatorType ->
                validateOperator(
                        operatorType,
                        scalarSignature.getReturnType(),
                        scalarSignature.getArgumentTypes()));

        return new ParametricScalar(scalarSignature, scalar.header(), implementations, constantSpecializations, deprecated);
    }

    private static ParametricImplementationsGroup<ParametricScalarImplementation> parseImplementations(
            List<Method> methods,
            Optional<Constructor<?>> constructor,
            boolean constantSpecialization)
    {
        Map<SpecializedSignature, ParametricScalarImplementation.Builder> signatures = new HashMap<>();
        for (Method method : methods) {
            ParametricScalarImplementation.Parser implementation = new ParametricScalarImplementation.Parser(method, constructor, constantSpecialization);
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
        return implementationsBuilder.build();
    }

    private static ParametricScalarConstantSpecialization parseConstantSpecialization(
            Class<?> specializationClass,
            Signature canonicalSignature,
            FunctionNullability canonicalNullability)
    {
        Set<Integer> constantPositions = new LinkedHashSet<>();
        Constructor<?> constructor = FunctionsParserHelper.findConstructor(specializationClass)
                .orElseThrow(() -> new IllegalArgumentException("@ConstantSpecialization class [%s] must have a public constructor".formatted(specializationClass.getName())));
        for (Parameter parameter : constructor.getParameters()) {
            ConstantArgument constantArgument = parameter.getAnnotation(ConstantArgument.class);
            if (constantArgument == null) {
                continue;
            }
            int position = constantArgument.position();
            checkArgument(position >= 0 && position < canonicalSignature.getArgumentTypes().size(), "@ConstantArgument position %s is outside canonical signature %s", position, canonicalSignature);
            checkArgument(constantPositions.add(position), "@ConstantArgument position %s is declared more than once in class [%s]", position, specializationClass.getName());
        }
        checkArgument(!constantPositions.isEmpty(), "@ConstantSpecialization class [%s] must declare at least one @ConstantArgument constructor parameter", specializationClass.getName());

        ParametricImplementationsGroup<ParametricScalarImplementation> implementations = parseImplementations(
                findImplementationMethods(specializationClass),
                Optional.of(constructor),
                true);
        Signature remainingSignature = implementations.getSignature();
        List<TypeTemplate> expectedArguments = new ArrayList<>();
        List<Boolean> expectedNullability = new ArrayList<>();
        for (int index = 0; index < canonicalSignature.getArgumentTypes().size(); index++) {
            if (!constantPositions.contains(index)) {
                expectedArguments.add(canonicalSignature.getArgumentTypes().get(index));
                expectedNullability.add(canonicalNullability.isArgumentNullable(index));
            }
        }
        checkArgument(remainingSignature.getReturnType().equals(canonicalSignature.getReturnType()) && remainingSignature.getArgumentTypes().equals(expectedArguments),
                "Constant-specialized signature %s must equal canonical signature %s with arguments %s removed",
                remainingSignature,
                canonicalSignature,
                constantPositions);
        checkArgument(implementations.getFunctionNullability().isReturnNullable() == canonicalNullability.isReturnNullable() && implementations.getFunctionNullability().getArgumentNullable().equals(expectedNullability),
                "Constant-specialized nullability must equal canonical nullability with arguments %s removed",
                constantPositions);

        return new ParametricScalarConstantSpecialization(constantPositions, implementations, canonicalNullability);
    }

    private static List<Method> findImplementationMethods(Class<?> implementationClass)
    {
        List<Method> methods = FunctionsParserHelper.findPublicMethodsWithAnnotation(implementationClass, SqlType.class, ScalarFunction.class, ScalarOperator.class);
        checkCondition(!methods.isEmpty(), FUNCTION_IMPLEMENTATION_ERROR, "Implementation class [%s] does not have any annotated methods", implementationClass.getName());
        for (Method method : methods) {
            checkArgument(method.getAnnotation(ScalarFunction.class) == null, "Nested implementation method [%s] is annotated with @ScalarFunction", method);
            checkArgument(method.getAnnotation(ScalarOperator.class) == null, "Nested implementation method [%s] is annotated with @ScalarOperator", method);
        }
        return methods;
    }

    private static void validateNestedImplementationClass(Class<?> implementationClass)
    {
        checkArgument(Modifier.isPublic(implementationClass.getModifiers()) && Modifier.isStatic(implementationClass.getModifiers()),
                "Nested implementation class [%s] must be public and static",
                implementationClass.getName());
    }

    private record ScalarHeaderAndMethods(ScalarHeader header, List<Method> methods)
    {
        private ScalarHeaderAndMethods
        {
            requireNonNull(header);
            requireNonNull(methods);
        }
    }
}
