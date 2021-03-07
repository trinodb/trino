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
package io.trino.operator.annotations;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.LongVariableConstraint;
import io.trino.metadata.Signature;
import io.trino.metadata.TypeVariableConstraint;
import io.trino.spi.function.Description;
import io.trino.spi.function.IsNull;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.function.TypeParameterSpecialization;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.type.Constraint;

import javax.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.ImmutableSortedSet.toImmutableSortedSet;
import static io.trino.operator.annotations.ImplementationDependency.isImplementationDependencyAnnotation;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_FIRST;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;
import static java.lang.String.CASE_INSENSITIVE_ORDER;

public final class FunctionsParserHelper
{
    private static final Set<OperatorType> COMPARABLE_TYPE_OPERATORS = ImmutableSet.of(EQUAL, HASH_CODE, XX_HASH_64, IS_DISTINCT_FROM, INDETERMINATE);
    private static final Set<OperatorType> ORDERABLE_TYPE_OPERATORS = ImmutableSet.of(COMPARISON_UNORDERED_LAST, COMPARISON_UNORDERED_FIRST, LESS_THAN, LESS_THAN_OR_EQUAL);

    private FunctionsParserHelper()
    {}

    public static boolean containsAnnotation(Annotation[] annotations, Predicate<Annotation> predicate)
    {
        return Arrays.stream(annotations).anyMatch(predicate);
    }

    public static boolean containsImplementationDependencyAnnotation(Annotation[] annotations)
    {
        return containsAnnotation(annotations, ImplementationDependency::isImplementationDependencyAnnotation);
    }

    public static List<TypeVariableConstraint> createTypeVariableConstraints(Collection<TypeParameter> typeParameters, List<ImplementationDependency> dependencies)
    {
        Set<String> typeParameterNames = typeParameters.stream()
                .map(TypeParameter::value)
                .collect(toImmutableSortedSet(CASE_INSENSITIVE_ORDER));

        Set<String> orderableRequired = new TreeSet<>(CASE_INSENSITIVE_ORDER);
        Set<String> comparableRequired = new TreeSet<>(CASE_INSENSITIVE_ORDER);
        HashMultimap<String, String> castableTo = HashMultimap.create();
        HashMultimap<String, String> castableFrom = HashMultimap.create();
        for (ImplementationDependency dependency : dependencies) {
            if (dependency instanceof OperatorImplementationDependency) {
                OperatorImplementationDependency operatorDependency = (OperatorImplementationDependency) dependency;
                OperatorType operator = operatorDependency.getOperator();
                List<TypeSignature> argumentTypes = operatorDependency.getArgumentTypes();
                if (COMPARABLE_TYPE_OPERATORS.contains(operator)) {
                    verifyOperatorSignature(operator, argumentTypes);
                    TypeSignature typeSignature = argumentTypes.get(0);
                    if (typeParameterNames.contains(typeSignature.getBase())) {
                        comparableRequired.add(typeSignature.toString());
                    }
                    else {
                        verifyTypeSignatureDoesNotContainAnyTypeParameters(typeSignature, typeSignature, typeParameterNames);
                    }
                }
                else if (ORDERABLE_TYPE_OPERATORS.contains(operator)) {
                    verifyOperatorSignature(operator, argumentTypes);
                    TypeSignature typeSignature = argumentTypes.get(0);
                    if (typeParameterNames.contains(typeSignature.getBase())) {
                        orderableRequired.add(typeSignature.toString());
                    }
                    else {
                        verifyTypeSignatureDoesNotContainAnyTypeParameters(typeSignature, typeSignature, typeParameterNames);
                    }
                }
                else {
                    throw new IllegalArgumentException("Operator dependency on " + operator + " is not allowed");
                }
            }
            else if (dependency instanceof CastImplementationDependency) {
                CastImplementationDependency castImplementationDependency = (CastImplementationDependency) dependency;

                TypeSignature fromType = castImplementationDependency.getFromType();
                TypeSignature toType = castImplementationDependency.getToType();
                if (typeParameterNames.contains(fromType.getBase())) {
                    // fromType is a type parameter, so it must be castable to the toType, which might also be a type parameter
                    castableTo.put(fromType.toString().toLowerCase(Locale.ENGLISH), toType.toString());
                }
                else if (typeParameterNames.contains(toType.getBase())) {
                    // toType is a type parameter, so it must be castable from the toType, which is not a type parameter
                    castableFrom.put(toType.toString().toLowerCase(Locale.ENGLISH), fromType.toString());
                }
                else {
                    verifyTypeSignatureDoesNotContainAnyTypeParameters(fromType, fromType, typeParameterNames);
                    verifyTypeSignatureDoesNotContainAnyTypeParameters(toType, toType, typeParameterNames);
                }
            }
        }

        ImmutableList.Builder<TypeVariableConstraint> typeVariableConstraints = ImmutableList.builder();
        for (String name : typeParameterNames) {
            typeVariableConstraints.add(new TypeVariableConstraint(
                    name,
                    comparableRequired.contains(name),
                    orderableRequired.contains(name),
                    null,
                    castableTo.get(name).stream()
                            .map(type -> parseTypeSignature(type, typeParameterNames))
                            .collect(toImmutableSet()),
                    castableFrom.get(name).stream()
                            .map(type -> parseTypeSignature(type, typeParameterNames))
                            .collect(toImmutableSet())));
        }
        return typeVariableConstraints.build();
    }

    private static void verifyOperatorSignature(OperatorType operator, List<TypeSignature> argumentTypes)
    {
        checkArgument(argumentTypes.size() == operator.getArgumentCount() && argumentTypes.stream().distinct().count() == 1,
                "%s requires %s arguments of the same type",
                operator,
                operator.getArgumentCount());
    }

    private static void verifyTypeSignatureDoesNotContainAnyTypeParameters(TypeSignature rootType, TypeSignature typeSignature, Set<String> typeParameterNames)
    {
        checkArgument(!typeParameterNames.contains(typeSignature.getBase()), "Nested type variables are not allowed: %s", rootType);

        for (TypeSignatureParameter parameter : typeSignature.getParameters()) {
            switch (parameter.getKind()) {
                case TYPE:
                    verifyTypeSignatureDoesNotContainAnyTypeParameters(rootType, parameter.getTypeSignature(), typeParameterNames);
                    break;
                case NAMED_TYPE:
                    verifyTypeSignatureDoesNotContainAnyTypeParameters(rootType, parameter.getNamedTypeSignature().getTypeSignature(), typeParameterNames);
                    break;
                case LONG:
                case VARIABLE:
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    public static void validateSignaturesCompatibility(Optional<Signature> signatureOld, Signature signatureNew)
    {
        if (signatureOld.isEmpty()) {
            return;
        }
        checkArgument(signatureOld.get().equals(signatureNew), "Implementations with type parameters must all have matching signatures. %s does not match %s", signatureOld.get(), signatureNew);
    }

    public static List<Method> findPublicStaticMethodsWithAnnotation(Class<?> clazz, Class<?> annotationClass)
    {
        ImmutableList.Builder<Method> methods = ImmutableList.builder();
        for (Method method : clazz.getMethods()) {
            for (Annotation annotation : method.getAnnotations()) {
                if (annotationClass.isInstance(annotation)) {
                    checkArgument(Modifier.isStatic(method.getModifiers()) && Modifier.isPublic(method.getModifiers()), "%s annotated with %s must be static and public", method.getName(), annotationClass.getSimpleName());
                    methods.add(method);
                }
            }
        }
        return methods.build();
    }

    @SafeVarargs
    public static Set<Method> findPublicMethodsWithAnnotation(Class<?> clazz, Class<? extends Annotation>... annotationClasses)
    {
        ImmutableSet.Builder<Method> methods = ImmutableSet.builder();
        for (Method method : clazz.getDeclaredMethods()) {
            for (Annotation annotation : method.getAnnotations()) {
                for (Class<?> annotationClass : annotationClasses) {
                    if (annotationClass.isInstance(annotation)) {
                        checkArgument(Modifier.isPublic(method.getModifiers()), "Method [%s] annotated with @%s must be public", method, annotationClass.getSimpleName());
                        methods.add(method);
                    }
                }
            }
        }
        return methods.build();
    }

    public static Optional<Constructor<?>> findConstructor(Class<?> clazz)
    {
        Constructor<?>[] constructors = clazz.getConstructors();
        checkArgument(constructors.length <= 1, "Class [%s] must have no more than 1 public constructor", clazz.getName());
        if (constructors.length == 0) {
            return Optional.empty();
        }
        return Optional.of(constructors[0]);
    }

    public static Set<String> parseLiteralParameters(Method method)
    {
        LiteralParameters literalParametersAnnotation = method.getAnnotation(LiteralParameters.class);
        if (literalParametersAnnotation == null) {
            return ImmutableSet.of();
        }

        Set<String> result = new TreeSet<>(CASE_INSENSITIVE_ORDER);
        result.addAll(Arrays.asList(literalParametersAnnotation.value()));
        return result;
    }

    public static boolean containsLegacyNullable(Annotation[] annotations)
    {
        return Arrays.stream(annotations)
                .map(Annotation::annotationType)
                .map(Class::getName)
                .anyMatch(name -> name.equals(Nullable.class.getName()));
    }

    public static boolean isTrinoAnnotation(Annotation annotation)
    {
        return isImplementationDependencyAnnotation(annotation) ||
                annotation instanceof SqlType ||
                annotation instanceof SqlNullable ||
                annotation instanceof IsNull;
    }

    public static Optional<String> parseDescription(AnnotatedElement base, AnnotatedElement override)
    {
        Optional<String> overrideDescription = parseDescription(override);
        if (overrideDescription.isPresent()) {
            return overrideDescription;
        }

        return parseDescription(base);
    }

    public static Optional<String> parseDescription(AnnotatedElement base)
    {
        Description description = base.getAnnotation(Description.class);
        return (description == null) ? Optional.empty() : Optional.of(description.value());
    }

    public static List<LongVariableConstraint> parseLongVariableConstraints(Method inputFunction)
    {
        return Stream.of(inputFunction.getAnnotationsByType(Constraint.class))
                .map(annotation -> new LongVariableConstraint(annotation.variable(), annotation.expression()))
                .collect(toImmutableList());
    }

    public static Map<String, Class<?>> getDeclaredSpecializedTypeParameters(Method method, Set<TypeParameter> typeParameters)
    {
        Map<String, Class<?>> specializedTypeParameters = new HashMap<>();
        TypeParameterSpecialization[] typeParameterSpecializations = method.getAnnotationsByType(TypeParameterSpecialization.class);
        ImmutableSet<String> typeParameterNames = typeParameters.stream()
                .map(TypeParameter::value)
                .collect(toImmutableSet());
        for (TypeParameterSpecialization specialization : typeParameterSpecializations) {
            checkArgument(typeParameterNames.contains(specialization.name()), "%s does not match any declared type parameters (%s) [%s]", specialization.name(), typeParameters, method);
            Class<?> existingSpecialization = specializedTypeParameters.get(specialization.name());
            checkArgument(existingSpecialization == null || existingSpecialization.equals(specialization.nativeContainerType()),
                    "%s has conflicting specializations %s and %s [%s]", specialization.name(), existingSpecialization, specialization.nativeContainerType(), method);
            specializedTypeParameters.put(specialization.name(), specialization.nativeContainerType());
        }
        return specializedTypeParameters;
    }
}
