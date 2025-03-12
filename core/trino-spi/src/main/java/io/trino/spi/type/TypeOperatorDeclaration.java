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
package io.trino.spi.type;

import com.google.errorprone.annotations.FormatMethod;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.FlatFixed;
import io.trino.spi.function.FlatFixedOffset;
import io.trino.spi.function.FlatVariableOffset;
import io.trino.spi.function.FlatVariableWidth;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.trino.spi.function.InvocationConvention.InvocationReturnConvention;
import io.trino.spi.function.IsNull;
import io.trino.spi.function.OperatorMethodHandle;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlNullable;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static java.lang.String.format;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;

public final class TypeOperatorDeclaration
{
    public static final TypeOperatorDeclaration NO_TYPE_OPERATOR_DECLARATION = builder(boolean.class).build();

    private final Collection<OperatorMethodHandle> readValueOperators;
    private final Collection<OperatorMethodHandle> equalOperators;
    private final Collection<OperatorMethodHandle> hashCodeOperators;
    private final Collection<OperatorMethodHandle> xxHash64Operators;
    private final Collection<OperatorMethodHandle> identicalOperators;
    private final Collection<OperatorMethodHandle> indeterminateOperators;
    private final Collection<OperatorMethodHandle> comparisonUnorderedLastOperators;
    private final Collection<OperatorMethodHandle> comparisonUnorderedFirstOperators;
    private final Collection<OperatorMethodHandle> lessThanOperators;
    private final Collection<OperatorMethodHandle> lessThanOrEqualOperators;

    private TypeOperatorDeclaration(
            Collection<OperatorMethodHandle> readValueOperators,
            Collection<OperatorMethodHandle> equalOperators,
            Collection<OperatorMethodHandle> hashCodeOperators,
            Collection<OperatorMethodHandle> xxHash64Operators,
            Collection<OperatorMethodHandle> identicalOperators,
            Collection<OperatorMethodHandle> indeterminateOperators,
            Collection<OperatorMethodHandle> comparisonUnorderedLastOperators,
            Collection<OperatorMethodHandle> comparisonUnorderedFirstOperators,
            Collection<OperatorMethodHandle> lessThanOperators,
            Collection<OperatorMethodHandle> lessThanOrEqualOperators)
    {
        this.readValueOperators = List.copyOf(requireNonNull(readValueOperators, "readValueOperators is null"));
        this.equalOperators = List.copyOf(requireNonNull(equalOperators, "equalOperators is null"));
        this.hashCodeOperators = List.copyOf(requireNonNull(hashCodeOperators, "hashCodeOperators is null"));
        this.xxHash64Operators = List.copyOf(requireNonNull(xxHash64Operators, "xxHash64Operators is null"));
        this.identicalOperators = List.copyOf(requireNonNull(identicalOperators, "identicalOperators is null"));
        this.indeterminateOperators = List.copyOf(requireNonNull(indeterminateOperators, "indeterminateOperators is null"));
        this.comparisonUnorderedLastOperators = List.copyOf(requireNonNull(comparisonUnorderedLastOperators, "comparisonUnorderedLastOperators is null"));
        this.comparisonUnorderedFirstOperators = List.copyOf(requireNonNull(comparisonUnorderedFirstOperators, "comparisonUnorderedFirstOperators is null"));
        this.lessThanOperators = List.copyOf(requireNonNull(lessThanOperators, "lessThanOperators is null"));
        this.lessThanOrEqualOperators = List.copyOf(requireNonNull(lessThanOrEqualOperators, "lessThanOrEqualOperators is null"));
    }

    public boolean isComparable()
    {
        return !equalOperators.isEmpty();
    }

    public boolean isOrderable()
    {
        return !comparisonUnorderedLastOperators.isEmpty();
    }

    public Collection<OperatorMethodHandle> getReadValueOperators()
    {
        return readValueOperators;
    }

    public Collection<OperatorMethodHandle> getEqualOperators()
    {
        return equalOperators;
    }

    public Collection<OperatorMethodHandle> getHashCodeOperators()
    {
        return hashCodeOperators;
    }

    public Collection<OperatorMethodHandle> getXxHash64Operators()
    {
        return xxHash64Operators;
    }

    public Collection<OperatorMethodHandle> getIdenticalOperators()
    {
        return identicalOperators;
    }

    public Collection<OperatorMethodHandle> getIndeterminateOperators()
    {
        return indeterminateOperators;
    }

    public Collection<OperatorMethodHandle> getComparisonUnorderedLastOperators()
    {
        return comparisonUnorderedLastOperators;
    }

    public Collection<OperatorMethodHandle> getComparisonUnorderedFirstOperators()
    {
        return comparisonUnorderedFirstOperators;
    }

    public Collection<OperatorMethodHandle> getLessThanOperators()
    {
        return lessThanOperators;
    }

    public Collection<OperatorMethodHandle> getLessThanOrEqualOperators()
    {
        return lessThanOrEqualOperators;
    }

    public static Builder builder(Class<?> typeJavaType)
    {
        return new Builder(typeJavaType);
    }

    public static TypeOperatorDeclaration extractOperatorDeclaration(Class<?> operatorsClass, Lookup lookup, Class<?> typeJavaType)
    {
        return new Builder(typeJavaType)
                .addOperators(operatorsClass, lookup)
                .build();
    }

    public static class Builder
    {
        private final Class<?> typeJavaType;

        private final Collection<OperatorMethodHandle> readValueOperators = new ArrayList<>();
        private final Collection<OperatorMethodHandle> equalOperators = new ArrayList<>();
        private final Collection<OperatorMethodHandle> hashCodeOperators = new ArrayList<>();
        private final Collection<OperatorMethodHandle> xxHash64Operators = new ArrayList<>();
        private final Collection<OperatorMethodHandle> identicalOperators = new ArrayList<>();
        private final Collection<OperatorMethodHandle> indeterminateOperators = new ArrayList<>();
        private final Collection<OperatorMethodHandle> comparisonUnorderedLastOperators = new ArrayList<>();
        private final Collection<OperatorMethodHandle> comparisonUnorderedFirstOperators = new ArrayList<>();
        private final Collection<OperatorMethodHandle> lessThanOperators = new ArrayList<>();
        private final Collection<OperatorMethodHandle> lessThanOrEqualOperators = new ArrayList<>();

        private Builder(Class<?> typeJavaType)
        {
            this.typeJavaType = requireNonNull(typeJavaType, "typeJavaType is null");
            checkArgument(!typeJavaType.equals(void.class), "void type is not supported");
        }

        public Builder addOperators(TypeOperatorDeclaration operatorDeclaration)
        {
            operatorDeclaration.getReadValueOperators().forEach(this::addReadValueOperator);
            operatorDeclaration.getEqualOperators().forEach(this::addEqualOperator);
            operatorDeclaration.getHashCodeOperators().forEach(this::addHashCodeOperator);
            operatorDeclaration.getXxHash64Operators().forEach(this::addXxHash64Operator);
            operatorDeclaration.getIdenticalOperators().forEach(this::addIdenticalOperator);
            operatorDeclaration.getIndeterminateOperators().forEach(this::addIndeterminateOperator);
            operatorDeclaration.getComparisonUnorderedLastOperators().forEach(this::addComparisonUnorderedLastOperator);
            operatorDeclaration.getComparisonUnorderedFirstOperators().forEach(this::addComparisonUnorderedFirstOperator);
            operatorDeclaration.getLessThanOperators().forEach(this::addLessThanOperator);
            operatorDeclaration.getLessThanOrEqualOperators().forEach(this::addLessThanOrEqualOperator);
            return this;
        }

        public Builder addReadValueOperator(OperatorMethodHandle readValueOperator)
        {
            verifyMethodHandleSignature(1, typeJavaType, readValueOperator);
            this.readValueOperators.add(readValueOperator);
            return this;
        }

        public Builder addReadValueOperators(Collection<OperatorMethodHandle> readValueOperators)
        {
            for (OperatorMethodHandle readValueOperator : readValueOperators) {
                verifyMethodHandleSignature(1, typeJavaType, readValueOperator);
            }
            this.readValueOperators.addAll(readValueOperators);
            return this;
        }

        public Builder addEqualOperator(OperatorMethodHandle equalOperator)
        {
            verifyMethodHandleSignature(2, boolean.class, equalOperator);
            this.equalOperators.add(equalOperator);
            return this;
        }

        public Builder addEqualOperators(Collection<OperatorMethodHandle> equalOperators)
        {
            for (OperatorMethodHandle equalOperator : equalOperators) {
                verifyMethodHandleSignature(2, boolean.class, equalOperator);
            }
            this.equalOperators.addAll(equalOperators);
            return this;
        }

        public Builder addHashCodeOperator(OperatorMethodHandle hashCodeOperator)
        {
            verifyMethodHandleSignature(1, long.class, hashCodeOperator);
            this.hashCodeOperators.add(hashCodeOperator);
            return this;
        }

        public Builder addHashCodeOperators(Collection<OperatorMethodHandle> hashCodeOperators)
        {
            for (OperatorMethodHandle hashCodeOperator : hashCodeOperators) {
                verifyMethodHandleSignature(1, long.class, hashCodeOperator);
            }
            this.hashCodeOperators.addAll(hashCodeOperators);
            return this;
        }

        public Builder addXxHash64Operator(OperatorMethodHandle xxHash64Operator)
        {
            verifyMethodHandleSignature(1, long.class, xxHash64Operator);
            this.xxHash64Operators.add(xxHash64Operator);
            return this;
        }

        public Builder addXxHash64Operators(Collection<OperatorMethodHandle> xxHash64Operators)
        {
            for (OperatorMethodHandle xxHash64Operator : xxHash64Operators) {
                verifyMethodHandleSignature(1, long.class, xxHash64Operator);
            }
            this.xxHash64Operators.addAll(xxHash64Operators);
            return this;
        }

        public Builder addIdenticalOperator(OperatorMethodHandle operator)
        {
            verifyMethodHandleSignature(2, boolean.class, operator);
            this.identicalOperators.add(operator);
            return this;
        }

        public Builder addIdenticalOperators(Collection<OperatorMethodHandle> operators)
        {
            for (OperatorMethodHandle operator : operators) {
                verifyMethodHandleSignature(2, boolean.class, operator);
            }
            this.identicalOperators.addAll(operators);
            return this;
        }

        public Builder addIndeterminateOperator(OperatorMethodHandle indeterminateOperator)
        {
            verifyMethodHandleSignature(1, boolean.class, indeterminateOperator);
            this.indeterminateOperators.add(indeterminateOperator);
            return this;
        }

        public Builder addIndeterminateOperators(Collection<OperatorMethodHandle> indeterminateOperators)
        {
            for (OperatorMethodHandle indeterminateOperator : indeterminateOperators) {
                verifyMethodHandleSignature(1, boolean.class, indeterminateOperator);
            }
            this.indeterminateOperators.addAll(indeterminateOperators);
            return this;
        }

        public Builder addComparisonUnorderedLastOperator(OperatorMethodHandle comparisonOperator)
        {
            verifyMethodHandleSignature(2, long.class, comparisonOperator);
            this.comparisonUnorderedLastOperators.add(comparisonOperator);
            return this;
        }

        public Builder addComparisonUnorderedLastOperators(Collection<OperatorMethodHandle> comparisonOperators)
        {
            for (OperatorMethodHandle comparisonOperator : comparisonOperators) {
                verifyMethodHandleSignature(2, long.class, comparisonOperator);
            }
            this.comparisonUnorderedLastOperators.addAll(comparisonOperators);
            return this;
        }

        public Builder addComparisonUnorderedFirstOperator(OperatorMethodHandle comparisonOperator)
        {
            verifyMethodHandleSignature(2, long.class, comparisonOperator);
            this.comparisonUnorderedFirstOperators.add(comparisonOperator);
            return this;
        }

        public Builder addComparisonUnorderedFirstOperators(Collection<OperatorMethodHandle> comparisonOperators)
        {
            for (OperatorMethodHandle comparisonOperator : comparisonOperators) {
                verifyMethodHandleSignature(2, long.class, comparisonOperator);
            }
            this.comparisonUnorderedFirstOperators.addAll(comparisonOperators);
            return this;
        }

        public Builder addLessThanOrEqualOperator(OperatorMethodHandle lessThanOrEqualOperator)
        {
            verifyMethodHandleSignature(2, boolean.class, lessThanOrEqualOperator);
            this.lessThanOrEqualOperators.add(lessThanOrEqualOperator);
            return this;
        }

        public Builder addLessThanOrEqualOperators(Collection<OperatorMethodHandle> lessThanOrEqualOperators)
        {
            for (OperatorMethodHandle lessThanOrEqualOperator : lessThanOrEqualOperators) {
                verifyMethodHandleSignature(2, boolean.class, lessThanOrEqualOperator);
            }
            this.lessThanOrEqualOperators.addAll(lessThanOrEqualOperators);
            return this;
        }

        public Builder addLessThanOperator(OperatorMethodHandle lessThanOperator)
        {
            verifyMethodHandleSignature(2, boolean.class, lessThanOperator);
            this.lessThanOperators.add(lessThanOperator);
            return this;
        }

        public Builder addLessThanOperators(Collection<OperatorMethodHandle> lessThanOperators)
        {
            for (OperatorMethodHandle lessThanOperator : lessThanOperators) {
                verifyMethodHandleSignature(2, boolean.class, lessThanOperator);
            }
            this.lessThanOperators.addAll(lessThanOperators);
            return this;
        }

        public Builder addOperators(Class<?> operatorsClass, Lookup lookup)
        {
            boolean addedOperator = false;
            for (Method method : operatorsClass.getDeclaredMethods()) {
                ScalarOperator scalarOperator = method.getAnnotation(ScalarOperator.class);
                if (scalarOperator == null) {
                    continue;
                }
                OperatorType operatorType = scalarOperator.value();

                MethodHandle methodHandle;
                try {
                    methodHandle = lookup.unreflect(method);
                }
                catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

                switch (operatorType) {
                    case READ_VALUE:
                        addReadValueOperator(new OperatorMethodHandle(parseInvocationConvention(operatorType, typeJavaType, method, typeJavaType), methodHandle));
                        break;
                    case EQUAL:
                        addEqualOperator(new OperatorMethodHandle(parseInvocationConvention(operatorType, typeJavaType, method, boolean.class), methodHandle));
                        break;
                    case HASH_CODE:
                        addHashCodeOperator(new OperatorMethodHandle(parseInvocationConvention(operatorType, typeJavaType, method, long.class), methodHandle));
                        break;
                    case XX_HASH_64:
                        addXxHash64Operator(new OperatorMethodHandle(parseInvocationConvention(operatorType, typeJavaType, method, long.class), methodHandle));
                        break;
                    case IDENTICAL:
                        addIdenticalOperator(new OperatorMethodHandle(parseInvocationConvention(operatorType, typeJavaType, method, boolean.class), methodHandle));
                        break;
                    case INDETERMINATE:
                        addIndeterminateOperator(new OperatorMethodHandle(parseInvocationConvention(operatorType, typeJavaType, method, boolean.class), methodHandle));
                        break;
                    case COMPARISON_UNORDERED_LAST:
                        addComparisonUnorderedLastOperator(new OperatorMethodHandle(parseInvocationConvention(operatorType, typeJavaType, method, long.class), methodHandle));
                        break;
                    case COMPARISON_UNORDERED_FIRST:
                        addComparisonUnorderedFirstOperator(new OperatorMethodHandle(parseInvocationConvention(operatorType, typeJavaType, method, long.class), methodHandle));
                        break;
                    case LESS_THAN:
                        addLessThanOperator(new OperatorMethodHandle(parseInvocationConvention(operatorType, typeJavaType, method, boolean.class), methodHandle));
                        break;
                    case LESS_THAN_OR_EQUAL:
                        addLessThanOrEqualOperator(new OperatorMethodHandle(parseInvocationConvention(operatorType, typeJavaType, method, boolean.class), methodHandle));
                        break;
                    default:
                        throw new IllegalArgumentException(operatorType + " operator is not supported: " + method);
                }
                addedOperator = true;
            }
            if (!addedOperator) {
                throw new IllegalArgumentException(operatorsClass + " does not contain any operators");
            }
            return this;
        }

        private void verifyMethodHandleSignature(int expectedArgumentCount, Class<?> returnJavaType, OperatorMethodHandle operatorMethodHandle)
        {
            MethodType methodType = operatorMethodHandle.getMethodHandle().type();
            InvocationConvention convention = operatorMethodHandle.getCallingConvention();

            checkArgument(convention.getArgumentConventions().size() == expectedArgumentCount,
                    "Expected %s arguments, but got %s", expectedArgumentCount, convention.getArgumentConventions().size());

            checkArgument(methodType.parameterList().stream().noneMatch(ConnectorSession.class::equals),
                    "Session is not supported in type operators");

            int expectedParameterCount = convention.getArgumentConventions().stream()
                    .mapToInt(InvocationArgumentConvention::getParameterCount)
                    .sum();
            expectedParameterCount += convention.getReturnConvention().getParameterCount();
            checkArgument(expectedParameterCount == methodType.parameterCount(),
                    "Expected %s method parameters, but got %s", expectedParameterCount, methodType.parameterCount());

            int parameterIndex = 0;
            for (InvocationArgumentConvention argumentConvention : convention.getArgumentConventions()) {
                Class<?> parameterType = methodType.parameterType(parameterIndex);
                checkArgument(!parameterType.equals(ConnectorSession.class), "Session is not supported in type operators");
                switch (argumentConvention) {
                    case NEVER_NULL:
                        checkArgument(parameterType.isAssignableFrom(typeJavaType),
                                "Expected argument type to be %s, but is %s", typeJavaType, parameterType);
                        break;
                    case NULL_FLAG:
                        checkArgument(parameterType.isAssignableFrom(typeJavaType),
                                "Expected argument type to be %s, but is %s", typeJavaType, parameterType);
                        checkArgument(methodType.parameterType(parameterIndex + 1).equals(boolean.class),
                                "Expected null flag parameter to be followed by a boolean parameter");
                        break;
                    case BOXED_NULLABLE:
                        checkArgument(parameterType.isAssignableFrom(wrap(typeJavaType)),
                                "Expected argument type to be %s, but is %s", wrap(typeJavaType), parameterType);
                        break;
                    case BLOCK_POSITION_NOT_NULL:
                    case BLOCK_POSITION:
                        checkArgument(parameterType.equals(Block.class) && methodType.parameterType(parameterIndex + 1).equals(int.class),
                                "Expected BLOCK_POSITION argument to have parameters Block and int");
                        break;
                    case VALUE_BLOCK_POSITION_NOT_NULL:
                    case VALUE_BLOCK_POSITION:
                        checkArgument(Block.class.isAssignableFrom(parameterType) && methodType.parameterType(parameterIndex + 1).equals(int.class),
                                "Expected VALUE_BLOCK_POSITION argument to have parameters ValueBlock and int");
                        break;
                    case FLAT:
                        checkArgument(parameterType.equals(byte[].class) &&
                                        methodType.parameterType(parameterIndex + 1).equals(int.class) &&
                                        methodType.parameterType(parameterIndex + 2).equals(byte[].class) &&
                                        methodType.parameterType(parameterIndex + 3).equals(int.class),
                                "Expected FLAT argument to have parameters byte[], int, byte[], and int");
                        break;
                    case FUNCTION:
                        throw new IllegalArgumentException("Function argument convention is not supported in type operators");
                    default:
                        throw new UnsupportedOperationException("Unknown argument convention: " + argumentConvention);
                }
                parameterIndex += argumentConvention.getParameterCount();
            }

            InvocationReturnConvention returnConvention = convention.getReturnConvention();
            switch (returnConvention) {
                case FAIL_ON_NULL:
                    checkArgument(methodType.returnType().equals(returnJavaType),
                            "Expected return type to be %s, but is %s", returnJavaType, methodType.returnType());
                    break;
                case NULLABLE_RETURN:
                    checkArgument(methodType.returnType().equals(wrap(returnJavaType)),
                            "Expected return type to be %s, but is %s", returnJavaType, wrap(methodType.returnType()));
                    break;
                case BLOCK_BUILDER:
                    checkArgument(methodType.lastParameterType().equals(BlockBuilder.class),
                            "Expected last argument type to be BlockBuilder, but is %s", methodType.returnType());
                    checkArgument(methodType.returnType().equals(void.class),
                            "Expected return type to be void, but is %s", methodType.returnType());
                    break;
                case FLAT_RETURN:
                    List<Class<?>> parameters = methodType.parameterList();
                    parameters = parameters.subList(parameters.size() - 4, parameters.size());
                    checkArgument(
                            parameters.equals(List.of(byte[].class, int.class, byte[].class, int.class)),
                            "Expected last argument types to be (byte[], int, byte[], int), but is %s", methodType);
                    checkArgument(methodType.returnType().equals(void.class),
                            "Expected return type to be void, but is %s", methodType.returnType());
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown return convention: " + returnConvention);
            }

            if (operatorMethodHandle.getCallingConvention().getArgumentConventions().stream().anyMatch(argumentConvention -> argumentConvention == BLOCK_POSITION || argumentConvention == BLOCK_POSITION_NOT_NULL)) {
                throw new IllegalArgumentException("BLOCK_POSITION argument convention is not allowed for type operators");
            }
        }

        private static InvocationConvention parseInvocationConvention(OperatorType operatorType, Class<?> typeJavaType, Method method, Class<?> expectedReturnType)
        {
            InvocationReturnConvention returnConvention = getReturnConvention(expectedReturnType, operatorType, method);

            List<Class<?>> parameterTypes = List.of(method.getParameterTypes());
            List<Annotation[]> parameterAnnotations = List.of(method.getParameterAnnotations());
            parameterTypes = parameterTypes.subList(0, parameterTypes.size() - returnConvention.getParameterCount());
            parameterAnnotations = parameterAnnotations.subList(0, parameterAnnotations.size() - returnConvention.getParameterCount());

            InvocationArgumentConvention leftArgumentConvention = extractNextArgumentConvention(typeJavaType, parameterTypes, parameterAnnotations, operatorType, method);
            if (leftArgumentConvention.getParameterCount() == parameterTypes.size()) {
                return simpleConvention(returnConvention, leftArgumentConvention);
            }

            InvocationArgumentConvention rightArgumentConvention = extractNextArgumentConvention(
                    typeJavaType,
                    parameterTypes.subList(leftArgumentConvention.getParameterCount(), parameterTypes.size()),
                    parameterAnnotations.subList(leftArgumentConvention.getParameterCount(), parameterTypes.size()),
                    operatorType,
                    method);

            checkArgument(leftArgumentConvention.getParameterCount() + rightArgumentConvention.getParameterCount() == parameterTypes.size(),
                    "Unexpected parameters for %s operator: %s", operatorType, method);

            return simpleConvention(returnConvention, leftArgumentConvention, rightArgumentConvention);
        }

        private static boolean isAnnotationPresent(Annotation[] annotations, Class<? extends Annotation> annotationType)
        {
            return Arrays.stream(annotations).anyMatch(annotationType::isInstance);
        }

        private static InvocationReturnConvention getReturnConvention(Class<?> expectedReturnType, OperatorType operatorType, Method method)
        {
            InvocationReturnConvention returnConvention;
            if (!method.isAnnotationPresent(SqlNullable.class) && method.getReturnType().equals(expectedReturnType)) {
                returnConvention = FAIL_ON_NULL;
            }
            else if (method.isAnnotationPresent(SqlNullable.class) && method.getReturnType().equals(wrap(expectedReturnType))) {
                returnConvention = NULLABLE_RETURN;
            }
            else if (method.getReturnType().equals(void.class) &&
                    method.getParameterCount() >= 1 &&
                    method.getParameterTypes()[method.getParameterCount() - 1].equals(BlockBuilder.class)) {
                returnConvention = BLOCK_BUILDER;
            }
            else if (method.getReturnType().equals(void.class) &&
                    method.getParameterCount() >= 4 &&
                    method.getParameterTypes()[method.getParameterCount() - 4].equals(byte[].class) &&
                    method.getParameterTypes()[method.getParameterCount() - 3].equals(int.class) &&
                    method.getParameterTypes()[method.getParameterCount() - 2].equals(byte[].class) &&
                    method.getParameterTypes()[method.getParameterCount() - 1].equals(int.class)) {
                returnConvention = FLAT_RETURN;
            }
            else {
                throw new IllegalArgumentException(format("Expected %s operator to return %s: %s", operatorType, expectedReturnType, method));
            }
            return returnConvention;
        }

        private static InvocationArgumentConvention extractNextArgumentConvention(
                Class<?> typeJavaType,
                List<Class<?>> parameterTypes,
                List<Annotation[]> parameterAnnotations,
                OperatorType operatorType,
                Method method)
        {
            if (isAnnotationPresent(parameterAnnotations.get(0), BlockPosition.class)) {
                if (parameterTypes.size() > 1 && isAnnotationPresent(parameterAnnotations.get(1), BlockIndex.class)) {
                    if (!ValueBlock.class.isAssignableFrom(parameterTypes.get(0))) {
                        throw new IllegalArgumentException("@BlockPosition argument must be a ValueBlock type for %s operator: %s".formatted(operatorType, method));
                    }
                    if (parameterTypes.get(1) != int.class) {
                        throw new IllegalArgumentException("@BlockIndex argument must be type int for %s operator: %s".formatted(operatorType, method));
                    }
                    return isAnnotationPresent(parameterAnnotations.get(0), SqlNullable.class) ? VALUE_BLOCK_POSITION : VALUE_BLOCK_POSITION_NOT_NULL;
                }
            }
            else if (isAnnotationPresent(parameterAnnotations.get(0), SqlNullable.class)) {
                if (parameterTypes.get(0).equals(wrap(typeJavaType))) {
                    return BOXED_NULLABLE;
                }
            }
            else if (isAnnotationPresent(parameterAnnotations.get(0), FlatFixed.class)) {
                if (parameterTypes.size() > 3 &&
                        isAnnotationPresent(parameterAnnotations.get(1), FlatFixedOffset.class) &&
                        isAnnotationPresent(parameterAnnotations.get(2), FlatVariableWidth.class) &&
                        isAnnotationPresent(parameterAnnotations.get(3), FlatVariableOffset.class) &&
                        parameterTypes.get(0).equals(byte[].class) &&
                        parameterTypes.get(1).equals(int.class) &&
                        parameterTypes.get(2).equals(byte[].class) &&
                        parameterTypes.get(3).equals(int.class)) {
                    return FLAT;
                }
            }
            else if (parameterTypes.size() > 1 && isAnnotationPresent(parameterAnnotations.get(1), IsNull.class)) {
                if (parameterTypes.size() > 1 &&
                        parameterTypes.get(0).equals(typeJavaType) &&
                        parameterTypes.get(1).equals(boolean.class)) {
                    return NULL_FLAG;
                }
            }
            else {
                if (parameterTypes.get(0).equals(typeJavaType)) {
                    return NEVER_NULL;
                }
            }
            throw new IllegalArgumentException(format("Unexpected parameters for %s operator: %s", operatorType, method));
        }

        @FormatMethod
        private static void checkArgument(boolean test, String message, Object... arguments)
        {
            if (!test) {
                throw new IllegalArgumentException(format(message, arguments));
            }
        }

        private static Class<?> wrap(Class<?> type)
        {
            return methodType(type).wrap().returnType();
        }

        public TypeOperatorDeclaration build()
        {
            if (equalOperators.isEmpty()) {
                if (!hashCodeOperators.isEmpty()) {
                    throw new IllegalStateException("Hash code operators can not be supplied when equal operators are not supplied");
                }
                if (!xxHash64Operators.isEmpty()) {
                    throw new IllegalStateException("xxHash64 operators can not be supplied when equal operators are not supplied");
                }
            }
            else {
                if (xxHash64Operators.isEmpty()) {
                    throw new IllegalStateException("xxHash64 operators must be supplied when equal operators are supplied");
                }
            }
            if (comparisonUnorderedLastOperators.isEmpty() && comparisonUnorderedFirstOperators.isEmpty()) {
                if (!lessThanOperators.isEmpty()) {
                    throw new IllegalStateException("Less-than-operators can not be supplied when comparison operators are not supplied");
                }
                if (!lessThanOrEqualOperators.isEmpty()) {
                    throw new IllegalStateException("Less-than-or-equals operators can not be supplied when comparison operators are not supplied");
                }
            }

            return new TypeOperatorDeclaration(
                    readValueOperators,
                    equalOperators,
                    hashCodeOperators,
                    xxHash64Operators,
                    identicalOperators,
                    indeterminateOperators,
                    comparisonUnorderedLastOperators,
                    comparisonUnorderedFirstOperators,
                    lessThanOperators,
                    lessThanOrEqualOperators);
        }
    }
}
