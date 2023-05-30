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

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.trino.spi.function.InvocationConvention.InvocationReturnConvention;
import io.trino.spi.function.OperatorMethodHandle;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarFunctionAdapter;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_FIRST;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.collectArguments;
import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.filterReturnValue;
import static java.lang.invoke.MethodHandles.guardWithTest;
import static java.lang.invoke.MethodHandles.identity;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class TypeOperators
{
    private static final InvocationConvention READ_BLOCK_NOT_NULL_CALLING_CONVENTION = simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL);
    private static final InvocationConvention WRITE_BLOCK_CALLING_CONVENTION = simpleConvention(BLOCK_BUILDER, NEVER_NULL);

    private final BiFunction<Object, Supplier<Object>, Object> cache;

    public TypeOperators()
    {
        ConcurrentHashMap<Object, Object> cache = new ConcurrentHashMap<>();
        this.cache = (operatorConvention, supplier) -> {
            // preform explicit get before calling computeIfAbsent since computeIfAbsent cause lock contention
            Object operator = cache.get(operatorConvention);
            if (operator != null) {
                return operator;
            }
            return cache.computeIfAbsent(operatorConvention, key -> supplier.get());
        };
    }

    public TypeOperators(BiFunction<Object, Supplier<Object>, Object> cache)
    {
        this.cache = cache;
    }

    public MethodHandle getReadValueOperator(Type type, InvocationConvention callingConvention)
    {
        return getOperatorAdaptor(type, callingConvention, READ_VALUE).get();
    }

    public MethodHandle getEqualOperator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isComparable()) {
            throw new UnsupportedOperationException(type + " is not comparable");
        }
        return getOperatorAdaptor(type, callingConvention, EQUAL).get();
    }

    public MethodHandle getHashCodeOperator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isComparable()) {
            throw new UnsupportedOperationException(type + " is not comparable");
        }
        return getOperatorAdaptor(type, callingConvention, OperatorType.HASH_CODE).get();
    }

    public MethodHandle getXxHash64Operator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isComparable()) {
            throw new UnsupportedOperationException(type + " is not comparable");
        }
        return getOperatorAdaptor(type, callingConvention, OperatorType.XX_HASH_64).get();
    }

    public MethodHandle getDistinctFromOperator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isComparable()) {
            throw new UnsupportedOperationException(type + " is not comparable");
        }
        return getOperatorAdaptor(type, callingConvention, OperatorType.IS_DISTINCT_FROM).get();
    }

    public MethodHandle getIndeterminateOperator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isComparable()) {
            throw new UnsupportedOperationException(type + " is not comparable");
        }
        return getOperatorAdaptor(type, callingConvention, OperatorType.INDETERMINATE).get();
    }

    public MethodHandle getComparisonUnorderedLastOperator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isOrderable()) {
            throw new UnsupportedOperationException(type + " is not orderable");
        }
        return getOperatorAdaptor(type, callingConvention, COMPARISON_UNORDERED_LAST).get();
    }

    public MethodHandle getComparisonUnorderedFirstOperator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isOrderable()) {
            throw new UnsupportedOperationException(type + " is not orderable");
        }
        return getOperatorAdaptor(type, callingConvention, COMPARISON_UNORDERED_FIRST).get();
    }

    public MethodHandle getOrderingOperator(Type type, SortOrder sortOrder, InvocationConvention callingConvention)
    {
        if (!type.isOrderable()) {
            throw new UnsupportedOperationException(type + " is not orderable");
        }
        OperatorType comparisonType = sortOrder.isNullsFirst() ? COMPARISON_UNORDERED_FIRST : COMPARISON_UNORDERED_LAST;
        return getOperatorAdaptor(type, Optional.of(sortOrder), callingConvention, comparisonType).get();
    }

    public MethodHandle getLessThanOperator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isOrderable()) {
            throw new UnsupportedOperationException(type + " is not orderable");
        }
        return getOperatorAdaptor(type, callingConvention, LESS_THAN).get();
    }

    public MethodHandle getLessThanOrEqualOperator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isOrderable()) {
            throw new UnsupportedOperationException(type + " is not orderable");
        }
        return getOperatorAdaptor(type, callingConvention, LESS_THAN_OR_EQUAL).get();
    }

    private OperatorAdaptor getOperatorAdaptor(Type type, InvocationConvention callingConvention, OperatorType operatorType)
    {
        return getOperatorAdaptor(type, Optional.empty(), callingConvention, operatorType);
    }

    private OperatorAdaptor getOperatorAdaptor(Type type, Optional<SortOrder> sortOrder, InvocationConvention callingConvention, OperatorType operatorType)
    {
        OperatorConvention operatorConvention = new OperatorConvention(type, operatorType, sortOrder, callingConvention);
        return (OperatorAdaptor) cache.apply(operatorConvention, () -> new OperatorAdaptor(operatorConvention));
    }

    private class OperatorAdaptor
    {
        private final OperatorConvention operatorConvention;
        private MethodHandle adapted;

        public OperatorAdaptor(OperatorConvention operatorConvention)
        {
            this.operatorConvention = operatorConvention;
        }

        public synchronized MethodHandle get()
        {
            if (adapted == null) {
                adapted = adaptOperator(operatorConvention);
            }
            return adapted;
        }

        private MethodHandle adaptOperator(OperatorConvention operatorConvention)
        {
            OperatorMethodHandle operatorMethodHandle = selectOperatorMethodHandleToAdapt(operatorConvention);
            MethodHandle methodHandle = adaptOperator(operatorConvention, operatorMethodHandle);
            return methodHandle;
        }

        private static MethodHandle adaptOperator(OperatorConvention operatorConvention, OperatorMethodHandle operatorMethodHandle)
        {
            return ScalarFunctionAdapter.adapt(
                    operatorMethodHandle.getMethodHandle(),
                    getOperatorReturnType(operatorConvention),
                    getOperatorArgumentTypes(operatorConvention),
                    operatorMethodHandle.getCallingConvention(),
                    operatorConvention.callingConvention());
        }

        private OperatorMethodHandle selectOperatorMethodHandleToAdapt(OperatorConvention operatorConvention)
        {
            List<OperatorMethodHandle> operatorMethodHandles = getOperatorMethodHandles(operatorConvention).stream()
                    .sorted(Comparator.comparing(TypeOperators::getScore).reversed())
                    .toList();

            // if a method handle exists for the exact convention, use it
            for (OperatorMethodHandle operatorMethodHandle : operatorMethodHandles) {
                if (operatorMethodHandle.getCallingConvention().equals(operatorConvention.callingConvention())) {
                    return operatorMethodHandle;
                }
            }

            for (OperatorMethodHandle operatorMethodHandle : operatorMethodHandles) {
                if (ScalarFunctionAdapter.canAdapt(operatorMethodHandle.getCallingConvention(), operatorConvention.callingConvention())) {
                    return operatorMethodHandle;
                }
            }

            throw new TrinoException(FUNCTION_NOT_FOUND, format(
                    "%s %s operator can not be adapted to convention (%s). Available implementations: %s",
                    operatorConvention.type(),
                    operatorConvention.operatorType(),
                    operatorConvention.callingConvention(),
                    operatorMethodHandles.stream()
                            .map(OperatorMethodHandle::getCallingConvention)
                            .map(Object::toString)
                            .collect(joining(", ", "[", "]"))));
        }

        private Collection<OperatorMethodHandle> getOperatorMethodHandles(OperatorConvention operatorConvention)
        {
            TypeOperatorDeclaration typeOperatorDeclaration = operatorConvention.type().getTypeOperatorDeclaration(TypeOperators.this);
            requireNonNull(typeOperatorDeclaration, "typeOperators is null for " + operatorConvention.type());
            return switch (operatorConvention.operatorType()) {
                case READ_VALUE -> {
                    List<OperatorMethodHandle> readValueOperators = new ArrayList<>(typeOperatorDeclaration.getReadValueOperators());
                    if (readValueOperators.stream().map(OperatorMethodHandle::getCallingConvention).noneMatch(READ_BLOCK_NOT_NULL_CALLING_CONVENTION::equals)) {
                        readValueOperators.add(new OperatorMethodHandle(READ_BLOCK_NOT_NULL_CALLING_CONVENTION, getDefaultReadBlockMethod(operatorConvention.type())));
                    }
                    if (readValueOperators.stream().map(OperatorMethodHandle::getCallingConvention).noneMatch(WRITE_BLOCK_CALLING_CONVENTION::equals)) {
                        readValueOperators.add(new OperatorMethodHandle(WRITE_BLOCK_CALLING_CONVENTION, getDefaultWriteMethod(operatorConvention.type())));
                    }
                    yield readValueOperators;
                }
                case EQUAL -> typeOperatorDeclaration.getEqualOperators();
                case HASH_CODE -> {
                    Collection<OperatorMethodHandle> hashCodeOperators = typeOperatorDeclaration.getHashCodeOperators();
                    if (hashCodeOperators.isEmpty()) {
                        yield typeOperatorDeclaration.getXxHash64Operators();
                    }
                    yield hashCodeOperators;
                }
                case XX_HASH_64 -> typeOperatorDeclaration.getXxHash64Operators();
                case IS_DISTINCT_FROM -> {
                    Collection<OperatorMethodHandle> distinctFromOperators = typeOperatorDeclaration.getDistinctFromOperators();
                    if (distinctFromOperators.isEmpty()) {
                        yield List.of(generateDistinctFromOperator(operatorConvention));
                    }
                    yield distinctFromOperators;
                }
                case INDETERMINATE -> {
                    Collection<OperatorMethodHandle> indeterminateOperators = typeOperatorDeclaration.getIndeterminateOperators();
                    if (indeterminateOperators.isEmpty()) {
                        yield List.of(defaultIndeterminateOperator(operatorConvention.type().getJavaType()));
                    }
                    yield indeterminateOperators;
                }
                case COMPARISON_UNORDERED_LAST -> {
                    if (operatorConvention.sortOrder().isPresent()) {
                        yield List.of(generateOrderingOperator(operatorConvention));
                    }
                    Collection<OperatorMethodHandle> comparisonUnorderedLastOperators = typeOperatorDeclaration.getComparisonUnorderedLastOperators();
                    if (comparisonUnorderedLastOperators.isEmpty()) {
                        // if a type only provides one comparison operator, it is assumed that the type does not have unordered values
                        yield typeOperatorDeclaration.getComparisonUnorderedFirstOperators();
                    }
                    yield comparisonUnorderedLastOperators;
                }
                case COMPARISON_UNORDERED_FIRST -> {
                    if (operatorConvention.sortOrder().isPresent()) {
                        yield List.of(generateOrderingOperator(operatorConvention));
                    }
                    Collection<OperatorMethodHandle> comparisonUnorderedFirstOperators = typeOperatorDeclaration.getComparisonUnorderedFirstOperators();
                    if (comparisonUnorderedFirstOperators.isEmpty()) {
                        // if a type only provides one comparison operator, it is assumed that the type does not have unordered values
                        yield typeOperatorDeclaration.getComparisonUnorderedLastOperators();
                    }
                    yield comparisonUnorderedFirstOperators;
                }
                case LESS_THAN -> {
                    Collection<OperatorMethodHandle> lessThanOperators = typeOperatorDeclaration.getLessThanOperators();
                    if (lessThanOperators.isEmpty()) {
                        yield List.of(generateLessThanOperator(operatorConvention, false));
                    }
                    yield lessThanOperators;
                }
                case LESS_THAN_OR_EQUAL -> {
                    Collection<OperatorMethodHandle> lessThanOrEqualOperators = typeOperatorDeclaration.getLessThanOrEqualOperators();
                    if (lessThanOrEqualOperators.isEmpty()) {
                        yield List.of(generateLessThanOperator(operatorConvention, true));
                    }
                    yield lessThanOrEqualOperators;
                }
                default -> throw new IllegalArgumentException("Unsupported operator type: " + operatorConvention.operatorType());
            };
        }

        private static MethodHandle getDefaultReadBlockMethod(Type type)
        {
            Class<?> javaType = type.getJavaType();
            if (boolean.class.equals(javaType)) {
                return TYPE_GET_BOOLEAN.bindTo(type);
            }
            if (long.class.equals(javaType)) {
                return TYPE_GET_LONG.bindTo(type);
            }
            if (double.class.equals(javaType)) {
                return TYPE_GET_DOUBLE.bindTo(type);
            }
            if (Slice.class.equals(javaType)) {
                return TYPE_GET_SLICE.bindTo(type);
            }
            return TYPE_GET_OBJECT
                    .asType(TYPE_GET_OBJECT.type().changeReturnType(type.getJavaType()))
                    .bindTo(type);
        }

        private static MethodHandle getDefaultWriteMethod(Type type)
        {
            Class<?> javaType = type.getJavaType();
            if (boolean.class.equals(javaType)) {
                return TYPE_WRITE_BOOLEAN.bindTo(type);
            }
            if (long.class.equals(javaType)) {
                return TYPE_WRITE_LONG.bindTo(type);
            }
            if (double.class.equals(javaType)) {
                return TYPE_WRITE_DOUBLE.bindTo(type);
            }
            if (Slice.class.equals(javaType)) {
                return TYPE_WRITE_SLICE.bindTo(type);
            }
            return TYPE_WRITE_OBJECT.bindTo(type);
        }

        private OperatorMethodHandle generateDistinctFromOperator(OperatorConvention operatorConvention)
        {
            // This code assumes that the declared equals method for the type is not nullable, which is true for all non-container types.
            // Container types directly define the distinct operator, so this assumption is reasonable.
            List<InvocationArgumentConvention> argumentConventions = operatorConvention.callingConvention().getArgumentConventions();

            // if none of the arguments are nullable, return "not equal"
            if (argumentConventions.stream().noneMatch(InvocationArgumentConvention::isNullable)) {
                InvocationConvention convention = new InvocationConvention(argumentConventions, FAIL_ON_NULL, false, false);
                MethodHandle equalMethodHandle = adaptOperator(new OperatorConvention(operatorConvention.type(), EQUAL, Optional.empty(), convention));
                return new OperatorMethodHandle(convention, filterReturnValue(equalMethodHandle, LOGICAL_NOT));
            }

            // one or both of the arguments are nullable
            List<InvocationArgumentConvention> equalArgumentConventions = new ArrayList<>();
            List<InvocationArgumentConvention> distinctArgumentConventions = new ArrayList<>();
            for (InvocationArgumentConvention argumentConvention : argumentConventions) {
                if (argumentConvention.isNullable()) {
                    if (argumentConvention == BLOCK_POSITION) {
                        equalArgumentConventions.add(BLOCK_POSITION_NOT_NULL);
                        distinctArgumentConventions.add(BLOCK_POSITION);
                    }
                    else {
                        equalArgumentConventions.add(NEVER_NULL);
                        distinctArgumentConventions.add(NULL_FLAG);
                    }
                }
                else {
                    equalArgumentConventions.add(argumentConvention);
                    distinctArgumentConventions.add(argumentConvention);
                }
            }
            InvocationArgumentConvention leftDistinctConvention = distinctArgumentConventions.get(0);
            InvocationArgumentConvention rightDistinctConvention = distinctArgumentConventions.get(1);

            // distinct is "not equal", with some extra handling for nulls
            MethodHandle notEqualMethodHandle = filterReturnValue(
                    adaptOperator(new OperatorConvention(
                            operatorConvention.type(),
                            EQUAL,
                            Optional.empty(),
                            new InvocationConvention(equalArgumentConventions, FAIL_ON_NULL, false, false))),
                    LOGICAL_NOT);
            // add the unused null flag if necessary
            if (rightDistinctConvention == NULL_FLAG) {
                notEqualMethodHandle = dropArguments(notEqualMethodHandle, notEqualMethodHandle.type().parameterCount(), boolean.class);
            }
            if (leftDistinctConvention == NULL_FLAG) {
                notEqualMethodHandle = dropArguments(notEqualMethodHandle, 1, boolean.class);
            }

            MethodHandle testNullHandle;
            if (leftDistinctConvention.isNullable() && rightDistinctConvention.isNullable()) {
                testNullHandle = LOGICAL_OR;
                testNullHandle = collectArguments(testNullHandle, 1, distinctArgumentNullTest(operatorConvention, rightDistinctConvention));
                testNullHandle = collectArguments(testNullHandle, 0, distinctArgumentNullTest(operatorConvention, leftDistinctConvention));
            }
            else if (leftDistinctConvention.isNullable()) {
                // test method can have fewer arguments than the operator method
                testNullHandle = distinctArgumentNullTest(operatorConvention, leftDistinctConvention);
            }
            else {
                testNullHandle = distinctArgumentNullTest(operatorConvention, rightDistinctConvention);
                testNullHandle = dropArguments(testNullHandle, 0, notEqualMethodHandle.type().parameterList().subList(0, leftDistinctConvention.getParameterCount()));
            }

            MethodHandle hasNullResultHandle;
            if (leftDistinctConvention.isNullable() && rightDistinctConvention.isNullable()) {
                hasNullResultHandle = BOOLEAN_NOT_EQUAL;
                hasNullResultHandle = collectArguments(hasNullResultHandle, 1, distinctArgumentNullTest(operatorConvention, rightDistinctConvention));
                hasNullResultHandle = collectArguments(hasNullResultHandle, 0, distinctArgumentNullTest(operatorConvention, leftDistinctConvention));
            }
            else {
                hasNullResultHandle = dropArguments(constant(boolean.class, true), 0, notEqualMethodHandle.type().parameterList());
            }

            return new OperatorMethodHandle(
                    simpleConvention(FAIL_ON_NULL, leftDistinctConvention, rightDistinctConvention),
                    guardWithTest(
                            testNullHandle,
                            hasNullResultHandle,
                            notEqualMethodHandle));
        }

        private static MethodHandle distinctArgumentNullTest(OperatorConvention operatorConvention, InvocationArgumentConvention distinctArgumentConvention)
        {
            if (distinctArgumentConvention == BLOCK_POSITION) {
                return BLOCK_IS_NULL;
            }
            if (distinctArgumentConvention == NULL_FLAG) {
                return dropArguments(identity(boolean.class), 0, operatorConvention.type().getJavaType());
            }
            throw new IllegalArgumentException("Unexpected argument convention: " + distinctArgumentConvention);
        }

        private OperatorMethodHandle generateLessThanOperator(OperatorConvention operatorConvention, boolean orEqual)
        {
            InvocationConvention comparisonCallingConvention;
            if (operatorConvention.callingConvention().getArgumentConventions().equals(List.of(BLOCK_POSITION, BLOCK_POSITION))) {
                comparisonCallingConvention = simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION);
            }
            else if (operatorConvention.callingConvention().getArgumentConventions().equals(List.of(NEVER_NULL, BLOCK_POSITION))) {
                comparisonCallingConvention = simpleConvention(FAIL_ON_NULL, NEVER_NULL, BLOCK_POSITION);
            }
            else if (operatorConvention.callingConvention().getArgumentConventions().equals(List.of(BLOCK_POSITION, NEVER_NULL))) {
                comparisonCallingConvention = simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, NEVER_NULL);
            }
            else {
                comparisonCallingConvention = simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL);
            }

            OperatorConvention comparisonOperator = new OperatorConvention(operatorConvention.type(), COMPARISON_UNORDERED_LAST, Optional.empty(), comparisonCallingConvention);
            MethodHandle comparisonMethod = adaptOperator(comparisonOperator);
            if (orEqual) {
                return adaptComparisonToLessThanOrEqual(new OperatorMethodHandle(comparisonCallingConvention, comparisonMethod));
            }
            return adaptComparisonToLessThan(new OperatorMethodHandle(comparisonCallingConvention, comparisonMethod));
        }

        private OperatorMethodHandle generateOrderingOperator(OperatorConvention operatorConvention)
        {
            SortOrder sortOrder = operatorConvention.sortOrder().orElseThrow(() -> new IllegalArgumentException("Operator convention does not contain a sort order"));
            OperatorType comparisonType = operatorConvention.operatorType();
            if (operatorConvention.callingConvention().getArgumentConventions().equals(List.of(BLOCK_POSITION, BLOCK_POSITION))) {
                OperatorConvention comparisonOperator = new OperatorConvention(operatorConvention.type(), comparisonType, Optional.empty(), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));
                MethodHandle comparisonInvoker = adaptOperator(comparisonOperator);
                return adaptBlockPositionComparisonToOrdering(sortOrder, comparisonInvoker);
            }

            OperatorConvention comparisonOperator = new OperatorConvention(operatorConvention.type(), comparisonType, Optional.empty(), simpleConvention(FAIL_ON_NULL, NULL_FLAG, NULL_FLAG));
            MethodHandle comparisonInvoker = adaptOperator(comparisonOperator);
            return adaptNeverNullComparisonToOrdering(sortOrder, comparisonInvoker);
        }

        private static Type getOperatorReturnType(OperatorConvention operatorConvention)
        {
            return switch (operatorConvention.operatorType()) {
                case EQUAL, IS_DISTINCT_FROM, LESS_THAN, LESS_THAN_OR_EQUAL, INDETERMINATE -> BOOLEAN;
                case COMPARISON_UNORDERED_LAST, COMPARISON_UNORDERED_FIRST -> INTEGER;
                case HASH_CODE, XX_HASH_64 -> BIGINT;
                case READ_VALUE -> operatorConvention.type();
                default -> throw new IllegalArgumentException("Unsupported operator type: " + operatorConvention.operatorType());
            };
        }

        private static List<Type> getOperatorArgumentTypes(OperatorConvention operatorConvention)
        {
            return switch (operatorConvention.operatorType()) {
                case EQUAL, IS_DISTINCT_FROM, COMPARISON_UNORDERED_LAST, COMPARISON_UNORDERED_FIRST, LESS_THAN, LESS_THAN_OR_EQUAL ->
                        List.of(operatorConvention.type(), operatorConvention.type());
                case READ_VALUE, HASH_CODE, XX_HASH_64, INDETERMINATE ->
                        List.of(operatorConvention.type());
                default -> throw new IllegalArgumentException("Unsupported operator type: " + operatorConvention.operatorType());
            };
        }
    }

    private static int getScore(OperatorMethodHandle operatorMethodHandle)
    {
        int score = 0;
        for (InvocationArgumentConvention argument : operatorMethodHandle.getCallingConvention().getArgumentConventions()) {
            if (argument == FLAT) {
                score += 1000;
            }
            if (argument == NULL_FLAG || argument == FLAT) {
                score += 100;
            }
            else if (argument == BLOCK_POSITION) {
                score += 1;
            }
        }
        return score;
    }

    private record OperatorConvention(Type type, OperatorType operatorType, Optional<SortOrder> sortOrder, InvocationConvention callingConvention)
    {
        private OperatorConvention
        {
            requireNonNull(type, "type is null");
            requireNonNull(operatorType, "operatorType is null");
            requireNonNull(sortOrder, "sortOrder is null");
            requireNonNull(callingConvention, "callingConvention is null");
        }
    }

    private static final MethodHandle LOGICAL_NOT;
    private static final MethodHandle LOGICAL_OR;
    private static final MethodHandle BOOLEAN_NOT_EQUAL;
    private static final MethodHandle IS_COMPARISON_LESS_THAN;
    private static final MethodHandle IS_COMPARISON_LESS_THAN_OR_EQUAL;
    private static final MethodHandle ORDER_NULLS;
    private static final MethodHandle ORDER_COMPARISON_RESULT;
    private static final MethodHandle BLOCK_IS_NULL;

    private static final MethodHandle TYPE_GET_BOOLEAN;
    private static final MethodHandle TYPE_GET_LONG;
    private static final MethodHandle TYPE_GET_DOUBLE;
    private static final MethodHandle TYPE_GET_SLICE;
    private static final MethodHandle TYPE_GET_OBJECT;

    private static final MethodHandle TYPE_WRITE_BOOLEAN;
    private static final MethodHandle TYPE_WRITE_LONG;
    private static final MethodHandle TYPE_WRITE_DOUBLE;
    private static final MethodHandle TYPE_WRITE_SLICE;
    private static final MethodHandle TYPE_WRITE_OBJECT;

    static {
        try {
            Lookup lookup = lookup();
            LOGICAL_NOT = lookup.findStatic(TypeOperators.class, "logicalNot", MethodType.methodType(boolean.class, boolean.class));
            LOGICAL_OR = lookup.findStatic(Boolean.class, "logicalOr", MethodType.methodType(boolean.class, boolean.class, boolean.class));
            BOOLEAN_NOT_EQUAL = lookup.findStatic(TypeOperators.class, "booleanNotEqual", MethodType.methodType(boolean.class, boolean.class, boolean.class));
            IS_COMPARISON_LESS_THAN = lookup.findStatic(TypeOperators.class, "isComparisonLessThan", MethodType.methodType(boolean.class, long.class));
            IS_COMPARISON_LESS_THAN_OR_EQUAL = lookup.findStatic(TypeOperators.class, "isComparisonLessThanOrEqual", MethodType.methodType(boolean.class, long.class));
            ORDER_NULLS = lookup.findStatic(TypeOperators.class, "orderNulls", MethodType.methodType(int.class, SortOrder.class, boolean.class, boolean.class));
            ORDER_COMPARISON_RESULT = lookup.findStatic(TypeOperators.class, "orderComparisonResult", MethodType.methodType(int.class, SortOrder.class, long.class));
            BLOCK_IS_NULL = lookup.findVirtual(Block.class, "isNull", MethodType.methodType(boolean.class, int.class));

            TYPE_GET_BOOLEAN = lookup.findVirtual(Type.class, "getBoolean", MethodType.methodType(boolean.class, Block.class, int.class));
            TYPE_GET_LONG = lookup.findVirtual(Type.class, "getLong", MethodType.methodType(long.class, Block.class, int.class));
            TYPE_GET_DOUBLE = lookup.findVirtual(Type.class, "getDouble", MethodType.methodType(double.class, Block.class, int.class));
            TYPE_GET_SLICE = lookup.findVirtual(Type.class, "getSlice", MethodType.methodType(Slice.class, Block.class, int.class));
            TYPE_GET_OBJECT = lookup.findVirtual(Type.class, "getObject", MethodType.methodType(Object.class, Block.class, int.class));

            TYPE_WRITE_BOOLEAN = lookupWriteBlockBuilderMethod(lookup, "writeBoolean", boolean.class);
            TYPE_WRITE_LONG = lookupWriteBlockBuilderMethod(lookup, "writeLong", long.class);
            TYPE_WRITE_DOUBLE = lookupWriteBlockBuilderMethod(lookup, "writeDouble", double.class);
            TYPE_WRITE_SLICE = lookupWriteBlockBuilderMethod(lookup, "writeSlice", Slice.class);
            TYPE_WRITE_OBJECT = lookupWriteBlockBuilderMethod(lookup, "writeObject", Object.class);
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static MethodHandle lookupWriteBlockBuilderMethod(Lookup lookup, String methodName, Class<?> javaType)
            throws NoSuchMethodException, IllegalAccessException
    {
        return permuteArguments(
                lookup.findVirtual(Type.class, methodName, MethodType.methodType(void.class, BlockBuilder.class, javaType)),
                MethodType.methodType(void.class, Type.class, javaType, BlockBuilder.class),
                0, 2, 1);
    }

    private static boolean logicalNot(boolean value)
    {
        return !value;
    }

    private static boolean booleanNotEqual(boolean left, boolean right)
    {
        return left != right;
    }

    //
    // Generate default indeterminate
    //

    private static OperatorMethodHandle defaultIndeterminateOperator(Class<?> javaType)
    {
        // boolean distinctFrom(T value, boolean valueIsNull)
        // {
        //     return valueIsNull;
        // }
        MethodHandle methodHandle = identity(boolean.class);
        methodHandle = dropArguments(methodHandle, 0, javaType);
        return new OperatorMethodHandle(simpleConvention(FAIL_ON_NULL, NULL_FLAG), methodHandle);
    }

    //
    // Adapt comparison to ordering
    //

    private static OperatorMethodHandle adaptNeverNullComparisonToOrdering(SortOrder sortOrder, MethodHandle neverNullComparison)
    {
        MethodType finalSignature = MethodType.methodType(
                int.class,
                boolean.class,
                neverNullComparison.type().parameterType(0),
                boolean.class,
                neverNullComparison.type().parameterType(1));

        // (leftIsNull, rightIsNull, leftValue, rightValue)::int
        MethodHandle order = adaptComparisonToOrdering(sortOrder, neverNullComparison);
        // (leftIsNull, leftValue, rightIsNull, rightValue)::int
        order = permuteArguments(order, finalSignature, 0, 2, 1, 3);
        return new OperatorMethodHandle(simpleConvention(FAIL_ON_NULL, NULL_FLAG, NULL_FLAG), order);
    }

    private static OperatorMethodHandle adaptBlockPositionComparisonToOrdering(SortOrder sortOrder, MethodHandle blockPositionComparison)
    {
        MethodType finalSignature = MethodType.methodType(
                int.class,
                Block.class,
                int.class,
                Block.class,
                int.class);

        // (leftIsNull, rightIsNull, leftBlock, leftPosition, rightBlock, rightPosition)::int
        MethodHandle order = adaptComparisonToOrdering(sortOrder, blockPositionComparison);
        // (leftBlock, leftPosition, rightBlock, rightPosition, leftBlock, leftPosition, rightBlock, rightPosition)::int
        order = collectArguments(order, 1, BLOCK_IS_NULL);
        order = collectArguments(order, 0, BLOCK_IS_NULL);
        // (leftBlock, leftPosition, rightBlock, rightPosition)::int
        order = permuteArguments(order, finalSignature, 0, 1, 2, 3, 0, 1, 2, 3);

        return new OperatorMethodHandle(simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION), order);
    }

    // input: (args)::int
    // output: (leftIsNull, rightIsNull, comparison_args)::int
    private static MethodHandle adaptComparisonToOrdering(SortOrder sortOrder, MethodHandle comparison)
    {
        // Guard: if (leftIsNull | rightIsNull)
        // (leftIsNull, rightIsNull)::boolean
        MethodHandle eitherIsNull = LOGICAL_OR;
        // (leftIsNull, rightIsNull, comparison_args)::boolean
        eitherIsNull = dropArguments(eitherIsNull, 2, comparison.type().parameterList());

        // True: return orderNulls(leftIsNull, rightIsNull)
        // (leftIsNull, rightIsNull)::int
        MethodHandle orderNulls = ORDER_NULLS.bindTo(sortOrder);
        // (leftIsNull, rightIsNull, comparison_args)::int
        orderNulls = dropArguments(orderNulls, 2, comparison.type().parameterList());

        // False; return orderComparisonResult(comparison(leftValue, rightValue))
        // (leftValue, rightValue)::int
        MethodHandle orderComparison = filterReturnValue(comparison, ORDER_COMPARISON_RESULT.bindTo(sortOrder));
        // (leftIsNull, rightIsNull, comparison_args)::int
        orderComparison = dropArguments(orderComparison, 0, boolean.class, boolean.class);

        // (leftIsNull, rightIsNull, comparison_args)::int
        return guardWithTest(eitherIsNull, orderNulls, orderComparison);
    }

    private static int orderNulls(SortOrder sortOrder, boolean leftIsNull, boolean rightIsNull)
    {
        if (leftIsNull && rightIsNull) {
            return 0;
        }
        if (leftIsNull) {
            return sortOrder.isNullsFirst() ? -1 : 1;
        }
        if (rightIsNull) {
            return sortOrder.isNullsFirst() ? 1 : -1;
        }
        throw new IllegalArgumentException("Neither left or right is null");
    }

    private static int orderComparisonResult(SortOrder sortOrder, long result)
    {
        return (int) (sortOrder.isAscending() ? result : -result);
    }

    //
    // Adapt comparison to less than
    //

    private static OperatorMethodHandle adaptComparisonToLessThan(OperatorMethodHandle invoker)
    {
        InvocationReturnConvention returnConvention = invoker.getCallingConvention().getReturnConvention();
        if (returnConvention != FAIL_ON_NULL) {
            throw new IllegalArgumentException("Return convention must be " + FAIL_ON_NULL + ", but is " + returnConvention);
        }
        return new OperatorMethodHandle(invoker.getCallingConvention(), filterReturnValue(invoker.getMethodHandle(), IS_COMPARISON_LESS_THAN));
    }

    private static boolean isComparisonLessThan(long comparisonResult)
    {
        return comparisonResult < 0;
    }

    private static OperatorMethodHandle adaptComparisonToLessThanOrEqual(OperatorMethodHandle invoker)
    {
        InvocationReturnConvention returnConvention = invoker.getCallingConvention().getReturnConvention();
        if (returnConvention != FAIL_ON_NULL) {
            throw new IllegalArgumentException("Return convention must be " + FAIL_ON_NULL + ", but is " + returnConvention);
        }
        return new OperatorMethodHandle(invoker.getCallingConvention(), filterReturnValue(invoker.getMethodHandle(), IS_COMPARISON_LESS_THAN_OR_EQUAL));
    }

    private static boolean isComparisonLessThanOrEqual(long comparisonResult)
    {
        return comparisonResult <= 0;
    }
}
