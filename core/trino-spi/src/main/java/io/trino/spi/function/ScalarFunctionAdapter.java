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
package io.trino.spi.function;

import io.airlift.slice.Slice;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.trino.spi.function.InvocationConvention.InvocationReturnConvention;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.IN_OUT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.DEFAULT_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static java.lang.invoke.MethodHandles.collectArguments;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.empty;
import static java.lang.invoke.MethodHandles.explicitCastArguments;
import static java.lang.invoke.MethodHandles.filterArguments;
import static java.lang.invoke.MethodHandles.guardWithTest;
import static java.lang.invoke.MethodHandles.identity;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.lang.invoke.MethodHandles.publicLookup;
import static java.lang.invoke.MethodHandles.throwException;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;

public final class ScalarFunctionAdapter
{
    private static final MethodHandle IS_NULL_METHOD = lookupIsNullMethod();
    private static final MethodHandle APPEND_NULL_METHOD = lookupAppendNullMethod();
    // This is needed to convert flat arguments to stack types
    private static final TypeOperators READ_VALUE_TYPE_OPERATORS = new TypeOperators();

    private ScalarFunctionAdapter() {}

    /**
     * Can the actual calling convention of a method be converted to the expected calling convention?
     */
    public static boolean canAdapt(InvocationConvention actualConvention, InvocationConvention expectedConvention)
    {
        requireNonNull(actualConvention, "actualConvention is null");
        requireNonNull(expectedConvention, "expectedConvention is null");
        if (actualConvention.getArgumentConventions().size() != expectedConvention.getArgumentConventions().size()) {
            throw new IllegalArgumentException("Actual and expected conventions have different number of arguments");
        }

        if (actualConvention.supportsSession() && !expectedConvention.supportsSession()) {
            return false;
        }

        if (actualConvention.supportsInstanceFactory() && !expectedConvention.supportsInstanceFactory()) {
            return false;
        }

        if (!canAdaptReturn(actualConvention.getReturnConvention(), expectedConvention.getReturnConvention())) {
            return false;
        }

        for (int argumentIndex = 0; argumentIndex < actualConvention.getArgumentConventions().size(); argumentIndex++) {
            InvocationArgumentConvention actualArgumentConvention = actualConvention.getArgumentConvention(argumentIndex);
            InvocationArgumentConvention expectedArgumentConvention = expectedConvention.getArgumentConvention(argumentIndex);
            if (!canAdaptParameter(
                    actualArgumentConvention,
                    expectedArgumentConvention,
                    expectedConvention.getReturnConvention())) {
                return false;
            }
        }
        return true;
    }

    private static boolean canAdaptReturn(
            InvocationReturnConvention actualReturnConvention,
            InvocationReturnConvention expectedReturnConvention)
    {
        if (actualReturnConvention == expectedReturnConvention) {
            return true;
        }

        return switch (actualReturnConvention) {
            case FAIL_ON_NULL -> expectedReturnConvention != FLAT_RETURN;
            case NULLABLE_RETURN -> expectedReturnConvention.isNullable() || expectedReturnConvention == DEFAULT_ON_NULL;
            case BLOCK_BUILDER, FLAT_RETURN -> false;
            case DEFAULT_ON_NULL -> throw new IllegalArgumentException("actual return convention cannot be DEFAULT_ON_NULL");
        };
    }

    private static boolean canAdaptParameter(
            InvocationArgumentConvention actualArgumentConvention,
            InvocationArgumentConvention expectedArgumentConvention,
            InvocationReturnConvention returnConvention)
    {
        // not a conversion
        if (actualArgumentConvention == expectedArgumentConvention) {
            return true;
        }

        // function cannot be adapted
        if (expectedArgumentConvention == FUNCTION || actualArgumentConvention == FUNCTION) {
            return false;
        }

        return switch (actualArgumentConvention) {
            case NEVER_NULL -> switch (expectedArgumentConvention) {
                case BLOCK_POSITION_NOT_NULL, FLAT -> true;
                case BOXED_NULLABLE, NULL_FLAG -> returnConvention != FAIL_ON_NULL;
                case BLOCK_POSITION, IN_OUT -> true; // todo only support these if the return convention is nullable
                case FUNCTION -> throw new IllegalStateException("Unexpected value: " + expectedArgumentConvention);
                // this is not needed as the case where actual and expected are the same is covered above,
                // but this means we will get a compile time error if a new convention is added in the future
                //noinspection DataFlowIssue
                case NEVER_NULL -> true;
            };
            case BLOCK_POSITION_NOT_NULL -> expectedArgumentConvention == BLOCK_POSITION && (returnConvention.isNullable() || returnConvention == DEFAULT_ON_NULL);
            case BLOCK_POSITION -> expectedArgumentConvention == BLOCK_POSITION_NOT_NULL;
            case BOXED_NULLABLE, NULL_FLAG -> true;
            case FLAT, IN_OUT -> false;
            case FUNCTION -> throw new IllegalArgumentException("Unsupported argument convention: " + actualArgumentConvention);
        };
    }

    /**
     * Adapt the method handle from the actual calling convention of a method be converted to the expected calling convention?
     */
    public static MethodHandle adapt(
            MethodHandle methodHandle,
            Type returnType,
            List<Type> actualArgumentTypes,
            InvocationConvention actualConvention,
            InvocationConvention expectedConvention)
    {
        requireNonNull(methodHandle, "methodHandle is null");
        requireNonNull(actualConvention, "actualConvention is null");
        requireNonNull(expectedConvention, "expectedConvention is null");
        if (actualConvention.getArgumentConventions().size() != expectedConvention.getArgumentConventions().size()) {
            throw new IllegalArgumentException("Actual and expected conventions have different number of arguments");
        }

        if (actualConvention.supportsSession() && !expectedConvention.supportsSession()) {
            throw new IllegalArgumentException("Session method cannot be adapted to no session");
        }
        if (!(expectedConvention.supportsInstanceFactory() || !actualConvention.supportsInstanceFactory())) {
            throw new IllegalArgumentException("Instance method cannot be adapted to no instance");
        }

        // adapt return first, since return-null-on-null parameter convention must know if the return type is nullable
        methodHandle = adaptReturn(methodHandle, returnType, actualConvention.getReturnConvention(), expectedConvention.getReturnConvention());

        // adapt parameters one at a time
        int parameterIndex = 0;

        if (actualConvention.supportsInstanceFactory()) {
            parameterIndex++;
        }
        if (actualConvention.supportsSession()) {
            parameterIndex++;
        }

        for (int argumentIndex = 0; argumentIndex < actualConvention.getArgumentConventions().size(); argumentIndex++) {
            Type argumentType = actualArgumentTypes.get(argumentIndex);
            InvocationArgumentConvention actualArgumentConvention = actualConvention.getArgumentConvention(argumentIndex);
            InvocationArgumentConvention expectedArgumentConvention = expectedConvention.getArgumentConvention(argumentIndex);
            methodHandle = adaptParameter(
                    methodHandle,
                    parameterIndex,
                    argumentType,
                    actualArgumentConvention,
                    expectedArgumentConvention,
                    expectedConvention.getReturnConvention());
            parameterIndex += expectedArgumentConvention.getParameterCount();
        }
        return methodHandle;
    }

    private static MethodHandle adaptReturn(
            MethodHandle methodHandle,
            Type returnType,
            InvocationReturnConvention actualReturnConvention,
            InvocationReturnConvention expectedReturnConvention)
    {
        if (actualReturnConvention == expectedReturnConvention) {
            return methodHandle;
        }

        if (expectedReturnConvention == NULLABLE_RETURN) {
            if (actualReturnConvention == FAIL_ON_NULL) {
                // box return
                return explicitCastArguments(methodHandle, methodHandle.type().changeReturnType(wrap(methodHandle.type().returnType())));
            }
        }

        if (expectedReturnConvention == BLOCK_BUILDER) {
            // write the result to block builder
            // type.writeValue(BlockBuilder, value), f(a,b)::value => method(BlockBuilder, a, b)::void
            methodHandle = collectArguments(writeBlockValue(returnType), 1, methodHandle);
            // f(BlockBuilder, a, b)::void => f(a, b, BlockBuilder)
            MethodType newType = methodHandle.type()
                    .dropParameterTypes(0, 1)
                    .appendParameterTypes(BlockBuilder.class);
            int[] reorder = IntStream.range(0, newType.parameterCount())
                    .map(i -> i > 0 ? i - 1 : newType.parameterCount() - 1)
                    .toArray();
            methodHandle = permuteArguments(methodHandle, newType, reorder);
            return methodHandle;
        }

        if (expectedReturnConvention == FAIL_ON_NULL && actualReturnConvention == NULLABLE_RETURN) {
            throw new IllegalArgumentException("Nullable return cannot be adapted fail on null");
        }

        if (expectedReturnConvention == DEFAULT_ON_NULL) {
            if (actualReturnConvention == FAIL_ON_NULL) {
                return methodHandle;
            }
            if (actualReturnConvention == NULLABLE_RETURN) {
                // perform unboxing, which converts nulls to Java primitive default value
                methodHandle = explicitCastArguments(methodHandle, methodHandle.type().changeReturnType(unwrap(returnType.getJavaType())));
                return methodHandle;
            }
        }
        throw new IllegalArgumentException("%s return convention cannot be adapted to %s".formatted(actualReturnConvention, expectedReturnConvention));
    }

    private static MethodHandle adaptParameter(
            MethodHandle methodHandle,
            int parameterIndex,
            Type argumentType,
            InvocationArgumentConvention actualArgumentConvention,
            InvocationArgumentConvention expectedArgumentConvention,
            InvocationReturnConvention returnConvention)
    {
        if (actualArgumentConvention == expectedArgumentConvention) {
            return methodHandle;
        }
        if (actualArgumentConvention == IN_OUT) {
            throw new IllegalArgumentException("In-out argument cannot be adapted");
        }
        if (actualArgumentConvention == FUNCTION || expectedArgumentConvention == FUNCTION) {
            throw new IllegalArgumentException("Function argument cannot be adapted");
        }
        if (actualArgumentConvention == FLAT) {
            throw new IllegalArgumentException("Flat argument cannot be adapted");
        }

        // caller will never pass null
        if (expectedArgumentConvention == NEVER_NULL) {
            if (actualArgumentConvention == BOXED_NULLABLE) {
                // if actual argument is boxed primitive, change method handle to accept a primitive and then box to actual method
                if (isWrapperType(methodHandle.type().parameterType(parameterIndex))) {
                    MethodType targetType = methodHandle.type().changeParameterType(parameterIndex, unwrap(methodHandle.type().parameterType(parameterIndex)));
                    methodHandle = explicitCastArguments(methodHandle, targetType);
                }
                return methodHandle;
            }

            if (actualArgumentConvention == NULL_FLAG) {
                // actual method takes value and null flag, so change method handles to not have the flag and always pass false to the actual method
                return insertArguments(methodHandle, parameterIndex + 1, false);
            }
        }

        // caller will pass Java null for SQL null
        if (expectedArgumentConvention == BOXED_NULLABLE) {
            if (actualArgumentConvention == NEVER_NULL) {
                // box argument
                Class<?> boxedType = wrap(methodHandle.type().parameterType(parameterIndex));
                MethodType targetType = methodHandle.type().changeParameterType(parameterIndex, boxedType);
                methodHandle = explicitCastArguments(methodHandle, targetType);

                if (returnConvention == FAIL_ON_NULL) {
                    throw new IllegalArgumentException("RETURN_NULL_ON_NULL adaptation cannot be used with FAIL_ON_NULL return convention");
                }
                return guardWithTest(
                        isNullArgument(methodHandle.type(), parameterIndex),
                        getNullShortCircuitResult(methodHandle, returnConvention),
                        methodHandle);
            }

            if (actualArgumentConvention == NULL_FLAG) {
                // The conversion is described below in reverse order as this is how method handle adaptation works.  The provided example
                // signature is based on a boxed Long argument.

                // 3. unbox the value (if null, the java default is sent)
                // long, boolean => Long, boolean
                Class<?> parameterType = methodHandle.type().parameterType(parameterIndex);
                methodHandle = explicitCastArguments(methodHandle, methodHandle.type().changeParameterType(parameterIndex, wrap(parameterType)));

                // 2. replace second argument with the result of isNull
                // long, boolean => Long, Long
                methodHandle = filterArguments(
                        methodHandle,
                        parameterIndex + 1,
                        explicitCastArguments(IS_NULL_METHOD, methodType(boolean.class, wrap(parameterType))));

                // 1. Duplicate the argument, so we have two copies of the value
                // Long, Long => Long
                int[] reorder = IntStream.range(0, methodHandle.type().parameterCount())
                        .map(i -> i <= parameterIndex ? i : i - 1)
                        .toArray();
                MethodType newType = methodHandle.type().dropParameterTypes(parameterIndex + 1, parameterIndex + 2);
                methodHandle = permuteArguments(methodHandle, newType, reorder);
                return methodHandle;
            }
        }

        // caller will pass boolean true in the next argument for SQL null
        if (expectedArgumentConvention == NULL_FLAG) {
            if (actualArgumentConvention == NEVER_NULL) {
                // if caller sets the null flag, return null, otherwise invoke target
                if (returnConvention == FAIL_ON_NULL) {
                    throw new IllegalArgumentException("RETURN_NULL_ON_NULL adaptation cannot be used with FAIL_ON_NULL return convention");
                }
                // add a null flag to call
                methodHandle = dropArguments(methodHandle, parameterIndex + 1, boolean.class);

                return guardWithTest(
                        isTrueNullFlag(methodHandle.type(), parameterIndex),
                        getNullShortCircuitResult(methodHandle, returnConvention),
                        methodHandle);
            }

            if (actualArgumentConvention == BOXED_NULLABLE) {
                return collectArguments(methodHandle, parameterIndex, boxedToNullFlagFilter(methodHandle.type().parameterType(parameterIndex)));
            }
        }

        if (expectedArgumentConvention == BLOCK_POSITION_NOT_NULL) {
            if (actualArgumentConvention == BLOCK_POSITION) {
                return methodHandle;
            }

            MethodHandle getBlockValue = getBlockValue(argumentType, methodHandle.type().parameterType(parameterIndex));
            if (actualArgumentConvention == NEVER_NULL) {
                return collectArguments(methodHandle, parameterIndex, getBlockValue);
            }
            if (actualArgumentConvention == BOXED_NULLABLE) {
                MethodType targetType = getBlockValue.type().changeReturnType(wrap(getBlockValue.type().returnType()));
                return collectArguments(methodHandle, parameterIndex, explicitCastArguments(getBlockValue, targetType));
            }
            if (actualArgumentConvention == NULL_FLAG) {
                // actual method takes value and null flag, so change method handles to not have the flag and always pass false to the actual method
                return collectArguments(insertArguments(methodHandle, parameterIndex + 1, false), parameterIndex, getBlockValue);
            }
        }

        // caller passes block and position which may contain a null
        if (expectedArgumentConvention == BLOCK_POSITION) {
            MethodHandle getBlockValue = getBlockValue(argumentType, methodHandle.type().parameterType(parameterIndex));

            if (actualArgumentConvention == NEVER_NULL) {
                if (returnConvention != FAIL_ON_NULL) {
                    // if caller sets the null flag, return null, otherwise invoke target
                    methodHandle = collectArguments(methodHandle, parameterIndex, getBlockValue);

                    return guardWithTest(
                            isBlockPositionNull(methodHandle.type(), parameterIndex),
                            getNullShortCircuitResult(methodHandle, returnConvention),
                            methodHandle);
                }

                MethodHandle adapter = guardWithTest(
                        isBlockPositionNull(getBlockValue.type(), 0),
                        throwTrinoNullArgumentException(getBlockValue.type()),
                        getBlockValue);

                return collectArguments(methodHandle, parameterIndex, adapter);
            }

            if (actualArgumentConvention == BOXED_NULLABLE) {
                getBlockValue = explicitCastArguments(getBlockValue, getBlockValue.type().changeReturnType(wrap(getBlockValue.type().returnType())));
                getBlockValue = guardWithTest(
                        isBlockPositionNull(getBlockValue.type(), 0),
                        empty(getBlockValue.type()),
                        getBlockValue);
                methodHandle = collectArguments(methodHandle, parameterIndex, getBlockValue);
                return methodHandle;
            }

            if (actualArgumentConvention == NULL_FLAG) {
                // long, boolean => long, Block, int
                MethodHandle isNull = isBlockPositionNull(getBlockValue.type(), 0);
                methodHandle = collectArguments(methodHandle, parameterIndex + 1, isNull);

                // convert get block value to be null safe
                getBlockValue = guardWithTest(
                        isBlockPositionNull(getBlockValue.type(), 0),
                        empty(getBlockValue.type()),
                        getBlockValue);

                // long, Block, int => Block, int, Block, int
                methodHandle = collectArguments(methodHandle, parameterIndex, getBlockValue);

                int[] reorder = IntStream.range(0, methodHandle.type().parameterCount())
                        .map(i -> i <= parameterIndex + 1 ? i : i - 2)
                        .toArray();
                MethodType newType = methodHandle.type().dropParameterTypes(parameterIndex + 2, parameterIndex + 4);
                methodHandle = permuteArguments(methodHandle, newType, reorder);
                return methodHandle;
            }

            if (actualArgumentConvention == BLOCK_POSITION_NOT_NULL) {
                if (returnConvention != FAIL_ON_NULL) {
                    MethodHandle nullReturnValue = getNullShortCircuitResult(methodHandle, returnConvention);
                    return guardWithTest(
                            isBlockPositionNull(methodHandle.type(), parameterIndex),
                            nullReturnValue,
                            methodHandle);
                }
            }
        }

        // caller will pass boolean true in the next argument for SQL null
        if (expectedArgumentConvention == FLAT) {
            if (actualArgumentConvention != NEVER_NULL && actualArgumentConvention != BOXED_NULLABLE && actualArgumentConvention != NULL_FLAG) {
                throw new IllegalArgumentException(actualArgumentConvention + " cannot be adapted to " + expectedArgumentConvention);
            }

            // if the actual method has a null flag, set the flag to false
            if (actualArgumentConvention == NULL_FLAG) {
                // actual method takes value and null flag, so change method handles to not have the flag and always pass false to the actual method
                methodHandle = insertArguments(methodHandle, parameterIndex + 1, false);
            }

            // if the actual method has a boxed argument, change it to accept the unboxed value
            if (actualArgumentConvention == BOXED_NULLABLE) {
                // if actual argument is boxed primitive, change method handle to accept a primitive and then box to actual method
                if (isWrapperType(methodHandle.type().parameterType(parameterIndex))) {
                    MethodType targetType = methodHandle.type().changeParameterType(parameterIndex, unwrap(methodHandle.type().parameterType(parameterIndex)));
                    methodHandle = explicitCastArguments(methodHandle, targetType);
                }
            }

            // read the value from flat memory
            return collectArguments(methodHandle, parameterIndex, getFlatValueNeverNull(argumentType, methodHandle.type().parameterType(parameterIndex)));
        }

        // caller passes in-out which may contain a null
        if (expectedArgumentConvention == IN_OUT) {
            MethodHandle getInOutValue = getInOutValue(argumentType, methodHandle.type().parameterType(parameterIndex));

            if (actualArgumentConvention == NEVER_NULL) {
                if (returnConvention != FAIL_ON_NULL) {
                    // if caller sets the null flag, return null, otherwise invoke target
                    methodHandle = collectArguments(methodHandle, parameterIndex, getInOutValue);

                    return guardWithTest(
                            isInOutNull(methodHandle.type(), parameterIndex),
                            getNullShortCircuitResult(methodHandle, returnConvention),
                            methodHandle);
                }

                MethodHandle adapter = guardWithTest(
                        isInOutNull(getInOutValue.type(), 0),
                        throwTrinoNullArgumentException(getInOutValue.type()),
                        getInOutValue);

                return collectArguments(methodHandle, parameterIndex, adapter);
            }

            if (actualArgumentConvention == BOXED_NULLABLE) {
                getInOutValue = explicitCastArguments(getInOutValue, getInOutValue.type().changeReturnType(wrap(getInOutValue.type().returnType())));
                getInOutValue = guardWithTest(
                        isInOutNull(getInOutValue.type(), 0),
                        empty(getInOutValue.type()),
                        getInOutValue);
                methodHandle = collectArguments(methodHandle, parameterIndex, getInOutValue);
                return methodHandle;
            }

            if (actualArgumentConvention == NULL_FLAG) {
                // long, boolean => long, InOut
                MethodHandle isNull = isInOutNull(getInOutValue.type(), 0);
                methodHandle = collectArguments(methodHandle, parameterIndex + 1, isNull);

                // long, InOut => InOut, InOut
                getInOutValue = guardWithTest(
                        isInOutNull(getInOutValue.type(), 0),
                        empty(getInOutValue.type()),
                        getInOutValue);
                methodHandle = collectArguments(methodHandle, parameterIndex, getInOutValue);

                // InOut, InOut => InOut
                int[] reorder = IntStream.range(0, methodHandle.type().parameterCount())
                        .map(i -> i <= parameterIndex ? i : i - 1)
                        .toArray();
                MethodType newType = methodHandle.type().dropParameterTypes(parameterIndex + 1, parameterIndex + 2);
                methodHandle = permuteArguments(methodHandle, newType, reorder);
                return methodHandle;
            }
        }

        throw new IllegalArgumentException("Cannot convert argument %s to %s with return convention %s".formatted(actualArgumentConvention, expectedArgumentConvention, returnConvention));
    }

    private static MethodHandle getBlockValue(Type argumentType, Class<?> expectedType)
    {
        Class<?> methodArgumentType = argumentType.getJavaType();
        String getterName;
        if (methodArgumentType == boolean.class) {
            getterName = "getBoolean";
        }
        else if (methodArgumentType == long.class) {
            getterName = "getLong";
        }
        else if (methodArgumentType == double.class) {
            getterName = "getDouble";
        }
        else if (methodArgumentType == Slice.class) {
            getterName = "getSlice";
        }
        else {
            getterName = "getObject";
            methodArgumentType = Object.class;
        }

        try {
            MethodHandle getValue = lookup().findVirtual(Type.class, getterName, methodType(methodArgumentType, Block.class, int.class))
                    .bindTo(argumentType);
            return explicitCastArguments(getValue, getValue.type().changeReturnType(expectedType));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static MethodHandle writeBlockValue(Type type)
    {
        Class<?> methodArgumentType = type.getJavaType();
        String getterName;
        if (methodArgumentType == boolean.class) {
            getterName = "writeBoolean";
        }
        else if (methodArgumentType == long.class) {
            getterName = "writeLong";
        }
        else if (methodArgumentType == double.class) {
            getterName = "writeDouble";
        }
        else if (methodArgumentType == Slice.class) {
            getterName = "writeSlice";
        }
        else {
            getterName = "writeObject";
            methodArgumentType = Object.class;
        }

        try {
            return lookup().findVirtual(Type.class, getterName, methodType(void.class, BlockBuilder.class, methodArgumentType))
                    .bindTo(type)
                    .asType(methodType(void.class, BlockBuilder.class, type.getJavaType()));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static MethodHandle getFlatValueNeverNull(Type argumentType, Class<?> expectedType)
    {
        MethodHandle readValueOperator = READ_VALUE_TYPE_OPERATORS.getReadValueOperator(argumentType, InvocationConvention.simpleConvention(FAIL_ON_NULL, FLAT));
        readValueOperator = explicitCastArguments(readValueOperator, readValueOperator.type().changeReturnType(expectedType));
        return readValueOperator;
    }

    private static MethodHandle getInOutValue(Type argumentType, Class<?> expectedType)
    {
        Class<?> methodArgumentType = argumentType.getJavaType();
        String getterName;
        if (methodArgumentType == boolean.class) {
            getterName = "getBooleanValue";
        }
        else if (methodArgumentType == long.class) {
            getterName = "getLongValue";
        }
        else if (methodArgumentType == double.class) {
            getterName = "getDoubleValue";
        }
        else {
            getterName = "getObjectValue";
            methodArgumentType = Object.class;
        }

        try {
            MethodHandle getValue = lookup().findVirtual(InternalDataAccessor.class, getterName, methodType(methodArgumentType));
            return explicitCastArguments(getValue, methodType(expectedType, InOut.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static MethodHandle boxedToNullFlagFilter(Class<?> argumentType)
    {
        // Start with identity
        MethodHandle handle = identity(argumentType);
        // if argument is a primitive, box it
        if (isWrapperType(argumentType)) {
            handle = explicitCastArguments(handle, handle.type().changeParameterType(0, unwrap(argumentType)));
        }
        // Add boolean null flag
        handle = dropArguments(handle, 1, boolean.class);
        // if the flag is true, return null, otherwise invoke identity
        return guardWithTest(
                isTrueNullFlag(handle.type(), 0),
                empty(handle.type()),
                handle);
    }

    private static MethodHandle isTrueNullFlag(MethodType methodType, int index)
    {
        return permuteArguments(identity(boolean.class), methodType.changeReturnType(boolean.class), index + 1);
    }

    private static MethodHandle isNullArgument(MethodType methodType, int index)
    {
        // Start with Objects.isNull(Object):boolean
        MethodHandle isNull = IS_NULL_METHOD;
        // Cast in incoming type: isNull(T):boolean
        isNull = explicitCastArguments(isNull, methodType(boolean.class, methodType.parameterType(index)));
        // Add extra argument to match the expected method type
        isNull = permuteArguments(isNull, methodType.changeReturnType(boolean.class), index);
        return isNull;
    }

    private static MethodHandle isBlockPositionNull(MethodType methodType, int index)
    {
        // Start with Objects.isNull(Object):boolean
        MethodHandle isNull;
        try {
            isNull = lookup().findVirtual(Block.class, "isNull", methodType(boolean.class, int.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
        // Add extra argument to match the expected method type
        isNull = permuteArguments(isNull, methodType.changeReturnType(boolean.class), index, index + 1);
        return isNull;
    }

    private static MethodHandle isInOutNull(MethodType methodType, int index)
    {
        MethodHandle isNull;
        try {
            isNull = lookup().findVirtual(InOut.class, "isNull", methodType(boolean.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
        isNull = permuteArguments(isNull, methodType.changeReturnType(boolean.class), index);
        return isNull;
    }

    private static MethodHandle lookupIsNullMethod()
    {
        try {
            return lookup().findStatic(Objects.class, "isNull", methodType(boolean.class, Object.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static MethodHandle getNullShortCircuitResult(MethodHandle methodHandle, InvocationReturnConvention returnConvention)
    {
        if (returnConvention == BLOCK_BUILDER) {
            return permuteArguments(APPEND_NULL_METHOD, methodHandle.type(), methodHandle.type().parameterCount() - 1);
        }
        return empty(methodHandle.type());
    }

    private static MethodHandle lookupAppendNullMethod()
    {
        try {
            return lookup().findVirtual(BlockBuilder.class, "appendNull", methodType(BlockBuilder.class))
                    .asType(methodType(void.class, BlockBuilder.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static MethodHandle throwTrinoNullArgumentException(MethodType type)
    {
        MethodHandle throwException = collectArguments(throwException(type.returnType(), TrinoException.class), 0, trinoNullArgumentException());
        return permuteArguments(throwException, type);
    }

    private static MethodHandle trinoNullArgumentException()
    {
        try {
            return publicLookup().findConstructor(TrinoException.class, methodType(void.class, ErrorCodeSupplier.class, String.class))
                    .bindTo(StandardErrorCode.INVALID_FUNCTION_ARGUMENT)
                    .bindTo("A never null argument is null");
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static boolean isWrapperType(Class<?> type)
    {
        return type != unwrap(type);
    }

    private static Class<?> wrap(Class<?> type)
    {
        return methodType(type).wrap().returnType();
    }

    private static Class<?> unwrap(Class<?> type)
    {
        return methodType(type).unwrap().returnType();
    }
}
