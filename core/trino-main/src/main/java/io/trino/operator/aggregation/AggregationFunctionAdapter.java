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

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.BLOCK_INDEX;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.NULLABLE_BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.STATE;
import static java.lang.invoke.MethodHandles.collectArguments;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.util.Objects.requireNonNull;

public final class AggregationFunctionAdapter
{
    public enum AggregationParameterKind
    {
        INPUT_CHANNEL,
        BLOCK_INPUT_CHANNEL,
        NULLABLE_BLOCK_INPUT_CHANNEL,
        BLOCK_INDEX,
        STATE
    }

    private static final MethodHandle BOOLEAN_TYPE_GETTER;
    private static final MethodHandle LONG_TYPE_GETTER;
    private static final MethodHandle DOUBLE_TYPE_GETTER;
    private static final MethodHandle OBJECT_TYPE_GETTER;

    static {
        try {
            BOOLEAN_TYPE_GETTER = lookup().findVirtual(Type.class, "getBoolean", MethodType.methodType(boolean.class, Block.class, int.class));
            LONG_TYPE_GETTER = lookup().findVirtual(Type.class, "getLong", MethodType.methodType(long.class, Block.class, int.class));
            DOUBLE_TYPE_GETTER = lookup().findVirtual(Type.class, "getDouble", MethodType.methodType(double.class, Block.class, int.class));
            OBJECT_TYPE_GETTER = lookup().findVirtual(Type.class, "getObject", MethodType.methodType(Object.class, Block.class, int.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private AggregationFunctionAdapter() {}

    public static MethodHandle normalizeInputMethod(
            MethodHandle inputMethod,
            BoundSignature boundSignature,
            AggregationParameterKind... parameterKinds)
    {
        return normalizeInputMethod(inputMethod, boundSignature, ImmutableList.copyOf(parameterKinds));
    }

    public static MethodHandle normalizeInputMethod(
            MethodHandle inputMethod,
            BoundSignature boundSignature,
            List<AggregationParameterKind> parameterKinds)
    {
        return normalizeInputMethod(inputMethod, boundSignature, parameterKinds, 0);
    }

    public static MethodHandle normalizeInputMethod(
            MethodHandle inputMethod,
            BoundSignature boundSignature,
            List<AggregationParameterKind> parameterKinds,
            int lambdaCount)
    {
        requireNonNull(inputMethod, "inputMethod is null");
        requireNonNull(parameterKinds, "parameterKinds is null");
        requireNonNull(boundSignature, "boundSignature is null");

        checkArgument(
                inputMethod.type().parameterCount() - lambdaCount == parameterKinds.size(),
                "Input method has %s parameters, but parameter kinds only has %s items",
                inputMethod.type().parameterCount() - lambdaCount,
                parameterKinds.size());

        List<AggregationParameterKind> stateArgumentKinds = parameterKinds.stream().filter(STATE::equals).collect(toImmutableList());
        List<AggregationParameterKind> inputArgumentKinds = parameterKinds.stream()
                .filter(kind -> kind == INPUT_CHANNEL || kind == BLOCK_INPUT_CHANNEL || kind == NULLABLE_BLOCK_INPUT_CHANNEL)
                .collect(toImmutableList());
        boolean hasInputChannel = parameterKinds.stream().anyMatch(kind -> kind == BLOCK_INPUT_CHANNEL || kind == NULLABLE_BLOCK_INPUT_CHANNEL);

        checkArgument(
                boundSignature.getArgumentTypes().size() - lambdaCount == inputArgumentKinds.size(),
                "Bound signature has %s arguments, but parameter kinds only has %s input arguments",
                boundSignature.getArgumentTypes().size() - lambdaCount,
                inputArgumentKinds.size());

        List<AggregationParameterKind> expectedInputArgumentKinds = new ArrayList<>();
        expectedInputArgumentKinds.addAll(stateArgumentKinds);
        expectedInputArgumentKinds.addAll(inputArgumentKinds);
        if (hasInputChannel) {
            expectedInputArgumentKinds.add(BLOCK_INDEX);
        }
        checkArgument(
                expectedInputArgumentKinds.equals(parameterKinds),
                "Expected input parameter kinds %s, but got %s",
                expectedInputArgumentKinds,
                parameterKinds);

        MethodType inputMethodType = inputMethod.type();
        for (int argumentIndex = 0; argumentIndex < inputArgumentKinds.size(); argumentIndex++) {
            int parameterIndex = stateArgumentKinds.size() + argumentIndex;
            AggregationParameterKind inputArgument = inputArgumentKinds.get(argumentIndex);
            if (inputArgument != INPUT_CHANNEL) {
                continue;
            }
            Type argumentType = boundSignature.getArgumentType(argumentIndex);

            // process argument through type value getter
            MethodHandle valueGetter;
            if (argumentType.getJavaType().equals(boolean.class)) {
                valueGetter = BOOLEAN_TYPE_GETTER.bindTo(argumentType);
            }
            else if (argumentType.getJavaType().equals(long.class)) {
                valueGetter = LONG_TYPE_GETTER.bindTo(argumentType);
            }
            else if (argumentType.getJavaType().equals(double.class)) {
                valueGetter = DOUBLE_TYPE_GETTER.bindTo(argumentType);
            }
            else {
                valueGetter = OBJECT_TYPE_GETTER.bindTo(argumentType);
                valueGetter = valueGetter.asType(valueGetter.type().changeReturnType(inputMethodType.parameterType(parameterIndex)));
            }
            inputMethod = collectArguments(inputMethod, parameterIndex, valueGetter);

            // move the position argument to the end (and combine with other existing position argument)
            inputMethodType = inputMethodType.changeParameterType(parameterIndex, Block.class);

            ArrayList<Integer> reorder;
            if (hasInputChannel) {
                reorder = IntStream.range(0, inputMethodType.parameterCount()).boxed().collect(Collectors.toCollection(ArrayList::new));
                reorder.add(parameterIndex + 1, inputMethodType.parameterCount() - 1 - lambdaCount);
            }
            else {
                inputMethodType = inputMethodType.insertParameterTypes(inputMethodType.parameterCount() - lambdaCount, int.class);
                reorder = IntStream.range(0, inputMethodType.parameterCount()).boxed().collect(Collectors.toCollection(ArrayList::new));
                int positionParameterIndex = inputMethodType.parameterCount() - 1 - lambdaCount;
                reorder.remove(positionParameterIndex);
                reorder.add(parameterIndex + 1, positionParameterIndex);
                hasInputChannel = true;
            }
            inputMethod = permuteArguments(inputMethod, inputMethodType, reorder.stream().mapToInt(Integer::intValue).toArray());
        }
        return inputMethod;
    }
}
