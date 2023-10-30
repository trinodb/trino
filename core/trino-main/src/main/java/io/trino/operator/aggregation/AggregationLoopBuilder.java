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
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.expression.BytecodeExpressions;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.GroupedAccumulatorState;
import io.trino.sql.gen.CallSiteBinder;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantString;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.trino.sql.gen.BytecodeUtils.invoke;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;

final class AggregationLoopBuilder
{
    private AggregationLoopBuilder() {}

    /**
     * Build a loop over the aggregation function.  Internally, there are multiple loops generated that are specialized for
     * RLE, Dictionary, and basic blocks, and for masked or unmasked input.  The method handle is expected to have a {@link Block} and int
     * position argument for each parameter.  The returned method handle signature, will start with as {@link AggregationMask}
     * and then a single {@link Block} for each parameter.
     */
    public static MethodHandle buildLoop(MethodHandle function, int stateCount, int parameterCount, boolean grouped)
    {
        verifyFunctionSignature(function, stateCount, parameterCount);
        CallSiteBinder binder = new CallSiteBinder();
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, STATIC, FINAL),
                makeClassName("AggregationLoop"),
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        buildSpecializedLoop(binder, definition, function, stateCount, parameterCount, grouped);

        Class<?> clazz = defineClass(definition, Object.class, binder.getBindings(), AggregationLoopBuilder.class.getClassLoader());

        // it is simpler to find the method with reflection than using lookup().findStatic because of the complex signature
        Method invokeMethod = Arrays.stream(clazz.getMethods())
                .filter(method -> method.getName().equals("invoke"))
                .collect(onlyElement());

        try {
            return lookup().unreflect(invokeMethod);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static void buildSpecializedLoop(CallSiteBinder binder, ClassDefinition classDefinition, MethodHandle function, int stateCount, int parameterCount, boolean grouped)
    {
        AggregationParameters aggregationParameters = AggregationParameters.create(function, stateCount, parameterCount, grouped);
        MethodDefinition methodDefinition = classDefinition.declareMethod(
                a(PUBLIC, STATIC),
                "invoke",
                type(void.class),
                aggregationParameters.allParameters());

        Function<List<BlockType>, BytecodeNode> coreLoopBuilder = (blockTypes) -> {
            MethodDefinition method = buildCoreLoop(binder, classDefinition, function, blockTypes, aggregationParameters);
            return invokeStatic(method, aggregationParameters.allParameters().toArray(new BytecodeExpression[0]));
        };

        BytecodeNode bytecodeNode = buildLoopSelection(coreLoopBuilder, new ArrayDeque<>(parameterCount), new ArrayDeque<>(aggregationParameters.blocks()));
        methodDefinition.getBody()
                .append(bytecodeNode)
                .ret();
    }

    private static BytecodeNode buildLoopSelection(Function<List<BlockType>, BytecodeNode> coreLoopBuilder, ArrayDeque<BlockType> currentTypes, ArrayDeque<Parameter> remainingParameters)
    {
        if (remainingParameters.isEmpty()) {
            return coreLoopBuilder.apply(ImmutableList.copyOf(currentTypes));
        }

        // remove the next parameter from the queue
        Parameter blockParameter = remainingParameters.removeFirst();

        currentTypes.addLast(BlockType.VALUE);
        BytecodeNode valueLoop = buildLoopSelection(coreLoopBuilder, currentTypes, remainingParameters);
        currentTypes.removeLast();

        currentTypes.addLast(BlockType.DICTIONARY);
        BytecodeNode dictionaryLoop = buildLoopSelection(coreLoopBuilder, currentTypes, remainingParameters);
        currentTypes.removeLast();

        currentTypes.addLast(BlockType.RLE);
        BytecodeNode rleLoop = buildLoopSelection(coreLoopBuilder, currentTypes, remainingParameters);
        currentTypes.removeLast();

        IfStatement blockTypeSelection = new IfStatement()
                .condition(blockParameter.instanceOf(ValueBlock.class))
                .ifTrue(valueLoop)
                .ifFalse(new IfStatement()
                        .condition(blockParameter.instanceOf(DictionaryBlock.class))
                        .ifTrue(dictionaryLoop)
                        .ifFalse(new IfStatement()
                                .condition(blockParameter.instanceOf(RunLengthEncodedBlock.class))
                                .ifTrue(rleLoop)
                                .ifFalse(new BytecodeBlock()
                                        .append(newInstance(UnsupportedOperationException.class, constantString("Aggregation is not decomposable")))
                                        .throwObject())));

        // restore the parameter to the queue
        remainingParameters.addFirst(blockParameter);

        return blockTypeSelection;
    }

    private static MethodDefinition buildCoreLoop(
            CallSiteBinder binder,
            ClassDefinition classDefinition,
            MethodHandle function,
            List<BlockType> blockTypes,
            AggregationParameters aggregationParameters)
    {
        StringBuilder methodName = new StringBuilder("invoke_");
        for (BlockType blockType : blockTypes) {
            methodName.append(blockType.name().charAt(0));
        }

        MethodDefinition methodDefinition = classDefinition.declareMethod(
                a(PUBLIC, STATIC),
                methodName.toString(),
                type(void.class),
                aggregationParameters.allParameters());
        Scope scope = methodDefinition.getScope();
        BytecodeBlock body = methodDefinition.getBody();

        Variable position = scope.declareVariable(int.class, "position");

        ImmutableList.Builder<BytecodeExpression> aggregationArguments = ImmutableList.builder();
        aggregationArguments.addAll(aggregationParameters.states());
        addBlockPositionArguments(methodDefinition, position, blockTypes, aggregationParameters.blocks(), aggregationArguments);
        aggregationArguments.addAll(aggregationParameters.lambdas());

        BytecodeBlock invokeFunction = new BytecodeBlock();
        if (aggregationParameters.groupIds().isPresent()) {
            // set groupId on state variables
            Variable groupId = scope.declareVariable(int.class, "groupId");
            invokeFunction.append(groupId.set(aggregationParameters.groupIds().get().getElement(position)));
            for (Parameter stateParameter : aggregationParameters.states()) {
                invokeFunction.append(stateParameter.cast(GroupedAccumulatorState.class).invoke("setGroupId", void.class, groupId.cast(long.class)));
            }
        }
        invokeFunction.append(invoke(binder.bind(function), "input", aggregationArguments.build()));

        Variable positionCount = scope.declareVariable("positionCount", body, aggregationParameters.mask().invoke("getSelectedPositionCount", int.class));

        ForLoop selectAllLoop = new ForLoop()
                .initialize(position.set(constantInt(0)))
                .condition(lessThan(position, positionCount))
                .update(position.increment())
                .body(invokeFunction);

        Variable index = scope.declareVariable("index", body, constantInt(0));
        Variable selectedPositions = scope.declareVariable(int[].class, "selectedPositions");
        ForLoop maskedLoop = new ForLoop()
                .initialize(selectedPositions.set(aggregationParameters.mask().invoke("getSelectedPositions", int[].class)))
                .condition(lessThan(index, positionCount))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(position.set(selectedPositions.getElement(index)))
                        .append(invokeFunction));

        body.append(new IfStatement()
                .condition(aggregationParameters.mask().invoke("isSelectAll", boolean.class))
                .ifTrue(selectAllLoop)
                .ifFalse(maskedLoop));
        body.ret();
        return methodDefinition;
    }

    private static void addBlockPositionArguments(
            MethodDefinition methodDefinition,
            Variable position,
            List<BlockType> blockTypes,
            List<Parameter> blockParameters,
            ImmutableList.Builder<BytecodeExpression> aggregationArguments)
    {
        Scope scope = methodDefinition.getScope();
        BytecodeBlock methodBody = methodDefinition.getBody();

        for (int i = 0; i < blockTypes.size(); i++) {
            BlockType blockType = blockTypes.get(i);
            switch (blockType) {
                case VALUE -> {
                    aggregationArguments.add(blockParameters.get(i).cast(ValueBlock.class));
                    aggregationArguments.add(position);
                }
                case DICTIONARY -> {
                    Variable valueBlock = scope.declareVariable(
                            "valueBlock" + i,
                            methodBody,
                            blockParameters.get(i).cast(DictionaryBlock.class).invoke("getDictionary", ValueBlock.class));
                    Variable rawIds = scope.declareVariable(
                            "rawIds" + i,
                            methodBody,
                            blockParameters.get(i).cast(DictionaryBlock.class).invoke("getRawIds", int[].class));
                    Variable rawIdsOffset = scope.declareVariable(
                            "rawIdsOffset" + i,
                            methodBody,
                            blockParameters.get(i).cast(DictionaryBlock.class).invoke("getRawIdsOffset", int.class));
                    aggregationArguments.add(valueBlock);
                    aggregationArguments.add(rawIds.getElement(BytecodeExpressions.add(rawIdsOffset, position)));
                }
                case RLE -> {
                    Variable valueBlock = scope.declareVariable(
                            "valueBlock" + i,
                            methodBody,
                            blockParameters.get(i).cast(RunLengthEncodedBlock.class).invoke("getValue", ValueBlock.class));
                    aggregationArguments.add(valueBlock);
                    aggregationArguments.add(constantInt(0));
                }
            }
        }
    }

    private static void verifyFunctionSignature(MethodHandle function, int stateCount, int parameterCount)
    {
        // verify signature
        List<Class<?>> expectedParameterTypes = ImmutableList.<Class<?>>builder()
                .addAll(function.type().parameterList().subList(0, stateCount))
                .addAll(limit(cycle(ValueBlock.class, int.class), parameterCount * 2))
                .addAll(function.type().parameterList().subList(stateCount + (parameterCount * 2), function.type().parameterCount()))
                .build();
        MethodType expectedSignature = methodType(void.class, expectedParameterTypes);
        checkArgument(function.type().equals(expectedSignature), "Expected function signature to be %s, but is %s", expectedSignature, function.type());
    }

    private record AggregationParameters(Parameter mask, Optional<Parameter> groupIds, List<Parameter> states, List<Parameter> blocks, List<Parameter> lambdas)
    {
        static AggregationParameters create(MethodHandle function, int stateCount, int parameterCount, boolean grouped)
        {
            Parameter mask = arg("aggregationMask", AggregationMask.class);

            Optional<Parameter> groupIds = Optional.empty();
            if (grouped) {
                groupIds = Optional.of(arg("groupIds", int[].class));
            }

            ImmutableList.Builder<Parameter> states = ImmutableList.builder();
            for (int i = 0; i < stateCount; i++) {
                states.add(arg("state" + i, function.type().parameterType(i)));
            }

            ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
            for (int i = 0; i < parameterCount; i++) {
                parameters.add(arg("block" + i, Block.class));
            }

            ImmutableList.Builder<Parameter> lambdas = ImmutableList.builder();
            int lambdaFunctionOffset = stateCount + (parameterCount * 2);
            for (int i = 0; i < function.type().parameterCount() - lambdaFunctionOffset; i++) {
                lambdas.add(arg("lambda" + i, function.type().parameterType(lambdaFunctionOffset + i)));
            }

            return new AggregationParameters(mask, groupIds, states.build(), parameters.build(), lambdas.build());
        }

        public List<Parameter> allParameters()
        {
            return ImmutableList.<Parameter>builder()
                    .add(mask)
                    .addAll(groupIds.stream().iterator())
                    .addAll(states)
                    .addAll(blocks)
                    .addAll(lambdas)
                    .build();
        }
    }

    private enum BlockType
    {
        RLE, DICTIONARY, VALUE
    }
}
