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
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.expression.BytecodeExpressions;
import io.trino.operator.window.InternalWindowIndex;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RowValueBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.function.AggregationImplementation.AccumulatorStateDescriptor;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.GroupedAccumulatorState;
import io.trino.spi.function.WindowIndex;
import io.trino.sql.gen.Binding;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.gen.CompilerOperations;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantLong;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantString;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.trino.operator.aggregation.AggregationMaskCompiler.generateAggregationMaskBuilder;
import static io.trino.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static io.trino.sql.gen.BytecodeUtils.invoke;
import static io.trino.sql.gen.BytecodeUtils.loadConstant;
import static io.trino.sql.gen.LambdaMetafactoryGenerator.generateMetafactory;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class AccumulatorCompiler
{
    private AccumulatorCompiler() {}

    public static AccumulatorFactory generateAccumulatorFactory(
            BoundSignature boundSignature,
            AggregationImplementation implementation,
            FunctionNullability functionNullability)
    {
        // change types used in Aggregation methods to types used in the core Trino engine to simplify code generation
        implementation = normalizeAggregationMethods(implementation);

        DynamicClassLoader classLoader = new DynamicClassLoader(AccumulatorCompiler.class.getClassLoader());

        List<Boolean> argumentNullable = functionNullability.getArgumentNullable()
                .subList(0, functionNullability.getArgumentNullable().size() - implementation.getLambdaInterfaces().size());

        Constructor<? extends Accumulator> accumulatorConstructor = generateAccumulatorClass(
                boundSignature,
                Accumulator.class,
                implementation,
                argumentNullable,
                classLoader);

        Constructor<? extends GroupedAccumulator> groupedAccumulatorConstructor = generateAccumulatorClass(
                boundSignature,
                GroupedAccumulator.class,
                implementation,
                argumentNullable,
                classLoader);

        List<Integer> nonNullArguments = new ArrayList<>();
        for (int argumentIndex = 0; argumentIndex < argumentNullable.size(); argumentIndex++) {
            if (!argumentNullable.get(argumentIndex)) {
                nonNullArguments.add(argumentIndex);
            }
        }
        Constructor<? extends AggregationMaskBuilder> maskBuilderConstructor = generateAggregationMaskBuilder(nonNullArguments.stream().mapToInt(Integer::intValue).toArray());

        return new CompiledAccumulatorFactory(
                accumulatorConstructor,
                groupedAccumulatorConstructor,
                implementation.getLambdaInterfaces(),
                maskBuilderConstructor);
    }

    private static <T> Constructor<? extends T> generateAccumulatorClass(
            BoundSignature boundSignature,
            Class<T> accumulatorInterface,
            AggregationImplementation implementation,
            List<Boolean> argumentNullable,
            DynamicClassLoader classLoader)
    {
        boolean grouped = accumulatorInterface == GroupedAccumulator.class;

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(boundSignature.getName() + accumulatorInterface.getSimpleName()),
                type(Object.class),
                type(accumulatorInterface));

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        List<AccumulatorStateDescriptor<?>> stateDescriptors = implementation.getAccumulatorStateDescriptors();
        List<StateFieldAndDescriptor> stateFieldAndDescriptors = new ArrayList<>();
        for (int i = 0; i < stateDescriptors.size(); i++) {
            stateFieldAndDescriptors.add(new StateFieldAndDescriptor(
                    stateDescriptors.get(i),
                    definition.declareField(a(PRIVATE, FINAL), "stateSerializer_" + i, AccumulatorStateSerializer.class),
                    definition.declareField(a(PRIVATE, FINAL), "stateFactory_" + i, AccumulatorStateFactory.class),
                    definition.declareField(a(PRIVATE, FINAL), "state_" + i, grouped ? GroupedAccumulatorState.class : AccumulatorState.class)));
        }
        List<FieldDefinition> stateFields = stateFieldAndDescriptors.stream()
                .map(StateFieldAndDescriptor::getStateField)
                .collect(toImmutableList());

        int lambdaCount = implementation.getLambdaInterfaces().size();
        List<FieldDefinition> lambdaProviderFields = new ArrayList<>(lambdaCount);
        for (int i = 0; i < lambdaCount; i++) {
            lambdaProviderFields.add(definition.declareField(a(PRIVATE, FINAL), "lambdaProvider_" + i, Supplier.class));
        }

        // Generate constructors
        generateConstructor(
                definition,
                stateFieldAndDescriptors,
                lambdaProviderFields,
                callSiteBinder,
                grouped);
        generateCopyConstructor(
                definition,
                stateFieldAndDescriptors,
                lambdaProviderFields);

        // Generate methods
        generateCopy(definition, Accumulator.class);

        generateAddInput(
                definition,
                stateFields,
                argumentNullable,
                lambdaProviderFields,
                implementation.getInputFunction(),
                callSiteBinder,
                grouped);
        generateGetEstimatedSize(definition, stateFields);

        if (grouped) {
            generateSetGroupCount(definition, stateFields);
        }

        generateAddIntermediateAsCombine(
                definition,
                stateFieldAndDescriptors,
                lambdaProviderFields,
                implementation.getCombineFunction(),
                callSiteBinder,
                grouped);

        if (grouped) {
            generateGroupedEvaluateIntermediate(definition, stateFieldAndDescriptors, true);
        }
        else {
            generateEvaluateIntermediate(definition, stateFieldAndDescriptors, true);
        }

        if (grouped) {
            generateGroupedEvaluateFinal(definition, stateFields, implementation.getOutputFunction(), callSiteBinder);
        }
        else {
            generateEvaluateFinal(definition, stateFields, implementation.getOutputFunction(), callSiteBinder);
        }

        if (grouped) {
            generatePrepareFinal(definition);
        }

        Class<? extends T> accumulatorClass = defineClass(definition, accumulatorInterface, callSiteBinder.getBindings(), classLoader);
        try {
            return accumulatorClass.getConstructor(List.class);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public static Constructor<? extends WindowAccumulator> generateWindowAccumulatorClass(
            BoundSignature boundSignature,
            AggregationImplementation implementation,
            FunctionNullability functionNullability)
    {
        // change types used in Aggregation methods to types used in the core Trino engine to simplify code generation
        implementation = normalizeAggregationMethods(implementation);

        DynamicClassLoader classLoader = new DynamicClassLoader(AccumulatorCompiler.class.getClassLoader());

        List<Boolean> argumentNullable = functionNullability.getArgumentNullable()
                .subList(0, functionNullability.getArgumentNullable().size() - implementation.getLambdaInterfaces().size());

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(boundSignature.getName() + WindowAccumulator.class.getSimpleName()),
                type(Object.class),
                type(WindowAccumulator.class));

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        List<AccumulatorStateDescriptor<?>> stateDescriptors = implementation.getAccumulatorStateDescriptors();
        List<StateFieldAndDescriptor> stateFieldAndDescriptors = new ArrayList<>();
        for (int i = 0; i < stateDescriptors.size(); i++) {
            stateFieldAndDescriptors.add(new StateFieldAndDescriptor(
                    stateDescriptors.get(i),
                    definition.declareField(a(PRIVATE, FINAL), "stateSerializer_" + i, AccumulatorStateSerializer.class),
                    definition.declareField(a(PRIVATE, FINAL), "stateFactory_" + i, AccumulatorStateFactory.class),
                    definition.declareField(a(PRIVATE, FINAL), "state_" + i, AccumulatorState.class)));
        }
        List<FieldDefinition> stateFields = stateFieldAndDescriptors.stream()
                .map(StateFieldAndDescriptor::getStateField)
                .collect(toImmutableList());

        int lambdaCount = implementation.getLambdaInterfaces().size();
        List<FieldDefinition> lambdaProviderFields = new ArrayList<>(lambdaCount);
        for (int i = 0; i < lambdaCount; i++) {
            lambdaProviderFields.add(definition.declareField(a(PRIVATE, FINAL), "lambdaProvider_" + i, Supplier.class));
        }

        // Generate constructor
        generateWindowAccumulatorConstructor(
                definition,
                stateFieldAndDescriptors,
                lambdaProviderFields,
                callSiteBinder);
        generateCopyConstructor(
                definition,
                stateFieldAndDescriptors,
                lambdaProviderFields);

        // Generate methods
        generateCopy(definition, WindowAccumulator.class);
        generateAddOrRemoveInputWindowIndex(
                definition,
                stateFields,
                argumentNullable,
                lambdaProviderFields,
                implementation.getInputFunction(),
                "addInput",
                callSiteBinder);
        implementation.getRemoveInputFunction().ifPresent(
                removeInputFunction -> generateAddOrRemoveInputWindowIndex(
                        definition,
                        stateFields,
                        argumentNullable,
                        lambdaProviderFields,
                        removeInputFunction,
                        "removeInput",
                        callSiteBinder));

        generateEvaluateFinal(definition, stateFields, implementation.getOutputFunction(), callSiteBinder);
        generateGetEstimatedSize(definition, stateFields);

        Class<? extends WindowAccumulator> windowAccumulatorClass = defineClass(definition, WindowAccumulator.class, callSiteBinder.getBindings(), classLoader);
        try {
            return windowAccumulatorClass.getConstructor(List.class);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static void generateWindowAccumulatorConstructor(
            ClassDefinition definition,
            List<StateFieldAndDescriptor> stateFieldAndDescriptors,
            List<FieldDefinition> lambdaProviderFields,
            CallSiteBinder callSiteBinder)
    {
        Parameter lambdaProviders = arg("lambdaProviders", type(List.class, Supplier.class));
        MethodDefinition method = definition.declareConstructor(
                a(PUBLIC),
                lambdaProviders);

        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);
        initializeStateFields(method, stateFieldAndDescriptors, callSiteBinder, false);
        initializeLambdaProviderFields(method, lambdaProviderFields, lambdaProviders);
        body.ret();
    }

    private static void generateGetEstimatedSize(ClassDefinition definition, List<FieldDefinition> stateFields)
    {
        MethodDefinition method = definition.declareMethod(a(PUBLIC), "getEstimatedSize", type(long.class));
        Variable estimatedSize = method.getScope().declareVariable(long.class, "estimatedSize");
        method.getBody().append(estimatedSize.set(constantLong(0L)));

        for (FieldDefinition stateField : stateFields) {
            method.getBody()
                    .append(estimatedSize.set(
                            BytecodeExpressions.add(
                                    estimatedSize,
                                    method.getThis().getField(stateField).invoke("getEstimatedSize", long.class))));
        }
        method.getBody().append(estimatedSize.ret());
    }

    private static void generateSetGroupCount(ClassDefinition definition, List<FieldDefinition> stateFields)
    {
        Parameter groupCount = arg("groupCount", long.class);

        MethodDefinition method = definition.declareMethod(a(PUBLIC), "setGroupCount", type(void.class), groupCount);
        BytecodeBlock body = method.getBody();
        for (FieldDefinition stateField : stateFields) {
            BytecodeExpression state = method.getScope().getThis().getField(stateField);
            body.append(state.invoke("ensureCapacity", void.class, groupCount));
        }
        body.ret();
    }

    private static void generateAddInput(
            ClassDefinition definition,
            List<FieldDefinition> stateField,
            List<Boolean> argumentNullable,
            List<FieldDefinition> lambdaProviderFields,
            MethodHandle inputFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        if (grouped) {
            parameters.add(arg("groupIds", int[].class));
        }
        Parameter arguments = arg("arguments", Page.class);
        parameters.add(arguments);
        Parameter mask = arg("mask", AggregationMask.class);
        parameters.add(mask);

        MethodDefinition method = definition.declareMethod(a(PUBLIC), "addInput", type(void.class), parameters.build());
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        List<Variable> parameterVariables = new ArrayList<>();
        for (int i = 0; i < argumentNullable.size(); i++) {
            parameterVariables.add(scope.declareVariable(Block.class, "block" + i));
        }

        // Get all parameter blocks
        for (int i = 0; i < parameterVariables.size(); i++) {
            body.comment("%s = arguments.getBlock(%d);", parameterVariables.get(i).getName(), i)
                    .append(parameterVariables.get(i).set(arguments.invoke("getBlock", Block.class, constantInt(i))));
        }

        BytecodeBlock block = generateInputForLoop(
                stateField,
                inputFunction,
                scope,
                parameterVariables,
                lambdaProviderFields,
                mask,
                callSiteBinder,
                grouped);

        body.append(block);
        body.ret();
    }

    private static void generateAddOrRemoveInputWindowIndex(
            ClassDefinition definition,
            List<FieldDefinition> stateField,
            List<Boolean> argumentNullable,
            List<FieldDefinition> lambdaProviderFields,
            MethodHandle inputFunction,
            String generatedFunctionName,
            CallSiteBinder callSiteBinder)
    {
        // TODO: implement masking based on maskChannel field once Window Functions support DISTINCT arguments to the functions.

        Parameter index = arg("index", WindowIndex.class);
        Parameter startPosition = arg("startPosition", int.class);
        Parameter endPosition = arg("endPosition", int.class);

        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                generatedFunctionName,
                type(void.class),
                ImmutableList.of(index, startPosition, endPosition));
        Scope scope = method.getScope();

        Variable position = scope.declareVariable(int.class, "position");

        Binding binding = callSiteBinder.bind(inputFunction);
        BytecodeExpression invokeInputFunction = invokeDynamic(
                BOOTSTRAP_METHOD,
                ImmutableList.of(binding.getBindingId()),
                generatedFunctionName,
                binding.getType(),
                getInvokeFunctionOnWindowIndexParameters(
                        scope,
                        argumentNullable.size(),
                        lambdaProviderFields,
                        stateField,
                        index,
                        position));

        method.getBody()
                .append(new ForLoop()
                        .initialize(position.set(startPosition))
                        .condition(BytecodeExpressions.lessThanOrEqual(position, endPosition))
                        .update(position.increment())
                        .body(new IfStatement()
                                .condition(anyParametersAreNull(argumentNullable, index, position))
                                .ifFalse(invokeInputFunction)))
                .ret();
    }

    private static BytecodeExpression anyParametersAreNull(
            List<Boolean> argumentNullable,
            Variable index,
            Variable position)
    {
        BytecodeExpression isNull = constantFalse();
        for (int inputChannel = 0; inputChannel < argumentNullable.size(); inputChannel++) {
            if (!argumentNullable.get(inputChannel)) {
                isNull = BytecodeExpressions.or(isNull, index.invoke("isNull", boolean.class, constantInt(inputChannel), position));
            }
        }

        return isNull;
    }

    private static List<BytecodeExpression> getInvokeFunctionOnWindowIndexParameters(
            Scope scope,
            int inputParameterCount,
            List<FieldDefinition> lambdaProviderFields,
            List<FieldDefinition> stateField,
            Variable index,
            Variable position)
    {
        List<BytecodeExpression> expressions = new ArrayList<>();

        // state parameters
        for (FieldDefinition field : stateField) {
            expressions.add(scope.getThis().getField(field));
        }

        // input parameters
        for (int i = 0; i < inputParameterCount; i++) {
            expressions.add(index.cast(InternalWindowIndex.class).invoke("getRawBlock", Block.class, constantInt(i), position));
        }

        // position parameter
        if (inputParameterCount > 0) {
            expressions.add(index.cast(InternalWindowIndex.class).invoke("getRawBlockPosition", int.class, position));
        }

        // lambda parameters
        for (FieldDefinition lambdaProviderField : lambdaProviderFields) {
            expressions.add(scope.getThis().getField(lambdaProviderField)
                    .invoke("get", Object.class));
        }

        return expressions;
    }

    private static BytecodeBlock generateInputForLoop(
            List<FieldDefinition> stateField,
            MethodHandle inputFunction,
            Scope scope,
            List<Variable> parameterVariables,
            List<FieldDefinition> lambdaProviderFields,
            Variable mask,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        // For-loop over rows
        Variable positionVariable = scope.declareVariable(int.class, "position");
        Variable rowsVariable = scope.declareVariable(int.class, "rows");
        Variable selectedPositionsArrayVariable = scope.declareVariable(int[].class, "selectedPositionsArray");
        Variable selectedPositionVariable = scope.declareVariable(int.class, "selectedPosition");

        BytecodeBlock block = new BytecodeBlock()
                .initializeVariable(rowsVariable)
                .initializeVariable(selectedPositionVariable)
                .initializeVariable(positionVariable);

        ForLoop selectAllLoop = new ForLoop()
                .initialize(new BytecodeBlock()
                        .append(rowsVariable.set(mask.invoke("getPositionCount", int.class)))
                        .append(positionVariable.set(constantInt(0))))
                .condition(BytecodeExpressions.lessThan(positionVariable, rowsVariable))
                .update(new BytecodeBlock().incrementVariable(positionVariable, (byte) 1))
                .body(generateInvokeInputFunction(
                        scope,
                        stateField,
                        positionVariable,
                        parameterVariables,
                        lambdaProviderFields,
                        inputFunction,
                        callSiteBinder,
                        grouped));

        ForLoop selectedPositionsLoop = new ForLoop()
                .initialize(new BytecodeBlock()
                        .append(rowsVariable.set(mask.invoke("getSelectedPositionCount", int.class)))
                        .append(selectedPositionsArrayVariable.set(mask.invoke("getSelectedPositions", int[].class)))
                        .append(positionVariable.set(constantInt(0))))
                .condition(BytecodeExpressions.lessThan(positionVariable, rowsVariable))
                .update(new BytecodeBlock().incrementVariable(positionVariable, (byte) 1))
                .body(new BytecodeBlock()
                        .append(selectedPositionVariable.set(selectedPositionsArrayVariable.getElement(positionVariable)))
                        .append(generateInvokeInputFunction(
                                scope,
                                stateField,
                                selectedPositionVariable,
                                parameterVariables,
                                lambdaProviderFields,
                                inputFunction,
                                callSiteBinder,
                                grouped)));

        block.append(new IfStatement()
                .condition(mask.invoke("isSelectAll", boolean.class))
                .ifTrue(selectAllLoop)
                .ifFalse(selectedPositionsLoop));

        return block;
    }

    private static BytecodeBlock generateInvokeInputFunction(
            Scope scope,
            List<FieldDefinition> stateField,
            Variable position,
            List<Variable> parameterVariables,
            List<FieldDefinition> lambdaProviderFields,
            MethodHandle inputFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        BytecodeBlock block = new BytecodeBlock();

        if (grouped) {
            generateSetGroupIdFromGroupIds(scope, stateField, block, position);
        }

        block.comment("Call input function with unpacked Block arguments");

        List<BytecodeExpression> parameters = new ArrayList<>();

        // state parameters
        for (FieldDefinition field : stateField) {
            parameters.add(scope.getThis().getField(field));
        }

        // input parameters
        parameters.addAll(parameterVariables);

        // position parameter
        if (!parameterVariables.isEmpty()) {
            parameters.add(position);
        }

        // lambda parameters
        for (FieldDefinition lambdaProviderField : lambdaProviderFields) {
            parameters.add(scope.getThis().getField(lambdaProviderField)
                    .invoke("get", Object.class));
        }

        block.append(invoke(callSiteBinder.bind(inputFunction), "input", parameters));
        return block;
    }

    private static void generateAddIntermediateAsCombine(
            ClassDefinition definition,
            List<StateFieldAndDescriptor> stateFieldAndDescriptors,
            List<FieldDefinition> lambdaProviderFields,
            Optional<MethodHandle> combineFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        MethodDefinition method = declareAddIntermediate(definition, grouped);
        if (combineFunction.isEmpty()) {
            method.getBody()
                    .append(newInstance(UnsupportedOperationException.class, constantString("Aggregation is not decomposable")))
                    .throwObject();
            return;
        }
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        int stateCount = stateFieldAndDescriptors.size();
        List<Variable> scratchStates = new ArrayList<>();
        for (int i = 0; i < stateCount; i++) {
            Class<?> scratchStateClass = AccumulatorState.class;
            scratchStates.add(scope.declareVariable(scratchStateClass, "scratchState_" + i));
        }

        List<Variable> block;
        if (stateCount == 1) {
            block = ImmutableList.of(scope.getVariable("block"));
        }
        else {
            // ColumnarRow is used to get the column blocks represents each state, this allows to
            //  1. handle single state and multiple states in a unified way
            //  2. avoid the cost of constructing SingleRowBlock for each group
            Variable columnarRow = scope.declareVariable(ColumnarRow.class, "columnarRow");
            body.append(columnarRow.set(
                    invokeStatic(ColumnarRow.class, "toColumnarRow", ColumnarRow.class, scope.getVariable("block"))));

            block = new ArrayList<>();
            for (int i = 0; i < stateCount; i++) {
                Variable columnBlock = scope.declareVariable(Block.class, "columnBlock_" + i);
                body.append(columnBlock.set(
                        columnarRow.invoke("getField", Block.class, constantInt(i))));
                block.add(columnBlock);
            }
        }

        Variable position = scope.declareVariable(int.class, "position");
        for (int i = 0; i < stateCount; i++) {
            FieldDefinition stateFactoryField = stateFieldAndDescriptors.get(i).getStateFactoryField();
            body.comment(format("scratchState_%s = stateFactory[%s].createSingleState();", i, i))
                    .append(thisVariable.getField(stateFactoryField))
                    .invokeInterface(AccumulatorStateFactory.class, "createSingleState", AccumulatorState.class)
                    .checkCast(scratchStates.get(i).getType())
                    .putVariable(scratchStates.get(i));
        }

        List<FieldDefinition> stateFields = stateFieldAndDescriptors.stream()
                .map(StateFieldAndDescriptor::getStateField)
                .collect(toImmutableList());

        BytecodeBlock loopBody = new BytecodeBlock();

        loopBody.comment("combine(state_0, state_1, ... scratchState_0, scratchState_1, ... lambda_0, lambda_1, ...)");
        for (FieldDefinition stateField : stateFields) {
            if (grouped) {
                Variable groupIds = scope.getVariable("groupIds");
                loopBody.append(thisVariable.getField(stateField).invoke("setGroupId", void.class, groupIds.getElement(position).cast(long.class)));
            }
            loopBody.append(thisVariable.getField(stateField));
        }
        for (int i = 0; i < stateCount; i++) {
            FieldDefinition stateSerializerField = stateFieldAndDescriptors.get(i).getStateSerializerField();
            loopBody.append(thisVariable.getField(stateSerializerField).invoke("deserialize", void.class, block.get(i), position, scratchStates.get(i).cast(AccumulatorState.class)));
            loopBody.append(scratchStates.get(i));
        }
        for (FieldDefinition lambdaProviderField : lambdaProviderFields) {
            loopBody.append(scope.getThis().getField(lambdaProviderField)
                    .invoke("get", Object.class));
        }
        loopBody.append(invoke(callSiteBinder.bind(combineFunction.get()), "combine"));

        body.append(generateBlockNonNullPositionForLoop(scope, position, loopBody))
                .ret();
    }

    private static void generateSetGroupIdFromGroupIds(Scope scope, List<FieldDefinition> stateFields, BytecodeBlock block, Variable position)
    {
        Variable groupIds = scope.getVariable("groupIds");
        for (FieldDefinition stateField : stateFields) {
            BytecodeExpression state = scope.getThis().getField(stateField);
            block.append(state.invoke("setGroupId", void.class, groupIds.getElement(position).cast(long.class)));
        }
    }

    private static MethodDefinition declareAddIntermediate(ClassDefinition definition, boolean grouped)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        if (grouped) {
            parameters.add(arg("groupIds", int[].class));
        }
        parameters.add(arg("block", Block.class));

        return definition.declareMethod(
                a(PUBLIC),
                "addIntermediate",
                type(void.class),
                parameters.build());
    }

    // Generates a for-loop with a local variable named "position" defined, with the current position in the block,
    // loopBody will only be executed for non-null positions in the Block
    private static BytecodeBlock generateBlockNonNullPositionForLoop(Scope scope, Variable positionVariable, BytecodeBlock loopBody)
    {
        Variable rowsVariable = scope.declareVariable(int.class, "rows");
        Variable blockVariable = scope.getVariable("block");

        BytecodeBlock block = new BytecodeBlock()
                .append(blockVariable)
                .invokeInterface(Block.class, "getPositionCount", int.class)
                .putVariable(rowsVariable);

        IfStatement ifStatement = new IfStatement("if(!block.isNull(position))")
                .condition(new BytecodeBlock()
                        .append(blockVariable)
                        .append(positionVariable)
                        .invokeInterface(Block.class, "isNull", boolean.class, int.class))
                .ifFalse(loopBody);

        block.append(new ForLoop()
                .initialize(positionVariable.set(constantInt(0)))
                .condition(new BytecodeBlock()
                        .append(positionVariable)
                        .append(rowsVariable)
                        .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
                .update(new BytecodeBlock().incrementVariable(positionVariable, (byte) 1))
                .body(ifStatement));

        return block;
    }

    private static void generateGroupedEvaluateIntermediate(ClassDefinition definition, List<StateFieldAndDescriptor> stateFieldAndDescriptors, boolean decomposable)
    {
        Parameter groupId = arg("groupId", int.class);
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(a(PUBLIC), "evaluateIntermediate", type(void.class), groupId, out);

        if (!decomposable) {
            method.getBody()
                    .append(newInstance(UnsupportedOperationException.class, constantString("Aggregation is not decomposable")))
                    .throwObject();
            return;
        }

        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        if (stateFieldAndDescriptors.size() == 1) {
            BytecodeExpression stateSerializer = thisVariable.getField(getOnlyElement(stateFieldAndDescriptors).getStateSerializerField());
            BytecodeExpression state = thisVariable.getField(getOnlyElement(stateFieldAndDescriptors).getStateField());

            body.append(state.invoke("setGroupId", void.class, groupId.cast(long.class)))
                    .append(stateSerializer.invoke("serialize", void.class, state.cast(AccumulatorState.class), out))
                    .ret();
        }
        else {
            for (StateFieldAndDescriptor stateFieldAndDescriptor : stateFieldAndDescriptors) {
                BytecodeExpression state = thisVariable.getField(stateFieldAndDescriptor.getStateField());
                body.append(state.invoke("setGroupId", void.class, groupId.cast(long.class)));
            }

            generateSerializeState(definition, stateFieldAndDescriptors, out, thisVariable, body);
            body.ret();
        }
    }

    private static void generateEvaluateIntermediate(ClassDefinition definition, List<StateFieldAndDescriptor> stateFieldAndDescriptors, boolean decomposable)
    {
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "evaluateIntermediate",
                type(void.class),
                out);

        if (!decomposable) {
            method.getBody()
                    .append(newInstance(UnsupportedOperationException.class, constantString("Aggregation is not decomposable")))
                    .throwObject();
            return;
        }

        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        if (stateFieldAndDescriptors.size() == 1) {
            BytecodeExpression stateSerializer = thisVariable.getField(getOnlyElement(stateFieldAndDescriptors).getStateSerializerField());
            BytecodeExpression state = thisVariable.getField(getOnlyElement(stateFieldAndDescriptors).getStateField());

            body.append(stateSerializer.invoke("serialize", void.class, state.cast(AccumulatorState.class), out))
                    .ret();
        }
        else {
            generateSerializeState(definition, stateFieldAndDescriptors, out, thisVariable, body);
            body.ret();
        }
    }

    private static void generateSerializeState(ClassDefinition definition, List<StateFieldAndDescriptor> stateFieldAndDescriptors, Parameter out, Variable thisVariable, BytecodeBlock body)
    {
        MethodDefinition serializeState = generateSerializeStateMethod(definition, stateFieldAndDescriptors);

        BytecodeExpression rowEntryBuilder = generateMetafactory(RowValueBuilder.class, serializeState, ImmutableList.of(thisVariable));
        body.append(out.cast(RowBlockBuilder.class).invoke("buildEntry", void.class, rowEntryBuilder));
    }

    private static MethodDefinition generateSerializeStateMethod(ClassDefinition definition, List<StateFieldAndDescriptor> stateFieldAndDescriptors)
    {
        Parameter fieldBuilders = arg("fieldBuilders", type(List.class, BlockBuilder.class));
        MethodDefinition method = definition.declareMethod(a(PRIVATE), "serializeState", type(void.class), fieldBuilders);

        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        for (int i = 0; i < stateFieldAndDescriptors.size(); i++) {
            StateFieldAndDescriptor stateFieldAndDescriptor = stateFieldAndDescriptors.get(i);
            BytecodeExpression stateSerializer = thisVariable.getField(stateFieldAndDescriptor.getStateSerializerField());
            BytecodeExpression state = thisVariable.getField(stateFieldAndDescriptor.getStateField());
            BytecodeExpression fieldBuilder = fieldBuilders.invoke("get", Object.class, constantInt(i)).cast(BlockBuilder.class);
            body.append(stateSerializer.invoke("serialize", void.class, state.cast(AccumulatorState.class), fieldBuilder));
        }
        body.ret();
        return method;
    }

    private static void generateGroupedEvaluateFinal(
            ClassDefinition definition,
            List<FieldDefinition> stateFields,
            MethodHandle outputFunction,
            CallSiteBinder callSiteBinder)
    {
        Parameter groupId = arg("groupId", int.class);
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(a(PUBLIC), "evaluateFinal", type(void.class), groupId, out);

        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        List<BytecodeExpression> states = new ArrayList<>();

        for (FieldDefinition stateField : stateFields) {
            BytecodeExpression state = thisVariable.getField(stateField);
            body.append(state.invoke("setGroupId", void.class, groupId.cast(long.class)));
            states.add(state);
        }

        body.comment("output(state_0, state_1, ..., out)");
        states.forEach(body::append);
        body.append(out);
        body.append(invoke(callSiteBinder.bind(outputFunction), "output"));

        body.ret();
    }

    private static void generateEvaluateFinal(
            ClassDefinition definition,
            List<FieldDefinition> stateFields,
            MethodHandle outputFunction,
            CallSiteBinder callSiteBinder)
    {
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "evaluateFinal",
                type(void.class),
                out);

        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        List<BytecodeExpression> states = new ArrayList<>();

        for (FieldDefinition stateField : stateFields) {
            BytecodeExpression state = thisVariable.getField(stateField);
            states.add(state);
        }

        body.comment("output(state_0, state_1, ..., out)");
        states.forEach(body::append);
        body.append(out);
        body.append(invoke(callSiteBinder.bind(outputFunction), "output"));

        body.ret();
    }

    private static void generatePrepareFinal(ClassDefinition definition)
    {
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "prepareFinal",
                type(void.class));
        method.getBody().ret();
    }

    private static void generateConstructor(
            ClassDefinition definition,
            List<StateFieldAndDescriptor> stateFieldAndDescriptors,
            List<FieldDefinition> lambdaProviderFields,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        Parameter lambdaProviders = arg("lambdaProviders", type(List.class, Supplier.class));
        MethodDefinition method = definition.declareConstructor(
                a(PUBLIC),
                lambdaProviders);

        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        body.append(generateRequireNotNull(lambdaProviders));
        initializeStateFields(method, stateFieldAndDescriptors, callSiteBinder, grouped);
        initializeLambdaProviderFields(method, lambdaProviderFields, lambdaProviders);

        body.ret();
    }

    private static void initializeStateFields(
            MethodDefinition method,
            List<StateFieldAndDescriptor> stateFieldAndDescriptors,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        for (StateFieldAndDescriptor fieldAndDescriptor : stateFieldAndDescriptors) {
            AccumulatorStateDescriptor<?> accumulatorStateDescriptor = fieldAndDescriptor.getAccumulatorStateDescriptor();
            body.append(thisVariable.setField(
                    fieldAndDescriptor.getStateSerializerField(),
                    loadConstant(callSiteBinder, accumulatorStateDescriptor.getSerializer(), AccumulatorStateSerializer.class)));
            body.append(generateRequireNotNull(thisVariable, fieldAndDescriptor.getStateSerializerField()));

            body.append(thisVariable.setField(
                    fieldAndDescriptor.getStateFactoryField(),
                    loadConstant(callSiteBinder, accumulatorStateDescriptor.getFactory(), AccumulatorStateFactory.class)));
            body.append(generateRequireNotNull(thisVariable, fieldAndDescriptor.getStateFactoryField()));

            // create the state object
            FieldDefinition stateField = fieldAndDescriptor.getStateField();
            BytecodeExpression stateFactory = thisVariable.getField(fieldAndDescriptor.getStateFactoryField());
            BytecodeExpression createStateInstance = stateFactory.invoke(grouped ? "createGroupedState" : "createSingleState", AccumulatorState.class);
            body.append(thisVariable.setField(stateField, createStateInstance.cast(stateField.getType())));
            body.append(generateRequireNotNull(thisVariable, stateField));
        }
    }

    private static void initializeLambdaProviderFields(MethodDefinition method, List<FieldDefinition> lambdaProviderFields, Parameter lambdaProviders)
    {
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        for (int i = 0; i < lambdaProviderFields.size(); i++) {
            body.append(thisVariable.setField(
                    lambdaProviderFields.get(i),
                    lambdaProviders.invoke("get", Object.class, constantInt(i))
                            .cast(Supplier.class)));
            body.append(generateRequireNotNull(thisVariable, lambdaProviderFields.get(i)));
        }
    }

    private static void generateCopyConstructor(
            ClassDefinition definition,
            List<StateFieldAndDescriptor> stateFieldAndDescriptors,
            List<FieldDefinition> lambdaProviderFields)
    {
        Parameter source = arg("source", definition.getType());
        MethodDefinition method = definition.declareConstructor(
                a(PUBLIC),
                source);

        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        body.append(generateRequireNotNull(source));

        for (StateFieldAndDescriptor descriptor : stateFieldAndDescriptors) {
            FieldDefinition stateSerializerField = descriptor.getStateSerializerField();
            body.append(thisVariable.setField(stateSerializerField, source.getField(stateSerializerField)));
            body.append(generateRequireNotNull(thisVariable, stateSerializerField));

            FieldDefinition stateFactoryField = descriptor.getStateFactoryField();
            body.append(thisVariable.setField(stateFactoryField, source.getField(stateFactoryField)));
            body.append(generateRequireNotNull(thisVariable, stateFactoryField));

            FieldDefinition stateField = descriptor.getStateField();
            body.append(thisVariable.setField(stateField, source.getField(stateField).invoke("copy", AccumulatorState.class).cast(stateField.getType())));
            body.append(generateRequireNotNull(thisVariable, stateField));
        }

        for (FieldDefinition lambdaProviderField : lambdaProviderFields) {
            body.append(thisVariable.setField(lambdaProviderField, source.getField(lambdaProviderField)));
            body.append(generateRequireNotNull(thisVariable, lambdaProviderField));
        }

        body.ret();
    }

    private static void generateCopy(ClassDefinition definition, Class<?> returnType)
    {
        MethodDefinition copy = definition.declareMethod(a(PUBLIC), "copy", type(returnType));
        copy.getBody()
                .append(newInstance(definition.getType(), copy.getScope().getThis()).ret());
    }

    private static BytecodeExpression generateRequireNotNull(Variable variable)
    {
        return generateRequireNotNull(variable, variable.getName() + " is null");
    }

    private static BytecodeExpression generateRequireNotNull(Variable variable, FieldDefinition field)
    {
        return generateRequireNotNull(variable.getField(field), field.getName() + " is null");
    }

    private static BytecodeExpression generateRequireNotNull(BytecodeExpression expression, String message)
    {
        return invokeStatic(Objects.class, "requireNonNull", Object.class, expression.cast(Object.class), constantString(message))
                .cast(expression.getType());
    }

    private static AggregationImplementation normalizeAggregationMethods(AggregationImplementation implementation)
    {
        // change aggregations state variables to simply AccumulatorState to avoid any class loader issues in generated code
        int stateParameterCount = implementation.getAccumulatorStateDescriptors().size();
        int lambdaParameterCount = implementation.getLambdaInterfaces().size();
        AggregationImplementation.Builder builder = AggregationImplementation.builder();
        builder.inputFunction(castStateParameters(implementation.getInputFunction(), stateParameterCount, lambdaParameterCount));
        implementation.getRemoveInputFunction()
                .map(removeFunction -> castStateParameters(removeFunction, stateParameterCount, lambdaParameterCount))
                .ifPresent(builder::removeInputFunction);
        implementation.getCombineFunction()
                .map(combineFunction -> castStateParameters(combineFunction, stateParameterCount * 2, lambdaParameterCount))
                .ifPresent(builder::combineFunction);
        builder.outputFunction(castStateParameters(implementation.getOutputFunction(), stateParameterCount, 0));
        builder.accumulatorStateDescriptors(implementation.getAccumulatorStateDescriptors());
        builder.lambdaInterfaces(implementation.getLambdaInterfaces());
        return builder.build();
    }

    private static MethodHandle castStateParameters(MethodHandle inputFunction, int stateParameterCount, int lambdaParameterCount)
    {
        Class<?>[] parameterTypes = inputFunction.type().parameterArray();
        for (int i = 0; i < stateParameterCount; i++) {
            parameterTypes[i] = AccumulatorState.class;
        }
        for (int i = parameterTypes.length - lambdaParameterCount; i < parameterTypes.length; i++) {
            parameterTypes[i] = Object.class;
        }
        return MethodHandles.explicitCastArguments(inputFunction, MethodType.methodType(inputFunction.type().returnType(), parameterTypes));
    }

    private static class StateFieldAndDescriptor
    {
        private final AccumulatorStateDescriptor<?> accumulatorStateDescriptor;
        private final FieldDefinition stateSerializerField;
        private final FieldDefinition stateFactoryField;
        private final FieldDefinition stateField;

        private StateFieldAndDescriptor(AccumulatorStateDescriptor<?> accumulatorStateDescriptor, FieldDefinition stateSerializerField, FieldDefinition stateFactoryField, FieldDefinition stateField)
        {
            this.accumulatorStateDescriptor = accumulatorStateDescriptor;
            this.stateSerializerField = requireNonNull(stateSerializerField, "stateSerializerField is null");
            this.stateFactoryField = requireNonNull(stateFactoryField, "stateFactoryField is null");
            this.stateField = requireNonNull(stateField, "stateField is null");
        }

        public AccumulatorStateDescriptor<?> getAccumulatorStateDescriptor()
        {
            return accumulatorStateDescriptor;
        }

        private FieldDefinition getStateSerializerField()
        {
            return stateSerializerField;
        }

        private FieldDefinition getStateFactoryField()
        {
            return stateFactoryField;
        }

        private FieldDefinition getStateField()
        {
            return stateField;
        }
    }
}
