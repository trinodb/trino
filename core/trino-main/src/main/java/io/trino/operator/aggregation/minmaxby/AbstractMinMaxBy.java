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
package io.trino.operator.aggregation.minmaxby;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.metadata.AggregationFunctionMetadata;
import io.trino.metadata.FunctionArgumentDefinition;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionDependencyDeclaration;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.aggregation.AccumulatorCompiler;
import io.trino.operator.aggregation.AggregationMetadata;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.aggregation.GenericAccumulatorFactoryBinder;
import io.trino.operator.aggregation.InternalAggregationFunction;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.gen.SqlTypeBytecodeExpression;
import io.trino.util.MinMaxCompare;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.and;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantBoolean;
import static io.airlift.bytecode.expression.BytecodeExpressions.not;
import static io.airlift.bytecode.expression.BytecodeExpressions.or;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.Signature.orderableTypeParameter;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.trino.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.trino.operator.aggregation.minmaxby.TwoNullableValueStateMapping.getStateClass;
import static io.trino.operator.aggregation.minmaxby.TwoNullableValueStateMapping.getStateSerializer;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.sql.gen.BytecodeUtils.loadConstant;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static io.trino.util.MinMaxCompare.getMinMaxCompareFunctionDependencies;
import static io.trino.util.Reflection.methodHandle;
import static java.util.Arrays.stream;

public abstract class AbstractMinMaxBy
        extends SqlAggregationFunction
{
    private final boolean min;

    protected AbstractMinMaxBy(boolean min, String description)
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                (min ? "min" : "max") + "_by",
                                ImmutableList.of(orderableTypeParameter("K"), typeVariable("V")),
                                ImmutableList.of(),
                                new TypeSignature("V"),
                                ImmutableList.of(new TypeSignature("V"), new TypeSignature("K")),
                                false),
                        true,
                        ImmutableList.of(
                                new FunctionArgumentDefinition(true),
                                new FunctionArgumentDefinition(false)),
                        false,
                        true,
                        description,
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        false,
                        new TypeSignature("K"),
                        new TypeSignature("V")));
        this.min = min;
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return getMinMaxCompareFunctionDependencies(new TypeSignature("K"), min);
    }

    @Override
    public InternalAggregationFunction specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        Type keyType = functionBinding.getTypeVariable("K");
        Type valueType = functionBinding.getTypeVariable("V");
        return generateAggregation(valueType, keyType, functionDependencies);
    }

    private InternalAggregationFunction generateAggregation(Type valueType, Type keyType, FunctionDependencies functionDependencies)
    {
        Class<?> stateClass = getStateClass(keyType.getJavaType(), valueType.getJavaType());
        DynamicClassLoader classLoader = new DynamicClassLoader(getClass().getClassLoader());

        // Generate states and serializers:
        // For value that is a Block or Slice, we store them as Block/position combination
        // to avoid generating long-living objects through getSlice or getObject.
        // This can also help reducing cross-region reference in G1GC engine.
        // TODO: keys can have the same problem. But usually they are primitive types (given the nature of comparison).
        AccumulatorStateFactory<?> stateFactory;
        AccumulatorStateSerializer<?> stateSerializer;
        if (valueType.getJavaType().isPrimitive()) {
            Map<String, Type> stateFieldTypes = ImmutableMap.of("First", keyType, "Second", valueType);
            stateFactory = StateCompiler.generateStateFactory(stateClass, stateFieldTypes, classLoader);
            stateSerializer = StateCompiler.generateStateSerializer(stateClass, stateFieldTypes, classLoader);
        }
        else {
            // StateCompiler checks type compatibility.
            // Given "Second" in this case is always a Block, we only need to make sure the getter and setter of the Blocks are properly generated.
            // We deliberately make "SecondBlock" an array type so that the compiler will treat it as a block to workaround the sanity check.
            stateFactory = StateCompiler.generateStateFactory(stateClass, ImmutableMap.of("First", keyType, "SecondBlock", new ArrayType(valueType)), classLoader);

            // States can be generated by StateCompiler given the they are simply classes with getters and setters.
            // However, serializers have logic in it. Creating serializers is better than generating them.
            stateSerializer = getStateSerializer(keyType, valueType);
        }

        Type intermediateType = stateSerializer.getSerializedType();

        List<Type> inputTypes = ImmutableList.of(valueType, keyType);

        CallSiteBinder binder = new CallSiteBinder();
        MethodHandle compareMethod = MinMaxCompare.getMinMaxCompare(functionDependencies, keyType, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL), min);

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("processMaxOrMinBy"),
                type(Object.class));
        definition.declareDefaultConstructor(a(PRIVATE));
        generateInputMethod(definition, binder, compareMethod, keyType, valueType, stateClass);
        generateCombineMethod(definition, binder, compareMethod, valueType, stateClass);
        generateOutputMethod(definition, binder, valueType, stateClass);
        Class<?> generatedClass = defineClass(definition, Object.class, binder.getBindings(), classLoader);
        MethodHandle inputMethod = methodHandle(generatedClass, "input", stateClass, Block.class, Block.class, int.class);
        MethodHandle combineMethod = methodHandle(generatedClass, "combine", stateClass, stateClass);
        MethodHandle outputMethod = methodHandle(generatedClass, "output", stateClass, BlockBuilder.class);
        String name = getFunctionMetadata().getSignature().getName();
        AggregationMetadata aggregationMetadata = new AggregationMetadata(
                generateAggregationName(name, valueType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(valueType, keyType),
                inputMethod,
                Optional.empty(),
                combineMethod,
                outputMethod,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateClass,
                        stateSerializer,
                        stateFactory)),
                valueType);
        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(aggregationMetadata, classLoader);
        return new InternalAggregationFunction(name, inputTypes, ImmutableList.of(intermediateType), valueType, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value, Type key)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INPUT_CHANNEL, key), new ParameterMetadata(BLOCK_INDEX));
    }

    private static void generateInputMethod(ClassDefinition definition, CallSiteBinder binder, MethodHandle compareMethod, Type keyType, Type valueType, Class<?> stateClass)
    {
        Parameter state = arg("state", stateClass);
        Parameter value = arg("value", Block.class);
        Parameter key = arg("key", Block.class);
        Parameter position = arg("position", int.class);
        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "input", type(void.class), state, value, key, position);
        SqlTypeBytecodeExpression keySqlType = constantType(binder, keyType);

        BytecodeBlock ifBlock = new BytecodeBlock()
                .append(invokeMethod(stateClass, state, "setFirst", keySqlType.getValue(key, position)))
                .append(state.invoke("setFirstNull", void.class, constantBoolean(false)))
                .append(state.invoke("setSecondNull", void.class, value.invoke("isNull", boolean.class, position)));
        BytecodeNode setValueNode;
        if (valueType.getJavaType().isPrimitive()) {
            SqlTypeBytecodeExpression valueSqlType = constantType(binder, valueType);
            setValueNode = invokeMethod(stateClass, state, "setSecond", valueSqlType.getValue(value, position));
        }
        else {
            // Do not get value directly given it creates object overhead.
            // Such objects would live long enough in Block or SliceBigArray to cause GC pressure.
            setValueNode = new BytecodeBlock()
                    .append(state.invoke("setSecondBlock", void.class, value))
                    .append(state.invoke("setSecondPosition", void.class, position));
        }
        ifBlock.append(new IfStatement()
                .condition(value.invoke("isNull", boolean.class, position))
                .ifFalse(setValueNode));

        method.getBody().append(new IfStatement()
                .condition(or(
                        state.invoke("isFirstNull", boolean.class),
                        and(
                                not(key.invoke("isNull", boolean.class, position)),
                                loadConstant(binder, compareMethod, MethodHandle.class).invoke(
                                        "invokeExact",
                                        boolean.class,
                                        keySqlType.getValue(key, position).cast(compareMethod.type().parameterType(0)),
                                        invokeMethod(stateClass, state, "getFirst").cast(compareMethod.type().parameterType(1))))))
                .ifTrue(ifBlock))
                .ret();
    }

    private static void generateCombineMethod(ClassDefinition definition, CallSiteBinder binder, MethodHandle compareMethod, Type valueType, Class<?> stateClass)
    {
        Parameter state = arg("state", stateClass);
        Parameter otherState = arg("otherState", stateClass);
        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "combine", type(void.class), state, otherState);

        BytecodeBlock ifBlock = new BytecodeBlock()
                .append(invokeMethod(stateClass, state, "setFirst", invokeMethod(stateClass, otherState, "getFirst")))
                .append(state.invoke("setFirstNull", void.class, otherState.invoke("isFirstNull", boolean.class)))
                .append(state.invoke("setSecondNull", void.class, otherState.invoke("isSecondNull", boolean.class)));
        if (valueType.getJavaType().isPrimitive()) {
            ifBlock.append(invokeMethod(stateClass, state, "setSecond", otherState.invoke("getSecond", valueType.getJavaType())));
        }
        else {
            ifBlock.append(new BytecodeBlock()
                    .append(state.invoke("setSecondBlock", void.class, otherState.invoke("getSecondBlock", Block.class)))
                    .append(state.invoke("setSecondPosition", void.class, otherState.invoke("getSecondPosition", int.class))));
        }

        method.getBody()
                .append(new IfStatement()
                        .condition(or(
                                state.invoke("isFirstNull", boolean.class),
                                and(
                                        not(otherState.invoke("isFirstNull", boolean.class)),
                                        loadConstant(binder, compareMethod, MethodHandle.class).invoke(
                                                "invokeExact",
                                                boolean.class,
                                                invokeMethod(stateClass, otherState, "getFirst").cast(compareMethod.type().parameterType(0)),
                                                invokeMethod(stateClass, state, "getFirst").cast(compareMethod.type().parameterType(1))))))
                        .ifTrue(ifBlock))
                .ret();
    }

    private static BytecodeExpression invokeMethod(Class<?> instanceType, Parameter instance, String methodName, BytecodeExpression... arguments)
    {
        Method method = getMethod(instanceType, methodName);
        Class<?>[] parameterTypes = method.getParameterTypes();
        checkArgument(parameterTypes.length == arguments.length, "Expected %s arguments, but got %s", parameterTypes.length, arguments.length);

        ImmutableList.Builder<BytecodeExpression> castedArguments = ImmutableList.builder();
        for (int i = 0; i < arguments.length; i++) {
            BytecodeExpression argument = arguments[i];
            Class<?> parameterType = parameterTypes[i];
            castedArguments.add(argument.cast(parameterType));
        }
        return instance.invoke(method, castedArguments.build());
    }

    private static void generateOutputMethod(ClassDefinition definition, CallSiteBinder binder, Type valueType, Class<?> stateClass)
    {
        Parameter state = arg("state", stateClass);
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "output", type(void.class), state, out);

        IfStatement ifStatement = new IfStatement()
                .condition(or(state.invoke("isFirstNull", boolean.class), state.invoke("isSecondNull", boolean.class)))
                .ifTrue(new BytecodeBlock().append(out.invoke("appendNull", BlockBuilder.class)).pop());
        SqlTypeBytecodeExpression valueSqlType = constantType(binder, valueType);
        BytecodeExpression getValueExpression;
        if (valueType.getJavaType().isPrimitive()) {
            getValueExpression = state.invoke("getSecond", valueType.getJavaType());
        }
        else {
            getValueExpression = valueSqlType.getValue(state.invoke("getSecondBlock", Block.class), state.invoke("getSecondPosition", int.class));
        }
        ifStatement.ifFalse(valueSqlType.writeValue(out, getValueExpression));
        method.getBody().append(ifStatement).ret();
    }

    private static Method getMethod(Class<?> stateClass, String name)
    {
        return stream(stateClass.getMethods())
                .filter(method -> method.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("State class does not have a method named " + name));
    }
}
