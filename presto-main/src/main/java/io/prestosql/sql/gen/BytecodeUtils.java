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
package io.prestosql.sql.gen;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.instruction.LabelNode;
import io.airlift.slice.Slice;
import io.prestosql.metadata.BoundSignature;
import io.prestosql.metadata.FunctionInvoker;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.InputReferenceCompiler.InputReferenceNode;
import io.prestosql.type.FunctionType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.OpCode.NOP;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.prestosql.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static java.lang.String.format;

public final class BytecodeUtils
{
    private BytecodeUtils() {}

    public static BytecodeNode ifWasNullPopAndGoto(Scope scope, LabelNode label, Class<?> returnType, Class<?>... stackArgsToPop)
    {
        return handleNullValue(scope, label, returnType, ImmutableList.copyOf(stackArgsToPop), false);
    }

    public static BytecodeNode ifWasNullPopAndGoto(Scope scope, LabelNode label, Class<?> returnType, Iterable<? extends Class<?>> stackArgsToPop)
    {
        return handleNullValue(scope, label, returnType, ImmutableList.copyOf(stackArgsToPop), false);
    }

    public static BytecodeNode ifWasNullClearPopAndGoto(Scope scope, LabelNode label, Class<?> returnType, Class<?>... stackArgsToPop)
    {
        return handleNullValue(scope, label, returnType, ImmutableList.copyOf(stackArgsToPop), true);
    }

    public static BytecodeNode handleNullValue(
            Scope scope,
            LabelNode label,
            Class<?> returnType,
            List<Class<?>> stackArgsToPop,
            boolean clearNullFlag)
    {
        Variable wasNull = scope.getVariable("wasNull");

        BytecodeBlock nullCheck = new BytecodeBlock()
                .setDescription("ifWasNullGoto")
                .append(wasNull);

        String clearComment = null;
        if (clearNullFlag) {
            nullCheck.append(wasNull.set(constantFalse()));
            clearComment = "clear wasNull";
        }

        BytecodeBlock isNull = new BytecodeBlock();
        for (Class<?> parameterType : stackArgsToPop) {
            isNull.pop(parameterType);
        }

        isNull.pushJavaDefault(returnType);
        String loadDefaultComment;
        loadDefaultComment = format("loadJavaDefault(%s)", returnType.getName());

        isNull.gotoLabel(label);

        String popComment = null;
        if (!stackArgsToPop.isEmpty()) {
            popComment = format("pop(%s)", Joiner.on(", ").join(stackArgsToPop));
        }

        return new IfStatement("if wasNull then %s", Joiner.on(", ").skipNulls().join(clearComment, popComment, loadDefaultComment, "goto " + label.getLabel()))
                .condition(nullCheck)
                .ifTrue(isNull);
    }

    public static BytecodeNode boxPrimitive(Class<?> type)
    {
        BytecodeBlock block = new BytecodeBlock().comment("box primitive");
        if (type == long.class) {
            return block.invokeStatic(Long.class, "valueOf", Long.class, long.class);
        }
        if (type == double.class) {
            return block.invokeStatic(Double.class, "valueOf", Double.class, double.class);
        }
        if (type == boolean.class) {
            return block.invokeStatic(Boolean.class, "valueOf", Boolean.class, boolean.class);
        }
        if (type.isPrimitive()) {
            throw new UnsupportedOperationException("not yet implemented: " + type);
        }

        return NOP;
    }

    public static BytecodeNode unboxPrimitive(Class<?> unboxedType)
    {
        BytecodeBlock block = new BytecodeBlock().comment("unbox primitive");
        if (unboxedType == long.class) {
            return block.invokeVirtual(Long.class, "longValue", long.class);
        }
        if (unboxedType == double.class) {
            return block.invokeVirtual(Double.class, "doubleValue", double.class);
        }
        if (unboxedType == boolean.class) {
            return block.invokeVirtual(Boolean.class, "booleanValue", boolean.class);
        }
        throw new UnsupportedOperationException("not yet implemented: " + unboxedType);
    }

    public static BytecodeExpression loadConstant(CallSiteBinder callSiteBinder, Object constant, Class<?> type)
    {
        Binding binding = callSiteBinder.bind(MethodHandles.constant(type, constant));
        return loadConstant(binding);
    }

    public static BytecodeExpression loadConstant(Binding binding)
    {
        return invokeDynamic(
                BOOTSTRAP_METHOD,
                ImmutableList.of(binding.getBindingId()),
                "constant_" + binding.getBindingId(),
                binding.getType().returnType());
    }

    public static BytecodeNode generateInvocation(
            Scope scope,
            ResolvedFunction resolvedFunction,
            Metadata metadata,
            List<BytecodeNode> arguments,
            CallSiteBinder binder)
    {
        return generateInvocation(
                scope,
                metadata.getFunctionMetadata(resolvedFunction),
                invocationConvention -> metadata.getScalarFunctionInvoker(resolvedFunction, Optional.of(invocationConvention)),
                arguments,
                binder);
    }

    public static BytecodeNode generateInvocation(
            Scope scope,
            FunctionMetadata functionMetadata,
            Function<InvocationConvention, FunctionInvoker> functionInvokerProvider,
            List<BytecodeNode> arguments,
            CallSiteBinder binder)
    {
        return generateFullInvocation(
                scope,
                functionMetadata,
                functionInvokerProvider,
                instanceFactory -> {
                    throw new IllegalArgumentException("Simple method invocation can not be used with functions that require an instance factory");
                },
                arguments.stream()
                        .map(BytecodeUtils::simpleArgument)
                        .collect(toImmutableList()),
                binder);
    }

    private static Function<Optional<Class<?>>, BytecodeNode> simpleArgument(BytecodeNode argument)
    {
        return lambdaInterface -> {
            checkArgument(!lambdaInterface.isPresent(), "Simple method invocation can not be used with functions that have lambda arguments");
            return argument;
        };
    }

    public static BytecodeNode generateFullInvocation(
            Scope scope,
            ResolvedFunction resolvedFunction,
            Metadata metadata,
            Function<MethodHandle, BytecodeNode> instanceFactory,
            List<Function<Optional<Class<?>>, BytecodeNode>> argumentCompilers,
            CallSiteBinder binder)
    {
        return generateFullInvocation(
                scope,
                metadata.getFunctionMetadata(resolvedFunction),
                invocationConvention -> metadata.getScalarFunctionInvoker(resolvedFunction, Optional.of(invocationConvention)),
                instanceFactory,
                argumentCompilers,
                binder);
    }

    public static BytecodeNode generateFullInvocation(
            Scope scope,
            FunctionMetadata functionMetadata,
            Function<InvocationConvention, FunctionInvoker> functionInvokerProvider,
            Function<MethodHandle, BytecodeNode> instanceFactory,
            List<Function<Optional<Class<?>>, BytecodeNode>> argumentCompilers,
            CallSiteBinder binder)
    {
        List<InvocationArgumentConvention> argumentConventions = new ArrayList<>();
        List<BytecodeNode> arguments = new ArrayList<>();
        for (int i = 0; i < functionMetadata.getSignature().getArgumentTypes().size(); i++) {
            if (functionMetadata.getSignature().getArgumentTypes().get(i).getBase().equalsIgnoreCase(FunctionType.NAME)) {
                argumentConventions.add(FUNCTION);
                arguments.add(null);
            }
            else {
                BytecodeNode argument = argumentCompilers.get(i).apply(Optional.empty());
                if (argument instanceof InputReferenceNode) {
                    argumentConventions.add(BLOCK_POSITION);
                }
                else if (functionMetadata.getArgumentDefinitions().get(i).isNullable()) {
                    // a Java function can only have 255 arguments, so if the count is high use boxed nullable instead of the more efficient null flag
                    argumentConventions.add(argumentCompilers.size() > 100 ? BOXED_NULLABLE : NULL_FLAG);
                }
                else {
                    argumentConventions.add(NEVER_NULL);
                }
                arguments.add(argument);
            }
        }

        InvocationConvention invocationConvention = new InvocationConvention(
                argumentConventions,
                functionMetadata.isNullable() ? NULLABLE_RETURN : FAIL_ON_NULL,
                true,
                true);
        FunctionInvoker functionInvoker = functionInvokerProvider.apply(invocationConvention);

        Binding binding = binder.bind(functionInvoker.getMethodHandle());

        LabelNode end = new LabelNode("end");
        BytecodeBlock block = new BytecodeBlock()
                .setDescription("invoke " + functionMetadata.getSignature().getName());

        Optional<BytecodeNode> instance = functionInvoker.getInstanceFactory()
                .map(instanceFactory);

        // Index of current parameter in the MethodHandle
        int currentParameterIndex = 0;

        // Index of parameter (without @IsNull) in Presto function
        int realParameterIndex = 0;

        MethodType methodType = binding.getType();
        Class<?> returnType = methodType.returnType();
        Class<?> unboxedReturnType = Primitives.unwrap(returnType);

        List<Class<?>> stackTypes = new ArrayList<>();
        boolean boundInstance = false;
        while (currentParameterIndex < methodType.parameterArray().length) {
            Class<?> type = methodType.parameterArray()[currentParameterIndex];
            stackTypes.add(type);
            if (instance.isPresent() && !boundInstance) {
                checkState(type.equals(functionInvoker.getInstanceFactory().get().type().returnType()), "Mismatched type for instance parameter");
                block.append(instance.get());
                boundInstance = true;
            }
            else if (type == ConnectorSession.class) {
                block.append(scope.getVariable("session"));
            }
            else {
                switch (invocationConvention.getArgumentConvention(realParameterIndex)) {
                    case NEVER_NULL:
                        block.append(arguments.get(realParameterIndex));
                        checkArgument(!Primitives.isWrapperType(type), "Non-nullable argument must not be primitive wrapper type");
                        block.append(ifWasNullPopAndGoto(scope, end, unboxedReturnType, Lists.reverse(stackTypes)));
                        break;
                    case NULL_FLAG:
                        block.append(arguments.get(realParameterIndex));
                        block.append(scope.getVariable("wasNull"));
                        block.append(scope.getVariable("wasNull").set(constantFalse()));
                        stackTypes.add(boolean.class);
                        currentParameterIndex++;
                        break;
                    case BOXED_NULLABLE:
                        block.append(arguments.get(realParameterIndex));
                        block.append(boxPrimitiveIfNecessary(scope, type));
                        block.append(scope.getVariable("wasNull").set(constantFalse()));
                        break;
                    case BLOCK_POSITION:
                        InputReferenceNode inputReferenceNode = (InputReferenceNode) arguments.get(realParameterIndex);
                        block.append(inputReferenceNode.produceBlockAndPosition());
                        stackTypes.add(int.class);
                        if (!functionMetadata.getArgumentDefinitions().get(realParameterIndex).isNullable()) {
                            block.append(scope.getVariable("wasNull").set(inputReferenceNode.blockAndPositionIsNull()));
                            block.append(ifWasNullPopAndGoto(scope, end, unboxedReturnType, Lists.reverse(stackTypes)));
                        }
                        currentParameterIndex++;
                        break;
                    case FUNCTION:
                        Optional<Class<?>> lambdaInterface = functionInvoker.getLambdaInterfaces().get(realParameterIndex);
                        block.append(argumentCompilers.get(realParameterIndex).apply(lambdaInterface));
                        break;
                    default:
                        throw new UnsupportedOperationException(format("Unsupported argument conventsion type: %s", invocationConvention.getArgumentConvention(realParameterIndex)));
                }
                realParameterIndex++;
            }
            currentParameterIndex++;
        }
        block.append(invoke(binding, functionMetadata.getSignature().getName()));

        if (functionMetadata.isNullable()) {
            block.append(unboxPrimitiveIfNecessary(scope, returnType));
        }
        block.visitLabel(end);

        return block;
    }

    public static BytecodeBlock unboxPrimitiveIfNecessary(Scope scope, Class<?> boxedType)
    {
        BytecodeBlock block = new BytecodeBlock();
        LabelNode end = new LabelNode("end");
        Class<?> unboxedType = Primitives.unwrap(boxedType);
        Variable wasNull = scope.getVariable("wasNull");

        if (unboxedType.isPrimitive()) {
            LabelNode notNull = new LabelNode("notNull");
            block.dup(boxedType)
                    .ifNotNullGoto(notNull)
                    .append(wasNull.set(constantTrue()))
                    .comment("swap boxed null with unboxed default")
                    .pop(boxedType)
                    .pushJavaDefault(unboxedType)
                    .gotoLabel(end)
                    .visitLabel(notNull)
                    .append(unboxPrimitive(unboxedType));
        }
        else {
            block.dup(boxedType)
                    .ifNotNullGoto(end)
                    .append(wasNull.set(constantTrue()));
        }
        block.visitLabel(end);

        return block;
    }

    public static BytecodeNode boxPrimitiveIfNecessary(Scope scope, Class<?> type)
    {
        checkArgument(!type.isPrimitive(), "cannot box into primitive type");
        if (!Primitives.isWrapperType(type)) {
            return NOP;
        }
        BytecodeBlock notNull = new BytecodeBlock().comment("box primitive");
        Class<?> expectedCurrentStackType;
        if (type == Long.class) {
            notNull.invokeStatic(Long.class, "valueOf", Long.class, long.class);
            expectedCurrentStackType = long.class;
        }
        else if (type == Double.class) {
            notNull.invokeStatic(Double.class, "valueOf", Double.class, double.class);
            expectedCurrentStackType = double.class;
        }
        else if (type == Boolean.class) {
            notNull.invokeStatic(Boolean.class, "valueOf", Boolean.class, boolean.class);
            expectedCurrentStackType = boolean.class;
        }
        else {
            throw new UnsupportedOperationException("not yet implemented: " + type);
        }

        BytecodeBlock condition = new BytecodeBlock().append(scope.getVariable("wasNull"));

        BytecodeBlock wasNull = new BytecodeBlock()
                .pop(expectedCurrentStackType)
                .pushNull()
                .checkCast(type);

        return new IfStatement()
                .condition(condition)
                .ifTrue(wasNull)
                .ifFalse(notNull);
    }

    public static BytecodeExpression invoke(Binding binding, String name)
    {
        // ensure that name doesn't have a special characters
        return invokeDynamic(BOOTSTRAP_METHOD, ImmutableList.of(binding.getBindingId()), sanitizeName(name), binding.getType());
    }

    public static BytecodeExpression invoke(Binding binding, BoundSignature signature)
    {
        return invoke(binding, signature.getName());
    }

    /**
     * Replace characters that are not safe to use in a JVM identifier.
     */
    public static String sanitizeName(String name)
    {
        return name.replaceAll("[^A-Za-z0-9_$]", "_");
    }

    public static BytecodeNode generateWrite(CallSiteBinder callSiteBinder, Scope scope, Variable wasNullVariable, Type type)
    {
        Class<?> valueJavaType = type.getJavaType();
        if (!valueJavaType.isPrimitive() && valueJavaType != Slice.class) {
            valueJavaType = Object.class;
        }
        String methodName = "write" + Primitives.wrap(valueJavaType).getSimpleName();

        // the stack contains [output, value]

        // We should be able to insert the code to get the output variable and compute the value
        // at the right place instead of assuming they are in the stack. We should also not need to
        // use temp variables to re-shuffle the stack to the right shape before Type.writeXXX is called
        // Unfortunately, because of the assumptions made by try_cast, we can't get around it yet.
        // TODO: clean up once try_cast is fixed
        Variable tempValue = scope.createTempVariable(valueJavaType);
        Variable tempOutput = scope.createTempVariable(BlockBuilder.class);
        return new BytecodeBlock()
                .comment("if (wasNull)")
                .append(new IfStatement()
                        .condition(wasNullVariable)
                        .ifTrue(new BytecodeBlock()
                                .comment("output.appendNull();")
                                .pop(valueJavaType)
                                .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                                .pop())
                        .ifFalse(new BytecodeBlock()
                                .comment("%s.%s(output, %s)", type.getTypeSignature(), methodName, valueJavaType.getSimpleName())
                                .putVariable(tempValue)
                                .putVariable(tempOutput)
                                .append(loadConstant(callSiteBinder.bind(type, Type.class)))
                                .getVariable(tempOutput)
                                .getVariable(tempValue)
                                .invokeInterface(Type.class, methodName, void.class, BlockBuilder.class, valueJavaType)));
    }
}
