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
package io.trino.sql.gen.columnar;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.sql.gen.Binding;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.type.FunctionType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.instruction.Constant.loadBoolean;
import static io.airlift.bytecode.instruction.Constant.loadDouble;
import static io.airlift.bytecode.instruction.Constant.loadLong;
import static io.airlift.bytecode.instruction.Constant.loadString;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.DEFAULT_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.sql.gen.BytecodeUtils.invoke;
import static io.trino.sql.gen.BytecodeUtils.loadConstant;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.createClassInstance;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.declareBlockVariables;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateBlockMayHaveNull;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateBlockPositionNotNull;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateGetInputChannels;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.updateOutputPositions;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CallColumnarFilterGenerator
{
    private final CallExpression callExpression;
    private final FunctionManager functionManager;

    public CallColumnarFilterGenerator(CallExpression callExpression, FunctionManager functionManager)
    {
        callExpression.arguments().forEach(rowExpression -> {
            if (!(rowExpression instanceof InputReferenceExpression) && !(rowExpression instanceof ConstantExpression)) {
                throw new UnsupportedOperationException("Call expression with unsupported argument: " + rowExpression);
            }
            if (rowExpression instanceof ConstantExpression constant) {
                if (constant.value() == null) {
                    throw new UnsupportedOperationException("Call expressions with null constant are not supported");
                }
            }
        });
        callExpression.resolvedFunction().signature().getArgumentTypes().forEach(type -> {
            if (type instanceof FunctionType) {
                throw new UnsupportedOperationException("Functions with lambda arguments are not supported");
            }
        });
        this.callExpression = callExpression;
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
    }

    public Supplier<ColumnarFilter> generateColumnarFilter()
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(ColumnarFilter.class.getSimpleName() + callExpression.resolvedFunction().signature().getName(), Optional.empty()),
                type(Object.class),
                type(ColumnarFilter.class));

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        generateGetInputChannels(callSiteBinder, classDefinition, callExpression);

        generateFilterRangeMethod(classDefinition, callSiteBinder, cachedInstanceBinder);
        generateFilterListMethod(classDefinition, callSiteBinder, cachedInstanceBinder);

        generateConstructor(classDefinition, cachedInstanceBinder);
        return createClassInstance(callSiteBinder, classDefinition);
    }

    private void generateFilterRangeMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, CachedInstanceBinder cachedInstanceBinder)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter outputPositions = arg("outputPositions", int[].class);
        Parameter offset = arg("offset", int.class);
        Parameter size = arg("size", int.class);
        Parameter page = arg("page", SourcePage.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filterPositionsRange",
                type(int.class),
                ImmutableList.of(session, outputPositions, offset, size, page));
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(callExpression.arguments(), page, scope, body);

        Variable outputPositionsCount = scope.declareVariable("outputPositionsCount", body, constantInt(0));
        Variable position = scope.declareVariable(int.class, "position");
        Variable result = scope.declareVariable(boolean.class, "result");

        FunctionNullability functionNullability = callExpression.resolvedFunction().functionNullability();
        IfStatement ifStatement = new IfStatement()
                .condition(generateBlockMayHaveNull(callExpression.arguments(), functionNullability.getArgumentNullable(), scope));
        body.append(ifStatement);
        Function<MethodHandle, BytecodeNode> instance = instanceFactory -> scope.getThis().getField(cachedInstanceBinder.getCachedInstance(instanceFactory));

        /* if (block_0.mayHaveNull() || block_1.mayHaveNull()...) {
         *     for (position = offset; position < offset + size; position++) {
         *         if (!block_0.isNull(position) && !block_1.isNull(position)...) {
         *             boolean result = call_function(position, block_0, block_1, ...);
         *             outputPositions[outputPositionsCount] = position;
         *             outputPositionsCount += result ? 1 : 0;
         *         }
         *     }
         * }
         */
        ifStatement.ifTrue(new ForLoop("nullable range based loop")
                .initialize(position.set(offset))
                .condition(lessThan(position, add(offset, size)))
                .update(position.increment())
                .body(new IfStatement()
                        .condition(generateBlockPositionNotNull(callExpression.arguments(), functionNullability.getArgumentNullable(), scope, position))
                        .ifTrue(new BytecodeBlock()
                                .append(generateFullInvocation(functionManager, instance, callSiteBinder, callExpression, scope, position)
                                        .putVariable(result))
                                .append(updateOutputPositions(result, position, outputPositions, outputPositionsCount)))));

        /* for (position = offset; position < offset + size; position++) {
         *     boolean result = call_function(position, block_0, block_1, ...);
         *     outputPositions[outputPositionsCount] = position;
         *     outputPositionsCount += result ? 1 : 0;
         * }
         */
        ifStatement.ifFalse(new ForLoop("nullable function range based loop")
                .initialize(position.set(offset))
                .condition(lessThan(position, add(offset, size)))
                .update(position.increment())
                .body(new BytecodeBlock()
                        .append(generateFullInvocation(functionManager, instance, callSiteBinder, callExpression, scope, position)
                                .putVariable(result))
                        .append(updateOutputPositions(result, position, outputPositions, outputPositionsCount))));

        body.append(outputPositionsCount.ret());
    }

    private void generateFilterListMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, CachedInstanceBinder cachedInstanceBinder)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter outputPositions = arg("outputPositions", int[].class);
        Parameter activePositions = arg("activePositions", int[].class);
        Parameter offset = arg("offset", int.class);
        Parameter size = arg("size", int.class);
        Parameter page = arg("page", SourcePage.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filterPositionsList",
                type(int.class),
                ImmutableList.of(session, outputPositions, activePositions, offset, size, page));
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(callExpression.arguments(), page, scope, body);

        Variable outputPositionsCount = scope.declareVariable("outputPositionsCount", body, constantInt(0));
        Variable index = scope.declareVariable(int.class, "index");
        Variable position = scope.declareVariable(int.class, "position");
        Variable result = scope.declareVariable(boolean.class, "result");

        FunctionNullability functionNullability = callExpression.resolvedFunction().functionNullability();
        IfStatement ifStatement = new IfStatement()
                .condition(generateBlockMayHaveNull(callExpression.arguments(), functionNullability.getArgumentNullable(), scope));
        body.append(ifStatement);
        Function<MethodHandle, BytecodeNode> instance = instanceFactory -> scope.getThis().getField(cachedInstanceBinder.getCachedInstance(instanceFactory));

        /* if (block_0.mayHaveNull() || block_1.mayHaveNull()...) {
         *     for (int index = offset; index < offset + size; index++) {
         *         int position = activePositions[index];
         *         if (!block_0.isNull(position) && !block_1.isNull(position)...) {
         *             boolean result = call_function(position, block_0, block_1, ...);
         *             outputPositions[outputPositionsCount] = position;
         *             outputPositionsCount += result ? 1 : 0;
         *         }
         *     }
         * }
         */
        ifStatement.ifTrue(new ForLoop("nullable positions loop")
                .initialize(index.set(offset))
                .condition(lessThan(index, add(offset, size)))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(position.set(activePositions.getElement(index)))
                        .append(new IfStatement()
                                .condition(generateBlockPositionNotNull(callExpression.arguments(), functionNullability.getArgumentNullable(), scope, position))
                                .ifTrue(new BytecodeBlock()
                                        .append(generateFullInvocation(functionManager, instance, callSiteBinder, callExpression, scope, position)
                                                .putVariable(result))
                                        .append(updateOutputPositions(result, position, outputPositions, outputPositionsCount))))));

        /* for (int index = offset; index < offset + size; index++) {
         *     int position = activePositions[index];
         *     boolean result = call_function(position, block_0, block_1, ...);
         *     outputPositions[outputPositionsCount] = position;
         *     outputPositionsCount += result ? 1 : 0;
         * }
         */
        ifStatement.ifFalse(new ForLoop("non-nullable positions loop")
                .initialize(index.set(offset))
                .condition(lessThan(index, add(offset, size)))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(position.set(activePositions.getElement(index)))
                        .append(generateFullInvocation(functionManager, instance, callSiteBinder, callExpression, scope, position)
                                .putVariable(result))
                        .append(updateOutputPositions(result, position, outputPositions, outputPositionsCount))));

        body.append(outputPositionsCount.ret());
    }

    static BytecodeBlock generateInvocation(
            FunctionManager functionManager,
            CallSiteBinder binder,
            CallExpression callExpression,
            Scope scope,
            BytecodeExpression position)
    {
        return generateFullInvocation(
                functionManager,
                _ -> {
                    throw new IllegalArgumentException("Simple method invocation can not be used with functions that require an instance factory");
                },
                binder,
                callExpression,
                scope,
                position);
    }

    private static BytecodeBlock generateFullInvocation(
            FunctionManager functionManager,
            Function<MethodHandle, BytecodeNode> instanceFactory,
            CallSiteBinder binder,
            CallExpression callExpression,
            Scope scope,
            BytecodeExpression position)
    {
        ResolvedFunction resolvedFunction = callExpression.resolvedFunction();
        String functionName = resolvedFunction.signature().getName().getFunctionName();
        BytecodeBlock block = new BytecodeBlock()
                .setDescription("invoke " + functionName);

        ScalarFunctionImplementation implementation = getScalarFunctionImplementation(functionManager, callExpression);

        Binding binding = binder.bind(implementation.getMethodHandle());

        Optional<BytecodeNode> instance = implementation.getInstanceFactory()
                .map(instanceFactory);

        // Index of current parameter in the MethodHandle
        int currentParameterIndex = 0;
        MethodType methodType = binding.getType();
        boolean instanceIsBound = false;
        while (currentParameterIndex < methodType.parameterArray().length) {
            Class<?> type = methodType.parameterArray()[currentParameterIndex];
            if (instance.isPresent() && !instanceIsBound) {
                checkState(type.equals(implementation.getInstanceFactory().get().type().returnType()), "Mismatched type for instance parameter");
                block.append(instance.get());
                instanceIsBound = true;
            }
            else if (type == ConnectorSession.class) {
                block.append(scope.getVariable("session"));
            }
            currentParameterIndex++;
        }
        for (RowExpression argumentExpression : callExpression.arguments()) {
            if (argumentExpression instanceof InputReferenceExpression inputReference) {
                block.append(generateInputReference(scope.getVariable("block_" + inputReference.field()), position));
            }
            else if (argumentExpression instanceof ConstantExpression constant) {
                block.append(generateConstant(binder, constant));
            }
            else {
                throw new UnsupportedOperationException(format("CallExpression %s is not supported", callExpression));
            }
        }
        block.append(invoke(binding, functionName));
        return block;
    }

    private static ScalarFunctionImplementation getScalarFunctionImplementation(FunctionManager functionManager, CallExpression callExpression)
    {
        ResolvedFunction resolvedFunction = callExpression.resolvedFunction();
        List<RowExpression> argumentExpressions = callExpression.arguments();

        ImmutableList.Builder<InvocationConvention.InvocationArgumentConvention> builder = ImmutableList.builderWithExpectedSize(argumentExpressions.size());
        for (RowExpression argumentExpression : argumentExpressions) {
            if (argumentExpression instanceof InputReferenceExpression) {
                builder.add(BLOCK_POSITION);
            }
            else if (argumentExpression instanceof ConstantExpression) {
                builder.add(NEVER_NULL);
            }
            else {
                throw new UnsupportedOperationException(format("CallExpression %s is not supported", callExpression));
            }
        }

        InvocationConvention invocationConvention = new InvocationConvention(
                builder.build(),
                resolvedFunction.functionNullability().isReturnNullable() ? DEFAULT_ON_NULL : FAIL_ON_NULL,
                true,
                true);
        return functionManager.getScalarFunctionImplementation(resolvedFunction, invocationConvention);
    }

    private static BytecodeNode generateInputReference(BytecodeExpression block, BytecodeExpression position)
    {
        BytecodeBlock blockAndPosition = new BytecodeBlock();
        blockAndPosition.append(block);
        blockAndPosition.append(position);
        return blockAndPosition;
    }

    private static BytecodeNode generateConstant(CallSiteBinder callSiteBinder, ConstantExpression constant)
    {
        Object value = constant.value();
        Class<?> javaType = constant.type().getJavaType();

        BytecodeBlock block = new BytecodeBlock();

        // use LDC for primitives (boolean, short, int, long, float, double)
        block.comment("constant " + constant.type().getTypeSignature());
        if (javaType == boolean.class) {
            return block.append(loadBoolean((Boolean) value));
        }
        if (javaType == long.class) {
            return block.append(loadLong((Long) value));
        }
        if (javaType == double.class) {
            return block.append(loadDouble((Double) value));
        }
        if (javaType == String.class) {
            return block.append(loadString((String) value));
        }

        // bind constant object directly into the call-site using invoke dynamic
        Binding binding = callSiteBinder.bind(value, constant.type().getJavaType());

        return new BytecodeBlock()
                .setDescription("constant " + constant.type())
                .comment(constant.toString())
                .append(loadConstant(binding));
    }

    private static void generateConstructor(ClassDefinition classDefinition, CachedInstanceBinder cachedInstanceBinder)
    {
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        cachedInstanceBinder.generateInitializations(thisVariable, body);
        body.ret();
    }

    private static final class CachedInstanceBinder
    {
        private final ClassDefinition classDefinition;
        private final CallSiteBinder callSiteBinder;
        private Optional<FieldDefinition> field = Optional.empty();
        private Optional<MethodHandle> method = Optional.empty();

        public CachedInstanceBinder(ClassDefinition classDefinition, CallSiteBinder callSiteBinder)
        {
            this.classDefinition = requireNonNull(classDefinition, "classDefinition is null");
            this.callSiteBinder = requireNonNull(callSiteBinder, "callSiteBinder is null");
        }

        public FieldDefinition getCachedInstance(MethodHandle methodHandle)
        {
            if (field.isEmpty()) {
                field = Optional.of(classDefinition.declareField(a(PRIVATE, FINAL), "__cachedInstance", methodHandle.type().returnType()));
                method = Optional.of(methodHandle);
            }
            return field.get();
        }

        public void generateInitializations(Variable thisVariable, BytecodeBlock block)
        {
            if (field.isPresent()) {
                Binding binding = callSiteBinder.bind(method.orElseThrow());
                block.append(thisVariable)
                        .append(invoke(binding, "instanceFieldConstructor"))
                        .putField(field.get());
            }
        }
    }
}
