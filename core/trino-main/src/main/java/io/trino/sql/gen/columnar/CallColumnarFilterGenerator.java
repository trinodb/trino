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
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.Page;
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

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.inlineIf;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.instruction.Constant.loadBoolean;
import static io.airlift.bytecode.instruction.Constant.loadDouble;
import static io.airlift.bytecode.instruction.Constant.loadLong;
import static io.airlift.bytecode.instruction.Constant.loadString;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.sql.gen.BytecodeUtils.invoke;
import static io.trino.sql.gen.BytecodeUtils.loadConstant;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.createClassInstance;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.declareBlockVariables;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateBlockMayHaveNull;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateBlockPositionNotNull;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateConstructor;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateGetInputChannels;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CallColumnarFilterGenerator
{
    private final CallExpression callExpression;
    private final FunctionManager functionManager;

    public CallColumnarFilterGenerator(CallExpression callExpression, FunctionManager functionManager)
    {
        callExpression.getResolvedFunction().signature().getArgumentTypes().forEach(type -> {
            if (type instanceof FunctionType) {
                throw new UnsupportedOperationException(format("Function with argument type %s is not supported", type));
            }
        });
        this.callExpression = callExpression;
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
    }

    public Supplier<ColumnarFilter> generateColumnarFilter()
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(ColumnarFilter.class.getSimpleName() + callExpression.getResolvedFunction().signature().getName(), Optional.empty()),
                type(Object.class),
                type(ColumnarFilter.class));
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        generateConstructor(classDefinition);

        generateGetInputChannels(callSiteBinder, classDefinition, callExpression);

        FunctionNullability functionNullability = callExpression.getResolvedFunction().functionNullability();
        if (functionNullability.getArgumentNullable().stream().noneMatch(nullable -> nullable)) {
            generateFilterRangeMethod(callSiteBinder, classDefinition, callExpression);
            generateFilterListMethod(callSiteBinder, classDefinition, callExpression);
        }
        else if (functionNullability.getArgumentNullable().stream().allMatch(nullable -> nullable)) {
            // IS DISTINCT FROM
            generateNullableFunctionFilterRangeMethod(callSiteBinder, classDefinition, callExpression);
            generateNullableFunctionFilterListMethod(callSiteBinder, classDefinition, callExpression);
        }
        else {
            throw new UnsupportedOperationException(format("FunctionNullability %s is not supported", functionNullability));
        }

        return createClassInstance(callSiteBinder, classDefinition);
    }

    private void generateFilterRangeMethod(CallSiteBinder binder, ClassDefinition classDefinition, CallExpression callExpression)
    {
        Parameter outputPositions = arg("outputPositions", int[].class);
        Parameter offset = arg("offset", int.class);
        Parameter size = arg("size", int.class);
        Parameter page = arg("page", Page.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filterPositionsRange",
                type(int.class),
                ImmutableList.of(outputPositions, offset, size, page));
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(callExpression.getArguments(), page, scope, body);

        Variable outputPositionsCount = scope.declareVariable("outputPositionsCount", body, constantInt(0));
        Variable position = scope.declareVariable(int.class, "position");
        Variable result = scope.declareVariable(boolean.class, "result");

        IfStatement ifStatement = new IfStatement()
                .condition(generateBlockMayHaveNull(callExpression.getArguments(), scope));
        body.append(ifStatement);

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
                        .condition(generateBlockPositionNotNull(callExpression.getArguments(), scope, position))
                        .ifTrue(new BytecodeBlock()
                                .append(generateFunctionCall(functionManager, binder, callExpression, scope, position)
                                        .putVariable(result))
                                .append(outputPositions.setElement(outputPositionsCount, position))
                                .append(outputPositionsCount.set(
                                        add(
                                                outputPositionsCount,
                                                inlineIf(result, constantInt(1), constantInt(0))))))));

        ifStatement.ifFalse(generateNonNullableRangeLoop(
                binder,
                callExpression,
                scope,
                offset,
                size,
                outputPositions,
                outputPositionsCount));

        body.append(outputPositionsCount.ret());
    }

    private void generateNullableFunctionFilterRangeMethod(CallSiteBinder binder, ClassDefinition classDefinition, CallExpression callExpression)
    {
        Parameter outputPositions = arg("outputPositions", int[].class);
        Parameter offset = arg("offset", int.class);
        Parameter size = arg("size", int.class);
        Parameter page = arg("page", Page.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filterPositionsRange",
                type(int.class),
                ImmutableList.of(outputPositions, offset, size, page));
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(callExpression.getArguments(), page, scope, body);

        Variable outputPositionsCount = scope.declareVariable("outputPositionsCount", body, constantInt(0));
        scope.declareVariable(int.class, "position");
        scope.declareVariable(boolean.class, "result");

        body.append(generateNonNullableRangeLoop(
                binder,
                callExpression,
                scope,
                offset,
                size,
                outputPositions,
                outputPositionsCount));

        body.append(outputPositionsCount.ret());
    }

    private void generateFilterListMethod(CallSiteBinder binder, ClassDefinition classDefinition, CallExpression callExpression)
    {
        Parameter outputPositions = arg("outputPositions", int[].class);
        Parameter activePositions = arg("activePositions", int[].class);
        Parameter offset = arg("offset", int.class);
        Parameter size = arg("size", int.class);
        Parameter page = arg("page", Page.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filterPositionsList",
                type(int.class),
                ImmutableList.of(outputPositions, activePositions, offset, size, page));
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(callExpression.getArguments(), page, scope, body);

        Variable outputPositionsCount = scope.declareVariable("outputPositionsCount", body, constantInt(0));
        Variable index = scope.declareVariable(int.class, "index");
        Variable position = scope.declareVariable(int.class, "position");
        Variable result = scope.declareVariable(boolean.class, "result");

        IfStatement ifStatement = new IfStatement()
                .condition(generateBlockMayHaveNull(callExpression.getArguments(), scope));
        body.append(ifStatement);

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
                                .condition(generateBlockPositionNotNull(callExpression.getArguments(), scope, position))
                                .ifTrue(new BytecodeBlock()
                                        .append(generateFunctionCall(functionManager, binder, callExpression, scope, position)
                                                .putVariable(result))
                                        .append(outputPositions.setElement(outputPositionsCount, position))
                                        .append(outputPositionsCount.set(
                                                add(
                                                        outputPositionsCount,
                                                        inlineIf(result, constantInt(1), constantInt(0)))))))));

        ifStatement.ifFalse(generateNonNullablePositionsListLoop(
                binder,
                callExpression,
                scope,
                offset,
                size,
                activePositions,
                outputPositions,
                outputPositionsCount));

        body.append(outputPositionsCount.ret());
    }

    private void generateNullableFunctionFilterListMethod(CallSiteBinder binder, ClassDefinition classDefinition, CallExpression callExpression)
    {
        Parameter outputPositions = arg("outputPositions", int[].class);
        Parameter activePositions = arg("activePositions", int[].class);
        Parameter offset = arg("offset", int.class);
        Parameter size = arg("size", int.class);
        Parameter page = arg("page", Page.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filterPositionsList",
                type(int.class),
                ImmutableList.of(outputPositions, activePositions, offset, size, page));
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(callExpression.getArguments(), page, scope, body);

        Variable outputPositionsCount = scope.declareVariable("outputPositionsCount", body, constantInt(0));
        scope.declareVariable(int.class, "position");
        scope.declareVariable(boolean.class, "result");

        body.append(
                generateNonNullablePositionsListLoop(
                        binder,
                        callExpression,
                        scope,
                        offset,
                        size,
                        activePositions,
                        outputPositions,
                        outputPositionsCount));

        body.append(outputPositionsCount.ret());
    }

    private BytecodeNode generateNonNullableRangeLoop(
            CallSiteBinder binder,
            CallExpression callExpression,
            Scope scope,
            BytecodeExpression offset,
            Variable size,
            Variable outputPositions,
            Variable outputPositionsCount)
    {
        Variable position = scope.getVariable("position");
        Variable result = scope.getVariable("result");

        /* for (position = offset; position < offset + size; position++) {
         *     boolean result = call_function(position, block_0, block_1, ...);
         *     outputPositions[outputPositionsCount] = position;
         *     outputPositionsCount += result ? 1 : 0;
         * }
         */
        return new ForLoop("nullable function range based loop")
                .initialize(position.set(offset))
                .condition(lessThan(position, add(offset, size)))
                .update(position.increment())
                .body(new BytecodeBlock()
                        .append(outputPositions.setElement(outputPositionsCount, position))
                        .append(generateFunctionCall(functionManager, binder, callExpression, scope, position)
                                .putVariable(result))
                        .append(outputPositionsCount.set(
                                add(
                                        outputPositionsCount,
                                        inlineIf(result, constantInt(1), constantInt(0))))));
    }

    private BytecodeNode generateNonNullablePositionsListLoop(
            CallSiteBinder binder,
            CallExpression callExpression,
            Scope scope,
            BytecodeExpression offset,
            Variable size,
            Variable activePositions,
            Variable outputPositions,
            Variable outputPositionsCount)
    {
        Variable positionsIndex = scope.declareVariable(int.class, "positionsIndex");
        Variable position = scope.getVariable("position");
        Variable result = scope.getVariable("result");

        /* for (int index = offset; index < offset + size; index++) {
         *     int position = activePositions[index];
         *     boolean result = call_function(position, block_0, block_1, ...);
         *     outputPositions[outputPositionsCount] = position;
         *     outputPositionsCount += result ? 1 : 0;
         * }
         */
        return new ForLoop("non-nullable positions loop")
                .initialize(positionsIndex.set(offset))
                .condition(lessThan(positionsIndex, add(offset, size)))
                .update(positionsIndex.increment())
                .body(new BytecodeBlock()
                        .append(position.set(activePositions.getElement(positionsIndex)))
                        .append(outputPositions.setElement(outputPositionsCount, position))
                        .append(generateFunctionCall(functionManager, binder, callExpression, scope, position)
                                .putVariable(result))
                        .append(outputPositionsCount.set(
                                add(
                                        outputPositionsCount,
                                        inlineIf(result, constantInt(1), constantInt(0))))));
    }

    public static BytecodeBlock generateFunctionCall(
            FunctionManager functionManager,
            CallSiteBinder binder,
            CallExpression callExpression,
            Scope scope,
            BytecodeExpression position)
    {
        List<RowExpression> arguments = callExpression.getArguments();
        ResolvedFunction resolvedFunction = callExpression.getResolvedFunction();
        String functionName = resolvedFunction.signature().getName().getFunctionName();
        BytecodeBlock block = new BytecodeBlock()
                .setDescription("invoke " + functionName);

        InvocationConvention.InvocationArgumentConvention[] argumentConventions = new InvocationConvention.InvocationArgumentConvention[arguments.size()];
        int channel = 0;
        for (int i = 0; i < arguments.size(); i++) {
            if (arguments.get(i) instanceof InputReferenceExpression) {
                argumentConventions[i] = BLOCK_POSITION;
                block.append(generateInputReference(scope.getVariable("block_" + channel), position));
                channel++;
            }
            else if (arguments.get(i) instanceof ConstantExpression) {
                argumentConventions[i] = NEVER_NULL;
                block.append(generateConstant(binder, (ConstantExpression) arguments.get(i)));
            }
            else {
                throw new UnsupportedOperationException(format("CallExpression %s is not supported", callExpression));
            }
        }

        ScalarFunctionImplementation scalarFunctionImplementation = functionManager.getScalarFunctionImplementation(
                resolvedFunction,
                simpleConvention(FAIL_ON_NULL, argumentConventions));

        Binding binding = binder.bind(scalarFunctionImplementation.getMethodHandle());
        block.append(invoke(binding, functionName));
        return block;
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
        Object value = constant.getValue();
        Class<?> javaType = constant.getType().getJavaType();

        BytecodeBlock block = new BytecodeBlock();

        // use LDC for primitives (boolean, short, int, long, float, double)
        block.comment("constant " + constant.getType().getTypeSignature());
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
        Binding binding = callSiteBinder.bind(value, constant.getType().getJavaType());

        return new BytecodeBlock()
                .setDescription("constant " + constant.getType())
                .comment(constant.toString())
                .append(loadConstant(binding));
    }
}
