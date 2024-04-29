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
package io.trino.sql.routine;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.ParameterizedType;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.DoWhileLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.control.WhileLoop;
import io.airlift.bytecode.instruction.LabelNode;
import io.trino.metadata.FunctionManager;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionAdapter;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.CachedInstanceBinder;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import io.trino.sql.gen.RowExpressionCompiler;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.RowExpressionVisitor;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.VariableReferenceExpression;
import io.trino.sql.routine.ir.DefaultIrNodeVisitor;
import io.trino.sql.routine.ir.IrBlock;
import io.trino.sql.routine.ir.IrBreak;
import io.trino.sql.routine.ir.IrContinue;
import io.trino.sql.routine.ir.IrIf;
import io.trino.sql.routine.ir.IrLabel;
import io.trino.sql.routine.ir.IrLoop;
import io.trino.sql.routine.ir.IrNode;
import io.trino.sql.routine.ir.IrNodeVisitor;
import io.trino.sql.routine.ir.IrRepeat;
import io.trino.sql.routine.ir.IrReturn;
import io.trino.sql.routine.ir.IrRoutine;
import io.trino.sql.routine.ir.IrSet;
import io.trino.sql.routine.ir.IrStatement;
import io.trino.sql.routine.ir.IrVariable;
import io.trino.sql.routine.ir.IrWhile;
import io.trino.util.Reflection;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.primitives.Primitives.wrap;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantString;
import static io.airlift.bytecode.expression.BytecodeExpressions.greaterThanOrEqual;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.airlift.bytecode.instruction.Constant.loadBoolean;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.sql.gen.BytecodeUtils.boxPrimitiveIfNecessary;
import static io.trino.sql.gen.BytecodeUtils.unboxPrimitiveIfNecessary;
import static io.trino.sql.gen.LambdaBytecodeGenerator.preGenerateLambdaExpression;
import static io.trino.sql.gen.LambdaExpressionExtractor.extractLambdaExpressions;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static io.trino.util.Reflection.constructorMethodHandle;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public final class SqlRoutineCompiler
{
    private final FunctionManager functionManager;

    public SqlRoutineCompiler(FunctionManager functionManager)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
    }

    public SpecializedSqlScalarFunction compile(IrRoutine routine)
    {
        Type returnType = routine.returnType();
        List<Type> parameterTypes = routine.parameters().stream()
                .map(IrVariable::type)
                .collect(toImmutableList());

        InvocationConvention callingConvention = new InvocationConvention(
                // todo this should be based on the declared nullability of the parameters
                Collections.nCopies(parameterTypes.size(), BOXED_NULLABLE),
                NULLABLE_RETURN,
                true,
                true);

        Class<?> clazz = compileClass(routine);

        MethodHandle handle = stream(clazz.getMethods())
                .filter(method -> method.getName().equals("run"))
                .map(Reflection::methodHandle)
                .collect(onlyElement());

        MethodHandle instanceFactory = constructorMethodHandle(clazz);

        MethodHandle objectHandle = handle.asType(handle.type().changeParameterType(0, Object.class));
        MethodHandle objectInstanceFactory = instanceFactory.asType(instanceFactory.type().changeReturnType(Object.class));

        return invocationConvention -> {
            MethodHandle adapted = ScalarFunctionAdapter.adapt(
                    objectHandle,
                    returnType,
                    parameterTypes,
                    callingConvention,
                    invocationConvention);
            return ScalarFunctionImplementation.builder()
                    .methodHandle(adapted)
                    .instanceFactory(objectInstanceFactory)
                    .build();
        };
    }

    @VisibleForTesting
    public Class<?> compileClass(IrRoutine routine)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("SqlRoutine"),
                type(Object.class));

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(classDefinition, cachedInstanceBinder, routine);

        generateRunMethod(classDefinition, cachedInstanceBinder, compiledLambdaMap, routine);

        declareConstructor(classDefinition, cachedInstanceBinder);

        return defineClass(classDefinition, Object.class, callSiteBinder.getBindings(), new DynamicClassLoader(getClass().getClassLoader()));
    }

    private Map<LambdaDefinitionExpression, CompiledLambda> generateMethodsForLambda(
            ClassDefinition containerClassDefinition,
            CachedInstanceBinder cachedInstanceBinder,
            IrNode node)
    {
        Set<LambdaDefinitionExpression> lambdaExpressions = extractLambda(node);
        ImmutableMap.Builder<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = ImmutableMap.builder();
        int counter = 0;
        for (LambdaDefinitionExpression lambdaExpression : lambdaExpressions) {
            CompiledLambda compiledLambda = preGenerateLambdaExpression(
                    lambdaExpression,
                    "lambda_" + counter,
                    containerClassDefinition,
                    compiledLambdaMap.buildOrThrow(),
                    cachedInstanceBinder.getCallSiteBinder(),
                    cachedInstanceBinder,
                    functionManager);
            compiledLambdaMap.put(lambdaExpression, compiledLambda);
            counter++;
        }
        return compiledLambdaMap.buildOrThrow();
    }

    private void generateRunMethod(
            ClassDefinition classDefinition,
            CachedInstanceBinder cachedInstanceBinder,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            IrRoutine routine)
    {
        ImmutableList.Builder<Parameter> parameterBuilder = ImmutableList.builder();
        parameterBuilder.add(arg("session", ConnectorSession.class));
        for (IrVariable sqlVariable : routine.parameters()) {
            parameterBuilder.add(arg(name(sqlVariable), compilerType(sqlVariable.type())));
        }

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "run",
                compilerType(routine.returnType()),
                parameterBuilder.build());

        Scope scope = method.getScope();

        scope.declareVariable(boolean.class, "wasNull");

        Map<IrVariable, Variable> variables = VariableExtractor.extract(routine).stream().distinct()
                .collect(toImmutableMap(identity(), variable -> getOrDeclareVariable(scope, variable)));

        BytecodeVisitor visitor = new BytecodeVisitor(cachedInstanceBinder, compiledLambdaMap, variables);
        method.getBody().append(visitor.process(routine, scope));
    }

    private static BytecodeNode throwIfInterrupted()
    {
        return new IfStatement()
                .condition(invokeStatic(Thread.class, "currentThread", Thread.class)
                        .invoke("isInterrupted", boolean.class))
                .ifTrue(new BytecodeBlock()
                        .append(newInstance(RuntimeException.class, constantString("Thread interrupted")))
                        .throwObject());
    }

    private static void declareConstructor(ClassDefinition classDefinition, CachedInstanceBinder cachedInstanceBinder)
    {
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));
        BytecodeBlock body = constructorDefinition.getBody();
        body.append(constructorDefinition.getThis())
                .invokeConstructor(Object.class);
        cachedInstanceBinder.generateInitializations(constructorDefinition.getThis(), body);
        body.ret();
    }

    private static Variable getOrDeclareVariable(Scope scope, IrVariable variable)
    {
        return getOrDeclareVariable(scope, compilerType(variable.type()), name(variable));
    }

    private static Variable getOrDeclareVariable(Scope scope, ParameterizedType type, String name)
    {
        try {
            return scope.getVariable(name);
        }
        catch (IllegalArgumentException e) {
            return scope.declareVariable(type, name);
        }
    }

    private static ParameterizedType compilerType(Type type)
    {
        return type(wrap(type.getJavaType()));
    }

    private static String name(IrVariable variable)
    {
        return name(variable.field());
    }

    private static String name(int field)
    {
        return "v" + field;
    }

    private class BytecodeVisitor
            implements IrNodeVisitor<Scope, BytecodeNode>
    {
        private final CachedInstanceBinder cachedInstanceBinder;
        private final Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap;
        private final Map<IrVariable, Variable> variables;

        private final Map<IrLabel, LabelNode> continueLabels = new HashMap<>();
        private final Map<IrLabel, LabelNode> breakLabels = new HashMap<>();

        public BytecodeVisitor(
                CachedInstanceBinder cachedInstanceBinder,
                Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
                Map<IrVariable, Variable> variables)
        {
            this.cachedInstanceBinder = requireNonNull(cachedInstanceBinder, "cachedInstanceBinder is null");
            this.compiledLambdaMap = requireNonNull(compiledLambdaMap, "compiledLambdaMap is null");
            this.variables = requireNonNull(variables, "variables is null");
        }

        @Override
        public BytecodeNode visitNode(IrNode node, Scope context)
        {
            throw new VerifyException("Unsupported node: " + node.getClass().getSimpleName());
        }

        @Override
        public BytecodeNode visitRoutine(IrRoutine node, Scope scope)
        {
            return process(node.body(), scope);
        }

        @Override
        public BytecodeNode visitSet(IrSet node, Scope scope)
        {
            return new BytecodeBlock()
                    .append(compile(node.value(), scope))
                    .putVariable(variables.get(node.target()));
        }

        @Override
        public BytecodeNode visitBlock(IrBlock node, Scope scope)
        {
            BytecodeBlock block = new BytecodeBlock();

            for (IrVariable sqlVariable : node.variables()) {
                block.append(compile(sqlVariable.defaultValue(), scope))
                        .putVariable(variables.get(sqlVariable));
            }

            LabelNode continueLabel = new LabelNode("continue");
            LabelNode breakLabel = new LabelNode("break");

            node.label().ifPresent(label -> {
                verify(continueLabels.putIfAbsent(label, continueLabel) == null, "continue label for loop label %s already exists", label);
                verify(breakLabels.putIfAbsent(label, breakLabel) == null, "break label for loop label %s already exists", label);
                block.visitLabel(continueLabel);
            });

            for (IrStatement statement : node.statements()) {
                block.append(process(statement, scope));
            }

            if (node.label().isPresent()) {
                block.visitLabel(breakLabel);
            }

            return block;
        }

        @Override
        public BytecodeNode visitReturn(IrReturn node, Scope scope)
        {
            return new BytecodeBlock()
                    .append(compile(node.value(), scope))
                    .ret(wrap(node.value().type().getJavaType()));
        }

        @Override
        public BytecodeNode visitContinue(IrContinue node, Scope scope)
        {
            LabelNode label = continueLabels.get(node.target());
            verify(label != null, "continue target does not exist");
            return new BytecodeBlock()
                    .gotoLabel(label);
        }

        @Override
        public BytecodeNode visitBreak(IrBreak node, Scope scope)
        {
            LabelNode label = breakLabels.get(node.target());
            verify(label != null, "break target does not exist");
            return new BytecodeBlock()
                    .gotoLabel(label);
        }

        @Override
        public BytecodeNode visitIf(IrIf node, Scope scope)
        {
            IfStatement ifStatement = new IfStatement()
                    .condition(compileBoolean(node.condition(), scope))
                    .ifTrue(process(node.ifTrue(), scope));

            if (node.ifFalse().isPresent()) {
                ifStatement.ifFalse(process(node.ifFalse().get(), scope));
            }

            return ifStatement;
        }

        @Override
        public BytecodeNode visitWhile(IrWhile node, Scope scope)
        {
            return compileLoop(scope, node.label(), interruption -> new WhileLoop()
                    .condition(compileBoolean(node.condition(), scope))
                    .body(new BytecodeBlock()
                            .append(interruption)
                            .append(process(node.body(), scope))));
        }

        @Override
        public BytecodeNode visitRepeat(IrRepeat node, Scope scope)
        {
            return compileLoop(scope, node.label(), interruption -> new DoWhileLoop()
                    .condition(not(compileBoolean(node.condition(), scope)))
                    .body(new BytecodeBlock()
                            .append(interruption)
                            .append(process(node.block(), scope))));
        }

        @Override
        public BytecodeNode visitLoop(IrLoop node, Scope scope)
        {
            return compileLoop(scope, node.label(), interruption -> new WhileLoop()
                    .condition(loadBoolean(true))
                    .body(new BytecodeBlock()
                            .append(interruption)
                            .append(process(node.block(), scope))));
        }

        private BytecodeNode compileLoop(Scope scope, Optional<IrLabel> label, Function<BytecodeBlock, BytecodeNode> loop)
        {
            BytecodeBlock block = new BytecodeBlock();

            Variable interruption = scope.createTempVariable(int.class);
            block.putVariable(interruption, 0);

            BytecodeBlock interruptionBlock = new BytecodeBlock()
                    .append(interruption.increment())
                    .append(new IfStatement()
                            .condition(greaterThanOrEqual(interruption, constantInt(1000)))
                            .ifTrue(new BytecodeBlock()
                                    .append(interruption.set(constantInt(0)))
                                    .append(throwIfInterrupted())));

            LabelNode continueLabel = new LabelNode("continue");
            LabelNode breakLabel = new LabelNode("break");

            if (label.isPresent()) {
                verify(continueLabels.putIfAbsent(label.get(), continueLabel) == null, "continue label for loop label %s already exists", label.get());
                verify(breakLabels.putIfAbsent(label.get(), breakLabel) == null, "break label for loop label %s already exists", label.get());
                block.visitLabel(continueLabel);
            }

            block.append(loop.apply(interruptionBlock));

            if (label.isPresent()) {
                block.visitLabel(breakLabel);
            }

            return block;
        }

        private BytecodeNode compile(RowExpression expression, Scope scope)
        {
            if (expression instanceof InputReferenceExpression input) {
                return scope.getVariable(name(input.field()));
            }

            RowExpressionCompiler rowExpressionCompiler = new RowExpressionCompiler(
                    cachedInstanceBinder.getCallSiteBinder(),
                    cachedInstanceBinder,
                    FieldReferenceCompiler.INSTANCE,
                    functionManager,
                    compiledLambdaMap);

            return new BytecodeBlock()
                    .comment("boolean wasNull = false;")
                    .putVariable(scope.getVariable("wasNull"), expression.type().getJavaType() == void.class)
                    .comment("expression: " + expression)
                    .append(rowExpressionCompiler.compile(expression, scope))
                    .append(boxPrimitiveIfNecessary(scope, wrap(expression.type().getJavaType())));
        }

        private BytecodeNode compileBoolean(RowExpression expression, Scope scope)
        {
            checkArgument(expression.type().equals(BooleanType.BOOLEAN), "type must be boolean");

            LabelNode notNull = new LabelNode("notNull");
            LabelNode done = new LabelNode("done");

            return new BytecodeBlock()
                    .append(compile(expression, scope))
                    .comment("if value is null, return false, otherwise unbox")
                    .dup()
                    .ifNotNullGoto(notNull)
                    .pop()
                    .push(false)
                    .gotoLabel(done)
                    .visitLabel(notNull)
                    .invokeVirtual(Boolean.class, "booleanValue", boolean.class)
                    .visitLabel(done);
        }

        private static BytecodeNode not(BytecodeNode node)
        {
            LabelNode trueLabel = new LabelNode("true");
            LabelNode endLabel = new LabelNode("end");
            return new BytecodeBlock()
                    .append(node)
                    .comment("boolean not")
                    .ifTrueGoto(trueLabel)
                    .push(true)
                    .gotoLabel(endLabel)
                    .visitLabel(trueLabel)
                    .push(false)
                    .visitLabel(endLabel);
        }
    }

    private static Set<LambdaDefinitionExpression> extractLambda(IrNode node)
    {
        ImmutableSet.Builder<LambdaDefinitionExpression> expressions = ImmutableSet.builder();
        node.accept(new DefaultIrNodeVisitor()
        {
            @Override
            public void visitRowExpression(RowExpression expression)
            {
                expressions.addAll(extractLambdaExpressions(expression));
            }
        }, null);
        return expressions.build();
    }

    private static class FieldReferenceCompiler
            implements RowExpressionVisitor<BytecodeNode, Scope>
    {
        public static final FieldReferenceCompiler INSTANCE = new FieldReferenceCompiler();

        @Override
        public BytecodeNode visitInputReference(InputReferenceExpression node, Scope scope)
        {
            Class<?> boxedType = wrap(node.type().getJavaType());
            return new BytecodeBlock()
                    .append(scope.getVariable(name(node.field())))
                    .append(unboxPrimitiveIfNecessary(scope, boxedType));
        }

        @Override
        public BytecodeNode visitCall(CallExpression call, Scope scope)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytecodeNode visitSpecialForm(SpecialForm specialForm, Scope context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytecodeNode visitConstant(ConstantExpression literal, Scope scope)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytecodeNode visitLambda(LambdaDefinitionExpression lambda, Scope context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytecodeNode visitVariableReference(VariableReferenceExpression reference, Scope context)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class VariableExtractor
            extends DefaultIrNodeVisitor
    {
        private final List<IrVariable> variables = new ArrayList<>();

        @Override
        public Void visitVariable(IrVariable node, Void context)
        {
            variables.add(node);
            return null;
        }

        public static List<IrVariable> extract(IrNode node)
        {
            VariableExtractor extractor = new VariableExtractor();
            extractor.process(node, null);
            return extractor.variables;
        }
    }
}
