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
package io.trino.sql.gen;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.control.WhileLoop;
import io.airlift.bytecode.instruction.LabelNode;
import io.airlift.slice.Slice;
import io.trino.cache.CacheStatsMBean;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.metadata.FunctionManager;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.CursorProcessor;
import io.trino.operator.project.CursorProcessorOutput;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import io.trino.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.RowExpressionVisitor;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.VariableReferenceExpression;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.airlift.bytecode.expression.BytecodeExpressions.or;
import static io.airlift.bytecode.instruction.JumpInstruction.jump;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.gen.BytecodeUtils.generateWrite;
import static io.trino.sql.gen.BytecodeUtils.invoke;
import static io.trino.sql.gen.LambdaExpressionExtractor.extractLambdaExpressions;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CursorProcessorCompiler
{
    private static final int MAX_PROJECTIONS_PER_CHUNK = 100;

    private final FunctionManager functionManager;

    private final NonEvictableLoadingCache<CacheKey, Class<? extends CursorProcessor>> cursorProcessors;
    private final CacheStatsMBean cacheStatsMBean;

    @Inject
    public CursorProcessorCompiler(FunctionManager functionManager)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.cursorProcessors = buildNonEvictableCache(CacheBuilder.newBuilder()
                        .recordStats()
                        .maximumSize(1000),
                CacheLoader.from(key -> compile(key.filter(), key.projections())));
        this.cacheStatsMBean = new CacheStatsMBean(cursorProcessors);
    }

    @Managed
    @Nested
    public CacheStatsMBean getCursorProcessorCache()
    {
        return cacheStatsMBean;
    }

    public Supplier<CursorProcessor> compileCursorProcessor(Optional<RowExpression> filter, List<RowExpression> projections, Object uniqueKey)
    {
        Class<? extends CursorProcessor> cursorProcessor;
        try {
            cursorProcessor = cursorProcessors.getUnchecked(new CacheKey(filter, projections, uniqueKey));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }

        return () -> {
            try {
                return cursorProcessor.getConstructor().newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private Class<? extends CursorProcessor> compile(Optional<RowExpression> filter, List<RowExpression> projections)
    {
        // create filter and project page iterator class
        return compileProcessor(filter.orElse(constant(true, BOOLEAN)), projections);
    }

    private Class<? extends CursorProcessor> compileProcessor(RowExpression filter, List<RowExpression> projections)
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        // Projections are split into multiple classes to prevent the process method from exceeding the JVM method size limit
        // and to avoid hitting the constant pool size limit in the generated CursorProcessor.
        List<CursorProcessorChunkClass> chunkClasses = new ArrayList<>();
        List<List<RowExpression>> partitions = Lists.partition(projections, MAX_PROJECTIONS_PER_CHUNK);
        int projectionStartIndex = 0;
        for (List<RowExpression> partition : partitions) {
            int chunkNumber = chunkClasses.size();

            CursorProcessorChunkClass chunkClass = generateCursorProcessorChunk(partition, projectionStartIndex, callSiteBinder, chunkNumber);
            chunkClasses.add(chunkClass);

            projectionStartIndex += partition.size();
        }

        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(CursorProcessor.class.getSimpleName()),
                type(Object.class),
                type(CursorProcessor.class));

        generateMethods(classDefinition, callSiteBinder, filter, chunkClasses);

        //
        // toString method
        //
        generateToString(
                classDefinition,
                callSiteBinder,
                toStringHelper(classDefinition.getType().getJavaClassName())
                        .add("filter", filter)
                        .add("projections", projections)
                        .toString());

        DynamicClassLoader dynamicClassLoader = new DynamicClassLoader(getClass().getClassLoader(), callSiteBinder.getBindings());
        for (CursorProcessorChunkClass chunkClass : chunkClasses) {
            defineClass(chunkClass.classDefinition(), Object.class, dynamicClassLoader);
        }
        return defineClass(classDefinition, CursorProcessor.class, dynamicClassLoader);
    }

    private void generateMethods(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, RowExpression filter, List<CursorProcessorChunkClass> chunkClasses)
    {
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        generateProcessMethod(classDefinition, chunkClasses);

        Map<LambdaDefinitionExpression, CompiledLambda> filterCompiledLambdaMap = generateMethodsForLambda(classDefinition, callSiteBinder, cachedInstanceBinder, filter, "filter");
        generateFilterMethod(classDefinition, callSiteBinder, cachedInstanceBinder, filterCompiledLambdaMap, filter);

        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));
        BytecodeBlock constructorBody = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();
        constructorBody.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        for (CursorProcessorChunkClass chunkClass : chunkClasses) {
            FieldDefinition chunkField = classDefinition.declareField(a(PRIVATE, FINAL), "chunk_" + chunkClass.chunkNumber(), chunkClass.classDefinition().getType());
            constructorBody.append(thisVariable.setField(chunkField, newInstance(chunkClass.classDefinition().getType())));
        }

        cachedInstanceBinder.generateInitializations(thisVariable, constructorBody);
        constructorBody.ret();
    }

    private static void generateProcessMethod(ClassDefinition classDefinition, List<CursorProcessorChunkClass> chunkClasses)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter yieldSignal = arg("yieldSignal", DriverYieldSignal.class);
        Parameter cursor = arg("cursor", RecordCursor.class);
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "process", type(CursorProcessorOutput.class), session, yieldSignal, cursor, pageBuilder);

        Scope scope = method.getScope();
        Variable completedPositionsVariable = scope.declareVariable(int.class, "completedPositions");
        Variable finishedVariable = scope.declareVariable(boolean.class, "finished");

        method.getBody()
                .comment("int completedPositions = 0;")
                .putVariable(completedPositionsVariable, 0)
                .comment("boolean finished = false;")
                .putVariable(finishedVariable, false);

        // while loop's body
        LabelNode done = new LabelNode("done");
        WhileLoop whileLoop = new WhileLoop()
                .condition(constantTrue())
                .body(new BytecodeBlock()
                        .comment("if (pageBuilder.isFull() || yieldSignal.isSet()) return new CursorProcessorOutput(completedPositions, false);")
                        .append(new IfStatement()
                                .condition(or(
                                        pageBuilder.invoke("isFull", boolean.class),
                                        yieldSignal.invoke("isSet", boolean.class)))
                                .ifTrue(jump(done)))
                        .comment("if (!cursor.advanceNextPosition()) return new CursorProcessorOutput(completedPositions, true);")
                        .append(new IfStatement()
                                .condition(cursor.invoke("advanceNextPosition", boolean.class))
                                .ifFalse(new BytecodeBlock()
                                        .putVariable(finishedVariable, true)
                                        .gotoLabel(done)))
                        .comment("do the projection")
                        .append(createProjectIfStatement(classDefinition, method, session, cursor, pageBuilder, chunkClasses))
                        .comment("completedPositions++;")
                        .incrementVariable(completedPositionsVariable, (byte) 1));

        method.getBody()
                .append(whileLoop)
                .visitLabel(done)
                .append(newInstance(CursorProcessorOutput.class, completedPositionsVariable, finishedVariable)
                        .ret());
    }

    private static IfStatement createProjectIfStatement(
            ClassDefinition classDefinition,
            MethodDefinition method,
            Parameter session,
            Parameter cursor,
            Parameter pageBuilder,
            List<CursorProcessorChunkClass> chunkClasses)
    {
        // if (filter(cursor))
        IfStatement ifStatement = new IfStatement();
        ifStatement.condition()
                .append(method.getThis())
                .getVariable(session)
                .getVariable(cursor)
                .invokeVirtual(classDefinition.getType(), "filter", type(boolean.class), type(ConnectorSession.class), type(RecordCursor.class));

        // pageBuilder.declarePosition();
        ifStatement.ifTrue()
                .getVariable(pageBuilder)
                .invokeVirtual(PageBuilder.class, "declarePosition", void.class);

        for (CursorProcessorChunkClass chunkClass : chunkClasses) {
            ifStatement.ifTrue()
                    .append(method.getThis()
                            .getField(classDefinition.getType(), "chunk_" + chunkClass.chunkNumber(), chunkClass.classDefinition().getType())
                            .invoke(chunkClass.projectMethod(), ImmutableList.of(session, cursor, pageBuilder)));
        }

        return ifStatement;
    }

    private CursorProcessorChunkClass generateCursorProcessorChunk(
            List<RowExpression> projections,
            int projectionStartIndex,
            CallSiteBinder callSiteBinder,
            int chunkNumber)
    {
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("CursorProcessorChunk"),
                type(Object.class));

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(definition, callSiteBinder);

        MethodDefinition constructorDefinition = definition.declareConstructor(a(PUBLIC));
        BytecodeBlock constructorBody = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();
        constructorBody.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        MethodDefinition project = generateProjectChunk(definition, projectionStartIndex, callSiteBinder, cachedInstanceBinder, projections);

        cachedInstanceBinder.generateInitializations(thisVariable, constructorBody);
        constructorBody.ret();

        return new CursorProcessorChunkClass(definition, project, chunkNumber);
    }

    private MethodDefinition generateProjectChunk(
            ClassDefinition classDefinition,
            int projectionStartIndex,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            List<RowExpression> projections)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter cursor = arg("cursor", RecordCursor.class);
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);

        MethodDefinition methodDefinition = classDefinition.declareMethod(
                a(PUBLIC),
                "project",
                type(void.class),
                session,
                cursor,
                pageBuilder);

        BytecodeBlock block = methodDefinition.getBody();

        // this.project_43(session, cursor, pageBuilder.getBlockBuilder(43)));
        int projectionIndex = projectionStartIndex;
        for (RowExpression projection : projections) {
            String methodName = "project_" + projectionIndex;
            Map<LambdaDefinitionExpression, CompiledLambda> projectCompiledLambdaMap = generateMethodsForLambda(
                    classDefinition,
                    callSiteBinder,
                    cachedInstanceBinder,
                    projection,
                    methodName);
            generateProjectMethod(
                    classDefinition,
                    callSiteBinder,
                    cachedInstanceBinder,
                    projectCompiledLambdaMap,
                    methodName,
                    projection);

            block.append(methodDefinition.getThis())
                    .getVariable(session)
                    .getVariable(cursor);

            // pageBuilder.getBlockBuilder(0)
            block.getVariable(pageBuilder)
                    .push(projectionIndex)
                    .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class);

            // project(block..., blockBuilder)gen
            block.invokeVirtual(classDefinition.getType(),
                    methodName,
                    type(void.class),
                    type(ConnectorSession.class),
                    type(RecordCursor.class),
                    type(BlockBuilder.class));

            projectionIndex++;
        }

        block.ret();

        return methodDefinition;
    }

    private Map<LambdaDefinitionExpression, CompiledLambda> generateMethodsForLambda(
            ClassDefinition containerClassDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpression projection,
            String methodPrefix)
    {
        Set<LambdaDefinitionExpression> lambdaExpressions = ImmutableSet.copyOf(extractLambdaExpressions(projection));

        ImmutableMap.Builder<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = ImmutableMap.builder();

        int counter = 0;
        for (LambdaDefinitionExpression lambdaExpression : lambdaExpressions) {
            String methodName = methodPrefix + "_lambda_" + counter;
            CompiledLambda compiledLambda = LambdaBytecodeGenerator.preGenerateLambdaExpression(
                    lambdaExpression,
                    methodName,
                    containerClassDefinition,
                    compiledLambdaMap.buildOrThrow(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    functionManager);
            compiledLambdaMap.put(lambdaExpression, compiledLambda);
            counter++;
        }

        return compiledLambdaMap.buildOrThrow();
    }

    private void generateFilterMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            RowExpression filter)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter cursor = arg("cursor", RecordCursor.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "filter", type(boolean.class), session, cursor);

        method.comment("Filter: %s", filter);

        Scope scope = method.getScope();
        Variable wasNullVariable = scope.declareVariable(type(boolean.class), "wasNull");

        RowExpressionCompiler compiler = new RowExpressionCompiler(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(cursor),
                functionManager,
                compiledLambdaMap,
                ImmutableList.of(session, cursor));

        LabelNode end = new LabelNode("end");
        method.getBody()
                .comment("boolean wasNull = false;")
                .putVariable(wasNullVariable, false)
                .comment("evaluate filter: " + filter)
                .append(compiler.compile(filter, scope))
                .comment("if (wasNull) return false;")
                .getVariable(wasNullVariable)
                .ifFalseGoto(end)
                .pop(boolean.class)
                .push(false)
                .visitLabel(end)
                .retBoolean();
    }

    private void generateProjectMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            String methodName,
            RowExpression projection)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter cursor = arg("cursor", RecordCursor.class);
        Parameter output = arg("output", BlockBuilder.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), methodName, type(void.class), session, cursor, output);

        method.comment("Projection: %s", projection);

        Scope scope = method.getScope();
        Variable wasNullVariable = scope.declareVariable(type(boolean.class), "wasNull");

        RowExpressionCompiler compiler = new RowExpressionCompiler(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(cursor),
                functionManager,
                compiledLambdaMap,
                ImmutableList.of(session, cursor, output));

        method.getBody()
                .comment("boolean wasNull = false;")
                .putVariable(wasNullVariable, false)
                .getVariable(output)
                .comment("evaluate projection: " + projection.toString())
                .append(compiler.compile(projection, scope))
                .append(generateWrite(callSiteBinder, scope, wasNullVariable, projection.type()))
                .ret();
    }

    private static RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler(Variable cursorVariable)
    {
        return new RowExpressionVisitor<>()
        {
            @Override
            public BytecodeNode visitInputReference(InputReferenceExpression node, Scope scope)
            {
                int field = node.field();
                Type type = node.type();
                Variable wasNullVariable = scope.getVariable("wasNull");

                Class<?> javaType = type.getJavaType();

                IfStatement ifStatement = new IfStatement();
                ifStatement.condition()
                        .setDescription(format("cursor.get%s(%d)", type, field))
                        .getVariable(cursorVariable)
                        .push(field)
                        .invokeInterface(RecordCursor.class, "isNull", boolean.class, int.class);

                ifStatement.ifTrue()
                        .putVariable(wasNullVariable, true)
                        .pushJavaDefault(javaType);

                ifStatement.ifFalse()
                        .getVariable(cursorVariable)
                        .push(field);
                if (javaType == boolean.class) {
                    ifStatement.ifFalse().invokeInterface(RecordCursor.class, "getBoolean", boolean.class, int.class);
                }
                else if (javaType == long.class) {
                    ifStatement.ifFalse().invokeInterface(RecordCursor.class, "getLong", long.class, int.class);
                }
                else if (javaType == double.class) {
                    ifStatement.ifFalse().invokeInterface(RecordCursor.class, "getDouble", double.class, int.class);
                }
                else if (javaType == Slice.class) {
                    ifStatement.ifFalse().invokeInterface(RecordCursor.class, "getSlice", Slice.class, int.class);
                }
                else {
                    ifStatement.ifFalse().invokeInterface(RecordCursor.class, "getObject", Object.class, int.class)
                            .checkCast(javaType);
                }

                return ifStatement;
            }

            @Override
            public BytecodeNode visitCall(CallExpression call, Scope scope)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }

            @Override
            public BytecodeNode visitSpecialForm(SpecialForm specialForm, Scope context)
            {
                throw new UnsupportedOperationException("not yet implemented");
            }

            @Override
            public BytecodeNode visitConstant(ConstantExpression literal, Scope scope)
            {
                throw new UnsupportedOperationException("not yet implemented");
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
        };
    }

    private static void generateToString(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, String string)
    {
        // bind constant via invokedynamic to avoid constant pool issues due to large strings
        classDefinition.declareMethod(a(PUBLIC), "toString", type(String.class))
                .getBody()
                .append(invoke(callSiteBinder.bind(string, String.class), "toString"))
                .retObject();
    }

    private record CacheKey(Optional<RowExpression> filter, List<RowExpression> projections, Object uniqueKey)
    {
        CacheKey
        {
            projections = ImmutableList.copyOf(requireNonNull(projections, "projections is null"));
        }
    }

    private record CursorProcessorChunkClass(ClassDefinition classDefinition, MethodDefinition projectMethod, int chunkNumber) {}
}
