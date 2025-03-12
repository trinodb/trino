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

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.ParameterizedType;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.trino.cache.CacheStatsMBean;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.metadata.FunctionManager;
import io.trino.operator.Work;
import io.trino.operator.project.ConstantPageProjection;
import io.trino.operator.project.GeneratedPageProjection;
import io.trino.operator.project.InputChannels;
import io.trino.operator.project.InputPageProjection;
import io.trino.operator.project.PageFieldsToInputParametersRewriter;
import io.trino.operator.project.PageFilter;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import io.trino.sql.planner.CompilerConfig;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.Expressions;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.RowExpressionVisitor;
import jakarta.annotation.Nullable;
import org.objectweb.asm.MethodTooLargeException;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.and;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantBoolean;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.expression.BytecodeExpressions.newArray;
import static io.airlift.bytecode.expression.BytecodeExpressions.not;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.operator.project.PageFieldsToInputParametersRewriter.rewritePageFieldsToInputParameters;
import static io.trino.spi.StandardErrorCode.COMPILER_ERROR;
import static io.trino.spi.StandardErrorCode.QUERY_EXCEEDED_COMPILER_LIMIT;
import static io.trino.sql.gen.BytecodeUtils.generateWrite;
import static io.trino.sql.gen.BytecodeUtils.invoke;
import static io.trino.sql.gen.LambdaExpressionExtractor.extractLambdaExpressions;
import static io.trino.sql.relational.DeterminismEvaluator.isDeterministic;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static io.trino.util.Reflection.constructorMethodHandle;
import static java.util.Objects.requireNonNull;

public class PageFunctionCompiler
{
    private final FunctionManager functionManager;

    private final NonEvictableLoadingCache<RowExpression, Supplier<PageProjection>> projectionCache;
    private final NonEvictableLoadingCache<RowExpression, Supplier<PageFilter>> filterCache;

    private final CacheStatsMBean projectionCacheStats;
    private final CacheStatsMBean filterCacheStats;

    @Inject
    public PageFunctionCompiler(FunctionManager functionManager, CompilerConfig config)
    {
        this(functionManager, config.getExpressionCacheSize());
    }

    public PageFunctionCompiler(FunctionManager functionManager, int expressionCacheSize)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");

        if (expressionCacheSize > 0) {
            projectionCache = buildNonEvictableCache(
                    CacheBuilder.newBuilder()
                            .recordStats()
                            .maximumSize(expressionCacheSize),
                    CacheLoader.from(projection -> compileProjectionInternal(projection, Optional.empty())));
            projectionCacheStats = new CacheStatsMBean(projectionCache);
        }
        else {
            projectionCache = null;
            projectionCacheStats = null;
        }

        if (expressionCacheSize > 0) {
            filterCache = buildNonEvictableCache(
                    CacheBuilder.newBuilder()
                            .recordStats()
                            .maximumSize(expressionCacheSize),
                    CacheLoader.from(filter -> compileFilterInternal(filter, Optional.empty())));
            filterCacheStats = new CacheStatsMBean(filterCache);
        }
        else {
            filterCache = null;
            filterCacheStats = null;
        }
    }

    @Nullable
    @Managed
    @Nested
    public CacheStatsMBean getProjectionCache()
    {
        return projectionCacheStats;
    }

    @Nullable
    @Managed
    @Nested
    public CacheStatsMBean getFilterCache()
    {
        return filterCacheStats;
    }

    public Supplier<PageProjection> compileProjection(RowExpression projection, Optional<String> classNameSuffix)
    {
        if (projectionCache == null) {
            return compileProjectionInternal(projection, classNameSuffix);
        }
        try {
            return projectionCache.getUnchecked(projection);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
    }

    private Supplier<PageProjection> compileProjectionInternal(RowExpression projection, Optional<String> classNameSuffix)
    {
        requireNonNull(projection, "projection is null");

        if (projection instanceof InputReferenceExpression input) {
            InputPageProjection projectionFunction = new InputPageProjection(input.field(), input.type());
            return () -> projectionFunction;
        }

        if (projection instanceof ConstantExpression constant) {
            ConstantPageProjection projectionFunction = new ConstantPageProjection(constant.value(), constant.type());
            return () -> projectionFunction;
        }

        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(projection);
        boolean isExpressionDeterministic = isDeterministic(result.getRewrittenExpression());

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        // generate Work
        ClassDefinition pageProjectionWorkDefinition = definePageProjectWorkClass(result.getRewrittenExpression(), callSiteBinder, classNameSuffix);

        Class<?> pageProjectionWorkClass;
        try {
            pageProjectionWorkClass = defineClass(pageProjectionWorkDefinition, Work.class, callSiteBinder.getBindings(), getClass().getClassLoader());
        }
        catch (Exception e) {
            if (Throwables.getRootCause(e) instanceof MethodTooLargeException) {
                throw new TrinoException(QUERY_EXCEEDED_COMPILER_LIMIT,
                        "Query exceeded maximum columns. Please reduce the number of columns referenced and re-run the query.", e);
            }
            throw new TrinoException(COMPILER_ERROR, e);
        }

        MethodHandle pageProjectionConstructor = constructorMethodHandle(pageProjectionWorkClass, BlockBuilder.class, ConnectorSession.class, SourcePage.class, SelectedPositions.class);
        return () -> new GeneratedPageProjection(
                result.getRewrittenExpression(),
                isExpressionDeterministic,
                result.getInputChannels(),
                pageProjectionConstructor);
    }

    private static ParameterizedType generateProjectionWorkClassName(Optional<String> classNameSuffix)
    {
        return makeClassName("PageProjectionWork", classNameSuffix);
    }

    private ClassDefinition definePageProjectWorkClass(RowExpression projection, CallSiteBinder callSiteBinder, Optional<String> classNameSuffix)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                generateProjectionWorkClassName(classNameSuffix),
                type(Object.class),
                type(Work.class));

        FieldDefinition blockBuilderField = classDefinition.declareField(a(PRIVATE), "blockBuilder", BlockBuilder.class);
        FieldDefinition sessionField = classDefinition.declareField(a(PRIVATE), "session", ConnectorSession.class);
        FieldDefinition selectedPositionsField = classDefinition.declareField(a(PRIVATE), "selectedPositions", SelectedPositions.class);
        FieldDefinition nextIndexOrPositionField = classDefinition.declareField(a(PRIVATE), "nextIndexOrPosition", int.class);
        FieldDefinition resultField = classDefinition.declareField(a(PRIVATE), "result", Block.class);

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        // process
        generateProcessMethod(classDefinition, blockBuilderField, sessionField, selectedPositionsField, nextIndexOrPositionField, resultField);

        // getResult
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "getResult", type(Object.class), ImmutableList.of());
        method.getBody().append(method.getThis().getField(resultField)).ret(Object.class);

        // evaluate
        Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(classDefinition, callSiteBinder, cachedInstanceBinder, projection);
        generateEvaluateMethod(classDefinition, callSiteBinder, cachedInstanceBinder, compiledLambdaMap, projection, blockBuilderField);

        // constructor
        Parameter blockBuilder = arg("blockBuilder", BlockBuilder.class);
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", SourcePage.class);
        Parameter selectedPositions = arg("selectedPositions", SelectedPositions.class);

        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), blockBuilder, session, page, selectedPositions);

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class)
                .append(thisVariable.setField(blockBuilderField, blockBuilder))
                .append(thisVariable.setField(sessionField, session))
                .append(thisVariable.setField(selectedPositionsField, selectedPositions))
                .append(thisVariable.setField(nextIndexOrPositionField, selectedPositions.invoke("getOffset", int.class)))
                .append(thisVariable.setField(resultField, constantNull(Block.class)));

        for (int channel : getInputChannels(projection)) {
            FieldDefinition blockField = classDefinition.declareField(a(PRIVATE, FINAL), "block_" + channel, Block.class);
            body.append(thisVariable.setField(blockField, page.invoke("getBlock", Block.class, constantInt(channel))));
        }

        cachedInstanceBinder.generateInitializations(thisVariable, body);
        body.ret();

        return classDefinition;
    }

    private static MethodDefinition generateProcessMethod(
            ClassDefinition classDefinition,
            FieldDefinition blockBuilder,
            FieldDefinition session,
            FieldDefinition selectedPositions,
            FieldDefinition nextIndexOrPosition,
            FieldDefinition result)
    {
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "process", type(boolean.class), ImmutableList.of());

        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        Variable from = scope.declareVariable("from", body, thisVariable.getField(nextIndexOrPosition));
        Variable to = scope.declareVariable("to", body, add(thisVariable.getField(selectedPositions).invoke("getOffset", int.class), thisVariable.getField(selectedPositions).invoke("size", int.class)));
        Variable positions = scope.declareVariable(int[].class, "positions");
        Variable index = scope.declareVariable(int.class, "index");

        IfStatement ifStatement = new IfStatement()
                .condition(thisVariable.getField(selectedPositions).invoke("isList", boolean.class));
        body.append(ifStatement);

        ifStatement.ifTrue(new BytecodeBlock()
                .append(positions.set(thisVariable.getField(selectedPositions).invoke("getPositions", int[].class)))
                .append(new ForLoop("positions loop")
                        .initialize(index.set(from))
                        .condition(lessThan(index, to))
                        .update(index.increment())
                        .body(new BytecodeBlock()
                                .append(thisVariable.invoke("evaluate", void.class, thisVariable.getField(session), positions.getElement(index))))));

        ifStatement.ifFalse(new ForLoop("range based loop")
                .initialize(index.set(from))
                .condition(lessThan(index, to))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(thisVariable.invoke("evaluate", void.class, thisVariable.getField(session), index))));

        body.comment("result = this.blockBuilder.build(); return true;")
                .append(thisVariable.setField(result, thisVariable.getField(blockBuilder).invoke("build", Block.class)))
                .push(true)
                .retBoolean();

        return method;
    }

    private MethodDefinition generateEvaluateMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            RowExpression projection,
            FieldDefinition blockBuilder)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter position = arg("position", int.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "evaluate",
                type(void.class),
                ImmutableList.<Parameter>builder()
                        .add(session)
                        .add(position)
                        .build());

        method.comment("Projection: %s", projection);

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        RowExpressionCompiler compiler = new RowExpressionCompiler(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompilerProjection(callSiteBinder),
                functionManager,
                compiledLambdaMap,
                ImmutableList.of(session, position));

        body.append(thisVariable.getField(blockBuilder))
                .append(compiler.compile(projection, scope))
                .append(generateWrite(callSiteBinder, scope, wasNullVariable, projection.type()))
                .ret();
        return method;
    }

    public Supplier<PageFilter> compileFilter(RowExpression filter, Optional<String> classNameSuffix)
    {
        if (filterCache == null) {
            return compileFilterInternal(filter, classNameSuffix);
        }
        try {
            return filterCache.getUnchecked(filter);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
    }

    private Supplier<PageFilter> compileFilterInternal(RowExpression filter, Optional<String> classNameSuffix)
    {
        requireNonNull(filter, "filter is null");

        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(filter);

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        ClassDefinition classDefinition = defineFilterClass(result.getRewrittenExpression(), result.getInputChannels(), callSiteBinder, classNameSuffix);

        Class<? extends PageFilter> functionClass;
        try {
            functionClass = defineClass(classDefinition, PageFilter.class, callSiteBinder.getBindings(), getClass().getClassLoader());
        }
        catch (Exception e) {
            if (Throwables.getRootCause(e) instanceof MethodTooLargeException) {
                throw new TrinoException(QUERY_EXCEEDED_COMPILER_LIMIT,
                        "Query exceeded maximum filters. Please reduce the number of filters referenced and re-run the query.", e);
            }
            throw new TrinoException(COMPILER_ERROR, filter.toString(), e.getCause());
        }

        return () -> {
            try {
                return functionClass.getConstructor().newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new TrinoException(COMPILER_ERROR, e);
            }
        };
    }

    private static ParameterizedType generateFilterClassName(Optional<String> classNameSuffix)
    {
        return makeClassName(PageFilter.class.getSimpleName(), classNameSuffix);
    }

    private ClassDefinition defineFilterClass(RowExpression filter, InputChannels inputChannels, CallSiteBinder callSiteBinder, Optional<String> classNameSuffix)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                generateFilterClassName(classNameSuffix),
                type(Object.class),
                type(PageFilter.class));

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(classDefinition, callSiteBinder, cachedInstanceBinder, filter);
        generateFilterMethod(classDefinition, callSiteBinder, cachedInstanceBinder, compiledLambdaMap, filter);

        FieldDefinition selectedPositions = classDefinition.declareField(a(PRIVATE), "selectedPositions", boolean[].class);
        generatePageFilterMethod(classDefinition, selectedPositions);

        // isDeterministic
        classDefinition.declareMethod(a(PUBLIC), "isDeterministic", type(boolean.class))
                .getBody()
                .append(constantBoolean(isDeterministic(filter)))
                .retBoolean();

        // getInputChannels
        classDefinition.declareMethod(a(PUBLIC), "getInputChannels", type(InputChannels.class))
                .getBody()
                .append(invoke(callSiteBinder.bind(inputChannels, InputChannels.class), "getInputChannels"))
                .retObject();

        // toString
        String toStringResult = toStringHelper(classDefinition.getType()
                .getJavaClassName())
                .add("filter", filter)
                .toString();
        classDefinition.declareMethod(a(PUBLIC), "toString", type(String.class))
                .getBody()
                // bind constant via invokedynamic to avoid constant pool issues due to large strings
                .append(invoke(callSiteBinder.bind(toStringResult, String.class), "toString"))
                .retObject();

        // constructor
        generateConstructor(classDefinition, cachedInstanceBinder, method -> {
            Variable thisVariable = method.getScope().getThis();
            method.getBody().append(thisVariable.setField(selectedPositions, newArray(type(boolean[].class), 0)));
        });

        return classDefinition;
    }

    private static MethodDefinition generatePageFilterMethod(ClassDefinition classDefinition, FieldDefinition selectedPositionsField)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", SourcePage.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filter",
                type(SelectedPositions.class),
                ImmutableList.<Parameter>builder()
                        .add(session)
                        .add(page)
                        .build());

        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        Variable positionCount = scope.declareVariable("positionCount", body, page.invoke("getPositionCount", int.class));

        body.append(new IfStatement("grow selectedPositions if necessary")
                .condition(lessThan(thisVariable.getField(selectedPositionsField).length(), positionCount))
                .ifTrue(thisVariable.setField(selectedPositionsField, newArray(type(boolean[].class), positionCount))));

        Variable selectedPositions = scope.declareVariable("selectedPositions", body, thisVariable.getField(selectedPositionsField));
        Variable position = scope.declareVariable(int.class, "position");
        body.append(new ForLoop()
                .initialize(position.set(constantInt(0)))
                .condition(lessThan(position, positionCount))
                .update(position.increment())
                .body(selectedPositions.setElement(position, thisVariable.invoke("filter", boolean.class, session, page, position))));

        body.append(invokeStatic(
                PageFilter.class,
                "positionsArrayToSelectedPositions",
                SelectedPositions.class,
                selectedPositions,
                positionCount)
                .ret());

        return method;
    }

    private MethodDefinition generateFilterMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            RowExpression filter)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", SourcePage.class);
        Parameter position = arg("position", int.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filter",
                type(boolean.class),
                ImmutableList.<Parameter>builder()
                        .add(session)
                        .add(page)
                        .add(position)
                        .build());

        method.comment("Filter: %s", filter);

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(filter, page, scope, body);

        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        RowExpressionCompiler compiler = new RowExpressionCompiler(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(callSiteBinder),
                functionManager,
                compiledLambdaMap,
                ImmutableList.of(page, position));

        Variable result = scope.declareVariable(boolean.class, "result");
        body.append(compiler.compile(filter, scope))
                // store result so we can check for null
                .putVariable(result)
                .append(and(not(wasNullVariable), result).ret());
        return method;
    }

    private Map<LambdaDefinitionExpression, CompiledLambda> generateMethodsForLambda(
            ClassDefinition containerClassDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpression expression)
    {
        Set<LambdaDefinitionExpression> lambdaExpressions = ImmutableSet.copyOf(extractLambdaExpressions(expression));
        ImmutableMap.Builder<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = ImmutableMap.builder();

        int counter = 0;
        for (LambdaDefinitionExpression lambdaExpression : lambdaExpressions) {
            CompiledLambda compiledLambda = LambdaBytecodeGenerator.preGenerateLambdaExpression(
                    lambdaExpression,
                    "lambda_" + counter,
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

    private static void generateConstructor(
            ClassDefinition classDefinition,
            CachedInstanceBinder cachedInstanceBinder,
            Consumer<MethodDefinition> additionalStatements)
    {
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        additionalStatements.accept(constructorDefinition);

        cachedInstanceBinder.generateInitializations(thisVariable, body);
        body.ret();
    }

    private static void declareBlockVariables(RowExpression expression, Parameter page, Scope scope, BytecodeBlock body)
    {
        for (int channel : getInputChannels(expression)) {
            scope.declareVariable("block_" + channel, body, page.invoke("getBlock", Block.class, constantInt(channel)));
        }
    }

    private static List<Integer> getInputChannels(Iterable<RowExpression> expressions)
    {
        TreeSet<Integer> channels = new TreeSet<>();
        for (RowExpression expression : Expressions.subExpressions(expressions)) {
            if (expression instanceof InputReferenceExpression) {
                channels.add(((InputReferenceExpression) expression).field());
            }
        }
        return ImmutableList.copyOf(channels);
    }

    private static List<Integer> getInputChannels(RowExpression expression)
    {
        return getInputChannels(ImmutableList.of(expression));
    }

    private static RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompilerProjection(CallSiteBinder callSiteBinder)
    {
        return new InputReferenceCompiler(
                (scope, field) -> scope.getThis().getField("block_" + field, Block.class),
                (scope, field) -> scope.getVariable("position"),
                callSiteBinder);
    }

    private static RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler(CallSiteBinder callSiteBinder)
    {
        return new InputReferenceCompiler(
                (scope, field) -> scope.getVariable("block_" + field),
                (scope, field) -> scope.getVariable("position"),
                callSiteBinder);
    }
}
