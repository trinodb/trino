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
import com.google.common.collect.ImmutableList;
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
import io.trino.cache.NonEvictableCache;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
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
import io.trino.spi.type.TypeManager;
import io.trino.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.CompilerConfig;
import io.trino.sql.planner.Symbol;
import jakarta.annotation.Nullable;
import org.objectweb.asm.MethodTooLargeException;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
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
import static io.trino.sql.gen.InputReferenceCompiler.generateInputReference;
import static io.trino.sql.gen.LambdaBytecodeGenerator.generateMethodsForLambda;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static io.trino.util.Reflection.constructorMethodHandle;
import static java.util.Objects.requireNonNull;

public class PageFunctionCompiler
{
    private final FunctionManager functionManager;
    private final Metadata metadata;
    private final TypeManager typeManager;

    private record CompiledProjection(MethodHandle constructor, boolean deterministic) {}

    private final NonEvictableCache<Expression, CompiledProjection> projectionCache;
    private final NonEvictableCache<Expression, Class<? extends PageFilter>> filterCache;

    private final CacheStatsMBean projectionCacheStats;
    private final CacheStatsMBean filterCacheStats;

    @Inject
    public PageFunctionCompiler(FunctionManager functionManager, Metadata metadata, TypeManager typeManager, CompilerConfig config)
    {
        this(functionManager, metadata, typeManager, config.getExpressionCacheSize());
    }

    public PageFunctionCompiler(FunctionManager functionManager, Metadata metadata, TypeManager typeManager, int expressionCacheSize)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        if (expressionCacheSize > 0) {
            projectionCache = buildNonEvictableCache(
                    CacheBuilder.newBuilder()
                            .recordStats()
                            .maximumSize(expressionCacheSize));
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
                            .maximumSize(expressionCacheSize));
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

    public Supplier<PageProjection> compileProjection(Expression projection, Map<Symbol, Integer> layout, Optional<String> classNameSuffix)
    {
        requireNonNull(projection, "projection is null");

        if (projection instanceof Reference reference) {
            Integer channel = layout.get(Symbol.from(reference));
            if (channel != null) {
                InputPageProjection projectionFunction = new InputPageProjection(channel);
                return () -> projectionFunction;
            }
        }

        if (projection instanceof Constant constant) {
            ConstantPageProjection projectionFunction = new ConstantPageProjection(constant.value(), constant.type());
            return () -> projectionFunction;
        }

        CompiledProjection compiled;
        try {
            if (projectionCache == null) {
                compiled = compileProjectionClass(projection, layout, classNameSuffix);
            }
            else {
                compiled = projectionCache.get(projection, () -> compileProjectionClass(projection, layout, Optional.empty()));
            }
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new UncheckedExecutionException(e);
        }

        // Always compute InputChannels from the caller's layout
        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(projection, layout);
        MethodHandle constructor = compiled.constructor();
        boolean deterministic = compiled.deterministic();
        return () -> new GeneratedPageProjection(projection, deterministic, result.inputChannels(), constructor);
    }

    private CompiledProjection compileProjectionClass(Expression projection, Map<Symbol, Integer> layout, Optional<String> classNameSuffix)
    {
        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(projection, layout);

        Class<?> pageProjectionWorkClass;
        try {
            CallSiteBinder callSiteBinder = new CallSiteBinder();
            ClassDefinition pageProjectionWorkDefinition = definePageProjectWorkClass(projection, result.compactLayout(), callSiteBinder, classNameSuffix);
            pageProjectionWorkClass = defineClass(pageProjectionWorkDefinition, PageProjectionWork.class, callSiteBinder.getBindings(), getClass().getClassLoader());
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            if (Throwables.getRootCause(e) instanceof MethodTooLargeException) {
                throw new TrinoException(QUERY_EXCEEDED_COMPILER_LIMIT,
                        "Failed to execute query; there may be too many columns used or expressions are too complex", e);
            }
            throw new TrinoException(COMPILER_ERROR, e);
        }

        return new CompiledProjection(
                constructorMethodHandle(pageProjectionWorkClass, BlockBuilder.class, ConnectorSession.class, SourcePage.class, SelectedPositions.class),
                isDeterministic(projection));
    }

    private static ParameterizedType generateProjectionWorkClassName(Optional<String> classNameSuffix)
    {
        return makeClassName("PageProjectionWork", classNameSuffix);
    }

    private ClassDefinition definePageProjectWorkClass(Expression projection, Map<Symbol, Integer> compactLayout, CallSiteBinder callSiteBinder, Optional<String> classNameSuffix)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                generateProjectionWorkClassName(classNameSuffix),
                type(Object.class),
                type(PageProjectionWork.class));

        FieldDefinition blockBuilderField = classDefinition.declareField(a(PRIVATE), "blockBuilder", BlockBuilder.class);
        FieldDefinition sessionField = classDefinition.declareField(a(PRIVATE), "session", ConnectorSession.class);
        FieldDefinition selectedPositionsField = classDefinition.declareField(a(PRIVATE), "selectedPositions", SelectedPositions.class);

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        // process
        generateProcessMethod(classDefinition, blockBuilderField, sessionField, selectedPositionsField);

        // evaluate
        Map<Lambda, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(classDefinition, callSiteBinder, cachedInstanceBinder, projection, functionManager, metadata, typeManager);
        generateEvaluateMethod(classDefinition, callSiteBinder, cachedInstanceBinder, compiledLambdaMap, projection, compactLayout, blockBuilderField);

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
                .append(thisVariable.setField(selectedPositionsField, selectedPositions));

        for (int channel : getInputChannels(projection, compactLayout)) {
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
            FieldDefinition selectedPositions)
    {
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "process", type(Block.class), ImmutableList.of());

        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        Variable from = scope.declareVariable("from", body, thisVariable.getField(selectedPositions).invoke("getOffset", int.class));
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

        body.comment("return this.blockBuilder.build();")
                .append(thisVariable.getField(blockBuilder).invoke("build", Block.class))
                .retObject();

        return method;
    }

    private MethodDefinition generateEvaluateMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            Map<Lambda, CompiledLambda> compiledLambdaMap,
            Expression projection,
            Map<Symbol, Integer> compactLayout,
            FieldDefinition blockBuilder)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter position = arg("position", int.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PRIVATE),
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
        ExpressionBytecodeCompiler compiler = new ExpressionBytecodeCompiler(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompilerProjection(compactLayout, callSiteBinder),
                functionManager,
                metadata,
                typeManager,
                compiledLambdaMap,
                ImmutableList.of(session, position));

        body.append(thisVariable.getField(blockBuilder))
                .append(compiler.compile(projection, scope))
                .append(generateWrite(callSiteBinder, scope, wasNullVariable, projection.type()))
                .ret();
        return method;
    }

    public Supplier<PageFilter> compileFilter(Expression filter, Map<Symbol, Integer> layout, Optional<String> classNameSuffix)
    {
        requireNonNull(filter, "filter is null");

        Class<? extends PageFilter> filterClass;
        try {
            if (filterCache == null) {
                filterClass = compileFilterClass(filter, layout, classNameSuffix);
            }
            else {
                filterClass = filterCache.get(filter, () -> compileFilterClass(filter, layout, Optional.empty()));
            }
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new UncheckedExecutionException(e);
        }

        // Always compute InputChannels from the caller's layout
        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(filter, layout);
        InputChannels inputChannels = result.inputChannels();
        return () -> {
            try {
                return filterClass.getConstructor(InputChannels.class).newInstance(inputChannels);
            }
            catch (ReflectiveOperationException e) {
                throw new TrinoException(COMPILER_ERROR, e);
            }
        };
    }

    private Class<? extends PageFilter> compileFilterClass(Expression filter, Map<Symbol, Integer> layout, Optional<String> classNameSuffix)
    {
        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(filter, layout);

        try {
            CallSiteBinder callSiteBinder = new CallSiteBinder();
            ClassDefinition classDefinition = defineFilterClass(filter, result.compactLayout(), callSiteBinder, classNameSuffix);
            return defineClass(classDefinition, PageFilter.class, callSiteBinder.getBindings(), getClass().getClassLoader());
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            if (Throwables.getRootCause(e) instanceof MethodTooLargeException) {
                throw new TrinoException(QUERY_EXCEEDED_COMPILER_LIMIT,
                        "Query exceeded maximum filters. Please reduce the number of filters referenced and re-run the query.", e);
            }
            throw new TrinoException(COMPILER_ERROR, filter.toString(), e.getCause());
        }
    }

    private static ParameterizedType generateFilterClassName(Optional<String> classNameSuffix)
    {
        return makeClassName(PageFilter.class.getSimpleName(), classNameSuffix);
    }

    private ClassDefinition defineFilterClass(Expression filter, Map<Symbol, Integer> compactLayout, CallSiteBinder callSiteBinder, Optional<String> classNameSuffix)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                generateFilterClassName(classNameSuffix),
                type(Object.class),
                type(PageFilter.class));

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        FieldDefinition inputChannelsField = classDefinition.declareField(a(PRIVATE, FINAL), "inputChannels", InputChannels.class);

        Map<Lambda, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(classDefinition, callSiteBinder, cachedInstanceBinder, filter, functionManager, metadata, typeManager);
        generateFilterMethod(classDefinition, callSiteBinder, cachedInstanceBinder, compiledLambdaMap, filter, compactLayout);

        FieldDefinition selectedPositions = classDefinition.declareField(a(PRIVATE), "selectedPositions", boolean[].class);
        generatePageFilterMethod(classDefinition, selectedPositions);

        // isDeterministic — baked into bytecode since it depends only on the expression, not the layout
        classDefinition.declareMethod(a(PUBLIC), "isDeterministic", type(boolean.class))
                .getBody()
                .append(constantBoolean(isDeterministic(filter)))
                .retBoolean();

        // getInputChannels
        MethodDefinition getInputChannelsMethod = classDefinition.declareMethod(a(PUBLIC), "getInputChannels", type(InputChannels.class));
        getInputChannelsMethod.getBody()
                .append(getInputChannelsMethod.getThis().getField(inputChannelsField))
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

        // constructor(InputChannels inputChannels)
        Parameter inputChannelsParam = arg("inputChannels", InputChannels.class);

        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), inputChannelsParam);
        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        body.append(thisVariable.setField(inputChannelsField, inputChannelsParam));
        body.append(thisVariable.setField(selectedPositions, newArray(type(boolean[].class), 0)));

        cachedInstanceBinder.generateInitializations(thisVariable, body);
        body.ret();

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
            Map<Lambda, CompiledLambda> compiledLambdaMap,
            Expression filter,
            Map<Symbol, Integer> compactLayout)
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

        declareBlockVariables(filter, compactLayout, page, scope, body);

        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        ExpressionBytecodeCompiler compiler = new ExpressionBytecodeCompiler(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(compactLayout, callSiteBinder),
                functionManager,
                metadata,
                typeManager,
                compiledLambdaMap,
                ImmutableList.of(page, position));

        Variable result = scope.declareVariable(boolean.class, "result");
        body.append(compiler.compile(filter, scope))
                // store result so we can check for null
                .putVariable(result)
                .append(and(not(wasNullVariable), result).ret());
        return method;
    }

    private static void declareBlockVariables(Expression expression, Map<Symbol, Integer> compactLayout, Parameter page, Scope scope, BytecodeBlock body)
    {
        for (int channel : getInputChannels(expression, compactLayout)) {
            scope.declareVariable("block_" + channel, body, page.invoke("getBlock", Block.class, constantInt(channel)));
        }
    }

    private static Set<Integer> getInputChannels(Expression expression, Map<Symbol, Integer> compactLayout)
    {
        Set<Integer> channels = new TreeSet<>();
        collectChannels(expression, compactLayout, channels);
        return channels;
    }

    private static void collectChannels(Expression expression, Map<Symbol, Integer> compactLayout, Set<Integer> channels)
    {
        if (expression instanceof Reference reference) {
            Integer channel = compactLayout.get(Symbol.from(reference));
            if (channel != null) {
                channels.add(channel);
            }
            return;
        }
        for (Expression child : expression.children()) {
            collectChannels(child, compactLayout, channels);
        }
    }

    private static BiFunction<Reference, Scope, BytecodeNode> fieldReferenceCompilerProjection(Map<Symbol, Integer> compactLayout, CallSiteBinder callSiteBinder)
    {
        return (reference, scope) -> {
            int field = compactLayout.get(Symbol.from(reference));
            return generateInputReference(callSiteBinder, scope, reference.type(),
                    scope.getThis().getField("block_" + field, Block.class),
                    scope.getVariable("position"));
        };
    }

    private static BiFunction<Reference, Scope, BytecodeNode> fieldReferenceCompiler(Map<Symbol, Integer> compactLayout, CallSiteBinder callSiteBinder)
    {
        return (reference, scope) -> {
            int field = compactLayout.get(Symbol.from(reference));
            return generateInputReference(callSiteBinder, scope, reference.type(),
                    scope.getVariable("block_" + field),
                    scope.getVariable("position"));
        };
    }
}
