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
import com.google.common.collect.ImmutableList;
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
import io.trino.cache.CacheStatsMBean;
import io.trino.cache.NonEvictableCache;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.operator.join.InternalJoinFilterFunction;
import io.trino.operator.join.JoinFilterFunction;
import io.trino.operator.join.RuntimeConstraintComparison;
import io.trino.operator.join.StandardJoinFilterFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.type.CharVarcharCoercion;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.gen.BytecodeUtils.invoke;
import static io.trino.sql.gen.InputReferenceCompiler.generateInputReference;
import static io.trino.sql.gen.LambdaBytecodeGenerator.generateMethodsForLambda;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.util.CompilerUtils.defineHiddenClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.util.Objects.requireNonNull;

public class JoinFilterFunctionCompiler
{
    private static final Set<ComparisonOperator> SUPPORTED_RUNTIME_CONSTRAINT_COMPARISONS = Set.of(EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL);
    private final FunctionManager functionManager;
    private final Metadata metadata;
    private final TypeManager typeManager;
    private final NonEvictableCache<JoinFilterCacheKey, JoinFilterFunctionFactory> joinFilterFunctionFactories;

    @Inject
    public JoinFilterFunctionCompiler(FunctionManager functionManager, Metadata metadata, TypeManager typeManager)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.joinFilterFunctionFactories = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .recordStats()
                        .maximumSize(1000));
    }

    @Managed
    @Nested
    public CacheStatsMBean getJoinFilterFunctionFactoryStats()
    {
        return new CacheStatsMBean(joinFilterFunctionFactories);
    }

    public JoinFilterFunctionFactory compileJoinFilterFunction(Expression filter, Map<Symbol, Integer> layout, int leftBlocksSize, CharVarcharCoercion charVarcharCoercion)
    {
        try {
            return joinFilterFunctionFactories.get(
                    new JoinFilterCacheKey(canonicalizeReferences(filter, layout), leftBlocksSize, charVarcharCoercion),
                    () -> internalCompileFilterFunctionFactory(filter, layout, leftBlocksSize, charVarcharCoercion));
        }
        catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    private JoinFilterFunctionFactory internalCompileFilterFunctionFactory(Expression filterExpression, Map<Symbol, Integer> layout, int leftBlocksSize, CharVarcharCoercion charVarcharCoercion)
    {
        Class<? extends InternalJoinFilterFunction> internalJoinFilterFunction = compileInternalJoinFilterFunction(filterExpression, layout, leftBlocksSize, charVarcharCoercion);
        return new IsolatedJoinFilterFunctionFactory(
                internalJoinFilterFunction,
                extractRuntimeConstraintComparisons(filterExpression, layout, leftBlocksSize));
    }

    private static List<RuntimeConstraintComparison> extractRuntimeConstraintComparisons(Expression filter, Map<Symbol, Integer> layout, int buildChannelCount)
    {
        ImmutableList.Builder<RuntimeConstraintComparison> comparisons = ImmutableList.builder();
        for (Expression conjunct : extractConjuncts(filter)) {
            Comparison comparison = matchComparison(conjunct);
            if (comparison == null) {
                continue;
            }
            ComparisonOperator operator = comparison.operator();
            boolean nullAllowed = operator == IDENTICAL;
            if (nullAllowed) {
                if (comparison.left().type().equals(REAL) || comparison.right().type().equals(REAL) || comparison.left().type().equals(DOUBLE) || comparison.right().type().equals(DOUBLE)) {
                    continue;
                }
                operator = EQUAL;
            }
            else if (!SUPPORTED_RUNTIME_CONSTRAINT_COMPARISONS.contains(operator)) {
                continue;
            }

            Optional<Reference> leftBuild = buildReference(comparison.left(), layout, buildChannelCount);
            Optional<Reference> rightBuild = buildReference(comparison.right(), layout, buildChannelCount);
            Optional<Reference> leftProbe = probeReference(comparison.left(), layout, buildChannelCount);
            Optional<Reference> rightProbe = probeReference(comparison.right(), layout, buildChannelCount);
            Reference build;
            Reference probe;
            if (leftBuild.isPresent() && rightProbe.isPresent()) {
                build = leftBuild.orElseThrow();
                probe = rightProbe.orElseThrow();
                operator = operator.flip();
            }
            else if (rightBuild.isPresent() && leftProbe.isPresent()) {
                build = rightBuild.orElseThrow();
                probe = leftProbe.orElseThrow();
            }
            else {
                continue;
            }
            comparisons.add(new RuntimeConstraintComparison(
                    layout.get(Symbol.from(build)),
                    layout.get(Symbol.from(probe)) - buildChannelCount,
                    operator,
                    nullAllowed,
                    probe.type()));
        }
        return comparisons.build();
    }

    private static Optional<Reference> buildReference(Expression expression, Map<Symbol, Integer> layout, int buildChannelCount)
    {
        if (expression instanceof Reference reference && layout.get(Symbol.from(reference)) < buildChannelCount) {
            return Optional.of(reference);
        }
        return Optional.empty();
    }

    private static Optional<Reference> probeReference(Expression expression, Map<Symbol, Integer> layout, int buildChannelCount)
    {
        Reference reference = switch (expression) {
            case Reference value -> value;
            case Cast(Reference value, _, _) -> value;
            default -> null;
        };
        if (reference != null && layout.get(Symbol.from(reference)) >= buildChannelCount) {
            return Optional.of(reference);
        }
        return Optional.empty();
    }

    private Class<? extends InternalJoinFilterFunction> compileInternalJoinFilterFunction(Expression filterExpression, Map<Symbol, Integer> layout, int leftBlocksSize, CharVarcharCoercion charVarcharCoercion)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("JoinFilterFunction"),
                type(Object.class),
                type(InternalJoinFilterFunction.class));

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        new JoinFilterFunctionCompiler(functionManager, metadata, typeManager)
                .generateMethods(classDefinition, callSiteBinder, filterExpression, layout, leftBlocksSize, charVarcharCoercion);

        //
        // toString method
        //
        generateToString(
                classDefinition,
                callSiteBinder,
                toStringHelper(classDefinition.getType().getJavaClassName())
                        .add("filter", filterExpression)
                        .add("leftBlocksSize", leftBlocksSize)
                        .toString());

        return defineHiddenClass(classDefinition, InternalJoinFilterFunction.class, callSiteBinder.getClassData());
    }

    private void generateMethods(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, Expression filter, Map<Symbol, Integer> layout, int leftBlocksSize, CharVarcharCoercion charVarcharCoercion)
    {
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        FieldDefinition sessionField = classDefinition.declareField(a(PRIVATE, FINAL), "session", ConnectorSession.class);

        Map<Lambda, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(classDefinition, callSiteBinder, cachedInstanceBinder, filter, functionManager, metadata, typeManager, charVarcharCoercion);
        generateFilterMethod(classDefinition, callSiteBinder, cachedInstanceBinder, compiledLambdaMap, filter, layout, leftBlocksSize, charVarcharCoercion, sessionField);

        generateConstructor(classDefinition, sessionField, cachedInstanceBinder);
    }

    private static void generateConstructor(
            ClassDefinition classDefinition,
            FieldDefinition sessionField,
            CachedInstanceBinder cachedInstanceBinder)
    {
        Parameter sessionParameter = arg("session", ConnectorSession.class);
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), sessionParameter);

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        body.append(thisVariable.setField(sessionField, sessionParameter));
        cachedInstanceBinder.generateInitializations(thisVariable, body);
        body.ret();
    }

    private void generateFilterMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            Map<Lambda, CompiledLambda> compiledLambdaMap,
            Expression filter,
            Map<Symbol, Integer> layout,
            int leftBlocksSize,
            CharVarcharCoercion charVarcharCoercion,
            FieldDefinition sessionField)
    {
        // int leftPosition, Page leftPage, int rightPosition, Page rightPage
        Parameter leftPosition = arg("leftPosition", int.class);
        Parameter leftPage = arg("leftPage", Page.class);
        Parameter rightPosition = arg("rightPosition", int.class);
        Parameter rightPage = arg("rightPage", Page.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filter",
                type(boolean.class),
                ImmutableList.<Parameter>builder()
                        .add(leftPosition)
                        .add(leftPage)
                        .add(rightPosition)
                        .add(rightPage)
                        .build());

        method.comment("filter: %s", filter.toString());
        BytecodeBlock body = method.getBody();

        Scope scope = method.getScope();
        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        scope.declareVariable("session", body, method.getThis().getField(sessionField));

        BiFunction<Reference, Scope, BytecodeNode> referenceCompiler = fieldReferenceCompiler(typeManager.getTypeOperators(), callSiteBinder, layout, leftPosition, leftPage, rightPosition, rightPage, leftBlocksSize);

        ExpressionBytecodeCompiler compiler = new ExpressionBytecodeCompiler(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                referenceCompiler,
                functionManager,
                metadata,
                typeManager,
                charVarcharCoercion,
                compiledLambdaMap,
                ImmutableList.of(leftPage, leftPosition, rightPage, rightPosition));

        BytecodeNode visitorBody = compiler.compile(filter, scope);

        Variable result = scope.declareVariable(boolean.class, "result");
        body.append(visitorBody)
                .putVariable(result)
                .append(new IfStatement()
                        .condition(wasNullVariable)
                        .ifTrue(constantFalse().ret())
                        .ifFalse(result.ret()));
    }

    private static void generateToString(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, String string)
    {
        // bind constant via invokedynamic to avoid constant pool issues due to large strings
        classDefinition.declareMethod(a(PUBLIC), "toString", type(String.class))
                .getBody()
                .append(invoke(callSiteBinder.bind(string, String.class), "toString"))
                .retObject();
    }

    public interface JoinFilterFunctionFactory
    {
        JoinFilterFunction create(ConnectorSession session, LongArrayList addresses, List<Page> pages);

        default List<RuntimeConstraintComparison> getRuntimeConstraintComparisons()
        {
            return ImmutableList.of();
        }
    }

    private static BiFunction<Reference, Scope, BytecodeNode> fieldReferenceCompiler(
            TypeOperators typeOperators,
            CallSiteBinder callSiteBinder,
            Map<Symbol, Integer> layout,
            Variable leftPosition,
            Variable leftPage,
            Variable rightPosition,
            Variable rightPage,
            int leftBlocksSize)
    {
        return (reference, scope) -> {
            Integer field = layout.get(Symbol.from(reference));
            if (field == null) {
                throw new UnsupportedOperationException("Reference not found in join layout: " + reference.name());
            }
            if (field < leftBlocksSize) {
                return generateInputReference(
                        typeOperators,
                        callSiteBinder,
                        scope,
                        reference.type(),
                        leftPage.invoke("getBlock", Block.class, constantInt(field)),
                        leftPosition);
            }
            return generateInputReference(
                    typeOperators,
                    callSiteBinder,
                    scope,
                    reference.type(),
                    rightPage.invoke("getBlock", Block.class, constantInt(field - leftBlocksSize)),
                    rightPosition);
        };
    }

    /**
     * Replaces Reference names with their layout field positions so that expressions
     * with different symbol names but identical field positions share a cache entry.
     * This matches the old RowExpression behavior where InputReferenceExpression
     * used field indices, not names.
     */
    private static Expression canonicalizeReferences(Expression expression, Map<Symbol, Integer> layout)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteReference(Reference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Integer field = layout.get(Symbol.from(node));
                if (field != null) {
                    return new Reference(node.type(), String.valueOf(field));
                }
                return node;
            }
        }, expression);
    }

    private record JoinFilterCacheKey(Expression filter, int leftBlocksSize, CharVarcharCoercion charVarcharCoercion)
    {
        JoinFilterCacheKey
        {
            requireNonNull(filter, "filter is null");
            requireNonNull(charVarcharCoercion, "charVarcharCoercion is null");
        }
    }

    private static class IsolatedJoinFilterFunctionFactory
            implements JoinFilterFunctionFactory
    {
        private final Constructor<? extends InternalJoinFilterFunction> internalJoinFilterFunctionConstructor;
        private final Constructor<? extends JoinFilterFunction> isolatedJoinFilterFunctionConstructor;
        private final List<RuntimeConstraintComparison> runtimeConstraintComparisons;

        public IsolatedJoinFilterFunctionFactory(
                Class<? extends InternalJoinFilterFunction> internalJoinFilterFunction,
                List<RuntimeConstraintComparison> runtimeConstraintComparisons)
        {
            this.runtimeConstraintComparisons = ImmutableList.copyOf(requireNonNull(runtimeConstraintComparisons, "runtimeConstraintComparisons is null"));
            try {
                internalJoinFilterFunctionConstructor = internalJoinFilterFunction
                        .getConstructor(ConnectorSession.class);

                Class<? extends JoinFilterFunction> isolatedJoinFilterFunction = IsolatedClass.isolateClass(
                        new DynamicClassLoader(getClass().getClassLoader()),
                        JoinFilterFunction.class,
                        StandardJoinFilterFunction.class);
                isolatedJoinFilterFunctionConstructor = isolatedJoinFilterFunction.getConstructor(InternalJoinFilterFunction.class, LongArrayList.class, List.class);
            }
            catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public JoinFilterFunction create(ConnectorSession session, LongArrayList addresses, List<Page> pages)
        {
            try {
                InternalJoinFilterFunction internalJoinFilterFunction = internalJoinFilterFunctionConstructor.newInstance(session);
                return isolatedJoinFilterFunctionConstructor.newInstance(internalJoinFilterFunction, addresses, pages);
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public List<RuntimeConstraintComparison> getRuntimeConstraintComparisons()
        {
            return runtimeConstraintComparisons;
        }
    }
}
