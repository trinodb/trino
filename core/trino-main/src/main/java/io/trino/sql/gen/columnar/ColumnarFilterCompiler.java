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

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.inject.Inject;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.expression.BytecodeExpressions;
import io.airlift.jmx.CacheStatsMBean;
import io.airlift.log.Logger;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.metadata.FunctionManager;
import io.trino.operator.project.InputChannels;
import io.trino.operator.project.PageFieldsToInputParametersRewriter;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.planner.CompilerConfig;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;
import jakarta.annotation.Nullable;
import org.objectweb.asm.MethodTooLargeException;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.operator.project.PageFieldsToInputParametersRewriter.rewritePageFieldsToInputParameters;
import static io.trino.spi.StandardErrorCode.COMPILER_ERROR;
import static io.trino.sql.gen.BytecodeUtils.invoke;
import static io.trino.sql.gen.columnar.ExpressionEvaluator.isNotExpression;
import static io.trino.sql.gen.columnar.IsolatedFilterFactory.createIsolatedIsNotNullColumnarFilter;
import static io.trino.sql.gen.columnar.IsolatedFilterFactory.createIsolatedIsNullColumnarFilter;
import static io.trino.sql.relational.SpecialForm.Form.BETWEEN;
import static io.trino.sql.relational.SpecialForm.Form.IN;
import static io.trino.sql.relational.SpecialForm.Form.IS_NULL;
import static io.trino.util.CompilerUtils.defineClass;
import static java.util.Objects.requireNonNull;

public class ColumnarFilterCompiler
{
    private static final Logger log = Logger.get(ColumnarFilterCompiler.class);

    private final FunctionManager functionManager;
    // Optional is used to cache failure to generate filter for unsupported cases
    private final NonEvictableLoadingCache<RowExpression, Optional<Supplier<ColumnarFilter>>> filterCache;
    private final CacheStatsMBean filterCacheStats;

    @Inject
    public ColumnarFilterCompiler(FunctionManager functionManager, CompilerConfig config)
    {
        this(functionManager, config.getExpressionCacheSize());
    }

    public ColumnarFilterCompiler(FunctionManager functionManager, int expressionCacheSize)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        if (expressionCacheSize > 0) {
            filterCache = buildNonEvictableCache(
                    CacheBuilder.newBuilder()
                            .recordStats()
                            .maximumSize(expressionCacheSize),
                    CacheLoader.from(this::generateFilterInternal));
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
    public CacheStatsMBean getFilterCache()
    {
        return filterCacheStats;
    }

    public Optional<Supplier<ColumnarFilter>> generateFilter(RowExpression filter)
    {
        if (filterCache == null) {
            return generateFilterInternal(filter);
        }
        return filterCache.getUnchecked(filter);
    }

    private Optional<Supplier<ColumnarFilter>> generateFilterInternal(RowExpression filter)
    {
        try {
            if (filter instanceof CallExpression callExpression) {
                if (isNotExpression(callExpression)) {
                    // "not(is_null(input_reference))" is handled explicitly as it is easy.
                    // more generic cases like "not(equal(input_reference, constant))" are not handled yet
                    if (callExpression.getArguments().get(0) instanceof SpecialForm specialForm && specialForm.getForm() == IS_NULL) {
                        return Optional.of(createIsolatedIsNotNullColumnarFilter(specialForm));
                    }
                    return Optional.empty();
                }
                return Optional.of(new CallColumnarFilterGenerator(callExpression, functionManager).generateColumnarFilter());
            }
            else if (filter instanceof SpecialForm specialForm) {
                if (specialForm.getForm() == IS_NULL) {
                    return Optional.of(createIsolatedIsNullColumnarFilter(specialForm));
                }
                if (specialForm.getForm() == IN) {
                    return Optional.of(new InColumnarFilterGenerator(specialForm, functionManager).generateColumnarFilter());
                }
                if (specialForm.getForm() == BETWEEN) {
                    return Optional.of(new BetweenInlineColumnarFilterGenerator(specialForm, functionManager).generateColumnarFilter());
                }
            }
            return Optional.empty();
        }
        catch (Throwable t) {
            if (t instanceof UnsupportedOperationException || t.getCause() instanceof UnsupportedOperationException) {
                log.debug("Unsupported filter for columnar evaluation %s, %s", filter, t);
            }
            else {
                log.warn("Failed to compile filter %s for columnar evaluation, %s", filter, t);
            }
            return Optional.empty();
        }
    }

    public static void generateConstructor(ClassDefinition classDefinition)
    {
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        body.ret();
    }

    public static void generateGetInputChannels(CallSiteBinder callSiteBinder, ClassDefinition classDefinition, RowExpression rowExpression)
    {
        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(rowExpression);

        // getInputChannels
        classDefinition.declareMethod(a(PUBLIC), "getInputChannels", type(InputChannels.class))
                .getBody()
                .append(invoke(callSiteBinder.bind(result.getInputChannels(), InputChannels.class), "getInputChannels"))
                .retObject();
    }

    public static Supplier<ColumnarFilter> createClassInstance(CallSiteBinder binder, ClassDefinition classDefinition)
    {
        Class<? extends ColumnarFilter> functionClass;
        try {
            functionClass = defineClass(classDefinition, ColumnarFilter.class, binder.getBindings(), ColumnarFilterCompiler.class.getClassLoader());
        }
        catch (Exception e) {
            if (Throwables.getRootCause(e) instanceof MethodTooLargeException) {
                throw new TrinoException(COMPILER_ERROR,
                        "Query exceeded maximum filters. Please reduce the number of filters referenced and re-run the query.", e);
            }
            throw new TrinoException(COMPILER_ERROR, e.getCause());
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

    public static void declareBlockVariables(List<RowExpression> rowExpressions, Parameter page, Scope scope, BytecodeBlock body)
    {
        int blocksCount = 0;
        AtomicInteger channel = new AtomicInteger();
        Map<Integer, Integer> fieldToChannel = new HashMap<>(); // There may be multiple InputReferenceExpression on the same input block
        for (RowExpression rowExpression : rowExpressions) {
            if (rowExpression instanceof InputReferenceExpression inputReference) {
                scope.declareVariable(
                        "block_" + blocksCount,
                        body,
                        page.invoke(
                                "getBlock",
                                Block.class,
                                constantInt(fieldToChannel.computeIfAbsent(inputReference.getField(), key -> channel.getAndIncrement()))));
                blocksCount++;
            }
        }
    }

    public static BytecodeExpression generateBlockMayHaveNull(List<RowExpression> rowExpressions, Scope scope)
    {
        BytecodeExpression mayHaveNull = constantFalse();
        int blocksCount = 0;
        for (RowExpression rowExpression : rowExpressions) {
            if (rowExpression instanceof InputReferenceExpression) {
                mayHaveNull = BytecodeExpressions.or(mayHaveNull, scope.getVariable("block_" + blocksCount).invoke("mayHaveNull", boolean.class));
                blocksCount++;
            }
        }
        return mayHaveNull;
    }

    public static BytecodeExpression generateBlockPositionNotNull(List<RowExpression> rowExpressions, Scope scope, Variable position)
    {
        BytecodeExpression isNotNull = constantTrue();
        int blocksCount = 0;
        for (RowExpression rowExpression : rowExpressions) {
            if (rowExpression instanceof InputReferenceExpression) {
                isNotNull = BytecodeExpressions.and(
                        isNotNull,
                        BytecodeExpressions.not(scope.getVariable("block_" + blocksCount).invoke("isNull", boolean.class, position)));
                blocksCount++;
            }
        }
        return isNotNull;
    }
}
