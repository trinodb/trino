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
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.expression.BytecodeExpressions;
import io.airlift.log.Logger;
import io.trino.cache.CacheStatsMBean;
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

import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.expression.BytecodeExpressions.inlineIf;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.operator.project.PageFieldsToInputParametersRewriter.rewritePageFieldsToInputParameters;
import static io.trino.spi.StandardErrorCode.COMPILER_ERROR;
import static io.trino.sql.gen.BytecodeUtils.invoke;
import static io.trino.sql.gen.columnar.FilterEvaluator.isNotExpression;
import static io.trino.sql.gen.columnar.IsNotNullColumnarFilter.createIsNotNullColumnarFilter;
import static io.trino.sql.gen.columnar.IsNullColumnarFilter.createIsNullColumnarFilter;
import static io.trino.sql.relational.SpecialForm.Form.BETWEEN;
import static io.trino.sql.relational.SpecialForm.Form.IN;
import static io.trino.sql.relational.SpecialForm.Form.IS_NULL;
import static io.trino.util.CompilerUtils.defineClass;
import static java.util.Collections.nCopies;
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
                    if (callExpression.arguments().getFirst() instanceof SpecialForm specialForm && specialForm.form() == IS_NULL) {
                        return Optional.of(createIsNotNullColumnarFilter(specialForm));
                    }
                    return Optional.empty();
                }
                return Optional.of(new CallColumnarFilterGenerator(callExpression, functionManager).generateColumnarFilter());
            }
            else if (filter instanceof SpecialForm specialForm) {
                if (specialForm.form() == IS_NULL) {
                    return Optional.of(createIsNullColumnarFilter(specialForm));
                }
                if (specialForm.form() == IN) {
                    return Optional.of(new InColumnarFilterGenerator(specialForm, functionManager).generateColumnarFilter());
                }
                if (specialForm.form() == BETWEEN) {
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

    static void generateGetInputChannels(CallSiteBinder callSiteBinder, ClassDefinition classDefinition, RowExpression rowExpression)
    {
        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(rowExpression);

        // getInputChannels
        classDefinition.declareMethod(a(PUBLIC), "getInputChannels", type(InputChannels.class))
                .getBody()
                .append(invoke(callSiteBinder.bind(result.getInputChannels(), InputChannels.class), "getInputChannels"))
                .retObject();
    }

    static Supplier<ColumnarFilter> createClassInstance(CallSiteBinder binder, ClassDefinition classDefinition)
    {
        Constructor<? extends ColumnarFilter> filterConstructor;
        try {
            Class<? extends ColumnarFilter> functionClass = defineClass(classDefinition, ColumnarFilter.class, binder.getBindings(), ColumnarFilterCompiler.class.getClassLoader());
            filterConstructor = functionClass.getConstructor();
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
                return filterConstructor.newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new TrinoException(COMPILER_ERROR, e);
            }
        };
    }

    static void declareBlockVariables(List<RowExpression> rowExpressions, Parameter page, Scope scope, BytecodeBlock body)
    {
        int channel = 0;
        Set<Integer> inputFields = new HashSet<>(); // There may be multiple InputReferenceExpression on the same input block
        for (RowExpression rowExpression : rowExpressions) {
            if (!(rowExpression instanceof InputReferenceExpression inputReference)) {
                continue;
            }
            if (inputFields.contains(inputReference.field())) {
                continue;
            }
            scope.declareVariable(
                    "block_" + inputReference.field(),
                    body,
                    page.invoke(
                            "getBlock",
                            Block.class,
                            constantInt(channel)));
            inputFields.add(inputReference.field());
            channel++;
        }
    }

    static BytecodeExpression generateBlockMayHaveNull(List<RowExpression> rowExpressions, Scope scope)
    {
        return generateBlockMayHaveNull(rowExpressions, nCopies(rowExpressions.size(), false), scope);
    }

    static BytecodeExpression generateBlockMayHaveNull(List<RowExpression> rowExpressions, List<Boolean> isNullableArgument, Scope scope)
    {
        checkArgument(
                rowExpressions.size() == isNullableArgument.size(),
                "rowExpressions size %s does not match isNullableArgument size %s",
                rowExpressions.size(),
                isNullableArgument.size());
        BytecodeExpression mayHaveNull = constantFalse();
        for (int i = 0; i < rowExpressions.size(); i++) {
            RowExpression rowExpression = rowExpressions.get(i);
            // Function with nullable argument should get adapted to a MethodHandle which returns false on NULL, so explicit isNull check isn't needed
            if (rowExpression instanceof InputReferenceExpression inputReference && !isNullableArgument.get(i)) {
                mayHaveNull = BytecodeExpressions.or(
                        mayHaveNull,
                        scope.getVariable("block_" + inputReference.field()).invoke("mayHaveNull", boolean.class));
            }
        }
        return mayHaveNull;
    }

    static BytecodeExpression generateBlockPositionNotNull(List<RowExpression> rowExpressions, Scope scope, Variable position)
    {
        return generateBlockPositionNotNull(rowExpressions, nCopies(rowExpressions.size(), false), scope, position);
    }

    static BytecodeExpression generateBlockPositionNotNull(List<RowExpression> rowExpressions, List<Boolean> isNullableArgument, Scope scope, Variable position)
    {
        checkArgument(
                rowExpressions.size() == isNullableArgument.size(),
                "rowExpressions size %s does not match isNullableArgument size %s",
                rowExpressions.size(),
                isNullableArgument.size());
        BytecodeExpression isNotNull = constantTrue();
        for (int i = 0; i < rowExpressions.size(); i++) {
            RowExpression rowExpression = rowExpressions.get(i);
            // Function with nullable argument should get adapted to a MethodHandle which returns false on NULL, so explicit isNull check isn't needed
            if (rowExpression instanceof InputReferenceExpression inputReference && !isNullableArgument.get(i)) {
                isNotNull = BytecodeExpressions.and(
                        isNotNull,
                        BytecodeExpressions.not(scope.getVariable("block_" + inputReference.field()).invoke("isNull", boolean.class, position)));
            }
        }
        return isNotNull;
    }

    static BytecodeBlock updateOutputPositions(Variable result, Variable position, Parameter outputPositions, Variable outputPositionsCount)
    {
        return new BytecodeBlock()
                .append(outputPositions.setElement(outputPositionsCount, position))
                .append(outputPositionsCount.set(
                        add(outputPositionsCount, inlineIf(result, constantInt(1), constantInt(0)))));
    }
}
