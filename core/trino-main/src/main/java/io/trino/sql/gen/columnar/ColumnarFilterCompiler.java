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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.expression.BytecodeExpressions;
import io.airlift.log.Logger;
import io.trino.cache.CacheStatsMBean;
import io.trino.cache.NonEvictableCache;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.project.InputChannels;
import io.trino.operator.project.PageFieldsToInputParametersRewriter;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.function.OperatorType;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.CompilerConfig;
import io.trino.sql.planner.Symbol;
import jakarta.annotation.Nullable;
import org.objectweb.asm.MethodTooLargeException;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
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
import static io.trino.spi.StandardErrorCode.QUERY_EXCEEDED_COMPILER_LIMIT;
import static io.trino.sql.gen.columnar.FilterEvaluator.isNotExpression;
import static io.trino.sql.gen.columnar.IsNotNullColumnarFilter.createIsNotNullColumnarFilter;
import static io.trino.sql.gen.columnar.IsNullColumnarFilter.createIsNullColumnarFilter;
import static io.trino.util.CompilerUtils.defineClass;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class ColumnarFilterCompiler
{
    private static final Logger log = Logger.get(ColumnarFilterCompiler.class);

    private final FunctionManager functionManager;
    private final Metadata metadata;
    // Optional is used to cache failure to generate filter for unsupported cases
    private final NonEvictableCache<Expression, Optional<Class<? extends ColumnarFilter>>> filterCache;
    private final CacheStatsMBean filterCacheStats;

    @Inject
    public ColumnarFilterCompiler(FunctionManager functionManager, Metadata metadata, CompilerConfig config)
    {
        this(functionManager, metadata, config.getExpressionCacheSize());
    }

    public ColumnarFilterCompiler(FunctionManager functionManager, Metadata metadata, int expressionCacheSize)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
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
    public CacheStatsMBean getFilterCache()
    {
        return filterCacheStats;
    }

    Metadata getMetadata()
    {
        return metadata;
    }

    public Optional<Supplier<ColumnarFilter>> generateFilter(Expression filter, Map<Symbol, Integer> layout)
    {
        // Compact the layout to consecutive indices (0, 1, 2, ...). The compiled filter uses these
        // compact indices to access blocks from the InputChannelsSourcePage, which translates
        // compact index back to the original page channel. This must be done before compilation
        // because operators like GREATER_THAN reorder arguments (e.g., to LESS_THAN with swapped args),
        // and the compiled filter must use indices consistent with the InputChannelsSourcePage.
        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(filter, layout);
        Map<Symbol, Integer> compactLayout = result.compactLayout();
        InputChannels inputChannels = result.inputChannels();

        Optional<Class<? extends ColumnarFilter>> filterClass;
        if (filterCache == null) {
            filterClass = generateFilterInternal(filter, compactLayout);
        }
        else {
            try {
                filterClass = filterCache.get(filter, () -> generateFilterInternal(filter, compactLayout));
            }
            catch (ExecutionException e) {
                throw new UncheckedExecutionException(e);
            }
        }

        if (filterClass.isEmpty()) {
            return Optional.empty();
        }
        Class<? extends ColumnarFilter> clazz = filterClass.get();
        return Optional.of(() -> {
            try {
                return clazz.getConstructor(InputChannels.class).newInstance(inputChannels);
            }
            catch (ReflectiveOperationException e) {
                throw new TrinoException(COMPILER_ERROR, e);
            }
        });
    }

    private Optional<Class<? extends ColumnarFilter>> generateFilterInternal(Expression filter, Map<Symbol, Integer> layout)
    {
        try {
            return switch (filter) {
                case Comparison comparison -> generateComparisonFilter(comparison, layout);
                case Call call -> {
                    if (isNotExpression(call)) {
                        // "not(is_null(reference))" is handled explicitly as it is easy.
                        // more generic cases like "not(equal(reference, constant))" are not handled yet
                        if (call.arguments().getFirst() instanceof IsNull isNull) {
                            yield Optional.of(createIsNotNullColumnarFilter(isNull));
                        }
                        yield Optional.empty();
                    }
                    yield Optional.of(new CallColumnarFilterGenerator(call.function(), call.arguments(), layout, functionManager).generateColumnarFilter());
                }
                case IsNull isNull -> Optional.of(createIsNullColumnarFilter(isNull));
                case In in -> Optional.of(new InColumnarFilterGenerator(in, layout, metadata, functionManager).generateColumnarFilter());
                case Between between -> Optional.of(new BetweenInlineColumnarFilterGenerator(between, layout, metadata, functionManager).generateColumnarFilter());
                default -> Optional.empty();
            };
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

    private Optional<Class<? extends ColumnarFilter>> generateComparisonFilter(Comparison comparison, Map<Symbol, Integer> layout)
    {
        Expression left = comparison.left();
        Expression right = comparison.right();

        return switch (comparison.operator()) {
            case NOT_EQUAL -> Optional.empty();
            case EQUAL ->
                    generateCallFilter(metadata.resolveOperator(OperatorType.EQUAL, ImmutableList.of(left.type(), right.type())), ImmutableList.of(left, right), layout);
            case LESS_THAN ->
                    generateCallFilter(metadata.resolveOperator(OperatorType.LESS_THAN, ImmutableList.of(left.type(), right.type())), ImmutableList.of(left, right), layout);
            case LESS_THAN_OR_EQUAL ->
                    generateCallFilter(metadata.resolveOperator(OperatorType.LESS_THAN_OR_EQUAL, ImmutableList.of(left.type(), right.type())), ImmutableList.of(left, right), layout);
            case GREATER_THAN ->
                    generateCallFilter(metadata.resolveOperator(OperatorType.LESS_THAN, ImmutableList.of(right.type(), left.type())), ImmutableList.of(right, left), layout);
            case GREATER_THAN_OR_EQUAL ->
                    generateCallFilter(metadata.resolveOperator(OperatorType.LESS_THAN_OR_EQUAL, ImmutableList.of(right.type(), left.type())), ImmutableList.of(right, left), layout);
            case IDENTICAL ->
                    generateCallFilter(metadata.resolveOperator(OperatorType.IDENTICAL, ImmutableList.of(left.type(), right.type())), ImmutableList.of(left, right), layout);
        };
    }

    private Optional<Class<? extends ColumnarFilter>> generateCallFilter(ResolvedFunction function, List<Expression> arguments, Map<Symbol, Integer> layout)
    {
        return Optional.of(new CallColumnarFilterGenerator(function, arguments, layout, functionManager).generateColumnarFilter());
    }

    static FieldDefinition generateGetInputChannels(ClassDefinition classDefinition)
    {
        FieldDefinition inputChannelsField = classDefinition.declareField(a(PRIVATE, FINAL), "inputChannels", InputChannels.class);

        MethodDefinition getInputChannelsMethod = classDefinition.declareMethod(a(PUBLIC), "getInputChannels", type(InputChannels.class));
        getInputChannelsMethod.getBody()
                .append(getInputChannelsMethod.getThis().getField(inputChannelsField))
                .retObject();

        return inputChannelsField;
    }

    static Class<? extends ColumnarFilter> createClassInstance(CallSiteBinder binder, ClassDefinition classDefinition)
    {
        try {
            return defineClass(classDefinition, ColumnarFilter.class, binder.getBindings(), ColumnarFilterCompiler.class.getClassLoader());
        }
        catch (Exception e) {
            if (Throwables.getRootCause(e) instanceof MethodTooLargeException) {
                throw new TrinoException(QUERY_EXCEEDED_COMPILER_LIMIT,
                        "Query exceeded maximum filters. Please reduce the number of filters referenced and re-run the query.", e);
            }
            throw new TrinoException(COMPILER_ERROR, e.getCause());
        }
    }

    static void declareBlockVariables(List<? extends Expression> expressions, Map<Symbol, Integer> layout, Parameter page, Scope scope, BytecodeBlock body)
    {
        Set<Integer> inputFields = new HashSet<>(); // There may be multiple References on the same input block
        for (Expression expression : expressions) {
            if (!(expression instanceof Reference reference)) {
                continue;
            }
            Integer field = layout.get(Symbol.from(reference));
            checkState(field != null, "Reference not in layout: %s", reference.name());
            if (inputFields.contains(field)) {
                continue;
            }
            scope.declareVariable(
                    "block_" + field,
                    body,
                    page.invoke(
                            "getBlock",
                            Block.class,
                            constantInt(field)));
            inputFields.add(field);
        }
    }

    static BytecodeExpression generateBlockMayHaveNull(List<? extends Expression> expressions, Map<Symbol, Integer> layout, Scope scope)
    {
        return generateBlockMayHaveNull(expressions, layout, nCopies(expressions.size(), false), scope);
    }

    static BytecodeExpression generateBlockMayHaveNull(List<? extends Expression> expressions, Map<Symbol, Integer> layout, List<Boolean> isNullableArgument, Scope scope)
    {
        checkArgument(
                expressions.size() == isNullableArgument.size(),
                "expressions size %s does not match isNullableArgument size %s",
                expressions.size(),
                isNullableArgument.size());
        BytecodeExpression mayHaveNull = constantFalse();
        for (int i = 0; i < expressions.size(); i++) {
            Expression expression = expressions.get(i);
            // Function with nullable argument should get adapted to a MethodHandle which returns false on NULL, so explicit isNull check isn't needed
            if (expression instanceof Reference reference && !isNullableArgument.get(i)) {
                Integer field = layout.get(Symbol.from(reference));
                checkState(field != null, "Reference not in layout: %s", reference.name());
                mayHaveNull = BytecodeExpressions.or(
                        mayHaveNull,
                        scope.getVariable("block_" + field).invoke("mayHaveNull", boolean.class));
            }
        }
        return mayHaveNull;
    }

    static BytecodeExpression generateBlockPositionNotNull(List<? extends Expression> expressions, Map<Symbol, Integer> layout, Scope scope, Variable position)
    {
        return generateBlockPositionNotNull(expressions, layout, nCopies(expressions.size(), false), scope, position);
    }

    static BytecodeExpression generateBlockPositionNotNull(List<? extends Expression> expressions, Map<Symbol, Integer> layout, List<Boolean> isNullableArgument, Scope scope, Variable position)
    {
        checkArgument(
                expressions.size() == isNullableArgument.size(),
                "expressions size %s does not match isNullableArgument size %s",
                expressions.size(),
                isNullableArgument.size());
        BytecodeExpression isNotNull = constantTrue();
        for (int i = 0; i < expressions.size(); i++) {
            Expression expression = expressions.get(i);
            // Function with nullable argument should get adapted to a MethodHandle which returns false on NULL, so explicit isNull check isn't needed
            if (expression instanceof Reference reference && !isNullableArgument.get(i)) {
                Integer field = layout.get(Symbol.from(reference));
                checkState(field != null, "Reference not in layout: %s", reference.name());
                isNotNull = BytecodeExpressions.and(
                        isNotNull,
                        BytecodeExpressions.not(scope.getVariable("block_" + field).invoke("isNull", boolean.class, position)));
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
