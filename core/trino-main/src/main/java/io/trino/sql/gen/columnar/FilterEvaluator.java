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
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.DeterminismEvaluator;
import io.trino.sql.planner.Symbol;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.sql.gen.columnar.AndFilterEvaluator.createAndExpressionEvaluator;
import static io.trino.sql.gen.columnar.DynamicPageFilter.DynamicFilterEvaluator;
import static io.trino.sql.gen.columnar.OrFilterEvaluator.createOrExpressionEvaluator;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.type.UnknownType.UNKNOWN;

/**
 * Used by PageProcessor to evaluate filter expression on input Page.
 * <p>
 * Implementations handle dictionary aware processing through {@link DictionaryAwareColumnarFilter}.
 */
public sealed interface FilterEvaluator
        permits
        AndFilterEvaluator,
        ColumnarFilterEvaluator,
        OrFilterEvaluator,
        PageFilterEvaluator,
        SelectAllEvaluator,
        SelectNoneEvaluator,
        DynamicFilterEvaluator
{
    SelectionResult evaluate(ConnectorSession session, SelectedPositions activePositions, SourcePage page);

    record SelectionResult(SelectedPositions selectedPositions, long filterTimeNanos) {}

    static Optional<Supplier<FilterEvaluator>> createColumnarFilterEvaluator(
            boolean columnarFilterEvaluationEnabled,
            Optional<Expression> filter,
            Map<Symbol, Integer> layout,
            ColumnarFilterCompiler columnarFilterCompiler)
    {
        if (columnarFilterEvaluationEnabled && filter.isPresent()) {
            return createColumnarFilterEvaluator(filter.get(), layout, columnarFilterCompiler);
        }
        return Optional.empty();
    }

    static Optional<Supplier<FilterEvaluator>> createColumnarFilterEvaluator(Expression expression, Map<Symbol, Integer> layout, ColumnarFilterCompiler compiler)
    {
        return switch (expression) {
            case Constant constant when constant.value() instanceof Boolean booleanValue ->
                    booleanValue ? Optional.of(SelectAllEvaluator::new) : Optional.of(SelectNoneEvaluator::new);
            case Comparison comparison -> createComparisonExpressionEvaluator(compiler, comparison, layout);
            case Call call -> {
                if (isNotExpression(call)) {
                    // "not(is_null(reference))" is handled explicitly as it is easy.
                    // more generic cases like "not(equal(reference, constant))" are not handled yet
                    if (call.arguments().getFirst() instanceof IsNull isNull) {
                        yield createIsNotNullExpressionEvaluator(compiler, call, isNull, layout);
                    }
                    yield Optional.empty();
                }
                yield createCallExpressionEvaluator(compiler, call, layout);
            }
            case IsNull isNull -> createIsNullExpressionEvaluator(compiler, isNull, layout);
            case Logical logical when logical.operator() == Logical.Operator.AND -> createAndExpressionEvaluator(compiler, logical, layout);
            case Logical logical when logical.operator() == Logical.Operator.OR -> createOrExpressionEvaluator(compiler, logical, layout);
            case Between between -> createBetweenEvaluator(compiler, between, layout);
            case In in -> createInExpressionEvaluator(compiler, in, layout);
            default -> Optional.empty();
        };
    }

    static boolean isNotExpression(Call call)
    {
        CatalogSchemaFunctionName functionName = call.function().name();
        return isBuiltinFunctionName(functionName) && functionName.functionName().equals("$not");
    }

    private static Optional<Supplier<FilterEvaluator>> createBetweenEvaluator(ColumnarFilterCompiler compiler, Between between, Map<Symbol, Integer> layout)
    {
        // Between requires evaluate once semantic for the value being tested
        // Until we can pre-project it into a temporary variable, we apply columnar evaluation only on Reference
        Expression valueExpression = between.value();
        if (!(valueExpression instanceof Reference)) {
            return Optional.empty();
        }

        // When the min and max arguments of a BETWEEN expression are both constants, evaluating them inline is cheaper than AND-ing subexpressions
        if (between.min() instanceof Constant && between.max() instanceof Constant) {
            Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(between, layout);
            return compiledFilter.map(filterSupplier -> () -> createDictionaryAwareEvaluator(filterSupplier.get()));
        }
        ResolvedFunction lessThanOrEqual = compiler.getMetadata().resolveOperator(
                LESS_THAN_OR_EQUAL,
                ImmutableList.of(valueExpression.type(), valueExpression.type()));
        return createAndExpressionEvaluator(
                compiler,
                new Logical(
                        Logical.Operator.AND,
                        ImmutableList.of(
                                call(lessThanOrEqual, between.min(), valueExpression),
                                call(lessThanOrEqual, valueExpression, between.max()))),
                layout);
    }

    private static Optional<Supplier<FilterEvaluator>> createInExpressionEvaluator(ColumnarFilterCompiler compiler, In in, Map<Symbol, Integer> layout)
    {
        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(in, layout);
        return compiledFilter.map(filterSupplier -> () -> createDictionaryAwareEvaluator(filterSupplier.get()));
    }

    private static Optional<Supplier<FilterEvaluator>> createCallExpressionEvaluator(ColumnarFilterCompiler compiler, Call call, Map<Symbol, Integer> layout)
    {
        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(call, layout);
        boolean isDeterministic = DeterminismEvaluator.isDeterministic(call);
        return compiledFilter.map(filterSupplier -> () -> {
            ColumnarFilter filter = filterSupplier.get();
            return filter.getInputChannels().size() == 1 && isDeterministic ? createDictionaryAwareEvaluator(filter) : new ColumnarFilterEvaluator(filter);
        });
    }

    private static Optional<Supplier<FilterEvaluator>> createIsNotNullExpressionEvaluator(ColumnarFilterCompiler compiler, Call call, IsNull isNull, Map<Symbol, Integer> layout)
    {
        checkArgument(isNotExpression(call), "call %s should be not", call);
        checkArgument(call.arguments().size() == 1);
        Type argumentType = isNull.value().type();
        checkArgument(!argumentType.equals(UNKNOWN), "argumentType %s should not be UNKNOWN", argumentType);

        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(call, layout);
        return compiledFilter.map(filterSupplier -> () -> createDictionaryAwareEvaluator(filterSupplier.get()));
    }

    private static Optional<Supplier<FilterEvaluator>> createIsNullExpressionEvaluator(ColumnarFilterCompiler compiler, IsNull isNull, Map<Symbol, Integer> layout)
    {
        Type argumentType = isNull.value().type();
        checkArgument(!argumentType.equals(UNKNOWN), "argumentType %s should not be UNKNOWN", argumentType);

        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(isNull, layout);
        return compiledFilter.map(filterSupplier -> () -> createDictionaryAwareEvaluator(filterSupplier.get()));
    }

    private static Optional<Supplier<FilterEvaluator>> createComparisonExpressionEvaluator(ColumnarFilterCompiler compiler, Comparison comparison, Map<Symbol, Integer> layout)
    {
        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(comparison, layout);
        return compiledFilter.map(filterSupplier -> () -> {
            ColumnarFilter filter = filterSupplier.get();
            // comparison operators are always deterministic
            return filter.getInputChannels().size() == 1 ? createDictionaryAwareEvaluator(filter) : new ColumnarFilterEvaluator(filter);
        });
    }

    private static FilterEvaluator createDictionaryAwareEvaluator(ColumnarFilter filter)
    {
        checkArgument(filter.getInputChannels().size() == 1, "filter should have 1 input channel");
        return new ColumnarFilterEvaluator(new DictionaryAwareColumnarFilter(filter));
    }
}
