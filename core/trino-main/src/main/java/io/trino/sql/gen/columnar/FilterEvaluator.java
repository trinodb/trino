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
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;

import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.gen.columnar.AndFilterEvaluator.createAndExpressionEvaluator;
import static io.trino.sql.gen.columnar.DynamicPageFilter.DynamicFilterEvaluator;
import static io.trino.sql.gen.columnar.OrFilterEvaluator.createOrExpressionEvaluator;
import static io.trino.sql.relational.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.SpecialForm.Form.AND;
import static io.trino.sql.relational.SpecialForm.Form.BETWEEN;
import static io.trino.sql.relational.SpecialForm.Form.IN;
import static io.trino.sql.relational.SpecialForm.Form.IS_NULL;
import static io.trino.sql.relational.SpecialForm.Form.OR;
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
            Optional<RowExpression> filter,
            ColumnarFilterCompiler columnarFilterCompiler)
    {
        if (columnarFilterEvaluationEnabled && filter.isPresent()) {
            return createColumnarFilterEvaluator(filter.get(), columnarFilterCompiler);
        }
        return Optional.empty();
    }

    static Optional<Supplier<FilterEvaluator>> createColumnarFilterEvaluator(RowExpression rowExpression, ColumnarFilterCompiler compiler)
    {
        // Eventually this should use RowExpressionVisitor when we handle nested RowExpressions
        if (rowExpression instanceof ConstantExpression constantExpression) {
            if (constantExpression.value() instanceof Boolean booleanValue) {
                return booleanValue ? Optional.of(SelectAllEvaluator::new) : Optional.of(SelectNoneEvaluator::new);
            }
        }
        if (rowExpression instanceof CallExpression callExpression) {
            if (isNotExpression(callExpression)) {
                // "not(is_null(input_reference))" is handled explicitly as it is easy.
                // more generic cases like "not(equal(input_reference, constant))" are not handled yet
                if (callExpression.arguments().getFirst() instanceof SpecialForm specialFormArg && specialFormArg.form() == IS_NULL) {
                    return createIsNotNullExpressionEvaluator(compiler, callExpression);
                }
                return Optional.empty();
            }
            return createCallExpressionEvaluator(compiler, callExpression);
        }
        if (rowExpression instanceof SpecialForm specialFormArg) {
            if (specialFormArg.form() == IS_NULL) {
                return createIsNullExpressionEvaluator(compiler, specialFormArg);
            }
            if (specialFormArg.form() == AND) {
                return createAndExpressionEvaluator(compiler, specialFormArg);
            }
            if (specialFormArg.form() == OR) {
                return createOrExpressionEvaluator(compiler, specialFormArg);
            }
            if (specialFormArg.form() == BETWEEN) {
                return createBetweenEvaluator(compiler, specialFormArg);
            }
            if (specialFormArg.form() == IN) {
                return createInExpressionEvaluator(compiler, specialFormArg);
            }
        }
        return Optional.empty();
    }

    static boolean isNotExpression(CallExpression callExpression)
    {
        CatalogSchemaFunctionName functionName = callExpression.resolvedFunction().name();
        return isBuiltinFunctionName(functionName) && functionName.getFunctionName().equals("$not");
    }

    private static Optional<Supplier<FilterEvaluator>> createBetweenEvaluator(ColumnarFilterCompiler compiler, SpecialForm specialForm)
    {
        checkArgument(specialForm.form() == BETWEEN, "specialForm should be BETWEEN");
        checkArgument(specialForm.arguments().size() == 3, "BETWEEN should have 3 arguments %s", specialForm.arguments());
        checkArgument(specialForm.functionDependencies().size() == 1, "BETWEEN should have 1 functional dependency %s", specialForm.functionDependencies());

        ResolvedFunction lessThanOrEqual = specialForm.getOperatorDependency(LESS_THAN_OR_EQUAL);
        // Between requires evaluate once semantic for the value being tested
        // Until we can pre-project it into a temporary variable, we apply columnar evaluation only on InputReference
        RowExpression valueExpression = specialForm.arguments().get(0);
        if (!(valueExpression instanceof InputReferenceExpression)) {
            return Optional.empty();
        }

        // When the min and max arguments of a BETWEEN expression are both constants, evaluating them inline is cheaper than AND-ing subexpressions
        if (specialForm.arguments().get(1) instanceof ConstantExpression && specialForm.arguments().get(2) instanceof ConstantExpression) {
            Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(specialForm);
            return compiledFilter.map(filterSupplier -> () -> createDictionaryAwareEvaluator(filterSupplier.get()));
        }
        return createAndExpressionEvaluator(
                compiler,
                new SpecialForm(
                        AND,
                        BOOLEAN,
                        ImmutableList.of(
                                call(lessThanOrEqual, specialForm.arguments().get(1), valueExpression),
                                call(lessThanOrEqual, valueExpression, specialForm.arguments().get(2))),
                        ImmutableList.of()));
    }

    private static Optional<Supplier<FilterEvaluator>> createInExpressionEvaluator(ColumnarFilterCompiler compiler, SpecialForm specialForm)
    {
        checkArgument(specialForm.form() == IN, "specialForm %s should be IN", specialForm);
        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(specialForm);
        return compiledFilter.map(filterSupplier -> () -> createDictionaryAwareEvaluator(filterSupplier.get()));
    }

    private static Optional<Supplier<FilterEvaluator>> createCallExpressionEvaluator(ColumnarFilterCompiler compiler, CallExpression callExpression)
    {
        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(callExpression);
        boolean isDeterministic = isDeterministic(callExpression);
        return compiledFilter.map(filterSupplier -> () -> {
            ColumnarFilter filter = filterSupplier.get();
            return filter.getInputChannels().size() == 1 && isDeterministic ? createDictionaryAwareEvaluator(filter) : new ColumnarFilterEvaluator(filter);
        });
    }

    private static Optional<Supplier<FilterEvaluator>> createIsNotNullExpressionEvaluator(ColumnarFilterCompiler compiler, CallExpression callExpression)
    {
        checkArgument(isNotExpression(callExpression), "callExpression %s should be not", callExpression);
        checkArgument(callExpression.arguments().size() == 1);
        SpecialForm specialForm = (SpecialForm) callExpression.arguments().getFirst();
        checkArgument(specialForm.form() == IS_NULL, "specialForm %s should be IS_NULL", specialForm);
        Type argumentType = specialForm.arguments().getFirst().type();
        checkArgument(!argumentType.equals(UNKNOWN), "argumentType %s should not be UNKNOWN", argumentType);

        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(callExpression);
        return compiledFilter.map(filterSupplier -> () -> createDictionaryAwareEvaluator(filterSupplier.get()));
    }

    private static Optional<Supplier<FilterEvaluator>> createIsNullExpressionEvaluator(ColumnarFilterCompiler compiler, SpecialForm specialForm)
    {
        checkArgument(specialForm.form() == IS_NULL, "specialForm %s should be IS_NULL", specialForm);
        Type argumentType = specialForm.arguments().getFirst().type();
        checkArgument(!argumentType.equals(UNKNOWN), "argumentType %s should not be UNKNOWN", argumentType);

        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(specialForm);
        return compiledFilter.map(filterSupplier -> () -> createDictionaryAwareEvaluator(filterSupplier.get()));
    }

    private static FilterEvaluator createDictionaryAwareEvaluator(ColumnarFilter filter)
    {
        checkArgument(filter.getInputChannels().size() == 1, "filter should have 1 input channel");
        return new ColumnarFilterEvaluator(new DictionaryAwareColumnarFilter(filter));
    }
}
