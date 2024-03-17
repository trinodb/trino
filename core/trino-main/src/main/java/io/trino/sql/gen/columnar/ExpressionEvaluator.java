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

import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.trino.sql.gen.columnar.AndExpressionEvaluator.createAndExpressionEvaluator;
import static io.trino.sql.gen.columnar.BetweenExpressionEvaluator.createBetweenEvaluator;
import static io.trino.sql.gen.columnar.CallExpressionEvaluator.createCallExpressionEvaluator;
import static io.trino.sql.gen.columnar.InExpressionEvaluator.createInExpressionEvaluator;
import static io.trino.sql.gen.columnar.IsNotNullExpressionEvaluator.createIsNotNullExpressionEvaluator;
import static io.trino.sql.gen.columnar.IsNullExpressionEvaluator.createIsNullExpressionEvaluator;
import static io.trino.sql.gen.columnar.OrExpressionEvaluator.createOrExpressionEvaluator;
import static io.trino.sql.relational.SpecialForm.Form.AND;
import static io.trino.sql.relational.SpecialForm.Form.BETWEEN;
import static io.trino.sql.relational.SpecialForm.Form.IN;
import static io.trino.sql.relational.SpecialForm.Form.IS_NULL;
import static io.trino.sql.relational.SpecialForm.Form.OR;

/**
 * Used by PageProcessor to evaluate filter expression on input Page.
 * <p>
 * Implementations handle instantiation of required {@link ColumnarFilter} classes and dictionary aware processing through {@link DictionaryAwareColumnarFilter}.
 * <p>
 * Implementations may internally compose multiple ExpressionEvaluator to evaluate filters with sub-expressions (e.g. AND/OR)
 */
public interface ExpressionEvaluator
{
    SelectedPositions evaluate(SelectedPositions activePositions, Page page, Consumer<Long> recordFilterTimeSince);

    static Optional<Supplier<ExpressionEvaluator>> createColumnarFilterEvaluator(
            boolean columnarFilterEvaluationEnabled,
            Optional<RowExpression> filter,
            ColumnarFilterCompiler columnarFilterCompiler)
    {
        if (columnarFilterEvaluationEnabled && filter.isPresent()) {
            return ExpressionEvaluator.createColumnarFilterEvaluator(filter.get(), columnarFilterCompiler);
        }
        return Optional.empty();
    }

    static Optional<Supplier<ExpressionEvaluator>> createColumnarFilterEvaluator(RowExpression rowExpression, ColumnarFilterCompiler compiler)
    {
        if (rowExpression instanceof CallExpression callExpression) {
            if (isNotExpression(callExpression)) {
                // "not(is_null(input_reference))" is handled explicitly as it is easy.
                // more generic cases like "not(equal(input_reference, constant))" are not handled yet
                if (callExpression.getArguments().get(0) instanceof SpecialForm specialFormArg && specialFormArg.getForm() == IS_NULL) {
                    return createIsNotNullExpressionEvaluator(compiler, callExpression);
                }
                return Optional.empty();
            }
            return createCallExpressionEvaluator(compiler, callExpression);
        }
        if (rowExpression instanceof SpecialForm specialFormArg) {
            if (specialFormArg.getForm() == IS_NULL) {
                return createIsNullExpressionEvaluator(compiler, specialFormArg);
            }
            if (specialFormArg.getForm() == AND) {
                return createAndExpressionEvaluator(compiler, specialFormArg);
            }
            if (specialFormArg.getForm() == OR) {
                return createOrExpressionEvaluator(compiler, specialFormArg);
            }
            if (specialFormArg.getForm() == BETWEEN) {
                return createBetweenEvaluator(compiler, specialFormArg);
            }
            if (specialFormArg.getForm() == IN) {
                return createInExpressionEvaluator(compiler, specialFormArg);
            }
        }
        return Optional.empty();
    }

    static boolean isNotExpression(CallExpression callExpression)
    {
        return callExpression.getResolvedFunction().getName().getFunctionName().equals("not");
    }
}
